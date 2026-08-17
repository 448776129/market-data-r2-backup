"""批量历史数据拉取脚本（一次性全量入库 R2）。

不再使用分散的多个 actions，一次性获取全部历史数据并写入 Cloudflare R2：
    - 日K（近 5 年）
    - 1分钟K（近 5 天）
    - 1小时K（近 6 个月）
    - 5m/15m/30m（由 1m 重采样派生）
    - 美股 1m/5m/15m/30m/1h 均含盘前盘后延长时段

数据经 Yahoo chart API + 反代（config.YAHOO_CHART_PROXY）拉取，
gzip 压缩后并发上传 R2（scripts/r2store.py）。

用法：
    export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET=...
    python scripts/fetch_history.py                 # 全部区域
    python scripts/fetch_history.py --region us     # 仅美股
    python scripts/fetch_history.py --region cn --batch 0 --batches 10

说明：
    - 首次部署跑一次本脚本即可获得全量历史；之后由 sync_incremental.py 定时增量。
    - --skip-kline 可跳过 K 线只处理清单；--skip-universe 可跳过清单。
    - 每个区域完成后会把该区域清单一并上传，供 Cloudflare Worker 读取。
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402
import kvstore  # noqa: E402
import marketlib  # noqa: E402
import r2store  # noqa: E402
import yahoo_chart  # noqa: E402

# 写入 CSV 的列（与 yfinance 一致）
COLS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
# 日K索引列 / 分钟K索引列
DATE_COL = "Date"
DT_COL = "Datetime"

# 直接由雅虎拉取的分钟周期；5m/15m/30m 由 1m 派生
SOURCE_INTERVALS = ["1m", "1h"]

# 各区域目标子目录（R2 key 前缀）
SUBDIR = {
    "1d": config.KLINE_SUBDIR,           # kline
    "1m": config.INTRADAY_M1_SUBDIR,     # kline_1m
    "5m": config.INTRADAY_M5_SUBDIR,
    "15m": config.INTRADAY_M15_SUBDIR,
    "30m": config.INTRADAY_M30_SUBDIR,
    "1h": config.INTRADAY_M1H_SUBDIR,
}
# 周期 -> yahoo_chart period 参数
PERIOD = {
    "1d": config.HISTORY_PERIOD,         # 5y
    "1m": config.INTRADAY_PERIOD["1m"],  # 5d
    "1h": config.INTRADAY_PERIOD["1h"],  # 6mo
}


def derive_5m_15m_30m(df_1m: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """从 1 分钟K线重采样计算 5m/15m/30m。返回 {target: df}。"""
    out: dict[str, pd.DataFrame] = {}
    for target, rule in config.INTRADAY_DERIVED.items():
        agg = df_1m.resample(rule).agg(
            {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
        )
        agg = agg.dropna(subset=["Close"])
        if agg.empty:
            continue
        agg["Adj Close"] = df_1m["Close"].resample(rule).last()
        agg = agg[COLS]
        out[target] = agg
    return out


def df_to_csv(df: pd.DataFrame, index_col: str) -> str:
    """DataFrame 转 CSV 文本（索引列名 = index_col）。"""
    d = df.copy()
    d.index.name = index_col
    return d.to_csv()


def fetch_symbol(region: str, symbol: str) -> dict[str, pd.DataFrame] | None:
    """拉取单只股票的全部周期数据，返回 {interval: df}；失败返回 None。"""
    result: dict[str, pd.DataFrame] = {}
    try:
        # 日K
        d1 = yahoo_chart.fetch_kline(symbol, interval="1d", period=PERIOD["1d"], prepost=False)
        if not d1.empty:
            result["1d"] = d1
        # 分钟K
        m1 = yahoo_chart.fetch_kline(symbol, interval="1m", period=PERIOD["1m"], prepost=True)
        if not m1.empty:
            result["1m"] = m1
            for target, d in derive_5m_15m_30m(m1).items():
                result[target] = d
        h1 = yahoo_chart.fetch_kline(symbol, interval="1h", period=PERIOD["1h"], prepost=True)
        if not h1.empty:
            result["1h"] = h1
    except Exception as exc:  # noqa: BLE001
        print(f"  [失败] {symbol}: {exc}", flush=True)
        return None
    return result or None


def _process_one(reg: str, symbol: str) -> tuple[str, dict | None]:
    """拉取并上传单只股票，返回 (symbol, data)。data 为 None 表示失败。"""
    try:
        data = fetch_symbol(reg, symbol)
        if data is None:
            return symbol, None
        items: list[tuple[str, str, bool]] = []
        for interval, df in data.items():
            subdir = SUBDIR[interval]
            index_col = DATE_COL if interval == "1d" else DT_COL
            key = f"{reg}/{subdir}/{symbol}.csv"
            items.append((key, df_to_csv(df, index_col), True))
        if items:
            res = r2store.upload_many(items)
            if res.get("fail", 0) > 0:
                return symbol, None
        return symbol, data
    except Exception as exc:  # noqa: BLE001 - 单只失败不中断整体
        print(f"  [失败] {reg}:{symbol}: {exc}", flush=True)
        return symbol, None


def run(region: str | None, batch: int = 0, batches: int = 1) -> int:
    regions = [region] if region else list(config.REGIONS)
    ok_files = 0
    fail_symbols: list[str] = []
    # 并发拉取线程数（可用环境变量 FETCH_CONCURRENCY 覆盖）
    concurrency = int(os.environ.get("FETCH_CONCURRENCY", "6"))

    for reg in regions:
        # 完整清单（用于 put_universe；切 batch 只影响本批拉取范围）
        all_symbols = marketlib.load_symbols(reg)
        symbols = marketlib.slice_batch(all_symbols, batch, batches)
        if not symbols:
            print(f"[警告] {reg}: 无符号（universe 缺失？）", flush=True)
            continue
        print(f"[区域] {reg} ({len(symbols)}/{len(all_symbols)} 只, 批 {batch+1}/{batches}, 并发 {concurrency})", flush=True)

        done = 0
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {pool.submit(_process_one, reg, sym): sym for sym in symbols}
            for fut in as_completed(futures):
                sym, data = fut.result()
                done += 1
                if data is None:
                    fail_symbols.append(f"{reg}:{sym}")
                else:
                    ok_files += sum(1 for _ in data)
                if done % 25 == 0 or done == len(symbols):
                    print(f"  [{done}/{len(symbols)}] {reg} 已处理，失败 {len(fail_symbols)}", flush=True)

        # 区域清单一并上传（供 Worker /universe 读取）——始终用完整清单，避免 batch 覆盖
        csv_text = "\n".join(all_symbols) + "\n"
        r2store.put_universe(reg, csv_text)
        # 双写 KV：Worker 优先读 KV（毫秒级、不耗 R2 读额度）
        kvstore.put_universe(reg, csv_text)
        print(f"[区域] {reg} 完整清单已上传 universe/{reg}.csv (R2+KV)", flush=True)

    # 全局状态
    r2store.put_status(
        {
            "mode": "historical",
            "completed_at": r2store.now_iso(),
            "regions": regions,
            "ok_files": ok_files,
            "failed": fail_symbols[:100],
            "fail_count": len(fail_symbols),
        }
    )

    print(f"完成: 上传 {ok_files} 个对象, 失败 {len(fail_symbols)} 项")
    # 单只股票失败不视为整体失败（避免 job 失败导致其余批次被取消），
    # 仅打印警告；失败明细已写入 _status.json 供后续重试。
    if fail_symbols:
        print(f"警告: {len(fail_symbols)} 只股票失败(不中断): {fail_symbols[:30]}", flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="批量历史数据拉取（一次性全量入库 R2）")
    parser.add_argument("--region", choices=list(config.REGIONS), help="仅处理指定区域")
    parser.add_argument("--batch", type=int, default=0, help="当前批次（0 起）")
    parser.add_argument("--batches", type=int, default=1, help="总批次数")
    args = parser.parse_args()
    return run(args.region, args.batch, args.batches)


if __name__ == "__main__":
    sys.exit(main())
