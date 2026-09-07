"""TradingView 数据采集（独立管道，与 Yahoo 管道互不干扰）。

用 tvdatafeed 库（TradingView WebSocket）拉取美股全周期 K 线，
gzip 压缩写入独立的 R2 bucket：stocks-tv。

存储结构（与 Yahoo 管道完全独立）：
    {region}/kline/{symbol}.csv.gz        # 日K
    {region}/kline_1m/{symbol}.csv.gz     # 1分钟K
    {region}/kline_5m/{symbol}.csv.gz
    {region}/kline_15m/{symbol}.csv.gz
    {region}/kline_30m/{symbol}.csv.gz
    {region}/kline_1h/{symbol}.csv.gz
    {region}/kline_1wk/{symbol}.csv.gz
    {region}/kline_1mo/{symbol}.csv.gz
    _status.json                          # 采集状态

用法：
    export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=...
    export R2_BUCKET=stocks-tv
    python scripts/fetch_tv.py --region us                    # 默认清单 us
    python scripts/fetch_tv.py --universe etf                 # 只采 ETF
    python scripts/fetch_tv.py --universe us,etf              # 股票 + ETF（自动去重）
    python scripts/fetch_tv.py --region us --interval 1h      # 只拉1h
    python scripts/fetch_tv.py --universe etf --limit 20      # 只拉20只测试

依赖：
    pip install tvdatafeed  # 从 GitHub: git+https://github.com/rongardF/tvdatafeed.git
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import r2s3  # noqa: E402

# TradingView 周期映射：名称 -> (tvdatafeed Interval, R2 子目录)
from tvDatafeed import Interval  # noqa: E402

TV_INTERVALS = {
    "1d": (Interval.in_daily, "kline"),
    "1m": (Interval.in_1_minute, "kline_1m"),
    "5m": (Interval.in_5_minute, "kline_5m"),
    "15m": (Interval.in_15_minute, "kline_15m"),
    "30m": (Interval.in_30_minute, "kline_30m"),
    "1h": (Interval.in_1_hour, "kline_1h"),
    "1wk": (Interval.in_weekly, "kline_1wk"),
    "1mo": (Interval.in_monthly, "kline_1mo"),
}

# 各周期默认拉取 bar 数
DEFAULT_BARS = {
    "1d": 1500,   # ~6 年日K
    "1m": 2000,   # ~3.5 天 1m
    "5m": 3000,   # ~10 天
    "15m": 3000,
    "30m": 3000,
    "1h": 3000,   # ~1 年 1h
    "1wk": 400,
    "1mo": 150,
}

# 并发：本脚本刻意保持串行。tvdatafeed 底层是单个 WebSocket 连接，
# 多线程并发调用会导致大面积 "Connection timed out"（详见 main() 采集循环注释）。
# 环境变量 FETCH_CONCURRENCY 仅用于 Yahoo 管道的 HTTP 请求，对 TV 管道无效。

# 可选股票清单：名称 -> universe 文件名
UNIVERSE_FILES = {
    "us": "us.csv",              # 罗素1000 ~1022 只
    "etf": "etf.csv",            # 美股 ETF ~830 只
    "nasdaq100": "nasdaq100.csv",
    "sp500": "sp500.csv",
}


def load_universe(names: list[str]) -> list[str]:
    """按名称加载并合并多个清单（跨清单去重、保序）。

    注意：etf.csv 与 us.csv 存在少量同名代码（如 ORCL/PSX/STAG，
    本质是普通股被误收录进 ETF 清单）。二者共用 us/ 命名空间，
    若重复采集会互相覆盖。这里按传入顺序去重，先出现的清单优先。
    """
    out: list[str] = []
    seen: set[str] = set()
    for name in names:
        fname = UNIVERSE_FILES.get(name)
        if not fname:
            print(f"[WARN] 未知清单: {name}，可选: {list(UNIVERSE_FILES)}")
            continue
        f = ROOT / "data" / "universe" / fname
        if not f.exists():
            print(f"[WARN] universe 文件不存在: {f}")
            continue
        syms = [l.strip() for l in f.read_text(encoding="utf-8").splitlines()
                if l.strip() and not l.startswith("#")]
        added = 0
        for s in syms:
            if s not in seen:
                seen.add(s)
                out.append(s)
                added += 1
        print(f"  清单 {name} ({fname}): {len(syms)} 只 → 新增 {added} 只，累计 {len(out)} 只")
    return out


def gzip_bytes(data: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as f:
        f.write(data)
    return buf.getvalue()


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.10g}"
    return str(v)


def df_to_csv(df) -> str:
    """tvdatafeed DataFrame → CSV 文本（兼容现有格式）。"""
    if df is None or len(df) == 0:
        return ""
    # df 列: symbol, open, high, low, close, volume；index 是 Datetime
    lines = ["Datetime,Open,High,Low,Close,Adj Close,Volume"]
    for idx, row in df.iterrows():
        dt_str = idx.strftime("%Y-%m-%d %H:%M:%S") if hasattr(idx, "strftime") else str(idx)
        lines.append(",".join([
            dt_str,
            _fmt(row["open"]), _fmt(row["high"]), _fmt(row["low"]), _fmt(row["close"]),
            _fmt(row["close"]),  # TradingView 无 adjclose，用 close 填充
            _fmt(row["volume"]),
        ]))
    return "\n".join(lines) + "\n"


def put_csv_gz(region: str, symbol: str, subdir: str, csv_text: str) -> None:
    key = f"{region}/{subdir}/{symbol}.csv"
    payload = ("\ufeff" + csv_text).encode("utf-8")
    r2s3.put_obj(key, gzip_bytes(payload),
                 content_type="text/csv; charset=utf-8", content_encoding="gzip")


def fetch_one(tv, region: str, symbol: str, interval: str, exchanges: list[str]) -> dict:
    """拉取单只股票单个周期的 K 线并写入 R2。

    依次尝试多个交易所前缀（NYSE/NASDAQ/AMEX），第一个成功即用。
    """
    tv_interval, subdir = TV_INTERVALS[interval]
    n_bars = DEFAULT_BARS[interval]
    result = {"symbol": symbol, "interval": interval, "status": "ok", "bars": 0, "exchange": None}

    for exchange in exchanges:
        try:
            df = tv.get_hist(symbol=symbol, exchange=exchange,
                             interval=tv_interval, n_bars=n_bars)
            if df is not None and len(df) > 0:
                csv_text = df_to_csv(df)
                if csv_text:
                    put_csv_gz(region, symbol, subdir, csv_text)
                    result["bars"] = len(df)
                    result["exchange"] = exchange
                    return result
        except Exception as exc:  # noqa: BLE001
            # 记录但继续尝试下一个交易所
            result["last_error"] = str(exc)

    result["status"] = "no_data"
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="TradingView 数据采集（独立管道）")
    parser.add_argument("--region", default="us", help="区域（目前仅 us）")
    parser.add_argument("--universe", default="us",
                        help=f"股票清单，逗号分隔。可选: {list(UNIVERSE_FILES)}（默认 us）")
    parser.add_argument("--interval", default="all",
                        help="周期：all=全部8个 / 单个如 1d / 逗号组合如 1d,1wk,1mo")
    parser.add_argument("--limit", type=int, default=0, help="限制拉取股票数（0=全部）")
    args = parser.parse_args()

    uni_names = [n.strip() for n in args.universe.split(",") if n.strip()]

    # 选择周期（支持逗号组合，如 1d,1wk,1mo）
    if args.interval == "all":
        intervals = list(TV_INTERVALS.keys())
    else:
        intervals = []
        for iv in args.interval.split(","):
            iv = iv.strip()
            if iv not in TV_INTERVALS:
                print(f"未知周期: {iv}，可选: {list(TV_INTERVALS.keys())} 或 all")
                return 1
            if iv not in intervals:
                intervals.append(iv)

    # 股票代码清单（支持多清单合并去重）
    symbols = load_universe(uni_names)
    if not symbols:
        print(f"❌ 未加载到任何代码，检查 --universe: {args.universe}")
        return 1
    if args.limit > 0:
        symbols = symbols[:args.limit]

    print(f"=== TradingView 采集 region={args.region} 清单={uni_names} "
          f"周期={intervals} 标的={len(symbols)} ===")
    print(f"时间: {datetime.now(timezone.utc).isoformat()}")
    print("连接 TradingView...")

    # 初始化 tvdatafeed（匿名）
    from tvDatafeed import TvDatafeed
    tv = TvDatafeed()

    # 逐周期采集：串行 + 分批轮询
    # 重要：tvdatafeed 底层是单个 WebSocket 连接，多线程并发读写会破坏 WS
    # 状态机，表现为大量 "Connection timed out / no data, please check the
    # exchange and symbol"。实测 826 只 ETF 以 CONCURRENCY=4 并发时成功率 <1%
    # （仅字母序最前几只成功），改串行后恢复正常。故不要改回线程池。
    BATCH_SIZE = int(os.environ.get("TV_BATCH_SIZE", "60"))
    BATCH_DELAY_SEC = float(os.environ.get("TV_BATCH_DELAY", "2"))
    MAX_CONSEC_FAIL = int(os.environ.get("TV_MAX_CONSEC_FAIL", "20"))

    for interval in intervals:
        print(f"\n--- 周期 {interval} ---")
        ok = err = skip = 0
        total_bars = 0
        consec_fail = 0
        # 交易所前缀（美股多个交易所，逐个尝试）
        exchanges = ["NYSE", "NASDAQ", "AMEX"]
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i + BATCH_SIZE]
            for sym in batch:
                try:
                    r = fetch_one(tv, args.region, sym, interval, exchanges)
                except Exception as exc:  # noqa: BLE001
                    r = {"status": f"exception: {exc}"}
                if r.get("status") == "ok":
                    ok += 1
                    total_bars += r.get("bars", 0)
                    consec_fail = 0
                else:
                    skip += 1
                    consec_fail += 1
                    if skip <= 3:
                        print(f"  [skip] {sym} {interval}: {r.get('status')}")
                # 连续失败过多 → WS 多半已假死，重建连接
                if consec_fail >= MAX_CONSEC_FAIL:
                    print(f"  连续失败 {consec_fail} 次，重建 TvDatafeed 连接...", flush=True)
                    try:
                        tv = TvDatafeed()
                    except Exception as exc:  # noqa: BLE001
                        print(f"  重建连接失败: {exc}", flush=True)
                    consec_fail = 0
                    time.sleep(BATCH_DELAY_SEC)
            if i + BATCH_SIZE < len(symbols):
                time.sleep(BATCH_DELAY_SEC)
        print(f"  {interval}: ok={ok} skip={skip} err={err} bars={total_bars}")

    # 状态（用 json.dumps 保证 intervals/universe 是合法 JSON，而非 Python 字面量）
    status = {
        "source": "tradingview",
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "region": args.region,
        "universe": uni_names,
        "intervals": intervals,
        "symbols": len(symbols),
    }
    r2s3.put_obj("_status.json", json.dumps(status, ensure_ascii=False).encode("utf-8"),
                 content_type="application/json")
    print("\n✅ TradingView 采集完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
