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
    python scripts/fetch_tv.py --region us              # 全部周期
    python scripts/fetch_tv.py --region us --interval 1h  # 只拉1h
    python scripts/fetch_tv.py --region us --limit 20    # 只拉20只测试

依赖：
    pip install tvdatafeed  # 从 GitHub: git+https://github.com/rongardF/tvdatafeed.git
"""

from __future__ import annotations

import argparse
import gzip
import io
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# 并发（TradingView WS 单连接，内部串行；多 symbol 用一个连接）
CONCURRENCY = int(os.environ.get("FETCH_CONCURRENCY", "4"))


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
    parser.add_argument("--interval", default="all", help="周期：all / 1d / 1m / 5m / 15m / 30m / 1h / 1wk / 1mo")
    parser.add_argument("--limit", type=int, default=0, help="限制拉取股票数（0=全部）")
    args = parser.parse_args()

    # 选择周期
    if args.interval == "all":
        intervals = list(TV_INTERVALS.keys())
    else:
        if args.interval not in TV_INTERVALS:
            print(f"未知周期: {args.interval}，可选: {list(TV_INTERVALS.keys())}")
            return 1
        intervals = [args.interval]

    # 美股代码清单
    uni_file = ROOT / "data" / "universe" / "us.csv"
    if not uni_file.exists():
        print(f"❌ universe 文件不存在: {uni_file}")
        return 1
    symbols = [line.strip() for line in uni_file.read_text(encoding="utf-8").splitlines()
               if line.strip() and not line.startswith("#")]
    if args.limit > 0:
        symbols = symbols[:args.limit]

    print(f"=== TradingView 采集 region={args.region} 周期={intervals} 股票={len(symbols)} ===")
    print(f"时间: {datetime.now(timezone.utc).isoformat()}")
    print("连接 TradingView...")

    # 初始化 tvdatafeed（匿名）
    from tvDatafeed import TvDatafeed
    tv = TvDatafeed()

    # 逐周期采集（每周期一个连接，避免 WS 状态混淆）
    for interval in intervals:
        print(f"\n--- 周期 {interval} ---")
        ok = err = skip = 0
        total_bars = 0
        # 交易所前缀（美股多个交易所，逐个尝试）
        exchanges = ["NYSE", "NASDAQ", "AMEX"]
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            futures = {
                pool.submit(fetch_one, tv, args.region, sym, interval, exchanges): sym
                for sym in symbols
            }
            for fut in as_completed(futures):
                r = fut.result()
                if r["status"] == "ok":
                    ok += 1
                    total_bars += r["bars"]
                elif r["status"] == "no_data":
                    skip += 1
                else:
                    err += 1
                    if err <= 3:
                        print(f"  [ERR] {r['symbol']} {interval}: {r['status']}")
        print(f"  {interval}: ok={ok} skip={skip} err={err} bars={total_bars}")

    # 状态
    r2s3.put_obj("_status.json",
                 (f'{{"source":"tradingview","completed_at":"{datetime.now(timezone.utc).isoformat()}",'
                  f'"region":"{args.region}","intervals":{intervals},"symbols":{len(symbols)}}}').encode(),
                 content_type="application/json")
    print("\n✅ TradingView 采集完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
