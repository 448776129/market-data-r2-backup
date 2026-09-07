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
    parser.add_argument("--interval", default="all", help="周期：all / 1d / 1m / 5m / 15m / 30m / 1h / 1wk / 1mo")
    parser.add_argument("--limit", type=int, default=0, help="限制拉取股票数（0=全部）")
    args = parser.parse_args()

    uni_names = [n.strip() for n in args.universe.split(",") if n.strip()]

    # 选择周期
    if args.interval == "all":
        intervals = list(TV_INTERVALS.keys())
    else:
        if args.interval not in TV_INTERVALS:
            print(f"未知周期: {args.interval}，可选: {list(TV_INTERVALS.keys())}")
            return 1
        intervals = [args.interval]

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
