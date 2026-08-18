"""高频分钟K同步脚本（每 2 分钟运行一次）。

仅处理指定指数成分股（沪深300 + 纳指100），高频增量拉取分钟K线。
- 拉取 1m K线（含延长时段）→ gzip 压缩写入 R2
- 5m/15m/30m 由 1m 重采样派生
- 完成后调用 screener_precompute 写入选股快照 KV

用法（GitHub Actions）：
    python scripts/sync_minute_realtime.py --index csi300 --region cn
    python scripts/sync_minute_realtime.py --index nasdaq100 --region us

环境变量（GitHub Secrets 注入）：
    R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
    CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID, KV_NAMESPACE_ID
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402
import marketlib  # noqa: E402
import r2store  # noqa: E402
import yahoo_chart  # noqa: E402

# 分钟K线子目录
SUBDIR = {
    "1m": config.INTRADAY_M1_SUBDIR,
    "5m": config.INTRADAY_M5_SUBDIR,
    "15m": config.INTRADAY_M15_SUBDIR,
    "30m": config.INTRADAY_M30_SUBDIR,
}

# 重采样规则
RESAMPLE_RULE = {"5m": "5min", "15m": "15min", "30m": "30min"}

# 并发数（GitHub Actions ubuntu-latest 默认 2 核，适度并发）
FETCH_CONCURRENCY = int(os.environ.get("FETCH_CONCURRENCY", "10"))


def load_index_symbols(index_name: str) -> list[str]:
    """从 universe 文件加载指数成分股。"""
    uni_file = ROOT / config.DATA_DIR / config.UNIVERSE_SUBDIR / f"{index_name}.csv"
    if not uni_file.exists():
        print(f"[WARN] universe 文件不存在: {uni_file}")
        return []
    text = uni_file.read_text(encoding="utf-8")
    return [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]


def key_for(region: str, symbol: str, interval: str) -> str:
    return f"{region}/{SUBDIR[interval]}/{symbol}.csv"


def sync_one_symbol(region: str, symbol: str) -> dict:
    """同步单只股票的分钟K线。返回状态 dict。"""
    result = {"symbol": symbol, "status": "ok", "updated": 0}

    try:
        # 判断是否在交易时段
        if not marketlib.is_market_session(region):
            result["status"] = "skip_offhours"
            return result

        # 拉取 1m K线（近 1 天，含延长时段）
        df = yahoo_chart.fetch_kline(symbol, "1m", period="1d", prepost=True)
        if df is None or len(df) == 0:
            result["status"] = "no_data"
            return result

        # 确保列名一致
        if "Datetime" not in df.columns:
            df = df.reset_index() if "Datetime" in df.index.name else df
        if "Datetime" not in df.columns and df.index.name == "Datetime":
            df = df.reset_index()

        # 写入 1m
        csv_text = df.to_csv(index=False)
        r2store.put_csv(key_for(region, symbol, "1m"), csv_text)
        result["updated"] = len(df)

        # 重采样 5m/15m/30m
        if "Datetime" in df.columns:
            df_dt = df.copy()
            df_dt["Datetime"] = pd.to_datetime(df_dt["Datetime"])
            df_dt = df_dt.set_index("Datetime")
            for derived, rule in RESAMPLE_RULE.items():
                try:
                    resampled = marketlib.resample_kline(df_dt, rule)
                    csv_derived = resampled.reset_index().to_csv(index=False)
                    r2store.put_csv(key_for(region, symbol, derived), csv_derived)
                except Exception:
                    pass  # 重采样失败不影响主流程

    except Exception as exc:
        result["status"] = f"error: {exc}"

    return result


def main():
    parser = argparse.ArgumentParser(description="高频分钟K同步（沪深300+纳指100）")
    parser.add_argument("--index", required=True, help="指数名：csi300 / nasdaq100")
    parser.add_argument("--region", required=True, help="区域：cn / us")
    args = parser.parse_args()

    symbols = load_index_symbols(args.index)
    if not symbols:
        print(f"无 {args.index} 成分股，退出")
        return

    print(f"=== {args.index} ({args.region}) 共 {len(symbols)} 只 ===")
    print(f"时间: {datetime.now(timezone.utc).isoformat()}")

    # 并发同步
    ok = 0
    skip = 0
    err = 0
    total_rows = 0

    with ThreadPoolExecutor(max_workers=FETCH_CONCURRENCY) as pool:
        futures = {pool.submit(sync_one_symbol, args.region, s): s for s in symbols}
        for i, future in enumerate(as_completed(futures)):
            sym = futures[future]
            r = future.result()
            if r["status"] == "ok":
                ok += 1
                total_rows += r["updated"]
            elif r["status"].startswith("skip"):
                skip += 1
            else:
                err += 1
                if err <= 3:
                    print(f"  [ERR] {sym}: {r['status']}")

            if (i + 1) % 100 == 0:
                print(f"  进度: {i+1}/{len(symbols)} (ok={ok} skip={skip} err={err})")

    print(f"\n=== {args.index} 完成: ok={ok} skip={skip} err={err} rows={total_rows} ===")

    # 选股指标预计算 → KV（只算这批股票）
    if ok > 0:
        print("\n--- 选股指标预计算 → KV ---")
        try:
            import indicators as ind  # noqa: E402
            import screener_precompute as sp  # noqa: E402
            import kvstore  # noqa: E402
            import json  # noqa: E402

            snapshot = {}
            for sym in symbols:
                df = sp.load_kline_from_r2(args.region, sym, "1m")
                if df is None:
                    continue
                snap = sp.compute_snapshot(df)
                if snap is not None:
                    snapshot[sym] = snap

            if snapshot:
                kv_key = f"screener:1m:{args.index}"
                kv_value = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
                size_kb = len(kv_value.encode("utf-8")) / 1024
                ok_kv = kvstore.put(kv_key, kv_value)
                print(f"  KV {kv_key}: {len(snapshot)} 只, {size_kb:.1f}KB, {'✅' if ok_kv else '❌'}")
            else:
                print("  无有效快照数据")

        except Exception as exc:
            print(f"  预计算失败（不影响 R2 数据）: {exc}")


if __name__ == "__main__":
    main()
