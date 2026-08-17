"""选股指标预计算脚本（GH Actions 端）。

从 R2 读取 K 线 → 用 indicators.py 计算技术指标 → 写入 KV 快照。
Worker 的 /screener 接口读 KV 快照做内存过滤，毫秒级返回。

KV key 约定：
    screener:daily:{region}       → 日K选股快照（每个市场一个 JSON）
    screener:watchlist:{interval} → 候选池分钟K选股快照（后续支持）

KV value 格式（JSON 字符串）：
    {
      "AAPL": {
        "close": 305.25, "change_1d": 2.3, "change_5d": 5.1, "change_20d": 12.0,
        "ma5": 302.1, "ma10": 298.5, "ma20": 295.0, "ma60": 280.0,
        "ema12": 303.0, "ema26": 299.0,
        "macd": 4.0, "macd_signal": 2.5, "macd_histogram": 1.5,
        "rsi14": 62.5,
        "kdj_k": 75.0, "kdj_d": 70.0, "kdj_j": 85.0,
        "bb_upper": 310.0, "bb_middle": 295.0, "bb_lower": 280.0,
        "volume": 41657800, "volume_ma5": 38000000, "volume_ma20": 35000000,
        "updated": "2026-08-17T23:59:00Z"
      },
      ...
    }

用法：
    # 配置 R2 + KV 凭据（环境变量）
    export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET=...
    export CLOUDFLARE_API_TOKEN=... CLOUDFLARE_ACCOUNT_ID=... KV_NAMESPACE_ID=...

    # 全市场日K选股快照
    python scripts/screener_precompute.py --interval 1d

    # 仅美股
    python scripts/screener_precompute.py --interval 1d --region us

    # 不写 KV，只打印结果（调试用）
    python scripts/screener_precompute.py --interval 1d --region us --dry-run
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402
import indicators as ind  # noqa: E402
import kvstore  # noqa: E402
import marketlib  # noqa: E402
import r2store  # noqa: E402

# R2 key 中的周期子目录映射
SUBDIR = {
    "1d": config.KLINE_SUBDIR,
    "1m": config.INTRADAY_M1_SUBDIR,
    "5m": config.INTRADAY_M5_SUBDIR,
    "15m": config.INTRADAY_M15_SUBDIR,
    "30m": config.INTRADAY_M30_SUBDIR,
    "1h": config.INTRADAY_M1H_SUBDIR,
}


def load_symbols(region: str) -> list[str]:
    """从 universe 文件加载该区域的股票代码列表。"""
    uni_file = ROOT / config.DATA_DIR / config.UNIVERSE_SUBDIR / f"{region}.csv"
    if not uni_file.exists():
        print(f"  [WARN] universe 文件不存在: {uni_file}")
        return []
    text = uni_file.read_text(encoding="utf-8")
    symbols = [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.startswith("#")
    ]
    return symbols


def load_kline_from_r2(region: str, symbol: str, interval: str) -> pd.DataFrame | None:
    """从 R2 读取该股票该周期的 K 线数据（自动解压 gzip）。

    R2 key: {region}/{subdir}/{symbol}.csv（存储为 gzip 压缩）
    """
    key = f"{region}/{SUBDIR[interval]}/{symbol}.csv"
    text = r2store.get_csv_text(key)
    if text is None:
        return None
    # 去掉 BOM
    if text.startswith("\ufeff"):
        text = text[1:]
    try:
        df = pd.read_csv(io.StringIO(text))
    except Exception as exc:
        print(f"  [WARN] {symbol} CSV 解析失败: {exc}")
        return None
    return df


def compute_snapshot(df: pd.DataFrame) -> dict | None:
    """对单只股票的 K 线 DataFrame 计算最新一行指标快照。

    返回最新一行的指标 dict，数据不足时返回 None。
    """
    if df is None or len(df) == 0:
        return None

    # 确定列名（日K用 Date，分钟用 Datetime）
    close_col = "Close"
    high_col = "High"
    low_col = "Low"
    volume_col = "Volume"

    for col in [close_col, high_col, low_col, volume_col]:
        if col not in df.columns:
            return None

    # 计算全套指标
    try:
        result = ind.compute_all(df)
    except Exception as exc:
        print(f"  [WARN] 指标计算异常: {exc}")
        return None

    # 取最新一行的值
    n = len(df) - 1
    close = float(df[close_col].iloc[n])
    volume = float(df[volume_col].iloc[n]) if not pd.isna(df[volume_col].iloc[n]) else 0.0

    def last(arr):
        """取数组最后一个非 NaN 值；全 NaN 返回 None。"""
        if arr is None:
            return None
        for i in range(len(arr) - 1, -1, -1):
            v = arr[i]
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                return float(v)
        return None

    # 逐字段提取最新值
    snapshot = {
        "close": close,
        "change_1d": last(result["change_1d"]),
        "change_5d": last(result["change_5d"]),
        "change_20d": last(result["change_20d"]),
        "ma5": last(result["ma5"]),
        "ma10": last(result["ma10"]),
        "ma20": last(result["ma20"]),
        "ma60": last(result["ma60"]),
        "ema12": last(result["ema12"]),
        "ema26": last(result["ema26"]),
        "macd": last(result["macd"]["macd"]),
        "macd_signal": last(result["macd"]["signal"]),
        "macd_histogram": last(result["macd"]["histogram"]),
        "rsi14": last(result["rsi14"]),
        "kdj_k": last(result["kdj"]["k"]),
        "kdj_d": last(result["kdj"]["d"]),
        "kdj_j": last(result["kdj"]["j"]),
        "bb_upper": last(result["bollinger"]["upper"]),
        "bb_middle": last(result["bollinger"]["middle"]),
        "bb_lower": last(result["bollinger"]["lower"]),
        "volume": volume,
        "volume_ma5": last(result["volume_ma5"]),
        "volume_ma20": last(result["volume_ma20"]),
    }

    # 过滤掉核心字段为 None 的（数据不足）
    if snapshot["ma5"] is None or snapshot["rsi14"] is None:
        return None

    # 清理 NaN → null
    for k, v in list(snapshot.items()):
        if v is not None and isinstance(v, float) and pd.isna(v):
            snapshot[k] = None

    snapshot["updated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return snapshot


def precompute_region(region: str, interval: str, dry_run: bool = False) -> dict:
    """对单个区域计算选股快照并写入 KV。

    Returns:
        {"region": region, "total": N, "computed": N, "written": bool}
    """
    symbols = load_symbols(region)
    total = len(symbols)
    if total == 0:
        print(f"  [{region}] 无股票代码，跳过")
        return {"region": region, "total": 0, "computed": 0, "written": False}

    print(f"  [{region}] 共 {total} 只股票，开始计算...")

    snapshot = {}
    computed = 0
    skipped = 0

    for i, symbol in enumerate(symbols):
        if (i + 1) % 100 == 0:
            print(f"    进度: {i+1}/{total} ({computed} 有效, {skipped} 跳过)")

        df = load_kline_from_r2(region, symbol, interval)
        if df is None:
            skipped += 1
            continue

        snap = compute_snapshot(df)
        if snap is None:
            skipped += 1
            continue

        snapshot[symbol] = snap
        computed += 1

    print(f"  [{region}] 完成: {computed} 有效, {skipped} 跳过, 共 {total}")

    if computed == 0:
        print(f"  [{region}] 无有效数据，跳过 KV 写入")
        return {"region": region, "total": total, "computed": 0, "written": False}

    # 写入 KV
    kv_key = f"screener:{interval}:{region}"
    kv_value = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
    size_kb = len(kv_value.encode("utf-8")) / 1024
    print(f"  [{region}] KV key={kv_key} size={size_kb:.1f}KB stocks={computed}")

    if dry_run:
        print(f"  [{region}] --dry-run 模式，不写入 KV")
        # 打印前 3 只样本供验证
        for sym in list(snapshot.keys())[:3]:
            print(f"    {sym}: {json.dumps(snapshot[sym], ensure_ascii=False)[:200]}")
        return {"region": region, "total": total, "computed": computed, "written": False}

    ok = kvstore.put(kv_key, kv_value)
    if ok:
        print(f"  [{region}] KV 写入成功 ✅")
    else:
        print(f"  [{region}] KV 写入失败 ❌（R2 数据仍可用）")

    return {"region": region, "total": total, "computed": computed, "written": ok}


def main():
    parser = argparse.ArgumentParser(description="选股指标预计算 → KV 快照")
    parser.add_argument("--interval", default="1d", help="K线周期，默认 1d")
    parser.add_argument("--region", default="all", help="区域，默认 all（全部）")
    parser.add_argument("--dry-run", action="store_true", help="只计算不写 KV，打印样本")
    args = parser.parse_args()

    if args.interval not in SUBDIR:
        print(f"不支持的周期: {args.interval}，可选: {list(SUBDIR.keys())}")
        sys.exit(1)

    # 选择区域
    if args.region == "all":
        regions = list(config.REGIONS.keys())
    else:
        if args.region not in config.REGIONS:
            print(f"未知区域: {args.region}，可选: {list(config.REGIONS.keys())}")
            sys.exit(1)
        regions = [args.region]

    print(f"=== 选股预计算 interval={args.interval} regions={regions} ===")

    results = []
    for region in regions:
        r = precompute_region(region, args.interval, dry_run=args.dry_run)
        results.append(r)

    # 汇总
    print(f"\n=== 汇总 ===")
    total_stocks = 0
    total_computed = 0
    for r in results:
        print(f"  {r['region']}: {r['computed']}/{r['total']} 有效, KV={'✅' if r['written'] else '❌'}")
        total_stocks += r["total"]
        total_computed += r["computed"]
    print(f"  总计: {total_computed}/{total_stocks} 有效")


if __name__ == "__main__":
    # 延迟导入 numpy（用于类型检查）
    import numpy as np  # noqa: E402
    main()
