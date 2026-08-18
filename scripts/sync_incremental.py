"""增量同步脚本（唯一的定时 action）。

在 fetch_history.py 全量入库之后运行，只获取新增数据并同步到 R2：
    - 日K：从已有最后日期往回看缓冲段拉取增量，合并去重
    - 分钟K：只拉已有最后时间点之后的新数据（含回看缓冲）
    - 5m/15m/30m：由 1m 增量数据重采样合并
    - 美股 1m/1h 含盘前盘后延长时段

增量优化：
    - 交易时段查重：市场休市（周末/非交易时段）且数据已最新时跳过请求
    - 只读 R2 已有对象（gzip 解压）判断最后时间点，避免每天全量重拉
    - 并发上传（scripts/r2store.py）

用法：
    export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET=...
    python scripts/sync_incremental.py                 # 全部区域
    python scripts/sync_incremental.py --region us     # 仅美股
    python scripts/sync_incremental.py --region cn --batch 0 --batches 10
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402
import marketlib  # noqa: E402
import r2store  # noqa: E402
import state  # noqa: E402
import yahoo_chart  # noqa: E402

# 内联指标计算（采集后直接算指标，不跑独立 precompute）
import indicators as _ind  # noqa: E402
import kvstore as _kv  # noqa: E402
import json as _json  # noqa: E402

COLS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
DATE_COL = "Date"
DT_COL = "Datetime"

SOURCE_INTERVALS = ["1m", "1h"]
# 各周期 R2 key 前缀
SUBDIR = {
    "1d": config.KLINE_SUBDIR,
    "1m": config.INTRADAY_M1_SUBDIR,
    "5m": config.INTRADAY_M5_SUBDIR,
    "15m": config.INTRADAY_M15_SUBDIR,
    "30m": config.INTRADAY_M30_SUBDIR,
    "1h": config.INTRADAY_M1H_SUBDIR,
}
# 日K增量回看缓冲天数（覆盖除权/分红修订）
DAILY_BUFFER_DAYS = 7
# 日K增量：已有数据距今超过该天数视为缺数据，强制全量补拉最近窗口
STALE_DAYS = 3


def key_for(region: str, symbol: str, interval: str) -> str:
    return f"{region}/{SUBDIR[interval]}/{symbol}.csv"


def load_existing(region: str, symbol: str, interval: str, index_col: str) -> pd.DataFrame | None:
    """从 R2 读取该股票该周期已有数据（自动解压 gzip）。

    **只在绝对需要时调用**：每调用一次就是一次 R2 Class B 读。
    若调用方知道 fresh 的所有行都严格晚于已有数据，应直接走「直接覆盖写入」路径。
    """
    text = r2store.get_csv_text(key_for(region, symbol, interval))
    if text is None:
        return None
    df = pd.read_csv(io.StringIO(text), index_col=index_col, parse_dates=True)
    df.index = pd.to_datetime(df.index)
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)
    if index_col == DATE_COL:
        df.index = df.index.normalize()
    return df


def _normalize_df(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    """把 fresh/existing DataFrame 的列和索引统一到约定格式。"""
    df = df[COLS].copy()
    df.index = pd.to_datetime(df.index)
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)
    if interval == "1d":
        df.index = df.index.normalize()
    return df


def merge_and_upload(
    region: str, symbol: str, interval: str, fresh: pd.DataFrame,
    known_last_ts: pd.Timestamp | None = None,
) -> tuple[int, pd.DataFrame]:
    """与 R2 已有数据合并去重后写回。

    Args:
        region, symbol, interval: 定位对象
        fresh: 本次拉到的新数据
        known_last_ts: 状态清单中记录的、R2 已存的最后时间。
            若已知且 fresh 的首行严格晚于它 → 说明 fresh 完全是新增数据，
            **不必读 R2**，直接写 fresh 本身即可（省一次 R2 读）。
            为 None 或 有重叠风险（例如 buffer 回看）时，退回读 R2 合并。

    返回 (新增行数, 合并后的完整 DataFrame)。
    """
    index_col = DATE_COL if interval == "1d" else DT_COL
    fresh = _normalize_df(fresh, interval)

    # ---- 快速路径：已知最后时间，且 fresh 完全在其之后 → 不必读 R2 ----
    if known_last_ts is not None and not fresh.empty:
        fresh_min = fresh.index.min()
        # 1d：日级别直接按天比即可
        # 分钟周期：要求 fresh 首行严格大于 known_last_ts
        if interval == "1d":
            no_overlap = fresh_min.date() > known_last_ts.date()
        else:
            no_overlap = fresh_min > known_last_ts
        if no_overlap:
            # 直接覆盖写入：fresh 本身就是完整新增
            csv_text = fresh.to_csv()
            r2store.put_csv(key_for(region, symbol, interval), csv_text)
            return len(fresh), fresh

    # ---- 常规路径：读 R2 已有数据做合并（仅在可能重叠时走这里）----
    existing = load_existing(region, symbol, interval, index_col)
    if existing is None or existing.empty:
        merged = fresh
    else:
        existing = _normalize_df(existing, interval)
        merged = pd.concat([existing, fresh])
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()

    before = len(existing) if existing is not None else 0
    added = len(merged) - before
    if added > 0:
        csv_text = merged.to_csv()
        r2store.put_csv(key_for(region, symbol, interval), csv_text)
    return max(added, 0), merged


def utc_now() -> pd.Timestamp:
    """当前 UTC 时间（naive，与 R2 中分钟K索引时区一致）。"""
    return pd.Timestamp.now(timezone.utc).tz_localize(None)


def _ts(entry_value: str | None) -> pd.Timestamp | None:
    """把状态清单里存的最后时间字符串转回 Timestamp；空/损坏返回 None。"""
    if not entry_value:
        return None
    try:
        return pd.to_datetime(entry_value)
    except Exception:  # noqa: BLE001 - 状态损坏按未知处理
        return None


def _fmt(ts: pd.Timestamp, interval: str) -> str:
    """把 Timestamp 转成状态清单里的字符串（1d 只留日期，其余留完整时间）。"""
    if ts is None:
        return ""
    ts = ts.normalize() if interval == "1d" else ts
    return ts.isoformat()


def fetch_incremental(
    region: str, symbol: str, interval: str, prev_ts: pd.Timestamp | None
) -> tuple[int, pd.Timestamp | None, pd.DataFrame | None]:
    """拉取单只股票指定周期增量并合并上传。

    返回 (新增行数, 合并后最后时间, 合并后的完整 DataFrame)。
    无新增时 merged=None，避免不必要的后续计算。
    """
    now_local = marketlib.region_now(region)
    now_utc = utc_now()

    # ---- 增量判重：直接用状态清单的最后时间，避免读 R2 全文件 ----
    if prev_ts is not None:
        if interval == "1d":
            in_session = marketlib.is_market_session(region, now_local)
            recent_cutoff = now_local.date() - timedelta(days=2)
            if not in_session and prev_ts.date() >= recent_cutoff:
                return 0, None
            start = (prev_ts - pd.Timedelta(days=DAILY_BUFFER_DAYS)).date()
            fresh = yahoo_chart.fetch_kline(
                symbol, interval="1d", start=start, prepost=False
            )
        else:
            if not marketlib.is_market_session(region, now_local):
                return 0, None
            minutes_since = (now_utc - prev_ts).total_seconds() / 60
            if minutes_since < config.INCREMENTAL_MIN_INTERVAL_MINUTES:
                return 0, None
            start = prev_ts - pd.Timedelta(days=config.INTRADAY_BUFFER_DAYS)
            fresh = yahoo_chart.fetch_kline(
                symbol, interval=interval, start=start, prepost=True
            )
    else:
        # 无状态：全量拉取该周期（首次/自愈）
        fresh = yahoo_chart.fetch_kline(
            symbol,
            interval=interval,
            period=period_for(interval),
            prepost=(interval != "1d"),
        )

    if fresh is None or fresh.empty:
        return 0, None, None
    added, merged = merge_and_upload(region, symbol, interval, fresh, known_last_ts=prev_ts)
    if merged is None or merged.empty:
        return added, None, None
    return added, merged.index.max(), merged


def period_for(interval: str) -> str:
    """该周期首次全量拉取的 period。"""
    if interval == "1d":
        return config.HISTORY_PERIOD
    return config.INTRADAY_PERIOD[interval]


def sync_minute_and_derived(
    region: str, symbol: str, entry: dict
) -> int:
    """拉取 1m + 1h 增量，并由 1m 增量重采样合并 5m/15m/30m。

    通过 entry（状态清单中该股票条目）记录各周期最后时间，返回新增行数。
    优化：1m 重采样得到的 5m/15m/30m 派生结果，若完全在 state 记录之后，
         直接追加写入，不必读 R2 旧数据；否则才退回读 R2 合并。
    """
    added = 0
    for interval in SOURCE_INTERVALS:
        prev = _ts(entry.get(interval))
        added_i, last_ts, _ = fetch_incremental(region, symbol, interval, prev)
        if last_ts is not None:
            entry[interval] = _fmt(last_ts, interval)
        added += added_i
    # 若无新增分钟数据，说明 1m 足够新鲜，派生K线也不会变，跳过重采样
    if added <= 0:
        return 0

    # 读一次 1m 完整数据用于重采样派生（1m 数据本身就是刚合并完的，
    # 这里无法完全避免；但 4 个派生周期可以共用这一次 1m 读）
    m1 = load_existing(region, symbol, "1m", DT_COL)
    if m1 is None or m1.empty:
        return added
    m1 = _normalize_df(m1, "1m")

    for target, rule in config.INTRADAY_DERIVED.items():
        agg = m1.resample(rule).agg(
            {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
        )
        agg = agg.dropna(subset=["Close"])
        if agg.empty:
            continue
        agg["Adj Close"] = m1["Close"].resample(rule).last()
        agg = agg[COLS]
        agg.index = pd.to_datetime(agg.index)
        # 派生周期的快速路径：完全在已知最后时间之后 → 不必读 R2
        known_last = _ts(entry.get(target))
        if known_last is not None and agg.index.min() > known_last:
            csv_text = agg.to_csv()
            r2store.put_csv(key_for(region, symbol, target), csv_text)
            added += len(agg)
            entry[target] = _fmt(agg.index.max(), target)
            continue
        # 常规路径：读 R2 已有派生数据做合并去重
        index_col = DT_COL
        existing = load_existing(region, symbol, target, index_col)
        if existing is None or existing.empty:
            merged = agg
        else:
            existing = _normalize_df(existing, target)
            merged = pd.concat([existing, agg])
            merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        before = len(existing) if existing is not None else 0
        if len(merged) > before:
            r2store.put_csv(key_for(region, symbol, target), merged.to_csv())
            added += len(merged) - before
            entry[target] = _fmt(merged.index.max(), target)
    return added


def _compute_snapshot(symbol: str, df: pd.DataFrame, interval: str) -> dict | None:
    """从 K 线 DataFrame 计算最新指标快照（单只股票）。

    返回指标 dict，数据不足时返回 None。
    """
    if df is None or len(df) < 5:
        return None
    try:
        result = _ind.compute_all(df)
    except Exception:
        return None

    n = len(df) - 1
    close = float(df["Close"].iloc[n]) if "Close" in df else None
    if close is None:
        return None

    def last(arr):
        for i in range(len(arr) - 1, -1, -1):
            v = arr[i]
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                return float(v)
        return None

    snap = {
        "close": close,
        "change_1d": last(result["change_1d"]),
        "change_5d": last(result["change_5d"]),
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
        "volume": float(df["Volume"].iloc[n]) if "Volume" in df else 0,
        "volume_ma5": last(result["volume_ma5"]),
        "volume_ma20": last(result["volume_ma20"]),
    }
    # 过滤掉核心字段为 None 的
    if snap["ma5"] is None or snap["rsi14"] is None:
        return None
    # 清理 NaN → null
    for k, v in list(snap.items()):
        if v is not None and isinstance(v, float) and pd.isna(v):
            snap[k] = None
    return snap


def _write_kv_snapshot(region: str, snapshots: dict[str, dict]) -> None:
    """将一批股票的指标快照写入 KV。

    读取已有 KV 快照 → 合并新数据 → 写回。
    """
    if not snapshots:
        return
    kv_key = f"screener:daily:{region}"
    try:
        # 读已有快照
        existing = _kv.get(kv_key)
        data = _json.loads(existing) if existing else {}
    except Exception:
        data = {}
    # 合并新数据
    data.update(snapshots)
    try:
        _kv.put(kv_key, _json.dumps(data, ensure_ascii=False, separators=(",", ":")))
    except Exception as exc:
        print(f"  [KV] 写入失败 {kv_key}: {exc}", flush=True)


def _process_one(
    reg: str, symbol: str, do_minute: bool, prev_entry: dict | None
) -> tuple[str, int, str, dict, dict | None]:
    """并发处理单只股票。

    返回 (symbol, added, err_msg, new_entry, indicator_snapshot)。
    indicator_snapshot 为计算出的指标快照，无新增或无数据时=None。
    """
    new_entry = dict(prev_entry) if prev_entry else {}
    indicator_snap = None
    try:
        prev = _ts(new_entry.get("1d"))
        added_d, last_ts, merged_d = fetch_incremental(reg, symbol, "1d", prev)
        if last_ts is not None:
            new_entry["1d"] = _fmt(last_ts, "1d")
        # 日K有新增 → 算指标
        if added_d > 0 and merged_d is not None:
            indicator_snap = _compute_snapshot(symbol, merged_d, "1d")
        added_m = 0
        if do_minute:
            added_m = sync_minute_and_derived(reg, symbol, new_entry)
        return symbol, added_d + added_m, "", new_entry, indicator_snap
    except Exception as exc:  # noqa: BLE001
        return symbol, 0, str(exc), (dict(prev_entry) if prev_entry else {}), None


def run(region: str | None, batch: int = 0, batches: int = 1) -> int:
    regions = [region] if region else list(config.REGIONS)
    # 并发线程数（可用环境变量 FETCH_CONCURRENCY 覆盖）
    concurrency = int(os.environ.get("FETCH_CONCURRENCY", "6"))

    # 分钟K：先判断各市场是否处于交易时段，休市市场跳过
    active_regions = set()
    for reg in regions:
        if marketlib.is_market_session(reg):
            active_regions.add(reg)
        else:
            print(f"[跳过] {reg}: 当前不在交易时段（周末/休市），跳过分钟K", flush=True)

    total_added = 0
    changed_symbols = 0
    failed: list[str] = []

    for reg in regions:
        symbols = marketlib.load_symbols(reg)
        symbols = marketlib.slice_batch(symbols, batch, batches)
        if not symbols:
            continue
        # 该 (region, batch) 独立的状态清单（各周期最后时间），替代逐个读 R2 判重
        snap = state.read("kline", reg, batch)
        print(f"[区域] {reg} ({len(symbols)} 只, 批 {batch+1}/{batches}, 并发 {concurrency})", flush=True)

        do_minute = reg in active_regions
        done = 0
        reg_changed = 0
        reg_snapshots: dict[str, dict] = {}  # 本批次新增的指标快照
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {
                pool.submit(_process_one, reg, sym, do_minute, snap.get(sym)): sym
                for sym in symbols
            }
            for fut in as_completed(futures):
                result = fut.result()
                sym, added, err, new_entry, indicator_snap = result
                done += 1
                if err:
                    failed.append(f"{reg}:{sym}")
                else:
                    total_added += added
                    if new_entry and new_entry != snap.get(sym):
                        snap[sym] = new_entry
                        reg_changed += 1
                        changed_symbols += 1
                    # 有新增数据且有指标快照 → 累计写入 KV
                    if indicator_snap is not None:
                        reg_snapshots[sym] = indicator_snap
                if done % 25 == 0 or done == len(symbols):
                    print(
                        f"  [{done}/{len(symbols)}] {reg} 已处理，累计新增 {total_added} 行，失败 {len(failed)}",
                        flush=True,
                    )
        # 写入选股指标快照 → KV（只写有新增的股票，不重算全量）
        if reg_snapshots:
            _write_kv_snapshot(reg, reg_snapshots)
            print(f"  [KV] {reg}: 更新 {len(reg_snapshots)} 只股票指标快照", flush=True)
        # 仅当本轮有状态变化才写回清单，其余大部分股票不触碰 R2
        if reg_changed > 0:
            state.write("kline", reg, batch, snap)

    r2store.put_status(
        {
            "mode": "incremental",
            "completed_at": r2store.now_iso(),
            "regions": regions,
            "regions_minute": list(active_regions),
            "added": total_added,
            "changed": changed_symbols,
            "failed": failed[:100],
            "fail_count": len(failed),
        }
    )
    print(f"增量完成: 新增 {total_added} 行, 变更 {changed_symbols} 只, 失败 {len(failed)} 项")
    # 单只股票失败不视为整体失败（避免 job 失败导致其余批次被取消），
    # 失败明细已写入 _status.json 供后续重试。
    if failed:
        print(f"警告: {len(failed)} 只股票失败(不中断): {failed[:30]}", flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="增量同步（定时，新增数据入库 R2）")
    parser.add_argument("--region", choices=list(config.REGIONS), help="仅处理指定区域")
    parser.add_argument("--batch", type=int, default=0, help="当前批次（0 起）")
    parser.add_argument("--batches", type=int, default=1, help="总批次数")
    args = parser.parse_args()
    return run(args.region, args.batch, args.batches)


if __name__ == "__main__":
    sys.exit(main())
