"""共享工具：解析区域股票列表、支持全市场模式与分批切片。

三个 fetch 脚本共用此模块以避免重复逻辑。
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402


def read_kline(path: Path, index_col: str = "Date") -> pd.DataFrame | None:
    """读取已有 K 线 CSV（索引为日期/时间轴），文件不存在时返回 None。

    日线（index_col="Date"）会 normalize 到当天零点；分钟线（index_col="Datetime"）
    保留完整时间戳，确保去重按"时分秒"精确匹配。
    """
    if not path.exists():
        return None
    df = pd.read_csv(path, index_col=index_col, parse_dates=True)
    # 稳健转为 DatetimeIndex 并去掉时区（兼容 pandas 解析返回普通 Index 的情况）
    df.index = pd.to_datetime(df.index)
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)
    if index_col == "Date":
        df.index = df.index.normalize()
    return df


def merge_kline(
    path: Path,
    fresh: pd.DataFrame,
    cols: list[str],
    index_col: str = "Date",
) -> pd.DataFrame:
    """将 fresh 数据与 path 处的已有 CSV 合并、按时间去重后写回。

    返回合并后的 DataFrame。若已有文件存在，仅追加/覆盖缺失与更新的时间点；
    否则直接写入 fresh。index_col 常用 "Date"（日线）或 "Datetime"（分钟线）。
    """
    fresh = fresh[cols].copy()
    fresh.index = pd.to_datetime(fresh.index)
    if getattr(fresh.index, "tz", None) is not None:
        fresh.index = fresh.index.tz_localize(None)
    if index_col == "Date":
        fresh.index = fresh.index.normalize()

    existing = read_kline(path, index_col=index_col)
    if existing is None or existing.empty:
        merged = fresh
    else:
        merged = pd.concat([existing, fresh])
        # 按时间去重，保留最新一行
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()

    path.parent.mkdir(parents=True, exist_ok=True)
    merged.index.name = index_col
    merged.to_csv(path, encoding="utf-8")
    return merged


def load_symbols(region: str) -> list[str]:
    """返回指定区域的股票代码列表。

    若 config.REGIONS[region] 为空（全市场模式），则从 universe 文件读取。
    否则返回硬编码列表。找不到则返回空列表。
    """
    hardcoded = config.REGIONS.get(region, [])
    if hardcoded:
        return list(hardcoded)

    # 全市场模式：从 data/universe/{region}.csv 读取
    universe_file = config.UNIVERSE_FILES.get(region)
    if not universe_file:
        return []
    path = ROOT / config.DATA_DIR / config.UNIVERSE_SUBDIR / universe_file
    if not path.exists():
        print(f"  [警告] universe 文件不存在: {path.relative_to(ROOT)}", file=sys.stderr)
        return []
    symbols = [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return symbols


def load_index_symbols(index: str) -> tuple[str, list[str]]:
    """返回指定指数的 (region, 成分股列表)。

    从 data/universe/{file}.csv 读取成分股（该文件由 fetch_universe.py 更新）。
    找不到指数或文件时返回 (None, [])。
    """
    cfg = config.INDEX_CONFIG.get(index)
    if not cfg:
        print(f"  [警告] 未知指数: {index}", file=sys.stderr)
        return "", []
    path = ROOT / config.DATA_DIR / config.UNIVERSE_SUBDIR / cfg["file"]
    if not path.exists():
        print(f"  [警告] 指数清单不存在: {path.relative_to(ROOT)}", file=sys.stderr)
        return cfg["region"], []
    symbols = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        # 兼容两种格式：纯符号（AAPL）或 CSV（Symbol,Name）
        sym = line.split(",")[0].strip()
        if sym and sym != "Symbol":
            symbols.append(sym)
    return cfg["region"], symbols


def infer_region(symbol: str) -> str:
    """根据符号后缀推断区域：.HK->hk, .SS/.SZ->cn, 其他->us。"""
    if symbol.endswith(".HK"):
        return "hk"
    if symbol.endswith(".SS") or symbol.endswith(".SZ"):
        return "cn"
    return "us"


def slice_batch(symbols: list[str], batch: int, batches: int) -> list[str]:
    """将符号列表按顺序切成 batches 批，返回第 batch 批（0 起）。

    batch 越界时返回空列表。
    """
    if batches <= 1 or batch < 0 or batch >= batches:
        if batch == 0 and batches <= 1:
            return symbols
        return []
    n = len(symbols)
    size = (n + batches - 1) // batches  # 向上取整
    start = batch * size
    return symbols[start : start + size]


def region_now(region: str) -> datetime:
    """返回该区域市场时区的当前时间（naive，便于与 pandas Timestamp 比较）。

    优先使用 IANA 时区（zoneinfo）；极少数无 tzdata 的环境退化为固定偏移近似。
    """
    tz_name = config.REGION_TZ.get(region, "Asia/Shanghai")
    try:
        return datetime.now(ZoneInfo(tz_name)).replace(tzinfo=None)
    except Exception:  # noqa: BLE001 - 无 tzdata 时退化为固定偏移
        offset_hours = {"cn": 8, "hk": 8, "kr": 9, "us": -5}.get(region, 8)
        return datetime.utcnow() + timedelta(hours=offset_hours)


def is_market_session(region: str, now: datetime | None = None) -> bool:
    """当前是否处于该市场可能产生新分钟K的时段（含美股延长时段）。

    周末或休市时段返回 False，调用方可整批跳过拉取，避免无效请求。
    """
    local = region_now(region) if now is None else now
    if local.weekday() >= 5:  # 周六 / 周日
        return False
    hm = local.hour * 60 + local.minute
    for start, end in config.MARKET_SESSIONS.get(region, ()):
        if start <= hm <= end:
            return True
    return False


def run_with_retry(fn, *args, retries: int | None = None, delay: float | None = None, **kwargs):
    """执行 fn，遇瞬时错误按指数退避重试；重试耗尽后抛出原异常。"""
    retries = config.MAX_RETRIES if retries is None else retries
    delay = config.REQUEST_DELAY if delay is None else delay
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - 记录并重试
            last_exc = exc
            if attempt < retries - 1:
                wait = delay * (2**attempt)
                print(f"    重试 {attempt+1}/{retries-1}（等 {wait:.0f}s）：{exc}", flush=True)
                time.sleep(wait)
    if last_exc is not None:
        raise last_exc
    return None