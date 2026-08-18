"""高频分钟K同步（纯 Python 版，无 pandas/numpy 依赖）。

替代 sync_minute_realtime.py，专为 GitHub Actions 高频运行优化：
  - 依赖: 仅标准库（urllib/gzip/json/threading）+ 项目内纯 Python 模块
  - 无需安装 pandas/boto3/numpy（pip install 近乎零耗时）
  - 拉取 1m K 线 → gzip 写入 R2 + 重采样 5m/15m/30m + 指标入库 KV

用法：
    python scripts/sync_minute_realtime_pure.py --index csi300 --region cn
    python scripts/sync_minute_realtime_pure.py --index nasdaq100 --region us

环境变量：
    R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
    CLOUDFLARE_API_TOKEN, CLOUDFLARE_ACCOUNT_ID, KV_NAMESPACE_ID
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

import config  # noqa: E402
import indicators_pure as ind  # noqa: E402

# R2 轻量客户端（urllib + SigV4）
sys.path.insert(0, str(ROOT / "scripts"))
import r2s3  # noqa: E402

# ── 交易时段判断（纯 stdlib，内联自 marketlib，避免 import pandas）────
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def _region_now(region: str) -> datetime:
    tz_name = config.REGION_TZ.get(region, "Asia/Shanghai")
    try:
        return datetime.now(ZoneInfo(tz_name)).replace(tzinfo=None)
    except Exception:  # noqa: BLE001 - 无 tzdata 退化为固定偏移
        offset_hours = {"cn": 8, "hk": 8, "kr": 9, "us": -5}.get(region, 8)
        return datetime.utcnow() + timedelta(hours=offset_hours)


def _is_market_session(region: str) -> bool:
    local = _region_now(region)
    if local.weekday() >= 5:  # 周六/周日
        return False
    hm = local.hour * 60 + local.minute
    for start, end in config.MARKET_SESSIONS.get(region, ()):
        if start <= hm <= end:
            return True
    return False

SUBDIR = {
    "1m": config.INTRADAY_M1_SUBDIR,
    "5m": config.INTRADAY_M5_SUBDIR,
    "15m": config.INTRADAY_M15_SUBDIR,
    "30m": config.INTRADAY_M30_SUBDIR,
}
# 重采样：分钟数
RESAMPLE_MIN = {"5m": 5, "15m": 15, "30m": 30}
FETCH_CONCURRENCY = int(os.environ.get("FETCH_CONCURRENCY", "10"))
NAN = float("nan")


# ── CSV 工具（纯 stdlib）───────────────────────────────────────

def rows_to_csv(rows: list[list]) -> str:
    """list[list] → CSV 文本（RFC4180 简化版）。"""
    lines = []
    for row in rows:
        cells = [str(c) if c is not None else "" for c in row]
        lines.append(",".join(cells))
    return "\n".join(lines) + "\n"


def csv_to_rows(text: str) -> list[list]:
    """CSV 文本 → list[list]（跳过空行，去 BOM）。"""
    if text.startswith("\ufeff"):
        text = text[1:]
    out = []
    for line in text.splitlines():
        if not line.strip():
            continue
        out.append([c.strip() for c in line.split(",")])
    return out


def gzip_bytes(data: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as f:
        f.write(data)
    return buf.getvalue()


# ── 重采样（纯 Python）────────────────────────────────────────

def resample_1m(rows: list[dict], minutes: int) -> list[dict]:
    """把 1m K 线按 N 分钟窗口聚合。

    rows: fetch_kline_pure 返回的 list[dict]（按时间升序）
    returns: 聚合后的 list[dict]（同样的字段结构）
    """
    if not rows:
        return []
    out: list[dict] = []
    bucket_start = rows[0]["ts"] - (rows[0]["ts"] % (minutes * 60))
    cur: dict | None = None
    for r in rows:
        ts = r["ts"]
        start = ts - (ts % (minutes * 60))
        if cur is None or start != bucket_start:
            if cur is not None:
                out.append(cur)
            bucket_start = start
            cur = {
                "ts": bucket_start,
                "datetime": datetime.fromtimestamp(bucket_start, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "open": r["open"], "high": r["high"], "low": r["low"],
                "close": r["close"], "adjclose": r["adjclose"], "volume": r["volume"] or 0.0,
            }
        else:
            if r["high"] is not None and (cur["high"] is None or r["high"] > cur["high"]):
                cur["high"] = r["high"]
            if r["low"] is not None and (cur["low"] is None or r["low"] < cur["low"]):
                cur["low"] = r["low"]
            cur["close"] = r["close"]
            if r["adjclose"] is not None:
                cur["adjclose"] = r["adjclose"]
            cur["volume"] = (cur.get("volume") or 0.0) + (r["volume"] or 0.0)
    if cur is not None:
        out.append(cur)
    return out


# ── R2 读写（r2s3 + gzip）──────────────────────────────────────

def key_for(region: str, symbol: str, interval: str) -> str:
    return f"{region}/{SUBDIR[interval]}/{symbol}.csv"


def put_csv_gz(region: str, symbol: str, interval: str, rows: list[list]) -> None:
    csv_text = rows_to_csv(rows)
    payload = ("\ufeff" + csv_text).encode("utf-8")
    r2s3.put_obj(key_for(region, symbol, interval), gzip_bytes(payload),
                 content_type="text/csv; charset=utf-8", content_encoding="gzip")


def load_csv_gz(region: str, symbol: str, interval: str) -> list[list] | None:
    raw = r2s3.get_obj(key_for(region, symbol, interval))
    if raw is None:
        return None
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return csv_to_rows(raw.decode("utf-8", errors="replace"))


# ── 指标快照（indicators_pure）─────────────────────────────────

def compute_snapshot_pure(rows: list[list]) -> dict | None:
    """从 1m CSV rows（含表头）计算最新指标快照。

    rows: [["Datetime","Open","High","Low","Close","Adj Close","Volume"], [...], ...]
    """
    if len(rows) < 2:
        return None
    header = rows[0]
    close_i = header.index("Close") if "Close" in header else 3
    high_i = header.index("High") if "High" in header else 2
    low_i = header.index("Low") if "Low" in header else 3 - 1
    vol_i = header.index("Volume") if "Volume" in header else 6

    closes = []
    highs = []
    lows = []
    vols = []
    for row in rows[1:]:
        try:
            closes.append(float(row[close_i]))
            highs.append(float(row[high_i]))
            lows.append(float(row[low_i]))
            vols.append(float(row[vol_i]) if row[vol_i] else 0.0)
        except (ValueError, IndexError):
            continue

    if len(closes) < 5:
        return None
    result = ind.compute_all(closes, highs, lows, vols)

    def last(arr):
        for v in reversed(arr):
            if v is not None and not (isinstance(v, float) and v != v):
                return float(v)
        return None

    snap = {
        "close": closes[-1],
        "change_1d": last(result["change_1d"]),
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
        "volume": vols[-1],
        "volume_ma5": last(result["volume_ma5"]),
        "volume_ma20": last(result["volume_ma20"]),
    }
    if snap["ma5"] is None or snap["rsi14"] is None:
        return None
    return snap


# ── Yahoo chart API（纯 stdlib，经反代）────────────────────────

def _fetch_kline_pure(
    symbol: str,
    interval: str = "1m",
    period: str | None = None,
    start: str | None = None,
    end: str | None = None,
    prepost: bool = True,
) -> list[dict]:
    """经反代拉取 K 线（无 pandas）。返回 list[dict]（含 ts/datetime/ohlcv）。"""
    import calendar
    import json as _json
    import urllib.parse
    import urllib.request

    def _parse_ts(value: str) -> int:
        if " " in value:
            dt = datetime.strptime(value, "%Y-%m-%d %H:%M")
        else:
            dt = datetime.strptime(value, "%Y-%m-%d")
        return int(calendar.timegm(dt.timetuple()))

    params: dict = {"interval": interval, "includePrePost": str(prepost).lower()}
    now = int(time.time())
    if period:
        params["range"] = period
    else:
        params["period1"] = _parse_ts(start) if start else now - 6 * 30 * 86400
        params["period2"] = _parse_ts(end) if end else now

    origin = "https://query1.finance.yahoo.com/v8/finance/chart/"
    raw = origin + urllib.parse.quote(symbol) + "?" + urllib.parse.urlencode(params)
    url = f"{config.YAHOO_CHART_PROXY.rstrip('/')}/{raw}"

    last_exc: Exception | None = None
    for attempt in range(config.MAX_RETRIES):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = _json.loads(resp.read().decode("utf-8"))
            return _parse_chart_pure(data, symbol, interval)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < config.MAX_RETRIES - 1:
                wait = config.REQUEST_DELAY * (2**attempt)
                print(f"    重试 {attempt+1}/{config.MAX_RETRIES-1}（等 {wait:.0f}s）：{exc}", flush=True)
                time.sleep(wait)
    if last_exc is not None:
        raise last_exc
    return []


def _parse_chart_pure(data: dict, symbol: str, interval: str) -> list[dict]:
    import json as _json  # noqa: F401
    result = (data.get("chart") or {}).get("result")
    if not result:
        err = (data.get("chart") or {}).get("error")
        raise RuntimeError(f"{symbol} chart API 无数据: {err}")

    res = result[0]
    ts = res.get("timestamp") or []
    quote = (res.get("indicators") or {}).get("quote") or []
    adjclose = ((res.get("indicators") or {}).get("adjclose") or [{}])[0]
    if not ts or not quote:
        return []

    q = quote[0]
    opens, highs, lows, closes, vols = (
        q.get("open") or [], q.get("high") or [], q.get("low") or [],
        q.get("close") or [], q.get("volume") or [],
    )
    adjcloses = adjclose.get("adjclose") or [None] * len(ts)

    rows: list[dict] = []
    for i, t in enumerate(ts):
        close = closes[i] if i < len(closes) else None
        if close is None:
            continue
        if interval == "1d":
            dt_str = datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d")
        else:
            dt_str = datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        rows.append({
            "ts": t,
            "datetime": dt_str,
            "open": float(opens[i]) if i < len(opens) else None,
            "high": float(highs[i]) if i < len(highs) else None,
            "low": float(lows[i]) if i < len(lows) else None,
            "close": float(close),
            "adjclose": float(adjcloses[i]) if i < len(adjcloses) and adjcloses[i] is not None else None,
            "volume": float(vols[i]) if i < len(vols) else 0.0,
        })
    return rows


# ── 单只股票同步 ───────────────────────────────────────────────

def sync_one(region: str, symbol: str) -> dict:
    """拉取 1m 增量 → 写入 R2 + 重采样（一次性全量当天数据）。"""
    result = {"symbol": symbol, "status": "ok", "rows": 0}
    try:
        if not _is_market_session(region):
            result["status"] = "offhours"
            return result

        rows = _fetch_kline_pure(symbol, "1m", period="1d", prepost=True)
        if not rows:
            result["status"] = "no_data"
            return result

        # 写 1m CSV（纯 Python 序列化 + gzip）
        csv_rows = [["Datetime", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]
        for r in rows:
            csv_rows.append([
                r["datetime"],
                _fmt(r["open"]), _fmt(r["high"]), _fmt(r["low"]), _fmt(r["close"]),
                _fmt(r["adjclose"]), _fmt(r["volume"]),
            ])
        put_csv_gz(region, symbol, "1m", csv_rows)

        # 重采样 5m/15m/30m
        for derived, minutes in RESAMPLE_MIN.items():
            agg = resample_1m(rows, minutes)
            if not agg:
                continue
            d_rows = [["Datetime", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]
            for r in agg:
                d_rows.append([
                    r["datetime"],
                    _fmt(r["open"]), _fmt(r["high"]), _fmt(r["low"]), _fmt(r["close"]),
                    _fmt(r["adjclose"]), _fmt(r["volume"]),
                ])
            put_csv_gz(region, symbol, derived, d_rows)

        result["rows"] = len(rows)
        result["status"] = "ok"
    except Exception as exc:  # noqa: BLE001
        result["status"] = f"error: {exc}"
    return result


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.6g}" if v == int(v) else repr(v)
    return str(v)


# ── KV 写入 ────────────────────────────────────────────────────

def write_kv_snapshot(index: str, region: str, symbols: list[str]) -> None:
    """从 R2 读 1m CSV → 算指标 → 写入 KV 快照。"""
    print("\n--- 指标快照 → KV ---")
    snapshot = {}
    for sym in symbols:
        rows = load_csv_gz(region, sym, "1m")
        if not rows or len(rows) < 2:
            continue
        snap = compute_snapshot_pure(rows)
        if snap is not None:
            snapshot[sym] = snap

    if not snapshot:
        print("  无有效快照数据")
        return

    kv_key = f"screener:1m:{index}"
    kv_value = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
    size_kb = len(kv_value.encode("utf-8")) / 1024
    print(f"  {kv_key}: {len(snapshot)} 只, {size_kb:.1f}KB", flush=True)

    # 走 Cloudflare REST API（kvstore 已是 stdlib）
    try:
        import kvstore  # noqa: E402
        ok = kvstore.put(kv_key, kv_value)
        print(f"  KV {'✅' if ok else '❌'}")
    except Exception as exc:  # noqa: BLE001
        print(f"  KV 写入异常: {exc}")


# ── main ───────────────────────────────────────────────────────

def load_index_symbols(index_name: str) -> list[str]:
    uni_file = ROOT / config.DATA_DIR / config.UNIVERSE_SUBDIR / f"{index_name}.csv"
    if not uni_file.exists():
        print(f"[WARN] universe 文件不存在: {uni_file}")
        return []
    return [line.strip() for line in uni_file.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.startswith("#")]


def main() -> int:
    parser = argparse.ArgumentParser(description="高频分钟K同步（纯 Python）")
    parser.add_argument("--index", required=True, help="csi300 / nasdaq100")
    parser.add_argument("--region", required=True, help="cn / us")
    args = parser.parse_args()

    symbols = load_index_symbols(args.index)
    if not symbols:
        print(f"无 {args.index} 成分股，退出")
        return 1

    print(f"=== {args.index} ({args.region}) {len(symbols)} 只 · 纯Python ===")
    print(f"时间: {datetime.now(timezone.utc).isoformat()}")

    ok = skip = err = 0
    with ThreadPoolExecutor(max_workers=FETCH_CONCURRENCY) as pool:
        futures = {pool.submit(sync_one, args.region, s): s for s in symbols}
        for fut in as_completed(futures):
            r = fut.result()
            if r["status"] == "ok":
                ok += 1
            elif r["status"] in ("offhours", "no_data"):
                skip += 1
            else:
                err += 1
                if err <= 3:
                    print(f"  [ERR] {r['symbol']}: {r['status']}")

    print(f"完成: ok={ok} skip={skip} err={err}")

    # 无论同步结果如何，只要有股票列表就写 KV（R2 已有历史数据）
    write_kv_snapshot(args.index, args.region, symbols)
    return 0


if __name__ == "__main__":
    sys.exit(main())