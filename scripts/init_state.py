"""一次性脚本：扫描 R2 现有对象，初始化 state manifest 到 _state/。

在部署了 state 优化之后、首次 Actions 运行之前执行。
若不执行，state.read() 返回 {}，所有 6800 只股票的首次优化逻辑会退化为
「无状态全量拉取 + 读 R2 合并」，R2 读写依然爆量。

覆盖三个 category：
    kline: 每只股票每个周期（1d/1m/5m/15m/30m/1h）最后时间
    meta : 每只股票 meta JSON 的内容指纹
    news : 每只股票 news JSON 的 url 集合指纹

读取所有 region 对象：cn / us / hk / kr / etf / cn_etf（来自 config.REGIONS）。
按 batch 分片写入 _state/kline/{region}_b{batch}.json，与生产端读取保持一致。

meta / news 不需要 batch 分片（采集端也是按 region 批量遍历，state.write 传同一个
batch=0 即可）。

R2 Class B 读开销：约等于 region × 6 周期 × 6800 股 ≈ 25 万次 Class B 读，
一次性付出，之后几乎不再读 R2。
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402
import marketlib  # noqa: E402
import r2store  # noqa: E402
import state  # noqa: E402


DATE_COL = "Date"
DT_COL = "Datetime"
INTERVALS = ["1d", "1m", "5m", "15m", "30m", "1h"]
SUBDIR = {
    "1d": config.KLINE_SUBDIR,
    "1m": config.INTRADAY_M1_SUBDIR,
    "5m": config.INTRADAY_M5_SUBDIR,
    "15m": config.INTRADAY_M15_SUBDIR,
    "30m": config.INTRADAY_M30_SUBDIR,
    "1h": config.INTRADAY_M1H_SUBDIR,
}


def _key(region: str, symbol: str, interval: str) -> str:
    # 注意：R2 中对象 key 后缀是 .csv（r2store.put_csv 写入时用的是 .csv，
    # 然后在 Content-Encoding 里标注 gzip；并不是 .csv.gz 后缀）。
    return f"{region}/{SUBDIR[interval]}/{symbol}.csv"


def _is_gzip(data: bytes) -> bool:
    return len(data) >= 2 and data[:2] == b"\x1f\x8b"


def _last_ts_of_kline(raw: bytes, interval: str):
    """从 CSV(gzip 或原始) 字节中，快速提取最后一行的时间，不解析整张表。

    为速度，只看末尾约 4KB（CSV 最后一行一般足够）。若失败，退回 pandas 解析。
    """
    if not raw:
        return None
    if _is_gzip(raw):
        try:
            raw = gzip.decompress(raw)
        except Exception:
            return None
    text = raw.decode("utf-8", errors="replace")
    # 从尾部找最后一行
    tail = text[-8192:] if len(text) > 8192 else text
    lines = [l for l in tail.split("\n") if l.strip()]
    if lines:
        last_line = lines[-1]
        first_cell = last_line.split(",")[0].strip()
        if first_cell and first_cell != DATE_COL and first_cell != DT_COL:
            try:
                return pd.to_datetime(first_cell)
            except Exception:
                pass
    # 退回完整 pandas 解析
    index_col = DATE_COL if interval == "1d" else DT_COL
    try:
        df = pd.read_csv(io.StringIO(text), index_col=index_col, parse_dates=True)
        if df.empty:
            return None
        return df.index.max()
    except Exception:
        return None


def _scan_kline_for_region(region: str, symbols: list[str], concurrency: int):
    """扫描一个 region 所有股票所有周期的最后时间。返回 {symbol: entry_dict}。"""
    snap: dict[str, dict] = {}

    def one(sym: str) -> tuple[str, dict | None]:
        entry: dict = {}
        for iv in INTERVALS:
            key = _key(region, sym, iv)
            raw = r2store.get_bytes(key)
            if raw is None:
                continue
            ts = _last_ts_of_kline(raw, iv)
            if ts is None:
                continue
            # 存字符串：1d 只留日，其余留完整时间
            if iv == "1d":
                entry[iv] = ts.normalize().isoformat()
            else:
                entry[iv] = ts.isoformat()
        return sym, (entry or None)

    done = 0
    total = len(symbols) * len(INTERVALS)
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(one, s): s for s in symbols}
        for fut in as_completed(futures):
            sym, entry = fut.result()
            done += 1
            if entry:
                snap[sym] = entry
            if done % 100 == 0 or done == len(symbols):
                print(
                    f"  kline [{done}/{len(symbols)}] {region}  state_entries={len(snap)}",
                    flush=True,
                )
    return snap


def _scan_meta_for_region(region: str, symbols: list[str], concurrency: int) -> dict:
    """扫描 meta 文件计算指纹。"""
    snap: dict[str, str] = {}

    def one(sym: str) -> tuple[str, str | None]:
        key = f"{region}/meta/{sym}.json"
        raw = r2store.get_bytes(key)
        if raw is None:
            return sym, None
        try:
            d = json.loads(raw.decode("utf-8"))
        except Exception:
            return sym, None
        body = json.dumps(d, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return sym, hashlib.md5(body.encode("utf-8")).hexdigest()

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(one, s): s for s in symbols}
        for i, fut in enumerate(as_completed(futures), 1):
            sym, h = fut.result()
            if h:
                snap[sym] = h
            if i % 100 == 0 or i == len(symbols):
                print(
                    f"  meta [{i}/{len(symbols)}] {region}  fingerprinted={len(snap)}",
                    flush=True,
                )
    return snap


def _scan_news_for_region(region: str, symbols: list[str], concurrency: int) -> dict:
    """扫描 news 文件计算 url 集合指纹。"""
    snap: dict[str, dict] = {}

    def one(sym: str) -> tuple[str, dict | None]:
        key = f"{region}/news/{sym}.json"
        raw = r2store.get_bytes(key)
        if raw is None:
            return sym, None
        try:
            d = json.loads(raw.decode("utf-8"))
        except Exception:
            return sym, None
        links = sorted([n.get("link", "") for n in d.get("news") or [] if n.get("link")])
        if not links:
            return sym, None
        body = "\n".join(links).encode("utf-8")
        h = hashlib.md5(body).hexdigest()
        return sym, {"h": h}

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(one, s): s for s in symbols}
        for i, fut in enumerate(as_completed(futures), 1):
            sym, entry = fut.result()
            if entry:
                snap[sym] = entry
            if i % 100 == 0 or i == len(symbols):
                print(
                    f"  news [{i}/{len(symbols)}] {region}  fingerprinted={len(snap)}",
                    flush=True,
                )
    return snap


def run(
    region: str | None = None,
    categories: list[str] | None = None,
    concurrency: int = 16,
    batches: dict[str, int] | None = None,
):
    categories = categories or ["kline", "meta", "news"]
    regions = [region] if region else list(config.REGIONS)
    batches = batches or {}

    for reg in regions:
        # 对不同 category，(region, cat) 的批次可能不同（kline vs meta vs news）
        n_batch = batches.get(reg, 1)
        symbols_all = marketlib.load_symbols(reg)
        if not symbols_all:
            continue
        print(f"\n[{reg}] 共 {len(symbols_all)} 只，分 {n_batch} 批，并发 {concurrency}", flush=True)

        # ---- kline：按 batch 分片，每片一份独立的 state 文件 ----
        if "kline" in categories:
            print(f"  — kline 初始化 (batches={n_batch}) —", flush=True)
            for b in range(n_batch):
                syms = marketlib.slice_batch(symbols_all, b, n_batch)
                if not syms:
                    continue
                print(f"    batch {b+1}/{n_batch}: {len(syms)} 只", flush=True)
                snap = _scan_kline_for_region(reg, syms, concurrency)
                if snap:
                    state.write("kline", reg, b, snap)
                    print(
                        f"    batch {b+1}/{n_batch} 写入 _state/kline/{reg}_b{b}.json "
                        f"({len(snap)} entries)",
                        flush=True,
                    )

        # ---- meta：按 batch 分片（与 fetch_meta.yml 批次一致） ----
        if "meta" in categories:
            print(f"  — meta 初始化 (batches={n_batch}) —", flush=True)
            # 单只不分片直接写 batch=0；多片才拆分
            if n_batch == 1:
                snap = _scan_meta_for_region(reg, symbols_all, concurrency)
                if snap:
                    state.write("meta", reg, 0, snap)
                    print(f"    写入 _state/meta/{reg}_b0.json ({len(snap)} entries)", flush=True)
            else:
                for b in range(n_batch):
                    syms = marketlib.slice_batch(symbols_all, b, n_batch)
                    if not syms:
                        continue
                    snap = _scan_meta_for_region(reg, syms, concurrency)
                    if snap:
                        state.write("meta", reg, b, snap)
                        print(
                            f"    batch {b+1}/{n_batch} 写入 _state/meta/{reg}_b{b}.json "
                            f"({len(snap)} entries)",
                            flush=True,
                        )

        # ---- news：同上，按 fetch_news.yml 批次分片 ----
        if "news" in categories:
            print(f"  — news 初始化 (batches={n_batch}) —", flush=True)
            if n_batch == 1:
                snap = _scan_news_for_region(reg, symbols_all, concurrency)
                if snap:
                    state.write("news", reg, 0, snap)
                    print(f"    写入 _state/news/{reg}_b0.json ({len(snap)} entries)", flush=True)
            else:
                for b in range(n_batch):
                    syms = marketlib.slice_batch(symbols_all, b, n_batch)
                    if not syms:
                        continue
                    snap = _scan_news_for_region(reg, syms, concurrency)
                    if snap:
                        state.write("news", reg, b, snap)
                        print(
                            f"    batch {b+1}/{n_batch} 写入 _state/news/{reg}_b{b}.json "
                            f"({len(snap)} entries)",
                            flush=True,
                        )


def main() -> int:
    parser = argparse.ArgumentParser(description="初始化 state manifest：扫描 R2 生成指纹和最后时间")
    parser.add_argument("--region", choices=list(config.REGIONS), help="仅处理指定区域（默认全部）")
    parser.add_argument("--cat", nargs="+", choices=["kline", "meta", "news"], help="仅处理指定类别")
    parser.add_argument("--concurrency", type=int, default=16, help="R2 读取并发线程数")
    args = parser.parse_args()

    # 各区域 × 类别的批次数，必须与 GitHub Actions workflow 完全一致：
    #   kline:  sync_data.yml + fetch_history.yml
    #   meta :  fetch_meta.yml
    #   news :  fetch_news.yml
    batches_kline = {"cn": 10, "us": 4, "etf": 4}
    batches_meta  = {"cn": 5,  "etf": 3}
    batches_news  = {"cn": 8,  "us": 3, "etf": 3}

    batches_by_cat = {
        "kline": batches_kline,
        "meta":  batches_meta,
        "news":  batches_news,
    }
    start = datetime.now()
    categories = args.cat or ["kline", "meta", "news"]
    # 每个 category 调一次 run，批次数按 category 分别控制
    for cat in categories:
        run(args.region, [cat], args.concurrency, batches_by_cat[cat])
    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n初始化完成，耗时 {elapsed:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
