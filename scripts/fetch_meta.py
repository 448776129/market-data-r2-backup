"""采集股票媒体/行情快照信息入库 R2（股价K线之外的基本面快照）。

从 Yahoo chart API 的 meta 字段采集（名称/代码/币种/交易所/52周高低/
实时价/当日高低/成交量/上市日期/涨跌幅），存为 JSON 到 R2：
    {region}/meta/{symbol}.json

供 API 的 /quote 和 /price 补全 name/currency 等字段。

用法：
    export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET=stocksmarkets
    python scripts/fetch_meta.py                  # 全部区域
    python scripts/fetch_meta.py --region us      # 仅美股
    python scripts/fetch_meta.py --region us --batch 0 --batches 4

说明：
    - 并发拉取（FETCH_CONCURRENCY 控制，默认 6）
    - 单只失败不中断整体（记录到 _status.json）
    - meta 体积小，一次性采集；后续可在增量工作流中低频刷新
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402
import marketlib  # noqa: E402
import r2store  # noqa: E402
import state  # noqa: E402
import yahoo_meta  # noqa: E402

# 由 meta 字典生成稳定的内容指纹（排序键 + 紧凑分隔，保证跨运行一致）。
def _fingerprint(meta: dict) -> str:
    body = json.dumps(meta, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(body.encode("utf-8")).hexdigest()


def upload_meta(region: str, symbol: str, meta: dict) -> bool:
    """上传单只股票的 meta JSON 到 R2。"""
    key = f"{region}/meta/{symbol}.json"
    data = json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")
    try:
        s3 = r2store.get_client()
        s3.put_object(
            Bucket=r2store.get_bucket(),
            Key=key,
            Body=data,
            ContentType="application/json; charset=utf-8",
        )
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"  [上传失败] {symbol}: {exc}", flush=True)
        return False


def _process_one(
    region: str, symbol: str, known: str | None, seek: bool
) -> tuple[str, bool, str | None]:
    """并发处理单只股票。

    返回 (symbol, ok, new_fingerprint_or_None)。
    - ok: 本次是否成功处理（用于状态统计）。
    - new_fingerprint: 只有内容真正变化（需要写 R2）时返回指纹，否则返回 None，
      调用方据此更新本地 state 并计为一次实际写入。seek=False 且该股票已有状态时
      （如休市时段）直接跳过，不再拉取，避免无谓的 Yahoo/反代开销。
    """
    try:
        # 休市且已有数据：跳过，meta 此时不会变化，省掉一次网络请求
        if not seek and known is not None:
            return symbol, True, None
        meta = yahoo_meta.fetch_meta_full(symbol)
        if not meta:
            return symbol, False, None
        h = _fingerprint(meta)
        if known == h:
            # 内容没变：不写 R2，也不更新指纹
            return symbol, True, None
        ok = upload_meta(region, symbol, meta)
        return symbol, ok and True, h if ok else None
    except Exception as exc:  # noqa: BLE001
        print(f"  [失败] {region}:{symbol}: {exc}", flush=True)
        return symbol, False, None


def run(region: str | None, batch: int = 0, batches: int = 1) -> int:
    regions = [region] if region else list(config.REGIONS)
    concurrency = int(os.environ.get("FETCH_CONCURRENCY", "6"))
    ok_count = 0
    changed_count = 0
    failed: list[str] = []

    for reg in regions:
        symbols = marketlib.load_symbols(reg)
        symbols = marketlib.slice_batch(symbols, batch, batches)
        if not symbols:
            print(f"[警告] {reg}: 无符号", flush=True)
            continue
        # 该 (region, batch) 独立的状态清单；只在真正有变化时触碰 R2
        snap = state.read("meta", reg, batch)
        seek = marketlib.is_market_session(reg)
        print(f"[区域] {reg} ({len(symbols)} 只, 批 {batch+1}/{batches}, 并发 {concurrency}, seek={'是' if seek else '否'})", flush=True)

        done = 0
        reg_changed = 0
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {
                pool.submit(_process_one, reg, sym, snap.get(sym), seek): sym
                for sym in symbols
            }
            for fut in as_completed(futures):
                sym, ok, fp = fut.result()
                done += 1
                if not ok:
                    failed.append(f"{reg}:{sym}")
                else:
                    ok_count += 1
                    if fp is not None:
                        snap[sym] = fp
                        reg_changed += 1
                if done % 50 == 0 or done == len(symbols):
                    print(
                        f"  [{done}/{len(symbols)}] {reg} 处理 {ok_count}，变更写入 {changed_count}，失败 {len(failed)}",
                        flush=True,
                    )
        # 仅当本轮确实有变化时才写回清单，避免无谓写操作（其余股票不触碰 R2）
        if reg_changed > 0:
            state.write("meta", reg, batch, snap)
        changed_count += reg_changed

    r2store.put_status(
        {
            "mode": "meta",
            "completed_at": r2store.now_iso(),
            "regions": regions,
            "ok": ok_count,
            "changed": changed_count,
            "failed": failed[:100],
            "fail_count": len(failed),
        }
    )
    print(f"meta 采集完成: 处理 {ok_count}, 变更写入 {changed_count}, 失败 {len(failed)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="采集股票媒体/行情快照信息入库 R2")
    parser.add_argument("--region", choices=list(config.REGIONS), help="仅处理指定区域")
    parser.add_argument("--batch", type=int, default=0, help="当前批次（0 起）")
    parser.add_argument("--batches", type=int, default=1, help="总批次数")
    args = parser.parse_args()
    return run(args.region, args.batch, args.batches)


if __name__ == "__main__":
    sys.exit(main())
