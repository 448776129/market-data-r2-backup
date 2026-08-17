"""从 Cloudflare R2 下载 K 线数据到本地 data/ 目录（自动解压 gzip）。

用法：
    export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET=stocksmarkets
    python scripts/download_r2.py                      # 下载全部
    python scripts/download_r2.py --region us          # 仅美股
    python scripts/download_r2.py --region us --symbol AAPL   # 单只
    python scripts/download_r2.py --symbol 600519.SS          # 跨区域单只

下载后文件为纯文本 CSV（已解压），保存在 data/{region}/{subdir}/{symbol}.csv。
"""

from __future__ import annotations

import argparse
import gzip
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
import r2store  # noqa: E402

# 本地目标目录
LOCAL_DATA = Path(__file__).resolve().parents[1] / config.DATA_DIR

# 周期 -> 子目录
SUBDIR = {
    "kline": config.KLINE_SUBDIR,
    "1m": config.INTRADAY_M1_SUBDIR,
    "5m": config.INTRADAY_M5_SUBDIR,
    "15m": config.INTRADAY_M15_SUBDIR,
    "30m": config.INTRADAY_M30_SUBDIR,
    "1h": config.INTRADAY_M1H_SUBDIR,
}


def list_keys(region: str | None) -> list[str]:
    """列出 R2 中所有 K 线对象 key。"""
    s3 = r2store.get_client()
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=r2store.get_bucket()):
        for o in page.get("Contents", []):
            key = o["Key"]
            if not key.endswith(".csv"):
                continue
            if key.startswith("universe/") or key.startswith("_"):
                continue
            if region and not key.startswith(f"{region}/"):
                continue
            keys.append(key)
    return keys


def key_to_local_path(key: str) -> Path:
    """把 R2 key (us/kline/AAPL.csv) 映射到本地路径。"""
    parts = key.split("/")
    region, subdir, filename = parts[0], parts[1], parts[2]
    local = LOCAL_DATA / region / subdir / filename
    return local


def download(key: str, out: Path) -> int:
    """下载单个对象并解压为纯文本 CSV，返回字节数。"""
    raw = r2store.get_bytes(key)
    if raw is None:
        return 0
    # 自动解压 gzip
    if raw[:2] == b"\x1f\x8b":
        text = gzip.decompress(raw).decode("utf-8-sig")  # 兼容带/不带 BOM
    else:
        text = raw.decode("utf-8-sig")
    out.parent.mkdir(parents=True, exist_ok=True)
    # utf-8-sig 编码写入：确保本地文件带 UTF-8 BOM，Excel 可直接打开
    out.write_text(text, encoding="utf-8-sig")
    return len(text.encode("utf-8-sig"))


def main() -> int:
    parser = argparse.ArgumentParser(description="从 R2 下载 K 线数据到本地")
    parser.add_argument("--region", choices=list(config.REGIONS), help="仅下载指定区域")
    parser.add_argument("--symbol", help="仅下载指定股票（如 AAPL / 600519.SS）")
    args = parser.parse_args()

    keys = list_keys(args.region)
    if args.symbol:
        keys = [k for k in keys if k.split("/")[-1].lower() == args.symbol.lower()]
        # 也尝试 .SS/.SZ/.KS/.HK 后缀匹配
        if not keys and "." not in args.symbol:
            keys = [
                k
                for k in keys
                if k.split("/")[-1].split(".")[0].lower() == args.symbol.lower()
            ]

    print(f"找到 {len(keys)} 个对象")
    total = 0
    for i, key in enumerate(sorted(keys), 1):
        out = key_to_local_path(key)
        n = download(key, out)
        total += n
        if i % 500 == 0 or i == len(keys):
            print(f"  [{i}/{len(keys)}] 已下载 {total/1024/1024:.1f} MB", flush=True)
    print(f"完成: 下载 {len(keys)} 个文件, 共 {total/1024/1024:.1f} MB")
    print(f"本地目录: {LOCAL_DATA}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
