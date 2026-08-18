"""Cloudflare R2 存储客户端（S3 兼容）。

负责将采集到的 K 线数据以 gzip 压缩后写入 Cloudflare R2，并通过
多线程并发上传提升吞吐。所有对象键遵循约定：

    universe/{region}.csv                # 股票清单（不压缩）
    {region}/kline/{symbol}.csv.gz       # 日K
    {region}/kline_1m/{symbol}.csv.gz    # 1分钟K
    {region}/kline_5m/{symbol}.csv.gz    # 5分钟K（由 1m 派生）
    {region}/kline_15m/{symbol}.csv.gz
    {region}/kline_30m/{symbol}.csv.gz
    {region}/kline_1h/{symbol}.csv.gz    # 1小时K
    _status.json                         # 最近一次采集状态

凭据通过环境变量注入（GitHub Actions Secrets / .env.local）：
    R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
"""

from __future__ import annotations

import gzip
import io
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import r2s3  # noqa: E402 - 极简 R2 S3 客户端（urllib + SigV4，替代 boto3）

# 并发上传线程数（R2 免费额度下平衡速度与限流）
UPLOAD_WORKERS = 16


def _env(name: str) -> str:
    """读取环境变量，缺失时抛出可读错误。"""
    val = os.environ.get(name, "").strip()
    if not val:
        raise RuntimeError(f"缺少环境变量 {name}（请在 GitHub Secrets 或 .env.local 中配置）")
    return val


def get_client():
    """兼容占位：R2 S3 客户端（r2s3 无状态，返回 None 即可）。"""
    return None


def get_bucket() -> str:
    return _env("R2_BUCKET")


def _gzip_bytes(data: bytes) -> bytes:
    """gzip 压缩数据（用于 CSV 存储，可压缩约 70~80%）。"""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as f:
        f.write(data)
    return buf.getvalue()


def put_bytes(key: str, data: bytes, content_type: str = "text/csv; charset=utf-8", compressed: bool = False):
    """写入单个对象。compressed=True 时数据为 gzip 压缩内容，附带 Content-Encoding。"""
    encoding = "gzip" if compressed else None
    r2s3.put_obj(key, data, content_type=content_type, content_encoding=encoding)


def put_csv(key: str, csv_text: str):
    """将 CSV 文本（UTF-8 带 BOM）压缩后写入 R2。

    UTF-8 BOM 使 Windows Excel 双击即可正确识别编码，避免乱码。
    """
    content = csv_text
    if not content.startswith("\ufeff"):
        content = "\ufeff" + content
    put_bytes(key, _gzip_bytes(content.encode("utf-8")), compressed=True)


def put_universe(region: str, csv_text: str):
    """写入股票清单（不压缩，便于 Cloudflare Worker 直接读取）。"""
    put_bytes(f"universe/{region}.csv", csv_text.encode("utf-8"), content_type="text/csv; charset=utf-8")


def get_bytes(key: str) -> bytes | None:
    """读取对象原始字节；不存在返回 None。"""
    try:
        return r2s3.get_obj(key)
    except Exception as exc:  # noqa: BLE001
        if "404" in str(exc) or "NoSuchKey" in str(exc):
            return None
        raise


def get_csv_text(key: str) -> str | None:
    """读取对象文本；自动解压 gzip（依据 Content-Encoding 或 .gz 后缀）。"""
    raw = get_bytes(key)
    if raw is None:
        return None
    if key.endswith(".gz") or _is_gzip(raw):
        raw = gzip.decompress(raw)
    return raw.decode("utf-8")


def _is_gzip(data: bytes) -> bool:
    return data[:2] == b"\x1f\x8b"


def exists(key: str) -> bool:
    """判断对象是否存在。"""
    try:
        return r2s3.exists(key)
    except Exception:  # noqa: BLE001
        return False


def last_modified(key: str) -> datetime | None:
    """返回对象最后修改时间；不存在返回 None。"""
    try:
        meta = r2s3.head_obj(key)
        if not meta:
            return None
        lm = meta.get("Last-Modified")
        if lm:
            return datetime.strptime(lm, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
        return None
    except Exception:  # noqa: BLE001
        return None


def upload_many(items: list[tuple[str, str, bool]]) -> dict[str, int]:
    """并发上传多个 CSV 对象。

    items: [(key, csv_text, compressed), ...]
    返回 {"ok": n, "fail": n}。单个失败不中断整体，打印错误。
    """
    if not items:
        return {"ok": 0, "fail": 0}
    results = {"ok": 0, "fail": 0}

    def _upload(item):
        key, text, compressed = item
        if compressed:
            put_csv(key, text)
        else:
            put_bytes(key, text.encode("utf-8"))
        return key

    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as pool:
        futures = [pool.submit(_upload, it) for it in items]
        for fut in as_completed(futures):
            try:
                fut.result()
                results["ok"] += 1
            except Exception as exc:  # noqa: BLE001
                results["fail"] += 1
                # 无法拿到 key，只计数
                print(f"    [上传失败] {type(exc).__name__}: {exc}", flush=True)
    return results


def put_status(meta: dict):
    """写入采集状态 JSON（供增量同步判断全局进度）。"""
    data = json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")
    put_bytes("_status.json", data, content_type="application/json")


def get_status() -> dict | None:
    raw = get_bytes("_status.json")
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:  # noqa: BLE001
        return None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
