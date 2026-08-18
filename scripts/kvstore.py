"""Cloudflare Workers KV 写入客户端（REST API）。

用于将静态清单（universe）等小体积、读多写少的数据写入 KV，
使 Worker 读取时绕过 R2（省 R2 读次数 + 省 Worker gzip 解压 CPU）。

凭据通过环境变量注入（GitHub Actions Secrets）：
    CLOUDFLARE_API_TOKEN   — 具有 Workers KV 编辑权限的 API Token
    CLOUDFLARE_ACCOUNT_ID   — Cloudflare 账户 ID
    KV_NAMESPACE_ID         — KV 命名空间 ID（面板创建后获得）

KV 与 R2 双写策略：
    采集端同时写 R2（保底）和 KV（快速读取）。
    Worker 优先读 KV，miss 时 fallback 到 R2，保证数据可用。
    KV 写失败不中断流程（R2 仍可用），仅打印警告。

KV 键约定：
    universe:{name}   → 股票清单文本（每行一个代码，与 R2 universe/{name}.csv 同内容）
"""

from __future__ import annotations

import os
import urllib.request
import json as _json


def _env(name: str) -> str | None:
    val = os.environ.get(name, "").strip()
    return val or None


def _api_base() -> str:
    account = _env("CLOUDFLARE_ACCOUNT_ID")
    if not account:
        raise RuntimeError("缺少环境变量 CLOUDFLARE_ACCOUNT_ID")
    return f"https://api.cloudflare.com/client/v4/accounts/{account}/storage/kv/namespaces"


def _namespace_id() -> str:
    ns = _env("KV_NAMESPACE_ID")
    if not ns:
        raise RuntimeError("缺少环境变量 KV_NAMESPACE_ID")
    return ns


def _token() -> str:
    tok = _env("CLOUDFLARE_API_TOKEN")
    if not tok:
        raise RuntimeError("缺少环境变量 CLOUDFLARE_API_TOKEN")
    return tok


def put(key: str, value: str) -> bool:
    """写入一个 KV 键值。value 为文本字符串。

    返回 True 表示成功，False 表示失败（调用方应继续流程，R2 仍有数据）。
    若 KV 凭据未配置，静默返回 False（不影响没有 KV 的环境）。
    """
    if not (_env("CLOUDFLARE_API_TOKEN") and _env("CLOUDFLARE_ACCOUNT_ID") and _env("KV_NAMESPACE_ID")):
        return False

    url = f"{_api_base()}/{_namespace_id()}/values/{key}"
    req = urllib.request.Request(
        url,
        data=value.encode("utf-8"),
        method="PUT",
        headers={
            "Authorization": f"Bearer {_token()}",
            "Content-Type": "text/plain; charset=utf-8",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = _json.loads(resp.read().decode("utf-8"))
            if not body.get("success"):
                print(f"  [KV] 写入失败 {key}: {body.get('errors')}", flush=True)
                return False
            return True
    except Exception as exc:  # noqa: BLE001 - KV 写失败不中断，R2 保底
        print(f"  [KV] 写入异常 {key}: {exc}", flush=True)
        return False


def get(key: str) -> str | None:
    """读取一个 KV 键值。返回文本字符串，不存在或失败返回 None。"""
    if not (_env("CLOUDFLARE_API_TOKEN") and _env("CLOUDFLARE_ACCOUNT_ID") and _env("KV_NAMESPACE_ID")):
        return None
    url = f"{_api_base()}/{_namespace_id()}/values/{key}"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {_token()}", "User-Agent": "Mozilla/5.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        return None
    except Exception:
        return None


def put_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
    """写入一个 KV 键值，字节形式（支持 JSON/任意二进制）。"""
    if not (_env("CLOUDFLARE_API_TOKEN") and _env("CLOUDFLARE_ACCOUNT_ID") and _env("KV_NAMESPACE_ID")):
        return False
    url = f"{_api_base()}/{_namespace_id()}/values/{urllib.parse.quote(key, safe='')}"
    req = urllib.request.Request(
        url,
        data=data,
        method="PUT",
        headers={
            "Authorization": f"Bearer {_token()}",
            "Content-Type": content_type,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = _json.loads(resp.read().decode("utf-8"))
            if not body.get("success"):
                print(f"  [KV] 写入失败 {key}: {body.get('errors')}", flush=True)
                return False
            return True
    except Exception as exc:  # noqa: BLE001 - KV 写失败不中断，R2 保底
        print(f"  [KV] 写入异常 {key}: {exc!r}", flush=True)
        return False


def put_universe(name: str, csv_text: str) -> bool:
    """写入股票清单到 KV。

    key:   universe:{name}    （与 R2 的 universe/{name}.csv 对应）
    value: 纯文本，每行一个股票代码（与 R2 内容一致，不含 BOM）
    """
    return put(f"universe:{name}", csv_text)
