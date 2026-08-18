"""极简 Cloudflare R2 S3 客户端（纯标准库，替代 boto3）。

用 urllib.request + AWS SigV4 签名直接调 R2 的 S3 兼容 API，
避免 GitHub Actions 安装 boto3/botocore（省 30-60 秒依赖安装）。

凭据（与 boto3 版本一致）：
    R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET

核心函数：
    get_obj(key) -> bytes | None
    put_obj(key, data, content_type="text/csv", content_encoding=None)
    head_obj(key) -> dict | None
"""

from __future__ import annotations

import hashlib
import hmac
import os
import urllib.request
from datetime import datetime, timezone

_SERVICE = "s3"
_REGION = "auto"  # R2 用 auto

# 单次请求超时（秒）。上传大 CSV 时给足时间。
_TIMEOUT = 60


def _env(name: str) -> str | None:
    val = os.environ.get(name, "").strip()
    return val or None


def _endpoint() -> str:
    account = _env("R2_ACCOUNT_ID")
    if not account:
        raise RuntimeError("缺少环境变量 R2_ACCOUNT_ID")
    return f"https://{account}.r2.cloudflarestorage.com"


def _bucket() -> str:
    b = _env("R2_BUCKET")
    if not b:
        raise RuntimeError("缺少环境变量 R2_BUCKET")
    return b


def _sign(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, hashlib.sha256).digest()


def _sigv4(method: str, path: str, payload: bytes) -> tuple[str, dict[str, str]]:
    """构造 SigV4 签名的 Authorization 头。

    Returns:
        (authorization_header, other_headers_dict)
    """
    access_key = _env("R2_ACCESS_KEY_ID")
    secret = _env("R2_SECRET_ACCESS_KEY")
    if not access_key or not secret:
        raise RuntimeError("缺少环境变量 R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY")

    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    # payload hash
    payload_hash = hashlib.sha256(payload).hexdigest()

    # headers 必须按字典序
    host = _endpoint().split("//")[1]
    headers_to_sign = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    # 拼接待签头
    signed_header_list = sorted(headers_to_sign.keys())
    canonical_headers = "".join(f"{k}:{headers_to_sign[k]}\n" for k in signed_header_list)
    signed_headers = ";".join(signed_header_list)

    # canonical request
    canonical_query = ""  # R2 请求不使用 query
    canonical_request = "\n".join([
        method,
        path,
        canonical_query,
        canonical_headers,
        signed_headers,
        payload_hash,
    ])

    credential_scope = f"{date_stamp}/{_REGION}/{_SERVICE}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])

    # 签名密钥
    k_date = _sign(("AWS4" + secret).encode("utf-8"), date_stamp.encode("utf-8"))
    k_region = _sign(k_date, _REGION.encode("utf-8"))
    k_service = _sign(k_region, _SERVICE.encode("utf-8"))
    k_signing = _sign(k_service, b"aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    return authorization, {
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }


def _url_for(key: str) -> str:
    import urllib.parse
    return f"{_endpoint()}/{_bucket()}/{urllib.parse.quote(key, safe='')}"


def _request(method: str, key: str, payload: bytes | None = None,
             content_type: str | None = None,
             content_encoding: str | None = None) -> urllib.response.addinfourl:
    """发起签名请求。"""
    url = _url_for(key)
    path = f"/{_bucket()}/" + key.split("/")[-1]  # path 仅含 key 最简形式
    # SigV4 的 canonical path 需要完整 path
    import urllib.parse
    path = f"/{_bucket()}/" + urllib.parse.quote(key, safe="/")
    if payload is None:
        payload = b""
    auth, sig_headers = _sigv4(method, path, payload)

    headers = {
        "Authorization": auth,
        "Host": _endpoint().split("//")[1],
        **sig_headers,
    }
    if content_type:
        headers["Content-Type"] = content_type
    if content_encoding:
        headers["Content-Encoding"] = content_encoding

    req = urllib.request.Request(url, data=payload, method=method, headers=headers)
    return urllib.request.urlopen(req, timeout=_TIMEOUT)


def get_obj(key: str) -> bytes | None:
    """读取对象原始字节；不存在返回 None。"""
    try:
        resp = _request("GET", key)
        return resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    except Exception:
        raise


def put_obj(key: str, data: bytes, content_type: str = "text/csv; charset=utf-8",
            content_encoding: str | None = None) -> None:
    """写入对象。content_encoding="gzip" 时附带 Content-Encoding 头。"""
    _request("PUT", key, payload=data, content_type=content_type,
             content_encoding=content_encoding)


def head_obj(key: str) -> dict | None:
    """返回对象元数据（内容长度/类型等）；不存在返回 None。"""
    try:
        resp = _request("HEAD", key)
        return dict(resp.headers)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def exists(key: str) -> bool:
    return head_obj(key) is not None


def list_keys(prefix: str = "", max_keys: int = 1000, max_results: int | None = None) -> list[str]:
    """列出 bucket 内对象 key（分页拉取）。

    返回所有匹配 key 的列表（按字典序）。max_results 限制总数。
    """
    import json as _json
    import urllib.parse

    all_keys: list[str] = []
    token: str | None = None
    while True:
        query = f"?list-type=2&max-keys={max_keys}"
        if prefix:
            query += f"&prefix={urllib.parse.quote(prefix, safe='')}"
        if token:
            query += f"&continuation-token={urllib.parse.quote(token, safe='')}"

        url = f"{_endpoint()}/{_bucket()}{query}"
        # 对 query 请求需要单独签名（canonical query 非空）
        req = urllib.request.Request(url, method="GET", headers=_sigv4_get_headers("GET", query))
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
                body = _json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return all_keys
            raise

        for obj in body.get("Contents", []) or []:
            all_keys.append(obj["Key"])
        if max_results and len(all_keys) >= max_results:
            return all_keys[:max_results]
        if not body.get("IsTruncated"):
            break
        token = body.get("NextContinuationToken")
        if not token:
            break
    return all_keys


def _sigv4_get_headers(method: str, query: str) -> dict:
    """为带 query 的请求生成签名头（不含 body）。"""
    import hashlib
    from datetime import datetime, timezone

    access_key = _env("R2_ACCESS_KEY_ID")
    secret = _env("R2_SECRET_ACCESS_KEY")
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(b"").hexdigest()

    host = _endpoint().split("//")[1]
    headers_to_sign = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }
    signed_list = sorted(headers_to_sign.keys())
    canonical_headers = "".join(f"{k}:{headers_to_sign[k]}\n" for k in signed_list)
    signed_headers = ";".join(signed_list)

    canonical_request = "\n".join([
        method, f"/{_bucket()}", query.lstrip("?"), canonical_headers,
        signed_headers, payload_hash,
    ])
    credential_scope = f"{date_stamp}/{_REGION}/{_SERVICE}/aws4_request"
    string_to_sign = "\n".join([
        "AWS4-HMAC-SHA256", amz_date, credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])
    k_date = _sign(("AWS4" + secret).encode("utf-8"), date_stamp.encode("utf-8"))
    k_region = _sign(k_date, _REGION.encode("utf-8"))
    k_service = _sign(k_region, _SERVICE.encode("utf-8"))
    k_signing = _sign(k_service, b"aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    return {
        "Authorization": (f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
                          f"SignedHeaders={signed_headers}, Signature={signature}"),
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }