"""新增新闻源采集：同花顺 + 新浪 + 金十数据（并入现有新闻管道）。

三家 A 股/全球 7x24 快讯：
    news/ths.json   — 同花顺 7x24 快讯
    news/sina.json  — 新浪 7x24 财经
    news/jin10.json — 金十数据快讯

存储到 R2 + KV（与现有 yh/em 一致）。Worker 端聚合到 /news。

独立测试：
    python fetch_news_cn.py --source ths
    python fetch_news_cn.py --source sina
    python fetch_news_cn.py --source jin10
    python fetch_news_cn.py --source all
"""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

import kvstore  # noqa: E402
import r2store  # noqa: E402

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept": "application/json,text/plain,*/*",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def _request(url: str, headers: dict | None = None, timeout: int = 30) -> str:
    req = urllib.request.Request(url, headers={**HEADERS, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


# ── 同花顺 7x24 ────────────────────────────────────────────────

def fetch_ths(page: int = 1) -> dict:
    """同花顺 7x24 快讯（news.10jqka.com.cn 公开 JSON API）。"""
    url = ("https://news.10jqka.com.cn/tapp/news/push/stock/"
           f"?page={page}&tag=&track=website")
    data = json.loads(_request(url))
    if str(data.get("code")) != "200":
        raise RuntimeError(f"同花顺返回异常: {data.get('msg')}")

    items = []
    for n in data["data"]["list"]:
        items.append({
            "id": str(n.get("id")),
            "title": html.unescape(n.get("title") or ""),
            "digest": n.get("digest") or "",
            "url": n.get("url") or f"https://news.10jqka.com.cn/field/{(n.get('id') or '')}.shtml",
            "source": n.get("source") or "同花顺",
            "pub_ts": int(n["ctime"]) if n.get("ctime") else None,
            "pub_time": datetime.fromtimestamp(int(n["ctime"])).astimezone().isoformat() if n.get("ctime") else None,
            "tag": n.get("tag") or "",
        })
    return {"source": "ths", "count": len(items), "news": items, "collected_at": _now_iso()}


# ── 新浪 7x24 ──────────────────────────────────────────────────

def fetch_sina(page: int = 1, page_size: int = 40) -> dict:
    """新浪 7x24 财经（zhibo.sina.com.cn 公开 JSON API）。"""
    url = ("https://zhibo.sina.com.cn/api/zhibo/feed"
           f"?page={page}&page_size={page_size}&zhibo_id=152&tag_id=0&dire=f&dpc=1")
    data = json.loads(_request(url))
    feed = data["result"]["data"]["feed"]["list"]

    items = []
    for n in feed:
        items.append({
            "id": str(n.get("id")),
            "title": n.get("rich_text") or "",
            "digest": n.get("rich_text") or "",
            "url": n.get("docurl") or "https://zhibo.sina.com.cn/finance/152",
            "source": "新浪财经",
            "pub_ts": None,
            "pub_time": None,
            "create_time": n.get("create_time"),
            "tag": str(n.get("tab") or ""),
            "anchor": (n.get("anchor") or {}).get("name") if n.get("anchor") else None,
        })
    return {"source": "sina", "count": len(items), "news": items, "collected_at": _now_iso()}


# ── 金十数据 ───────────────────────────────────────────────────

def fetch_jin10() -> dict:
    """金十数据 7x24 快讯（www.jin10.com/flash_newest.js）。

    返回 `var newest = [...]`，每条含 id/time/type/data{title,content,source...}。
    """
    url = "https://www.jin10.com/flash_newest.js"
    body = _request(url, headers={"Referer": "https://www.jin10.com/"})
    m = re.search(r"var newest\s*=\s*(\[.*?\]);", body, re.S)
    if not m:
        raise RuntimeError("金十返回格式异常")
    items_raw = json.loads(m.group(1))

    items = []
    for n in items_raw:
        d = n.get("data") or {}
        title = (d.get("title") or "").strip()
        content = (d.get("content") or "").strip()
        # type=1 是特殊卡片（如行情），跳过；type=0 是普通快讯
        if n.get("type") not in (0, None):
            continue
        items.append({
            "id": str(n.get("id")),
            "title": title or content[:60],
            "digest": content or title,
            "content": content,
            "url": f"https://www.jin10.com/flash/{n.get('id')}",
            "source": d.get("source") or "金十数据",
            "pub_time": n.get("time"),
            "pub_ts": None,
            "exclusive_to": d.get("exclusive_to"),
        })
    return {"source": "jin10", "count": len(items), "news": items, "collected_at": _now_iso()}


# ── 写入（指纹去重，与现有管道一致）────────────────────────────

def _fingerprint(data: dict) -> str:
    body = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(body.encode("utf-8")).hexdigest()


def write_if_changed(key: str, data: dict) -> bool:
    name = key.replace("news/", "").replace(".json", "")
    state_key = f"_state/news_{name}.fp"
    new_fp = _fingerprint(data)

    old_fp_bytes = r2store.get_bytes(state_key)
    old_fp = old_fp_bytes.decode("ascii").strip() if old_fp_bytes else None
    if old_fp == new_fp:
        print(f"  [skip] {key}: 内容指纹未变 ({new_fp[:10]})")
        return False

    payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
    r2store.put_bytes(key, payload, content_type="application/json; charset=utf-8")
    r2store.put_bytes(state_key, new_fp.encode("ascii"), content_type="text/plain; charset=utf-8")

    kv_key = f"news:{name}"
    kv_ok = kvstore.put_bytes(kv_key, payload, content_type="application/json; charset=utf-8")
    print(f"  [write] {key}: {len(payload)//1024}KB, 指纹 {old_fp[:10] if old_fp else 'null'} -> {new_fp[:10]} KV={'ok' if kv_ok else 'skip'}")
    return True


# ── 主入口 ─────────────────────────────────────────────────────

def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(description="新增新闻源采集（同花顺/新浪/金十数据）")
    parser.add_argument("--source", default="all", help="ths / sina / jin10 / all")
    args = parser.parse_args()

    sources = []
    if args.source in ("all", "ths"):
        sources.append(("news/ths.json", fetch_ths))
    if args.source in ("all", "sina"):
        sources.append(("news/sina.json", fetch_sina))
    if args.source in ("all", "jin10"):
        sources.append(("news/jin10.json", fetch_jin10))
    if not sources:
        print(f"未知源: {args.source}"); return 1

    for key, fn in sources:
        try:
            print(f"[1/1] {key} ...")
            data = fn()
            write_if_changed(key, data)
            print(f"      → {data['count']} 条")
        except Exception as e:
            print(f"      ✗ {key} 失败: {e!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())