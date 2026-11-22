"""采集聚合新闻：雅虎香港 latest-news + 东方财富 7x24h 快讯。

只生成 2 份聚合 JSON：
  news/yh.json  — Yahoo hk.finance.yahoo.com/topic/latest-news 香港/亚洲头条
  news/em.json  — 东方财富 newsapi.eastmoney.com 7x24h 快讯（LivesList）

每 10 分钟 / 5 分钟各跑一次，比较指纹不变就跳过写 R2。
"""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

import r2store
import kvstore  # 采集侧双写到 KV：读更省 Worker 配额，KV 读免费 + 低延迟

PROXY = "https://img2.365200.xyz"


def _news_url(raw_url: str) -> str:
    """直连优先，YAHOO_USE_PROXY=1 时经反代（国内环境）。"""
    if os.environ.get("YAHOO_USE_PROXY") == "1":
        return f"{PROXY}/{raw_url}"
    return raw_url
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


# ============================================================
# Yahoo HK latest-news
# ============================================================
def fetch_yahoo_hk() -> dict:
    url = _news_url("https://hk.finance.yahoo.com/topic/latest-news/")
    req = urllib.request.Request(url, headers={**HEADERS, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"})
    with urllib.request.urlopen(req, timeout=45) as resp:
        body = resp.read().decode("utf-8", errors="replace")

    items: list[dict] = []
    seen_urls: set[str] = set()
    today = datetime.now().astimezone()  # 本地今天的日期，用于拼接 URL 中的 HHMMSS

    # 用卡片正则 <a ... href=...html...> 一次性抓标题+来源+相对时间
    # 结构： <a href=URL...> <h3>TITLE</h3> </a> </div> <div class="byline ..."> <span...publisher>SOURCE</span> ... <span class="published-date">18分前</span>
    cards = re.findall(
        r'<a [^>]*href="(https?://hk\.finance\.yahoo\.com/news/[^"]+\.html[^"]*)"[^>]*>\s*'
        r'(?:<[^>]*>)*(.+?)(?:<[^>]*>)*\s*</a>\s*</div>\s*'
        r'<div class="byline[^"]*">(.*?)</div>',
        body,
        re.S,
    )
    if not cards:
        # fallback: 老版本正则
        cards = re.findall(
            r'<a [^>]*href="(https?://hk\.finance\.yahoo\.com/news/[^"]+\.html[^"]*)"[^>]*>(.*?)</a>',
            body,
            re.S,
        )

    for rec in cards:
        if len(rec) == 3:
            url_abs, a_content, byline = rec
        else:
            url_abs, a_content = rec
            byline = ""
        if url_abs in seen_urls:
            continue
        seen_urls.add(url_abs)

        # 标题
        title_raw = re.sub(r"<[^>]+>", "", a_content).strip()
        if not title_raw or len(title_raw) < 4:
            continue
        title = html.unescape(title_raw)

        # 来源
        pub = ""
        m = re.search(r'<span class="publisher">(.*?)</span>', byline, re.S)
        if m:
            pub = re.sub(r"<[^>]+>", "", m.group(1)).strip()

        # 相对时间
        rel_time = None
        m = re.search(r'<span class="published-date">(.*?)</span>', byline, re.S)
        if m:
            rel_time = re.sub(r"<[^>]+>", "", m.group(1)).strip()

        pub_ts = _extract_time_near_url(body, url_abs, rel_time, today)

        items.append({
            "title": title,
            "url": url_abs,
            "pub_ts": pub_ts,
            "pub_time": datetime.fromtimestamp(pub_ts).astimezone().isoformat() if pub_ts else None,
            "rel_time": rel_time,
            "publisher": pub,
            "source": "Yahoo Finance HK",
        })

    # 按发布时间倒序（无时间的按原序，放尾部）
    items.sort(key=lambda x: x["pub_ts"] or 0, reverse=True)
    return {
        "source": "yahoo_hk",
        "homepage": "https://hk.finance.yahoo.com/topic/latest-news/",
        "collected_at": _now_iso(),
        "count": len(items),
        "news": items,
    }


def _parse_relative_time(rel: str | None, now: datetime) -> int | None:
    """解析 Yahoo 的相对时间文本（繁体中文）。

    例: 剛剛 / 前一分鐘 / 18分前 / 2小時前 / 1天前 / 昨日 / 1個月前
    """
    if not rel:
        return None
    r = rel.strip()
    now_ts = int(now.timestamp())
    if r in ("剛剛", "刚刚", "即時", "即时", "現在", "现在"):
        return now_ts
    if r in ("前一分鐘", "前一分钟"):
        return now_ts - 60
    if r in ("昨日", "昨天", "一日前"):
        return now_ts - 86400

    # 分鐘前
    m = re.match(r'(?:約\s*)?([\d一二三四五六七八九十兩]+)\s*(?:分鐘|分钟|分)\s*前?\s*$', r)
    if m:
        n = _cn_to_int(m.group(1))
        if n:
            return now_ts - n * 60
    # 小時前
    m = re.match(r'(?:約\s*)?([\d一二三四五六七八九十兩]+)\s*(?:小時|小时|時|时)\s*前?\s*$', r)
    if m:
        n = _cn_to_int(m.group(1))
        if n:
            return now_ts - n * 3600
    # 天前
    m = re.match(r'(?:約\s*)?([\d一二三四五六七八九十兩]+)\s*天\s*前?\s*$', r)
    if m:
        n = _cn_to_int(m.group(1))
        if n:
            return now_ts - n * 86400
    # 個月前
    m = re.match(r'(?:約\s*)?([\d一二三四五六七八九十兩]+)\s*(?:個月|个月|月)\s*前?\s*$', r)
    if m:
        n = _cn_to_int(m.group(1))
        if n:
            return now_ts - n * 30 * 86400
    return None


_CN_DIGIT = {"零": 0, "〇": 0, "一": 1, "二": 2, "兩": 2, "两": 2, "三": 3,
             "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}

def _cn_to_int(s: str) -> int | None:
    if s.isdigit():
        return int(s)
    if s in _CN_DIGIT and s != "十":
        return _CN_DIGIT[s]
    # 十幾 / 幾十 / 幾十幾
    if s == "十":
        return 10
    if s.startswith("十") and len(s) == 2 and s[1] in _CN_DIGIT:
        return 10 + _CN_DIGIT[s[1]]
    if s.endswith("十") and len(s) == 2 and s[0] in _CN_DIGIT:
        return _CN_DIGIT[s[0]] * 10
    if len(s) == 3 and s[1] == "十" and s[0] in _CN_DIGIT and s[2] in _CN_DIGIT:
        return _CN_DIGIT[s[0]] * 10 + _CN_DIGIT[s[2]]
    return None


def _extract_time_near_url(body: str, url: str, rel_time: str | None, today: datetime) -> int | None:
    """时间提取优先级：
    1) 相对时间（"18分前"） → 最可靠
    2) URL 尾部 -HHMMSSsssss.html → 今天日期 + HH:MM:SS（跨午夜时会扣掉一天）
    3) snippet 中的 data-timestamp / providerPublishTime / <time datetime>
    """
    now = datetime.now().astimezone()
    ts = _parse_relative_time(rel_time, now)
    if ts:
        return ts

    # URL 末尾提取时间戳：
    #   /news/...-052829386.html  → HHMMSS + 序号 → 取 05:28:29
    #   /news/...-043555007.html  → 04:35:55
    try:
        fname = url.split("/")[-1].split("?")[0]
        if fname.endswith(".html"):
            fname = fname[:-5]
        # 找最后一个 "-" 后的数字
        i = fname.rfind("-")
        if i >= 0:
            digits = fname[i + 1:]
            if digits.isdigit() and len(digits) >= 6:
                hh = int(digits[0:2])
                mm = int(digits[2:4])
                ss = int(digits[4:6])
                if 0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59:
                    t = today.replace(hour=hh, minute=mm, second=ss, microsecond=0)
                    # 如果算出的时间比"现在"晚了超过 1 小时 → 可能是昨天
                    if (t - now).total_seconds() > 3600:
                        t = t.fromtimestamp(t.timestamp() - 86400) if False else t
                        # 用时区替换方式回拨
                        t = datetime.fromtimestamp(t.timestamp() - 86400, tz=today.tzinfo)
                    return int(t.timestamp())
    except Exception:
        pass

    # snippet 兜底
    idx = body.find(url)
    if idx < 0:
        return None
    start = max(0, idx - 800)
    end = min(len(body), idx + 1200)
    snippet = body[start:end]
    m = re.search(r'data-(?:timestamp|publish-time)="(\d{9,13})"', snippet)
    if m:
        ts = int(m.group(1))
        if ts > 10**12:
            ts //= 1000
        return ts
    m = re.search(r'"providerPublishTime"\s*:\s*(\d{9,12})', snippet)
    if m:
        return int(m.group(1))
    m = re.search(r'<time[^>]*datetime="([^"]+)"', snippet)
    if m:
        try:
            dt = datetime.fromisoformat(m.group(1).replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            pass
    return None


# ============================================================
# 东方财富 7x24h 快讯
# ============================================================
def fetch_eastmoney(page_size: int = 80) -> dict:
    url = _news_url(
        f"https://newsapi.eastmoney.com/kuaixun/v1/getlist_102_ajaxResult_{page_size}_1_.html"
    )
    req = urllib.request.Request(url, headers={
        **HEADERS,
        "Accept": "*/*",
        "Referer": "https://kuaixun.eastmoney.com/",
        "X-Requested-With": "XMLHttpRequest",
    })
    with urllib.request.urlopen(req, timeout=45) as resp:
        body = resp.read().decode("utf-8", errors="replace")

    # 解析：var ajaxResult={...} ;
    m = re.search(r"ajaxResult\s*=\s*(\{.*\})\s*;?\s*$", body.strip(), re.S)
    if not m:
        raise ValueError(f"东方财富返回格式异常: {body[:200]}")
    raw = json.loads(m.group(1))
    lives = raw.get("LivesList") or []

    items: list[dict] = []
    for n in lives:
        sort = str(n.get("sort") or "")
        pub_ts = None
        if sort.isdigit() and len(sort) >= 10:
            pub_ts = int(sort[:10])

        title = (n.get("title") or "").strip()
        simtitle = (n.get("simtitle") or "").strip()
        digest = (n.get("digest") or "").strip()
        simdigest = (n.get("simdigest") or "").strip()
        showtime = (n.get("showtime") or "").strip()
        editor = (n.get("editor_name") or "").strip()
        columns = (n.get("column") or "").strip()

        items.append({
            "id": n.get("id"),
            "title": title or simtitle,
            "digest": digest or simdigest,
            "showtime": showtime,
            "editor": editor,
            "columns": [c for c in columns.split(",") if c],
            "comment_num": n.get("commentnum"),
            "news_type": n.get("newstype"),
            "url_pc": n.get("url_w"),
            "url_mobile": n.get("url_m"),
            "image": n.get("image") or None,
            "pub_ts": pub_ts,
            "pub_time": datetime.fromtimestamp(pub_ts).astimezone().isoformat() if pub_ts else None,
            "source": "东方财富 7x24h",
        })

    return {
        "source": "eastmoney",
        "homepage": "https://kuaixun.eastmoney.com/",
        "collected_at": _now_iso(),
        "total": raw.get("AllCount"),
        "count": len(items),
        "news": items,
    }


# ============================================================
# 写入 R2（内容指纹门控）
# ============================================================
def _fingerprint(data: dict) -> str:
    body = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(body.encode("utf-8")).hexdigest()


# Cloudflare 账号未开通 R2 时，R2 会返回 NotEntitled。
# 这种情况下自动降级为 KV 单写，避免定时 workflow 每半小时失败一次。
_r2_not_entitled = False


def _r2_disabled(exc: Exception) -> bool:
    return isinstance(exc, RuntimeError) and "NotEntitled" in str(exc)


def write_if_changed(key: str, data: dict) -> bool:
    """R2 写入口：指纹相同跳过 → 否则双写 R2 + KV。

    R2 未开通（NotEntitled）时自动降级：指纹与数据都写 KV，不影响采集主流程。
    """
    global _r2_not_entitled

    name = key.replace("news/", "").replace(".json", "")
    state_key = f"_state/news_{name}.fp"
    kv_key = f"news:{name}"          # news:yh / news:em
    kv_fp_key = f"news:{name}.fp"    # R2 不可用时的指纹存放处
    new_fp = _fingerprint(data)

    # 读旧指纹：优先 R2，R2 未开通时回退 KV
    r2_ok = True
    try:
        old_fp_bytes = r2store.get_bytes(state_key)
        old_fp = old_fp_bytes.decode("ascii").strip() if old_fp_bytes else None
    except RuntimeError as exc:
        if not _r2_disabled(exc):
            raise
        r2_ok = False
        _r2_not_entitled = True
        print(f"  [warn] R2 未开通，降级为 KV 存储: {exc}", flush=True)
        old_fp = kvstore.get(kv_fp_key)

    if old_fp == new_fp:
        print(f"  [skip] {key}: 内容指纹未变 ({new_fp[:10]})")
        return False

    payload = json.dumps(data, ensure_ascii=False).encode("utf-8")

    if r2_ok:
        r2store.put_bytes(key, payload, content_type="application/json; charset=utf-8")
        r2store.put_bytes(state_key, new_fp.encode("ascii"), content_type="text/plain; charset=utf-8")

    # KV 始终写：Worker 读优先走 KV，省 R2 配额。
    kv_ok = kvstore.put_bytes(kv_key, payload, content_type="application/json; charset=utf-8")
    if not r2_ok:
        kv_ok = kvstore.put(kv_fp_key, new_fp) and kv_ok
    kv_str = f" KV→{'ok' if kv_ok else 'skip'}"

    print(f"  [write] {key}: {len(payload)//1024} KB, 指纹 {old_fp[:10] if old_fp else 'null'} -> {new_fp[:10]}{kv_str}")
    return True


def main() -> int:
    import traceback

    ok = 0
    # Yahoo HK
    try:
        print("[1/2] Yahoo HK latest-news ...")
        yh = fetch_yahoo_hk()
        write_if_changed("news/yh.json", yh)
        ok += 1
        print(f"      → {yh['count']} 条")
    except Exception as e:
        print(f"      ✗ Yahoo 失败: {e!r}")
        traceback.print_exc()

    # 东方财富
    try:
        print("[2/2] 东方财富 7x24h ...")
        em = fetch_eastmoney()
        write_if_changed("news/em.json", em)
        ok += 1
        print(f"      → {em['count']} 条 (全部 {em.get('total')})")
    except Exception as e:
        print(f"      ✗ 东方财富 失败: {e!r}")
        traceback.print_exc()

    # 更新状态：R2 未开通时只打警告，不让整个 workflow 失败。
    last_error = "R2 未开通(NotEntitled)，数据仅写入 KV" if _r2_not_entitled else None
    try:
        r2store.put_status({
            "last_run": _now_iso(),
            "modules_ok": f"{ok}/2",
            "last_error": last_error,
        })
    except RuntimeError as exc:
        if not _r2_disabled(exc):
            raise
        print(f"  [warn] 状态写入跳过: {exc}", flush=True)

    return 0 if ok == 2 else 2


if __name__ == "__main__":
    raise SystemExit(main())
