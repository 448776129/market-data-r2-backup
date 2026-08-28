"""探测三家新闻源接口"""
import json, urllib.request, urllib.parse

proxy = urllib.request.ProxyHandler({'https': 'http://127.0.0.1:7897', 'http': 'http://127.0.0.1:7897'})
opener = urllib.request.build_opener(proxy)

def probe(url, extra_headers=None):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
               'Accept': 'application/json,text/html,*/*'}
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, headers=headers)
    try:
        resp = opener.open(req, timeout=15)
        body = resp.read().decode('utf-8', errors='replace')
        ct = resp.headers.get('Content-Type', '')
        print(f"  HTTP {resp.status} | type={ct.split(';')[0]} | len={len(body)}")
        return body
    except Exception as e:
        print(f"  ERR {e}")
        return None

print("=== 1. 同花顺 7x24 快讯 ===")
# 同花顺公开 JSONP 接口
body = probe("https://news.10jqka.com.cn/tapp/news/push/stock/?page=1&tag=&track=website")
if body and 'callback' not in body[:20]:
    print(f"  前200字符: {body[:200]}")

print("\n=== 2. 新浪 7x24 ===")
# 新浪财经 API
body = probe("https://zhibo.sina.com.cn/api/zhibo/feed?page=1&page_size=20&zhibo_id=152&tag_id=0&dire=f&dpc=1")
if body:
    print(f"  前300字符: {body[:300]}")

print("\n=== 3. 财联社电报 ===")
# 财联社公开接口
body = probe("https://www.cls.cn/nodeapi/updateTelegraphList",
             {'Referer': 'https://www.cls.cn/telegraph'})
if body:
    print(f"  前300字符: {body[:300]}")