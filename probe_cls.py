"""财联社接口探测"""
import json, urllib.request, urllib.parse, time, hashlib

proxy = urllib.request.ProxyHandler({'https': 'http://127.0.0.1:7897', 'http': 'http://127.0.0.1:7897'})
opener = urllib.request.build_opener(proxy)

def probe(url, headers=None):
    h = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0',
         'Accept': 'application/json,text/plain,*/*'}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    try:
        resp = opener.open(req, timeout=15)
        body = resp.read().decode('utf-8', errors='replace')
        print(f"  HTTP {resp.status} | len={len(body)}")
        return body
    except Exception as e:
        print(f"  ERR {e}")
        return None

print("=== 财联社公开接口探测 ===")
# 尝试几个已知的财联社公开接口
endpoints = [
    ("/nodeapi/roll_list", {}),
    ("/nodeapi/telegraphList", {}),
    ("/api/telegraph/list", {}),
    ("/v1/roll/get_roll_list", {}),
    ("/telegraph", {}),  # 直接拿页面找接口
]
for path, extra in endpoints:
    print(f"--- {path} ---")
    body = probe(f"https://www.cls.cn{path}", {'Referer': 'https://www.cls.cn/telegraph'})
    if body and len(body) < 500 and 'DOCTYPE' not in body[:100]:
        print(f"  {body[:200]}")

# 财联社有 sign 签名，先看页面 HTML 找接口
print("\n--- 财联社页面 HTML 找接口 ---")
body = probe("https://www.cls.cn/telegraph", {'Referer': 'https://www.cls.cn/'})
if body:
    import re
    apis = re.findall(r'["\'](/[a-zA-Z0-9_/]+telegraph[a-zA-Z0-9_/]*)["\']', body)
    print(f"  页面中找到的接口: {set(apis) if apis else '无'}")