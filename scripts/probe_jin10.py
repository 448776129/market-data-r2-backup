"""探测金十数据（jin10.com）接口"""
import re, json, urllib.request

proxy = urllib.request.ProxyHandler({'https': 'http://127.0.0.1:7897', 'http': 'http://127.0.0.1:7897'})
opener = urllib.request.build_opener(proxy)

def probe(url, headers=None):
    h = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0',
         'Accept': 'application/json,text/plain,*/*'}
    if headers: h.update(headers)
    req = urllib.request.Request(url, headers=h)
    try:
        resp = opener.open(req, timeout=15)
        body = resp.read().decode('utf-8', errors='replace')
        print(f"  HTTP {resp.status} | len={len(body)}")
        return body
    except Exception as e:
        print(f"  ERR {e}")
        return None

print("=== 金十数据 7x24 快讯 ===")
# 金十的公开接口
body = probe("https://www.jin10.com/", {'Referer': 'https://www.jin10.com/'})
if body:
    import re
    # 找 API 路径
    apis = re.findall(r'["\'](/[a-zA-Z0-9_/-]+flash[a-zA-Z0-9_/-]*)["\']', body)
    print(f"  页面接口线索: {set(apis) if apis else '无'}")
    print(f"  len={len(body)}")

# 金十常见 API: flash 快讯
print("\n--- flash API 尝试 ---")
for url in [
    "https://www.jin10.com/flash_newest.js",
    "https://flash-api.jin10.com/get_flash_list?max_time=0&channel=-8200",
    "https://fs.jin10.com/get_flash_list?max_time=0&channel=-8200",
    "https://www.jin10.com/api/flash/list?max_time=0",
]:
    body = probe(url, {'Referer': 'https://www.jin10.com/'})
    if body and len(body) < 300:
        print(f"    {body[:200]}")