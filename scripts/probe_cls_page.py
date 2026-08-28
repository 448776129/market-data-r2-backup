"""财联社页面 HTML 数据调查"""
import re, urllib.request

proxy = urllib.request.ProxyHandler({'https': 'http://127.0.0.1:7897', 'http': 'http://127.0.0.1:7897'})
opener = urllib.request.build_opener(proxy)
req = urllib.request.Request('https://www.cls.cn/telegraph', headers={'User-Agent': 'Mozilla/5.0'})
body = opener.open(req, timeout=15).read().decode('utf-8', errors='replace')

print(f"页面 len: {len(body)}")
# 找电报内容关键词
for kw in ['电报', '涨停', '快讯', 'telegram', 'feed']:
    idxs = [m.start() for m in re.finditer(kw, body)]
    print(f"'{kw}': {len(idxs)} 处")
    if idxs:
        s = max(0, idxs[0]-100)
        print(f"  样例: {body[s:idxs[0]+150].replace(chr(10),' ')[:250]}")

# 检查 __NEXT_DATA__ 完整内容里的数据
m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', body, re.S)
if m:
    data = m.group(1)
    print(f"\n__NEXT_DATA__ len: {len(data)}")
    # 有没有电报/新闻数据
    for kw in ['roll_data', 'telegraphList', 'newsList']:
        print(f"  '{kw}' in NEXT_DATA: {kw in data}")