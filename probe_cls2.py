"""财联社页面分析找真实接口"""
import re, urllib.request

proxy = urllib.request.ProxyHandler({'https': 'http://127.0.0.1:7897', 'http': 'http://127.0.0.1:7897'})
opener = urllib.request.build_opener(proxy)
req = urllib.request.Request('https://www.cls.cn/telegraph', headers={'User-Agent': 'Mozilla/5.0'})
body = opener.open(req, timeout=15).read().decode('utf-8', errors='replace')
print('页面 len', len(body))

# 找 JS 文件
js = re.findall(r'<script[^>]*src="([^"]+)"', body)
print('JS 文件:', js[:5])

# 找含 api/sign 的字符串
for kw in ['nodeapi', 'updateTelegraphList', 'sign', 'app=CailianpressWeb']:
    idx = body.find(kw)
    if idx >= 0:
        print(f"'{kw}' 在页面位置 {idx}: ...{body[max(0,idx-80):idx+120]}...")
    else:
        print(f"'{kw}' 不在页面")