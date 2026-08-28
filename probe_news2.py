"""确认财联社页面内嵌数据 + 各源数据结构"""
import re, urllib.request, json

proxy = urllib.request.ProxyHandler({'https': 'http://127.0.0.1:7897', 'http': 'http://127.0.0.1:7897'})
opener = urllib.request.build_opener(proxy)

# 财联社页面找内嵌 JSON (Next.js __NEXT_DATA__)
req = urllib.request.Request('https://www.cls.cn/telegraph', headers={'User-Agent': 'Mozilla/5.0'})
body = opener.open(req, timeout=15).read().decode('utf-8', errors='replace')

# __NEXT_DATA__ 内嵌数据
m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', body, re.S)
if m:
    try:
        data = json.loads(m.group(1))
        print(f"财联社 __NEXT_DATA__ 存在, 含 props: {list(data.get('props',{}).keys())}")
        # 找电报数据
        prop = json.dumps(data, ensure_ascii=False)
        if '"telegraph"' in prop or '"roll"' in prop:
            print("  找到电报相关数据!")
            idx = prop.find('"telegraph"')
            print(f"  ...{prop[max(0,idx-50):idx+200]}...")
        else:
            print("  无电报数据（需请求接口）")
    except Exception as e:
        print(f"  JSON 解析失败: {e}")
else:
    print("财联社无 __NEXT_DATA__")

# 确认同花顺完整字段
req2 = urllib.request.Request('https://news.10jqka.com.cn/tapp/news/push/stock/?page=1&tag=&track=website',
    headers={'User-Agent': 'Mozilla/5.0'})
d2 = json.loads(opener.open(req2, timeout=15).read().decode())
item = d2['data']['list'][0]
print(f"\n同花顺字段: {list(item.keys())}")
print(f"  样例: title={item.get('title','')[:30]}")

# 新浪 feed 字段
req3 = urllib.request.Request('https://zhibo.sina.com.cn/api/zhibo/feed?page=1&page_size=10&zhibo_id=152&tag_id=0&dire=f&dpc=1',
    headers={'User-Agent': 'Mozilla/5.0'})
d3 = json.loads(opener.open(req3, timeout=15).read().decode())
feed = d3['result']['data']['feed']['list']
if feed:
    print(f"\n新浪 feed 字段: {list(feed[0].keys())}")
    print(f"  样例: rich_text={feed[0].get('rich_text','')[:50]}")
else:
    print("\n新浪 feed 为空（需要页码/当前非7x24时段）")