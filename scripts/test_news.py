"""新闻采集测试脚本（不写 R2，只测试抓取+解析）。
"""
import sys, os, json
sys.path.insert(0, os.path.dirname(__file__))

import fetch_agg_news as fn

print("=" * 60)
print("  新闻采集测试（不写 R2）")
print("=" * 60)

# 1. Yahoo HK
print("\n--- Yahoo HK latest-news ---")
try:
    yh = fn.fetch_yahoo_hk()
    print(f"  source: {yh['source']}")
    print(f"  count: {yh['count']}")
    if yh['count'] > 0:
        print(f"  前3条:")
        for i, n in enumerate(yh['news'][:3]):
            print(f"    {i+1}. {n['title'][:50]}")
            print(f"       rel_time={n.get('rel_time')} pub_ts={n.get('pub_ts')}")
            print(f"       publisher={n.get('publisher')} url={n.get('url','')[:60]}...")
        print(f"  ✅ Yahoo HK 采集成功")
    else:
        print(f"  ⚠️ Yahoo HK 返回 0 条（可能页面结构变化）")
except Exception as e:
    print(f"  ❌ Yahoo HK 失败: {e!r}")
    import traceback
    traceback.print_exc()

# 2. 东方财富
print("\n--- 东方财富 7x24h ---")
try:
    em = fn.fetch_eastmoney()
    print(f"  source: {em['source']}")
    print(f"  count: {em['count']}")
    print(f"  total: {em.get('total')}")
    if em['count'] > 0:
        print(f"  前3条:")
        for i, n in enumerate(em['news'][:3]):
            print(f"    {i+1}. {n['title'][:50]}")
            print(f"       showtime={n.get('showtime')} pub_ts={n.get('pub_ts')}")
            print(f"       digest={n.get('digest','')[:40]}...")
        print(f"  ✅ 东方财富采集成功")
    else:
        print(f"  ⚠️ 东方财富返回 0 条")
except Exception as e:
    print(f"  ❌ 东方财富失败: {e!r}")
    import traceback
    traceback.print_exc()

print(f"\n{'='*60}")
