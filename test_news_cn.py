"""测试三家新闻源（不写 R2）"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_news_cn as fn

print("=== 同花顺 ===")
try:
    d = fn.fetch_ths()
    print(f"  ✅ {d['count']} 条")
    for n in d['news'][:2]:
        print(f"    [{n['source']}] {n['title'][:40]}")
except Exception as e:
    print(f"  ❌ {e!r}")

print("\n=== 新浪 ===")
try:
    d = fn.fetch_sina()
    print(f"  ✅ {d['count']} 条")
    for n in d['news'][:2]:
        print(f"    [{n['source']}] {n['title'][:40]}")
except Exception as e:
    print(f"  ❌ {e!r}")

print("\n=== 财联社 ===")
try:
    d = fn.fetch_cls()
    print(f"  ✅ {d['count']} 条")
    for n in d['news'][:2]:
        print(f"    [{n['source']}] {n['title'][:40]}")
except Exception as e:
    print(f"  ❌ {e!r}")