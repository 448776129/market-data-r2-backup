"""检查 KV 选股快照完整性"""
import os, sys, json
sys.path.insert(0, os.path.dirname(__file__))
import kvstore

checks = {
    "screener:1d:us": "美股日K",
    "screener:1d:cn": "A股日K",
    "screener:1d:hk": "港股日K",
    "screener:1d:kr": "韩股日K",
    "screener:1d:etf": "美股ETF日K",
    "screener:1d:cn_etf": "中国ETF日K",
    "screener:1m:csi300": "沪深300分钟K",
    "screener:1m:nasdaq100": "纳指100分钟K",
}

print("KV 选股快照状态:")
print("=" * 50)
total = 0
all_ok = True
for k, label in checks.items():
    d = kvstore.get(k)
    if d:
        data = json.loads(d)
        count = len(data)
        size = len(d) // 1024
        print(f"  ✅ {label:15s} | {k:25s} | {count:5d} 只 | {size:5d}KB")
        total += count
    else:
        print(f"  ❌ {label:15s} | {k:25s} | 无数据")
        all_ok = False

print("=" * 50)
print(f"  总计: {total} 只股票指标快照")
if all_ok:
    print("  ✅ 全部采集完成")
else:
    print("  ❌ 有缺失项")