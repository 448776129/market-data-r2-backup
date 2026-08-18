"""测试 kvstore 修复后写入"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
import kvstore

# 写入带冒号的 key
ok = kvstore.put("screener:test:fix", '{"fixed": true}')
print("put:", ok)
d = kvstore.get("screener:test:fix")
print("get:", d[:50] if d else "None")