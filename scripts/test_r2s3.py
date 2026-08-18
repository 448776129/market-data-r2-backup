"""测试 r2s3 写入"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import r2s3

key = "_test_write.json"
r2s3.put_obj(key, b'{"ok":1}', content_type="application/json")
d = r2s3.get_obj(key)
print("write+read OK:", d.decode() if d else "None")
# 清理
r2s3.put_obj(key, b"deleted")  # 覆盖占位，实际删除走 DELETE；这里只验证读
print("head:", bool(r2s3.head_obj(key)))