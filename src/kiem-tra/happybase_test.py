"""Kiểm tra HBase qua Thrift bằng happybase."""
import happybase
conn = happybase.Connection("hbase", 9090)
if b"kt_happy" not in conn.tables():
    conn.create_table("kt_happy", {"cf": dict()})
t = conn.table("kt_happy")
for i in range(3):
    t.put(f"row{i}".encode(), {b"cf:val": str(i * 10).encode()})
rows = list(t.scan())
print("happybase rows", len(rows), rows[0])
