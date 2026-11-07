# Buổi 4 — Lưu trữ NoSQL: HBase và MongoDB (GitHub Codespaces)

Thời lượng: 2 tiết (khoảng 90–100 phút trên lớp; đọc phần lý thuyết nên làm trước ở nhà; các Bước 17 và 19 có thể làm ở nhà) · Tuần 9 · Chuẩn đầu ra: CLO3 — "Vận dụng được các hệ quản trị dữ liệu NoSQL để lưu trữ và truy vấn dữ liệu lớn với mô hình dữ liệu phi quan hệ". Môi trường: GitHub Codespaces, máy mặc định **2 nhân / 8 GB RAM / 32 GB đĩa là đủ** (đã chạy trọn 21 bước, không container nào bị OOM; máy 4 nhân không bắt buộc). Dịch vụ cần bật: cụm lõi (tự bật khi mở codespace) và `lab nosql` (HBase 2.6.7 lưu dữ liệu trên HDFS, MongoDB 8.0); cuối buổi `lab stop-extras`. Dữ liệu: `sensor_readings.csv` (500.000 bản ghi cảm biến của 2.000 thiết bị) cho phần HBase và `retail_transactions.csv` (5.000.000 giao dịch) cho phần MongoDB — cần bộ dữ liệu đầy đủ `lab data 5000000`. Quota ước tính: khoảng 4 core-hour cho buổi (máy 2 nhân × khoảng 2 giờ mở codespace); bài dùng thêm khoảng 4 GB đĩa (image MongoDB, dữ liệu HBase/MongoDB, tệp JSON trung gian). Mọi kết quả mong đợi được lấy từ một lần chạy thật trên bản sao Codespaces (cùng `.devcontainer` của repo, giới hạn 2 nhân / 8 GB); thời gian chạy chỉ mang tính tham khảo.

## Mục tiêu

Sau khi hoàn thành bài thực hành, sinh viên có khả năng:

- Thao tác các lệnh cơ bản của HBase shell: `create`, `put`, `get`, `scan`, `delete`, `deleteall`, `alter`, `disable`, `drop` trên bảng có column family, và giải thích cơ chế phiên bản (VERSIONS) của ô dữ liệu.
- Thiết kế row key cho dữ liệu chuỗi thời gian của cảm biến IoT bằng kỹ thuật tiền tố băm (salting) để tránh hiện tượng nóng vùng (region hotspotting), đồng thời chỉ ra được cái giá phải trả của salting khi truy vấn.
- Tạo bảng HBase có pre-splitting khớp với sơ đồ row key, nạp dữ liệu bằng tệp lệnh shell và bằng thư viện Python `happybase` (Thrift), rồi kiểm chứng phân bố dữ liệu qua các region bằng `list_regions` và Web UI.
- Đọc dữ liệu HBase từ Python và từ Spark 4 (đọc song song theo từng bucket salt) để phân tích tiếp.
- Thực hiện CRUD trên MongoDB bằng `mongosh`, nhập dữ liệu JSON bằng `mongoimport`, tạo chỉ mục và đọc kết quả `explain()` để phân biệt `COLLSCAN` với `IXSCAN`.
- Viết aggregation pipeline (`$project`, `$group`, `$sort`, `$match`, `$lookup`) để tính doanh thu theo khu vực, theo tháng, và truy cập MongoDB từ Python (`pymongo`) và từ Spark (MongoDB Spark Connector).
- So sánh mô hình dữ liệu wide-column (HBase) và document (MongoDB) để lựa chọn công nghệ phù hợp với từng bài toán.

## Chuẩn bị

### Mở codespace và kiểm tra cụm lõi

1. Vào https://github.com/codespaces, mở codespace của repo thực hành (repo tạo từ template của giảng viên ở Buổi 0). Không cần tạo codespace mới cho mỗi buổi: mở lại codespace cũ để giữ image, dữ liệu HDFS và bài làm.
2. Chờ cụm lõi tự khởi động: ở lần mở lại, terminal "postStartCommand" in dòng `[khoi-dong] Cụm đã sẵn sàng...` sau khoảng 1–2 phút.
3. Nếu giảng viên có cập nhật repo, lấy bản mới rồi kiểm tra trạng thái cụm. Mọi lệnh "Terminal codespace" chạy ở gốc repo (terminal mặc định đã đứng ở đây).

<!-- shell: Terminal codespace -->
```bash
git pull
lab status
```

**Kết quả mong đợi:**

```text
NAME              STATUS
datanode1         Up About a minute (healthy)
datanode2         Up About a minute (healthy)
historyserver     Up About a minute (healthy)
namenode          Up About a minute (healthy)
nodemanager1      Up 36 seconds (healthy)
nodemanager2      Up 36 seconds (healthy)
resourcemanager   Up About a minute (healthy)
spark-master      Up About a minute (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 2.2 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

> 💡 Nếu `git pull` báo xung đột với bài làm của bạn, commit bài làm trước (`git add src && git commit -m "..."`) rồi `git pull` lại.

### Dữ liệu đầy đủ 5 triệu dòng

Khi tạo codespace, dữ liệu mẫu chỉ có 200.000 giao dịch. Bài này cần bộ đầy đủ; kiểm tra số dòng và sinh lại nếu chưa đủ (chỉ cần làm **một lần** cho cả học kỳ, khoảng 30 giây):

<!-- shell: Terminal codespace -->
```bash
wc -l data/retail_transactions.csv data/sensor_readings.csv
```

Nếu dòng đầu nhỏ hơn `5000001` thì chạy:

<!-- shell: Terminal codespace -->
```bash
lab data 5000000
wc -l data/retail_transactions.csv data/sensor_readings.csv
```

**Kết quả mong đợi (lần chạy kiểm thử mất 28 giây):**

```text
...
  giao dich: 4,500,000
  products.csv: 0.0 MB
  retail_transactions.csv: 405.2 MB
  web_access.log: 121.4 MB
  sensor_readings.csv: 22.7 MB
  5000001 data/retail_transactions.csv
   500001 data/sensor_readings.csv
  5500002 total
```

> 💡 Thư mục `data/` nằm trong `.gitignore` nên dữ liệu (khoảng 550 MB, cộng 1,25 GB tệp JSON ở Bước 14) không bị đưa lên GitHub nhưng vẫn chiếm đĩa của codespace.

### Bật HBase và MongoDB

<!-- shell: Terminal codespace -->
```bash
lab nosql
lab status
```

**Kết quả mong đợi:** 10 container `healthy` (lần kiểm thử: 22 giây; lần đầu tiên trong codespace còn phải tải image `mongo:8.0` khoảng 1,3 GB nên lâu hơn):

```text
 Container mongodb  Healthy
 Container hbase  Healthy
NAME              STATUS
datanode1         Up 2 minutes (healthy)
datanode2         Up 2 minutes (healthy)
hbase             Up 20 seconds (healthy)
historyserver     Up 2 minutes (healthy)
mongodb           Up 21 seconds (healthy)
namenode          Up 2 minutes (healthy)
nodemanager1      Up About a minute (healthy)
nodemanager2      Up About a minute (healthy)
resourcemanager   Up 2 minutes (healthy)
spark-master      Up 2 minutes (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 2.8 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

> 💡 Trên máy 2 nhân / 8 GB, cụm lõi + HBase + MongoDB rảnh dùng khoảng 2,8 GB; trong lúc chạy job Spark (Bước 12, 19) và `mongoimport` (Bước 14), tổng RAM các container lên tới khoảng 6–7 GB nhưng không có container nào bị OOM. **Không** bật thêm `lab hive`/`lab kafka` trong buổi này.

> ⚠️ Nếu `mongodb` khởi động lại liên tục và log có dòng `Linux kernel versions 6.19 and newer has a known incompatibility`, xem mục Lỗi thường gặp. Compose đã đặt sẵn biến `GLIBC_TUNABLES=glibc.pthread.rseq=1` để khắc phục.

### Giao diện Web dùng trong bài

Trong Codespaces, Web UI không mở bằng `localhost` trên trình duyệt mà qua đường dẫn chuyển tiếp cổng. Lấy đường dẫn bằng `lab ui` (hoặc tab PORTS bên cạnh TERMINAL → biểu tượng quả địa cầu ở cổng tương ứng):

<!-- shell: Terminal codespace -->
```bash
lab ui
```

**Kết quả mong đợi (ví dụ; phần `ten-codespace-abc123` là tên codespace của bạn):**

```text
HDFS NameNode            https://ten-codespace-abc123-9870.app.github.dev
YARN ResourceManager     https://ten-codespace-abc123-8088.app.github.dev
Spark Master             https://ten-codespace-abc123-8080.app.github.dev
Spark UI (job đang chạy) https://ten-codespace-abc123-4040.app.github.dev
Spark History            https://ten-codespace-abc123-18080.app.github.dev
MapReduce JobHistory     https://ten-codespace-abc123-19888.app.github.dev
HiveServer2 UI           https://ten-codespace-abc123-10002.app.github.dev
HBase Master             https://ten-codespace-abc123-16010.app.github.dev
HBase RegionServer       https://ten-codespace-abc123-16030.app.github.dev
NodeManager 1            https://ten-codespace-abc123-8042.app.github.dev
NodeManager 2            https://ten-codespace-abc123-8043.app.github.dev
DataNode 1               https://ten-codespace-abc123-9864.app.github.dev
DataNode 2               https://ten-codespace-abc123-9865.app.github.dev
(Hoặc mở tab PORTS bên cạnh TERMINAL, bấm biểu tượng quả địa cầu.)
```

| Giao diện | Cổng (tab PORTS) | Dùng ở bước |
|---|---|---|
| HBase Master UI | 16010 | Bước 2, 9 |
| HBase RegionServer UI | 16030 (dòng *HBase RegionServer* trong `lab ui`) | tham khảo |
| HDFS NameNode (xem thư mục `/hbase`) | 9870 | Bước 2 |
| Spark Master | 8080 | Bước 12, 19 |
| Spark UI của job đang chạy trong `spark-master` | 4040 | Bước 12, 19 |
| Spark History Server | 18080 | Bước 12, 19 |

> ⚠️ Các liên kết bên trong Web UI trỏ tới tên container (ví dụ `http://hbase:16030`, `http://nodemanager1:8042`) không mở được từ trình duyệt. Mở cổng tương ứng từ tab PORTS thay vì bấm các liên kết đó.

MongoDB không có Web UI; mọi thao tác MongoDB trong bài dùng `mongosh` trong container `mongodb` hoặc `pymongo` trong container `spark-master`. Bên trong codespace, `localhost:27017` (MongoDB) và `localhost:9090` (Thrift của HBase) vẫn mở, nhưng terminal codespace không có sẵn `python3`/`happybase`, vì vậy mọi script Python của bài chạy trong container `spark-master`.

### Mã nguồn của bài

Thư mục `./src` của repo được mount thành `/src` trong `namenode`, `spark-master`, `nodemanager1/2` và `hbase`; thư mục `./data` được mount thành `/data` trong các container đó và thành `/import` trong `mongodb` (container `mongodb` không mount `./src`). Vì vậy **không cần `docker cp`**: tệp tạo trong VS Code ở `src/lab4` thấy ngay ở `/src/lab4` trong container.

<!-- shell: Terminal codespace -->
```bash
mkdir -p src/lab4
```

Các tệp mã nguồn được cho ở từng bước dưới dạng khối tệp (dòng ghi chú `file:` cho biết đường dẫn): tạo tệp trong VS Code (chuột phải thư mục `src/lab4` → New File) rồi dán nội dung, hoặc kéo-thả các tệp trong gói `buoi4_src.zip` do giảng viên cung cấp vào thư mục `src/lab4` ở khung Explorer.

## PHẦN 1: KHỞI ĐỘNG VÀ KIỂM TRA DỊCH VỤ

### Bước 1: Kiểm tra phiên bản HBase và kết nối MongoDB

Mục đích: xác nhận hai dịch vụ NoSQL đã sẵn sàng trước khi thao tác dữ liệu, tránh mất thời gian gỡ lỗi giữa bài.

<!-- shell: Terminal codespace -->
```bash
docker exec -it hbase hbase version
docker exec -it mongodb mongosh --quiet --eval "db.runCommand({ping:1})"
docker exec -it mongodb mongosh --quiet --eval "db.version()"
```

**Kết quả mong đợi:**

```text
HBase 2.6.7-hadoop3
Source code repository git://buildbox.localdomain/home/apurtell/tmp/RM-2.6/hbase revision=a1fad67ad9df4e81335b0524efbc7743fd6be811
...
{ ok: 1 }
8.0.32
```

### Bước 2: Xem trạng thái HBase và nơi HBase lưu dữ liệu

HBase trong môi trường này chạy chế độ standalone (HMaster, RegionServer và ZooKeeper nhúng trong cùng container `hbase`) nhưng **lưu dữ liệu trên HDFS** tại thư mục `/hbase`. Mở HBase shell:

<!-- shell: Terminal codespace -->
```bash
docker exec -it hbase hbase shell
```

Trong shell, gõ:

<!-- shell: HBase shell -->
```bash
status 'simple'
```

**Kết quả mong đợi:**

```text
0 backup masters
1 live servers
    hbase:16020 1793895731163
        requestsPerSecond=0.0, numberOfOnlineRegions=2, usedHeapMB=71, maxHeapMB=768, ...
0 dead servers
Aggregate load: 0, regions: 2
```

Gõ `exit` để thoát shell, rồi xem thư mục dữ liệu của HBase trên HDFS:

<!-- shell: Terminal codespace -->
```bash
docker exec -it namenode hdfs dfs -ls /hbase
```

**Kết quả mong đợi (rút gọn):**

```text
Found 12 items
...
drwxr-xr-x   - root supergroup          0 2026-11-05 23:22 /hbase/MasterData
drwxr-xr-x   - root supergroup          0 2026-11-05 23:22 /hbase/WALs
drwxr-xr-x   - root supergroup          0 2026-11-05 23:22 /hbase/data
-rw-r--r--   2 root supergroup         42 2026-11-05 23:22 /hbase/hbase.id
...
```

Mở đường dẫn "HBase Master" in bởi `lab ui` (cổng 16010) phải thấy trang "HBase Master" với một RegionServer `hbase,16020,...` trong mục "Region Servers". Có thể kiểm tra nhanh ngay trong terminal codespace:

<!-- shell: Terminal codespace -->
```bash
curl -sL localhost:16010/master-status | grep -o "hbase,16020,[0-9]*" | head -1
```

**Kết quả mong đợi (dãy số là thời điểm khởi động, khác trên máy bạn):**

```text
hbase,16020,1793895731163
```

> 💡 `WALs` là Write-Ahead Log: mọi `put` được ghi vào WAL trên HDFS trước khi vào MemStore, nhờ đó dữ liệu không mất nếu RegionServer hỏng. Hai region có sẵn lúc đầu thuộc các bảng hệ thống (`hbase:meta`, `hbase:namespace`).

## PHẦN 2: LÀM QUEN HBASE SHELL

### Bước 3: CRUD cơ bản trên bảng demo_users

Mục đích: nắm cú pháp thao tác trên mô hình row key → column family → column qualifier → timestamp trước khi làm việc với dữ liệu lớn hơn. Trong HBase mọi giá trị là mảng byte; HBase shell hiển thị chúng dưới dạng chuỗi.

<!-- shell: Terminal codespace -->
```bash
docker exec -it hbase hbase shell
```

<!-- shell: HBase shell -->
```bash
create 'demo_users', 'info'
list
describe 'demo_users'
put 'demo_users', 'u001', 'info:name', 'Nguyen Van A'
put 'demo_users', 'u001', 'info:age', '21'
put 'demo_users', 'u002', 'info:name', 'Tran Thi B'
put 'demo_users', 'u002', 'info:email', 'b@ctu.edu.vn'
get 'demo_users', 'u001'
scan 'demo_users'
count 'demo_users'
delete 'demo_users', 'u001', 'info:age'
get 'demo_users', 'u001'
```

**Kết quả mong đợi (rút gọn, bỏ các dòng `Took ... seconds`):**

```text
hbase:001:0> create 'demo_users', 'info'
Created table demo_users
=> Hbase::Table - demo_users
...
hbase:003:0> describe 'demo_users'
Table demo_users is ENABLED
...
{NAME => 'info', INDEX_BLOCK_ENCODING => 'NONE', VERSIONS => '1', KEEP_DELETED_CELLS => 'FALSE', ..., TTL => 'FOREVER', ..., BLOOMFILTER => 'ROW', ..., BLOCKSIZE => '65536 B (64KB)'}
...
hbase:008:0> get 'demo_users', 'u001'
COLUMN  CELL
 info:age timestamp=2026-11-05T23:23:03.351, value=21
 info:name timestamp=2026-11-05T23:23:03.344, value=Nguyen Van A
1 row(s)
hbase:009:0> scan 'demo_users'
ROW  COLUMN+CELL
 u001 column=info:age, timestamp=2026-11-05T23:23:03.351, value=21
 u001 column=info:name, timestamp=2026-11-05T23:23:03.344, value=Nguyen Van A
 u002 column=info:email, timestamp=2026-11-05T23:23:03.432, value=b@ctu.edu.vn
 u002 column=info:name, timestamp=2026-11-05T23:23:03.408, value=Tran Thi B
2 row(s)
hbase:010:0> count 'demo_users'
2 row(s)
=> 2
hbase:011:0> delete 'demo_users', 'u001', 'info:age'
hbase:012:0> get 'demo_users', 'u001'
COLUMN  CELL
 info:name timestamp=2026-11-05T23:23:03.344, value=Nguyen Van A
1 row(s)
```

Nhận xét: hai hàng `u001`, `u002` có tập cột khác nhau (`age` và `email`) mà không cần khai báo trước — chỉ column family `info` phải khai báo khi tạo bảng. `scan` trả kết quả theo thứ tự byte tăng dần của row key, trong một hàng theo thứ tự tên cột.

### Bước 4: Phiên bản của ô dữ liệu, xoá hàng và xoá bảng

Mỗi ô (row, column) có thể giữ nhiều phiên bản phân biệt bằng timestamp. Số phiên bản tối đa được giữ là thuộc tính `VERSIONS` của column family. Tiếp tục trong HBase shell:

<!-- shell: HBase shell -->
```bash
alter 'demo_users', NAME => 'info', VERSIONS => 3
put 'demo_users', 'u002', 'info:email', 'b2@ctu.edu.vn'
put 'demo_users', 'u002', 'info:email', 'b3@ctu.edu.vn'
get 'demo_users', 'u002', {COLUMN => 'info:email', VERSIONS => 3}
get 'demo_users', 'u002', 'info:email'
deleteall 'demo_users', 'u002'
scan 'demo_users'
disable 'demo_users'
drop 'demo_users'
exists 'demo_users'
exit
```

**Kết quả mong đợi (rút gọn):**

```text
hbase:013:0> alter 'demo_users', NAME => 'info', VERSIONS => 3
Updating all regions with the new schema...
1/1 regions updated.
Done.
...
hbase:016:0> get 'demo_users', 'u002', {COLUMN => 'info:email', VERSIONS => 3}
COLUMN  CELL
 info:email timestamp=2026-11-05T23:23:05.920, value=b3@ctu.edu.vn
 info:email timestamp=2026-11-05T23:23:05.915, value=b2@ctu.edu.vn
 info:email timestamp=2026-11-05T23:23:03.432, value=b@ctu.edu.vn
1 row(s)
hbase:017:0> get 'demo_users', 'u002', 'info:email'
COLUMN  CELL
 info:email timestamp=2026-11-05T23:23:05.920, value=b3@ctu.edu.vn
1 row(s)
...
hbase:019:0> scan 'demo_users'
ROW  COLUMN+CELL
 u001 column=info:name, timestamp=2026-11-05T23:23:03.344, value=Nguyen Van A
1 row(s)
...
hbase:022:0> exists 'demo_users'
Table demo_users does not exist
=> false
```

> 💡 Giá trị cũ `b@ctu.edu.vn` được ghi khi bảng còn `VERSIONS => 1` nhưng vẫn hiện ra vì nó còn nằm trong MemStore, chưa bị flush/compaction loại bỏ. `get` không ghi `VERSIONS` chỉ trả phiên bản mới nhất. `delete` xoá một ô, `deleteall` xoá cả hàng; thực chất HBase ghi một "tombstone" (dấu xoá) và dữ liệu chỉ bị loại bỏ vật lý khi major compaction chạy.

> ⚠️ Phải `disable` trước khi `drop`. Gõ `drop` lên bảng đang bật sẽ báo `ERROR: Table demo_users is enabled. Disable it first.`

## PHẦN 3: THIẾT KẾ ROW KEY CHO DỮ LIỆU CẢM BIẾN

### Bước 5: Khảo sát dữ liệu và thiết kế row key có salt

Tệp `sensor_readings.csv` mô phỏng 2.000 thiết bị gửi số đo liên tục, mỗi giây một bản ghi, từ 2025-06-01 đến 2025-06-06:

<!-- shell: Terminal codespace -->
```bash
head -5 data/sensor_readings.csv
```

**Kết quả mong đợi:**

```text
device_id,ts,temperature,humidity,pm25,battery
D00177,2025-06-01 00:00:00,29.7,46.4,156.6,21
D01500,2025-06-01 00:00:01,31.0,61.3,170.0,97
D00562,2025-06-01 00:00:02,21.2,84.6,49.5,91
D01756,2025-06-01 00:00:03,38.7,57.1,99.2,82
```

HBase sắp xếp hàng theo row key và chia bảng thành các region liên tiếp theo khoảng khoá. Nếu row key bắt đầu bằng thời gian (`20250601000000#D00177`), mọi bản ghi mới luôn có khoá lớn nhất nên dồn vào **một** region cuối bảng — đó là hotspot. Ta so sánh hai thiết kế:

| Thiết kế | Ví dụ khoá | Ưu điểm | Nhược điểm |
|---|---|---|---|
| Thời gian đứng đầu (`time_first_key`) | `20250601000005#D00001` | Quét theo khoảng thời gian của mọi thiết bị bằng 1 lệnh `scan` | Ghi tuần tự dồn vào 1 region (hotspot) |
| Salt + thiết bị + thời gian (`salted_key`) | `d#D00001#20250601000005` | Ghi trải đều 16 bucket; quét lịch sử 1 thiết bị rất nhanh | Quét theo thời gian cho mọi thiết bị phải chạy 16 lệnh `scan` |

Tiền tố salt là ký tự hexa đầu tiên của MD5(`device_id`), nên luôn **tính lại được** từ `device_id` khi truy vấn. Tạo hai tệp sau:

<!-- file: src/lab4/rowkey.py -->
```python
"""Thiết kế row key cho dữ liệu cảm biến (Buổi 4)."""
import hashlib

SO_BUCKET = 16          # 1 ký tự hexa: 0..f


def salt(device_id: str) -> str:
    """Tiền tố băm: ký tự hexa đầu tiên của MD5(device_id)."""
    return hashlib.md5(device_id.encode()).hexdigest()[0]


def ts_compact(ts: str) -> str:
    """'2025-06-01 00:00:05' -> '20250601000005' (sắp xếp được theo thời gian)."""
    return ts.replace("-", "").replace(" ", "").replace(":", "")


def salted_key(device_id: str, ts: str) -> str:
    """Khóa có salt: <salt>#<device_id>#<yyyyMMddHHmmss>."""
    return f"{salt(device_id)}#{device_id}#{ts_compact(ts)}"


def time_first_key(device_id: str, ts: str) -> str:
    """Khóa 'ngây thơ' đặt thời gian lên đầu: <yyyyMMddHHmmss>#<device_id>."""
    return f"{ts_compact(ts)}#{device_id}"
```

<!-- file: src/lab4/kiem_tra_rowkey.py -->
```python
import csv
from collections import Counter
from rowkey import salt, salted_key, time_first_key

print(salted_key("D00001", "2025-06-01 00:00:05"))
print(time_first_key("D00001", "2025-06-01 00:00:05"))

thiet_bi, dong = Counter(), Counter()
devices = set()
with open("/data/sensor_readings.csv") as f:
    for r in csv.DictReader(f):
        devices.add(r["device_id"])
        dong[salt(r["device_id"])] += 1
for d in devices:
    thiet_bi[salt(d)] += 1

print("So thiet bi:", len(devices), "- so dong:", sum(dong.values()))
print("bucket  so_thiet_bi  so_dong")
for b in sorted(dong):
    print(f"   {b}      {thiet_bi[b]:5d}     {dong[b]:6d}")
print("Lech (max/min so dong):", round(max(dong.values()) / min(dong.values()), 3))
```

Chạy thử (terminal codespace không có Python của bài; dùng Python 3.12 có sẵn trong container `spark-master`; `-w` đặt thư mục làm việc để `import rowkey` được):

<!-- shell: Terminal codespace -->
```bash
docker exec -it -w /src/lab4 spark-master python3 kiem_tra_rowkey.py
```

**Kết quả mong đợi:**

```text
d#D00001#20250601000005
20250601000005#D00001
So thiet bi: 2000 - so dong: 500000
bucket  so_thiet_bi  so_dong
   0        114      28378
   1        132      32697
   2        114      28636
   3        129      32254
   4        135      33803
   5        112      27959
   6        113      28140
   7        111      27879
   8        138      34795
   9        141      35119
   a        130      32692
   b        126      31805
   c        120      29858
   d        126      31682
   e        127      31455
   f        132      32848
Lech (max/min so dong): 1.26
```

Mỗi bucket nhận khoảng 2000/16 ≈ 125 thiết bị và 500000/16 ≈ 31250 dòng; bucket lớn nhất chỉ gấp 1,26 lần bucket nhỏ nhất, tức hàm băm trải dữ liệu khá đều.

### Bước 6: Tạo bảng pre-split và bảng đối chứng

Nếu tạo bảng mặc định (1 region), toàn bộ dữ liệu ban đầu đi vào một region cho đến khi HBase tự chia. Pre-splitting tạo sẵn nhiều region theo đúng ranh giới tiền tố salt. Ta tạo hai bảng có **cùng** 7 điểm chia (8 region): `iot_sensor_data` dùng khoá có salt, `iot_time_first` dùng khoá thời gian đứng đầu để đối chứng.

<!-- shell: Terminal codespace -->
```bash
docker exec -it hbase hbase shell
```

<!-- shell: HBase shell -->
```bash
create 'iot_sensor_data', {NAME => 'cf', VERSIONS => 1}, {SPLITS => ['2','4','6','8','a','c','e']}
create 'iot_time_first', {NAME => 'cf', VERSIONS => 1}, {SPLITS => ['2','4','6','8','a','c','e']}
list_regions 'iot_sensor_data'
exit
```

**Kết quả mong đợi (cột REGION_NAME rút gọn):**

```text
Created table iot_sensor_data
=> Hbase::Table - iot_sensor_data
Created table iot_time_first
=> Hbase::Table - iot_time_first
               SERVER_NAME |                  REGION_NAME |  START_KEY |    END_KEY |  SIZE |   REQ |   LOCALITY |
 ------------------------- | ---------------------------- | ---------- | ---------- | ----- | ----- | ---------- |
 hbase,16020,1793895731163 |  iot_sensor_data,,17938...   |            |          2 |     0 |     0 |        0.0 |
 hbase,16020,1793895731163 | iot_sensor_data,2,17938...   |          2 |          4 |     0 |     0 |        0.0 |
 hbase,16020,1793895731163 | iot_sensor_data,4,17938...   |          4 |          6 |     0 |     0 |        0.0 |
 hbase,16020,1793895731163 | iot_sensor_data,6,17938...   |          6 |          8 |     0 |     0 |        0.0 |
 hbase,16020,1793895731163 | iot_sensor_data,8,17938...   |          8 |          a |     0 |     0 |        0.0 |
 hbase,16020,1793895731163 | iot_sensor_data,a,17938...   |          a |          c |     0 |     0 |        0.0 |
 hbase,16020,1793895731163 | iot_sensor_data,c,17938...   |          c |          e |     0 |     0 |        0.0 |
 hbase,16020,1793895731163 | iot_sensor_data,e,17938...   |          e |            |     0 |     0 |        0.0 |
 8 rows
```

> 💡 Môi trường lớp học chỉ có một RegionServer nên cả 8 region cùng nằm trên `hbase,16020,...`; trên cụm thật, HMaster sẽ phân các region này cho nhiều RegionServer, và đó là lúc salting phát huy tác dụng tải song song. Ranh giới khoá đã đúng thiết kế ngay cả khi chỉ có một máy.

### Bước 7: Nạp thử 2.000 dòng bằng tệp lệnh HBase shell

Cách nạp đơn giản nhất là sinh sẵn các lệnh `put` thành một tệp rồi cho HBase shell chạy. Tệp được ghi vào `/src/lab4` (thư mục `src/lab4` của repo), container `hbase` đọc được trực tiếp.

<!-- file: src/lab4/tao_lenh_put.py -->
```python
"""Sinh tệp lệnh HBase shell (put) từ N dòng đầu của sensor_readings.csv."""
import csv, sys
from rowkey import salted_key

N = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
with open("/data/sensor_readings.csv") as fin, open("/src/lab4/load_iot.hbase", "w") as fout:
    for i, r in enumerate(csv.DictReader(fin)):
        if i >= N:
            break
        k = salted_key(r["device_id"], r["ts"])
        for col in ("temperature", "humidity", "pm25", "battery"):
            fout.write(f"put 'iot_sensor_data', '{k}', 'cf:{col}', '{r[col]}'\n")
    fout.write("exit\n")
print(f"Da ghi {N} dong ({4 * N} lenh put) vao /src/lab4/load_iot.hbase")
```

<!-- shell: Terminal codespace -->
```bash
docker exec -it -w /src/lab4 spark-master python3 tao_lenh_put.py 2000
head -4 src/lab4/load_iot.hbase
time docker exec -it hbase hbase shell -n /src/lab4/load_iot.hbase
```

**Kết quả mong đợi (thời gian là ví dụ trên máy 2 nhân):**

```text
Da ghi 2000 dong (8000 lenh put) vao /src/lab4/load_iot.hbase
put 'iot_sensor_data', '4#D00177#20250601000000', 'cf:temperature', '29.7'
put 'iot_sensor_data', '4#D00177#20250601000000', 'cf:humidity', '46.4'
put 'iot_sensor_data', '4#D00177#20250601000000', 'cf:pm25', '156.6'
put 'iot_sensor_data', '4#D00177#20250601000000', 'cf:battery', '21'
...
hbase:8001:0> exit

real	0m20.144s
```

Kiểm tra số hàng:

<!-- shell: Terminal codespace -->
```bash
echo "count 'iot_sensor_data'" | docker exec -i hbase hbase shell -n
```

**Kết quả mong đợi:**

```text
...
=> 2000
```

> 💡 Mỗi dòng CSV sinh 4 lệnh `put` cùng row key nên `count` (đếm theo row key) bằng 2.000 chứ không phải 8.000. Tham số `-n` (non-interactive) khiến shell dừng ngay khi gặp lỗi và trả mã lỗi khác 0 — hữu ích khi chạy tệp lệnh. Dòng `exit` cuối tệp giúp shell thoát sau khi chạy xong.

### Bước 8: Nạp toàn bộ 500.000 dòng bằng happybase (Thrift)

Gửi từng lệnh `put` qua shell không phù hợp cho dữ liệu lớn. Thư viện `happybase` (đã cài sẵn trong image) kết nối Thrift server của HBase ở cổng 9090 và gom nhiều `put` thành một lô (batch) cho mỗi lần gửi mạng.

<!-- file: src/lab4/nap_happybase.py -->
```python
"""Nạp sensor_readings.csv vào HBase qua Thrift (happybase), ghi theo lô."""
import csv, sys, time
import happybase
from rowkey import salted_key, time_first_key

BANG = sys.argv[1]                                  # iot_sensor_data | iot_time_first
N = int(sys.argv[2]) if len(sys.argv) > 2 else 10**9
tao_khoa = salted_key if BANG == "iot_sensor_data" else time_first_key

conn = happybase.Connection("hbase", 9090)          # Thrift server trong container hbase
table = conn.table(BANG)
t0 = time.time()
n = 0
with open("/data/sensor_readings.csv") as f, table.batch(batch_size=2000) as b:
    for r in csv.DictReader(f):
        if n >= N:
            break
        b.put(tao_khoa(r["device_id"], r["ts"]).encode(), {
            b"cf:temperature": r["temperature"].encode(),
            b"cf:humidity": r["humidity"].encode(),
            b"cf:pm25": r["pm25"].encode(),
            b"cf:battery": r["battery"].encode(),
        })
        n += 1
print(f"{BANG}: da nap {n} dong trong {time.time() - t0:.1f} giay")
conn.close()
```

<!-- shell: Terminal codespace -->
```bash
docker exec -it -w /src/lab4 spark-master python3 nap_happybase.py iot_sensor_data
docker exec -it -w /src/lab4 spark-master python3 nap_happybase.py iot_time_first
```

**Kết quả mong đợi (thời gian là ví dụ trên máy 2 nhân):**

```text
iot_sensor_data: da nap 500000 dong trong 15.2 giay
iot_time_first: da nap 500000 dong trong 11.8 giay
```

Đếm lại hai bảng trong HBase shell (mở bằng `docker exec -it hbase hbase shell`; `INTERVAL` in tiến độ, `CACHE` tăng số hàng mỗi lần đọc):

<!-- shell: HBase shell -->
```bash
count 'iot_sensor_data', INTERVAL => 100000, CACHE => 10000
count 'iot_time_first', INTERVAL => 100000, CACHE => 10000
```

**Kết quả mong đợi:**

```text
Current count: 100000, row: 3#D00651#20250603055432
Current count: 200000, row: 6#D01106#20250606094119
Current count: 300000, row: 9#D01441#20250601005013
Current count: 400000, row: c#D01639#20250605142948
Current count: 500000, row: f#D01986#20250606172453
500000 row(s)
=> 500000
Current count: 100000, row: 20250602034639#D01718
...
Current count: 500000, row: 20250606185319#D00336
500000 row(s)
=> 500000
```

> 💡 2.000 hàng nạp ở Bước 7 bị ghi đè (cùng row key, `VERSIONS => 1`) nên tổng vẫn là 500.000. So sánh trên máy 2 nhân: shell cần khoảng 20 giây cho 2.000 hàng, happybase cần khoảng 12–15 giây cho 500.000 hàng. Trong thực tế với hàng trăm triệu hàng, người ta dùng bulk load (`ImportTsv` + `completebulkload`) để ghi thẳng HFile, bỏ qua đường ghi thông thường.

### Bước 9: Kiểm chứng phân bố dữ liệu qua các region

`flush` buộc HBase ghi MemStore xuống HFile trên HDFS để cột `SIZE` (MB) của `list_regions` phản ánh dữ liệu thật; cột `REQ` là số request đọc/ghi mà region đã phục vụ.

<!-- shell: HBase shell -->
```bash
flush 'iot_sensor_data'
flush 'iot_time_first'
list_regions 'iot_sensor_data'
list_regions 'iot_time_first'
```

**Kết quả mong đợi (cột SERVER_NAME và REGION_NAME rút gọn):**

```text
hbase:005:0> list_regions 'iot_sensor_data'
 REGION_NAME          |  START_KEY |    END_KEY |  SIZE |    REQ |   LOCALITY |
 iot_sensor_data,,... |            |          2 |    13 | 123370 |        0.0 |
 iot_sensor_data,2,...|          2 |          4 |    13 | 122935 |        0.0 |
 iot_sensor_data,4,...|          4 |          6 |    14 | 124649 |        0.0 |
 iot_sensor_data,6,...|          6 |          8 |    12 | 113303 |        0.0 |
 iot_sensor_data,8,...|          8 |          a |    15 | 141058 |        0.0 |
 iot_sensor_data,a,...|          a |          c |    14 | 130314 |        0.0 |
 iot_sensor_data,c,...|          c |          e |    14 | 124460 |        0.0 |
 iot_sensor_data,e,...|          e |            |    14 | 129911 |        0.0 |
 8 rows
hbase:006:0> list_regions 'iot_time_first'
 REGION_NAME          |  START_KEY |    END_KEY |  SIZE |     REQ |   LOCALITY |
 iot_time_first,,...  |            |          2 |     0 |       1 |        0.0 |
 iot_time_first,2,... |          2 |          4 |   109 | 1000000 |        0.0 |
 iot_time_first,4,... |          4 |          6 |     0 |       1 |        0.0 |
 iot_time_first,6,... |          6 |          8 |     0 |       1 |        0.0 |
 iot_time_first,8,... |          8 |          a |     0 |       1 |        0.0 |
 iot_time_first,a,... |          a |          c |     0 |       1 |        0.0 |
 iot_time_first,c,... |          c |          e |     0 |       1 |        0.0 |
 iot_time_first,e,... |          e |            |     0 |       1 |        0.0 |
 8 rows
```

Đây là bằng chứng trực tiếp của hotspot: với khoá thời gian đứng đầu, **mọi** khoá bắt đầu bằng `2025...` nên rơi vào region `[2, 4)` — một region nhận 109 MB và toàn bộ 1.000.000 request (500.000 lần ghi + 500.000 lần đọc khi `count`), bảy region còn lại trống. Với khoá có salt, 8 region nhận 12–15 MB và 113–141 nghìn request mỗi region.

Kiểm chứng trực quan: mở đường dẫn "HBase Master" của `lab ui` (cổng 16010), mục "Tables" → bấm `iot_time_first` rồi `iot_sensor_data`; trong bảng danh sách region (8 dòng, có cột `Start Key`, `End Key`), so sánh các cột `StorefileSize`, `ReadRequests` và `WriteRequests` giữa hai bảng.

> 💡 Hai bảng trong bài có 500.000 hàng và dung lượng 109 MB nên size đều được tính bằng MB nguyên; giá trị `SIZE` nhỏ hơn 1 MB hiện là 0. Nếu chưa `flush`, cột `SIZE` sẽ là 0 dù đã có dữ liệu trong MemStore.

> 💡 Số liệu region được RegionServer báo về HMaster định kỳ. Trong lần kiểm thử, `list_regions` chạy **ngay** sau `flush` còn hiện `SIZE` = 0 ở hai region của `iot_sensor_data` và 104 MB ở bảng `iot_time_first`; chờ khoảng 10 giây rồi chạy lại `list_regions` thì ra đúng các số ở trên.

### Bước 10: Truy vấn theo row key — get, scan khoảng, scan tiền tố

Thiết kế row key quyết định truy vấn nào nhanh. Trước hết tính salt của thiết bị cần tìm (salt luôn suy ra được từ `device_id`):

<!-- shell: Terminal codespace -->
```bash
docker exec -it -w /src/lab4 spark-master python3 -c "from rowkey import salt; print(salt('D00001'), salt('D00177'))"
```

**Kết quả mong đợi:**

```text
d 4
```

<!-- shell: HBase shell -->
```bash
get 'iot_sensor_data', '4#D00177#20250601000000'
scan 'iot_sensor_data', {STARTROW => 'd#D00001#20250601', STOPROW => 'd#D00001#20250602', COLUMNS => ['cf:temperature']}
scan 'iot_sensor_data', {ROWPREFIXFILTER => 'd#D00001#', LIMIT => 3}
scan 'iot_sensor_data', {STARTROW => '2', STOPROW => '4', LIMIT => 3, COLUMNS => ['cf:temperature']}
scan 'iot_time_first', {STARTROW => '20250603120000', STOPROW => '20250603120005'}
```

**Kết quả mong đợi (rút gọn):**

```text
hbase:001:0> get 'iot_sensor_data', '4#D00177#20250601000000'
COLUMN  CELL
 cf:battery timestamp=2026-11-05T23:24:07.837, value=21
 cf:humidity timestamp=2026-11-05T23:24:07.837, value=46.4
 cf:pm25 timestamp=2026-11-05T23:24:07.837, value=156.6
 cf:temperature timestamp=2026-11-05T23:24:07.837, value=29.7
1 row(s)
hbase:002:0> scan 'iot_sensor_data', {STARTROW => 'd#D00001#20250601', STOPROW => 'd#D00001#20250602', COLUMNS => ['cf:temperature']}
ROW  COLUMN+CELL
 d#D00001#20250601001233 column=cf:temperature, timestamp=2026-11-05T23:24:07.926, value=27.9
 d#D00001#20250601005820 column=cf:temperature, timestamp=2026-11-05T23:24:08.312, value=18.8
 ...
 d#D00001#20250601231801 column=cf:temperature, timestamp=2026-11-05T23:24:11.313, value=29.5
45 row(s)
hbase:003:0> scan 'iot_sensor_data', {ROWPREFIXFILTER => 'd#D00001#', LIMIT => 3}
ROW  COLUMN+CELL
 d#D00001#20250601001233 column=cf:battery, timestamp=2026-11-05T23:24:07.926, value=68
 d#D00001#20250601001233 column=cf:humidity, timestamp=2026-11-05T23:24:07.926, value=88.2
 ...
3 row(s)
hbase:004:0> scan 'iot_sensor_data', {STARTROW => '2', STOPROW => '4', LIMIT => 3, COLUMNS => ['cf:temperature']}
ROW  COLUMN+CELL
 2#D00004#20250601010208 column=cf:temperature, timestamp=2026-11-05T23:24:08.310, value=25.3
 2#D00004#20250601015606 column=cf:temperature, timestamp=2026-11-05T23:24:08.605, value=27.6
 2#D00004#20250601015827 column=cf:temperature, timestamp=2026-11-05T23:24:08.619, value=26.8
3 row(s)
hbase:005:0> scan 'iot_time_first', {STARTROW => '20250603120000', STOPROW => '20250603120005'}
ROW  COLUMN+CELL
 20250603120000#D01446 column=cf:battery, timestamp=2026-11-05T23:24:27.274, value=49
 ...
 20250603120004#D00602 column=cf:temperature, timestamp=2026-11-05T23:24:27.274, value=37.1
5 row(s)
```

Nhận xét:

- Lịch sử một thiết bị trong một ngày (45 bản ghi của `D00001`) là một đoạn khoá liên tục trong bảng có salt → một lệnh `scan` có `STARTROW`/`STOPROW` đọc đúng 45 hàng, không quét thừa.
- Câu hỏi "mọi thiết bị trong 5 giây lúc 12:00 ngày 03/06" chỉ cần **một** lệnh `scan` trên bảng `iot_time_first`; trên bảng có salt phải chạy 16 lệnh (một lệnh cho mỗi tiền tố `0#…` đến `f#…`) rồi lọc theo thời gian. Đó là cái giá của salting: đổi khả năng ghi phân tán lấy độ phức tạp khi đọc theo thời gian.
- `STOPROW` là cận **không** bao gồm. `ROWPREFIXFILTER` tương đương với `STARTROW = tiền tố` và `STOPROW = tiền tố + 1`.

Gõ `exit` để thoát HBase shell.

## PHẦN 4: TRUY CẬP HBASE TỪ PYTHON VÀ SPARK

### Bước 11: Đọc và ghi HBase từ Python bằng happybase

<!-- file: src/lab4/doc_happybase.py -->
```python
"""Đọc HBase từ Python qua Thrift (happybase)."""
import happybase
from rowkey import salt

conn = happybase.Connection("hbase", 9090)
print("Cac bang:", [t.decode() for t in conn.tables()])
table = conn.table("iot_sensor_data")

# 1) get theo row key day du
row = table.row(b"4#D00177#20250601000000")
print("get:", {k.decode(): v.decode() for k, v in row.items()})

# 2) scan 1 thiet bi trong 1 ngay: tinh tien to salt roi quet theo khoang khoa
dev = "D00001"
start = f"{salt(dev)}#{dev}#20250601".encode()
stop = f"{salt(dev)}#{dev}#20250602".encode()
temps = [float(d[b"cf:temperature"]) for _, d in
         table.scan(row_start=start, row_stop=stop, columns=[b"cf:temperature"])]
print(f"{dev} ngay 2025-06-01: {len(temps)} ban ghi, nhiet do TB = {sum(temps) / len(temps):.2f}")

# 3) put + delete mot o du lieu
table.put(b"0#TEST#20250101000000", {b"cf:temperature": b"99.9"})
print("sau put:", table.row(b"0#TEST#20250101000000"))
table.delete(b"0#TEST#20250101000000")
print("sau delete:", table.row(b"0#TEST#20250101000000"))
conn.close()
```

<!-- shell: Terminal codespace -->
```bash
docker exec -it -w /src/lab4 spark-master python3 doc_happybase.py
```

**Kết quả mong đợi:**

```text
Cac bang: ['iot_sensor_data', 'iot_time_first']
get: {'cf:battery': '21', 'cf:humidity': '46.4', 'cf:pm25': '156.6', 'cf:temperature': '29.7'}
D00001 ngay 2025-06-01: 45 ban ghi, nhiet do TB = 29.86
sau put: {b'cf:temperature': b'99.9'}
sau delete: {}
```

> 💡 Tên `hbase` chỉ phân giải được bên trong mạng Docker của cụm, vì vậy script chạy trong container `spark-master`. Terminal codespace không có sẵn `python3`/`happybase`; nếu tự cài thì phải đổi thành `happybase.Connection("localhost", 9090)` (cổng 9090 đã mở trong codespace).

### Bước 12: Spark 4 đọc HBase song song theo 16 bucket salt

Bài cũ dùng connector `hbase-spark` (`org.apache.hadoop.hbase.spark`). Connector này **chưa có bản cho Spark 4 / Scala 2.13**; chạy bản 1.0.0 trên Spark 4.1.3 báo `java.lang.NoClassDefFoundError: scala/Serializable` (lớp chỉ có trong Scala 2.12). Thay vào đó ta tận dụng chính thiết kế row key: 16 bucket salt là 16 khoảng khoá độc lập, nên tạo một RDD gồm 16 partition, mỗi task (chạy trên executor ở `nodemanager1`/`nodemanager2`) mở kết nối happybase và quét đúng một bucket.

<!-- file: src/lab4/spark_doc_hbase.py -->
```python
"""Spark 4 đọc HBase song song theo 16 bucket salt (thay cho connector hbase-spark)."""
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("Buoi4_Spark_doc_HBase").getOrCreate()
sc = spark.sparkContext
BUCKETS = list("0123456789abcdef")


def quet_bucket(buckets):
    import happybase                       # import trong executor (nodemanager1/2)
    conn = happybase.Connection("hbase", 9090)
    table = conn.table("iot_sensor_data")
    for b in buckets:
        for key, d in table.scan(row_prefix=f"{b}#".encode(), batch_size=5000):
            _, device_id, ts = key.decode().split("#")
            yield (device_id, ts, float(d[b"cf:temperature"]), float(d[b"cf:humidity"]),
                   float(d[b"cf:pm25"]), int(d[b"cf:battery"]))
    conn.close()


schema = T.StructType([
    T.StructField("device_id", T.StringType()), T.StructField("ts", T.StringType()),
    T.StructField("temperature", T.DoubleType()), T.StructField("humidity", T.DoubleType()),
    T.StructField("pm25", T.DoubleType()), T.StructField("battery", T.IntegerType())])

rdd = sc.parallelize(BUCKETS, len(BUCKETS)).mapPartitions(quet_bucket)   # 16 task = 16 bucket
df = (spark.createDataFrame(rdd, schema)
      .withColumn("ts", F.to_timestamp("ts", "yyyyMMddHHmmss")))
df.cache()
print("So dong doc tu HBase:", df.count(), "- so partition:", df.rdd.getNumPartitions())
df.printSchema()

(df.groupBy(F.to_date("ts").alias("ngay"))
   .agg(F.count("*").alias("so_ban_ghi"),
        F.round(F.avg("temperature"), 2).alias("nhiet_do_tb"),
        F.round(F.avg("pm25"), 2).alias("pm25_tb"))
   .orderBy("ngay").show())

(df.groupBy("device_id").agg(F.round(F.avg("pm25"), 1).alias("pm25_tb"), F.count("*").alias("n"))
   .orderBy(F.desc("pm25_tb")).show(5))
spark.stop()
```

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.log.level=WARN /src/lab4/spark_doc_hbase.py
```

**Kết quả mong đợi (toàn bộ lệnh khoảng 37–42 giây trên máy 2 nhân):**

```text
So dong doc tu HBase: 500000 - so partition: 16
root
 |-- device_id: string (nullable = true)
 |-- ts: timestamp (nullable = true)
 |-- temperature: double (nullable = true)
 |-- humidity: double (nullable = true)
 |-- pm25: double (nullable = true)
 |-- battery: integer (nullable = true)

+----------+----------+-----------+-------+
|      ngay|so_ban_ghi|nhiet_do_tb|pm25_tb|
+----------+----------+-----------+-------+
|2025-06-01|     86400|      29.52|  92.24|
|2025-06-02|     86400|      29.48|  92.52|
|2025-06-03|     86400|      29.47|   92.4|
|2025-06-04|     86400|       29.5|  92.34|
|2025-06-05|     86400|      29.53|   92.5|
|2025-06-06|     68000|       29.5|  92.72|
+----------+----------+-----------+-------+

+---------+-------+---+
|device_id|pm25_tb|  n|
+---------+-------+---+
|   D00859|  105.2|249|
|   D00558|  103.7|277|
|   D00239|  102.2|253|
|   D00248|  102.0|230|
|   D00841|  101.1|260|
+---------+-------+---+
only showing top 5 rows
```

Kiểm chứng: số dòng Spark đọc được (500000) phải khớp `count 'iot_sensor_data'` ở Bước 8; tổng cột `so_ban_ghi` = 5 × 86400 + 68000 = 500000. Trong lúc job chạy, mở cổng 4040 (đường dẫn "Spark UI (job đang chạy)" của `lab ui`, hoặc tab PORTS) → tab "Stages" phải thấy stage `count` có 16 task. Job chỉ chạy khoảng 40 giây; nếu không kịp, xem lại ở Spark History Server (cổng 18080) → bấm ứng dụng `Buoi4_Spark_doc_HBase` → tab "Stages".

> 💡 `--conf spark.log.level=WARN` tắt bớt các dòng log `INFO` của `spark-submit` (vài dòng `INFO` đầu tiên lúc khởi động vẫn hiện). Khi job kết thúc, cổng 4040 đóng lại; xem lại job ở Spark History Server (cổng 18080).

> ⚠️ Cách đọc này phù hợp cho phân tích quy mô lớp học. Mỗi task giữ một kết nối Thrift; với bảng hàng tỉ hàng trên cụm thật, nên dùng `TableInputFormat` của HBase qua `newAPIHadoopRDD` (Java/Scala) hoặc xuất snapshot HBase ra HDFS rồi đọc bằng Spark.

## PHẦN 5: CRUD, NHẬP DỮ LIỆU VÀ CHỈ MỤC TRONG MONGODB

### Bước 13: CRUD với mongosh trên collection nhỏ

MongoDB lưu document BSON (JSON mở rộng) trong collection; mỗi document có thể có tập trường khác nhau, có trường lồng (`dia_chi.quan`) và mảng (`so_thich`). Mở `mongosh` vào database `retail` (database và collection được tạo tự động ở lần ghi đầu tiên):

<!-- shell: Terminal codespace -->
```bash
docker exec -it mongodb mongosh retail
```

<!-- shell: mongosh -->
```bash
db.khach_hang.insertOne({ _id: "C0000001", ten: "Nguyen Van A", thanh_pho: "CanTho", hang: "silver", so_thich: ["sach", "the_thao"] })
db.khach_hang.insertMany([
  { _id: "C0000002", ten: "Tran Thi B", thanh_pho: "HCM", hang: "gold", dia_chi: { quan: "Q1", phuong: "Ben Nghe" } },
  { _id: "C0000003", ten: "Le Van C", thanh_pho: "Hanoi", hang: "silver" }
])
db.khach_hang.find({ hang: "silver" }, { ten: 1, thanh_pho: 1 })
db.khach_hang.find({ "dia_chi.quan": "Q1" })
db.khach_hang.updateOne({ _id: "C0000003" }, { $set: { hang: "gold" }, $push: { so_thich: "dien_tu" } })
db.khach_hang.updateMany({ hang: "gold" }, { $inc: { diem: 100 } })
db.khach_hang.find({ hang: "gold" })
db.khach_hang.deleteOne({ _id: "C0000001" })
db.khach_hang.countDocuments()
db.khach_hang.insertOne({ _id: "C0000002", ten: "Trung khoa" })
```

**Kết quả mong đợi (rút gọn):**

```text
retail> { acknowledged: true, insertedId: 'C0000001' }
retail> { acknowledged: true, insertedIds: { '0': 'C0000002', '1': 'C0000003' } }
retail> [
  { _id: 'C0000001', ten: 'Nguyen Van A', thanh_pho: 'CanTho' },
  { _id: 'C0000003', ten: 'Le Van C', thanh_pho: 'Hanoi' }
]
retail> [ { _id: 'C0000002', ten: 'Tran Thi B', thanh_pho: 'HCM', hang: 'gold', dia_chi: { quan: 'Q1', phuong: 'Ben Nghe' } } ]
retail> { acknowledged: true, insertedId: null, matchedCount: 1, modifiedCount: 1, upsertedCount: 0 }
retail> { acknowledged: true, insertedId: null, matchedCount: 2, modifiedCount: 2, upsertedCount: 0 }
retail> [
  { _id: 'C0000002', ..., hang: 'gold', dia_chi: { quan: 'Q1', phuong: 'Ben Nghe' }, diem: 100 },
  { _id: 'C0000003', ten: 'Le Van C', thanh_pho: 'Hanoi', hang: 'gold', so_thich: [ 'dien_tu' ], diem: 100 }
]
retail> { acknowledged: true, deletedCount: 1 }
retail> 2
retail> Uncaught MongoServerError: E11000 duplicate key error collection: retail.khach_hang index: _id_ dup key: { _id: "C0000002" }
```

Nhận xét: `$push` tạo mảng `so_thich` ngay cả khi document chưa có trường này; `$inc` tạo trường `diem` với giá trị 100. Lệnh cuối cố ý chèn trùng `_id` để thấy MongoDB bảo đảm tính duy nhất của khoá chính (`E11000`). Gõ `exit` để thoát.

> 💡 Kết quả trong `mongosh` thật có thể xuống dòng khác một chút (mongosh tự ngắt dòng các document dài); nội dung các trường là như nhau.

> 💡 Khi mở `mongosh` (không có `--quiet`), đầu màn hình có một khối "The server generated these startup warnings" — trong đó có dòng khuyên đặt `GLIBC_TUNABLES=glibc.pthread.rseq=0`, cảnh báo về XFS, access control và swappiness. Đây là cảnh báo bình thường của môi trường lớp học; **không** sửa biến `GLIBC_TUNABLES` trong compose (giá trị `rseq=1` là cách khắc phục lỗi kernel mới, xem Lỗi thường gặp).

### Bước 14: Chuyển CSV sang JSON Lines và nhập bằng mongoimport

Mỗi dòng giao dịch trở thành một document: dùng `transaction_id` làm `_id`, gom `store_id` và `region` thành document lồng `store`, và ghi `ts` dưới dạng `{"$date": ...}` (Extended JSON) để MongoDB lưu kiểu Date thật (nhờ đó dùng được toán tử ngày tháng ở Bước 16).

<!-- file: src/lab4/csv_sang_jsonl.py -->
```python
"""Chuyển retail_transactions.csv -> retail_transactions.jsonl (mỗi dòng 1 document)."""
import csv, json, sys, time

N = int(sys.argv[1]) if len(sys.argv) > 1 else None      # tuỳ chọn: chỉ lấy N dòng đầu
t0, n = time.time(), 0
with open("/data/retail_transactions.csv", encoding="utf-8") as fin, \
     open("/data/retail_transactions.jsonl", "w", encoding="utf-8") as fout:
    for row in csv.DictReader(fin):
        if N is not None and n >= N:
            break
        doc = {
            "_id": row["transaction_id"],                 # dùng mã giao dịch làm khóa chính
            "customer_id": row["customer_id"],
            "product_id": row["product_id"],
            "category": row["category"],
            "quantity": int(row["quantity"]),
            "unit_price": float(row["unit_price"]),
            "discount": float(row["discount"]),
            "store": {"store_id": row["store_id"], "region": row["region"]},   # document lồng
            "ts": {"$date": row["ts"].replace(" ", "T") + "+07:00"},           # kiểu Date thật
        }
        fout.write(json.dumps(doc, ensure_ascii=False) + "\n")
        n += 1
print(f"Da ghi {n} document vao /data/retail_transactions.jsonl trong {time.time() - t0:.1f} giay")
```

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master python3 /src/lab4/csv_sang_jsonl.py
head -1 data/retail_transactions.jsonl
docker exec -it mongodb mongoimport --db retail --collection transactions --drop \
  --file /import/retail_transactions.jsonl --numInsertionWorkers 4
```

**Kết quả mong đợi (thời gian là ví dụ trên máy 2 nhân; riêng `mongoimport` mất khoảng 2 phút 12 giây):**

```text
Da ghi 5000000 document vao /data/retail_transactions.jsonl trong 27.2 giay
{"_id": "T0000000000", "customer_id": "C0015081", "product_id": "P00343", "category": "me_va_be", "quantity": 3, "unit_price": 13977000.0, "discount": 0.0, "store": {"store_id": "S026", "region": "NgheAn"}, "ts": {"$date": "2025-07-14T06:46:58+07:00"}}
...
2026-11-05T16:29:38.716+0000	[########################] retail.transactions	1.17GB/1.17GB (100.0%)
2026-11-05T16:29:38.716+0000	5000000 document(s) imported successfully. 0 document(s) failed to import.
```

Tệp JSONL có dung lượng khoảng 1,25 GB (gấp 3 lần CSV vì mỗi document lặp lại tên trường). Số document nhập được phải bằng số dòng dữ liệu của CSV (5.000.000, không tính dòng tiêu đề). Container `mongodb` thấy thư mục `./data` của repo tại `/import`.

> 💡 `--drop` xoá collection cũ trước khi nhập, nhờ đó chạy lại lệnh không bị lỗi trùng `_id`. Nếu cần làm nhanh có thể chỉ chuyển 500.000 dòng đầu: `docker exec -it spark-master python3 /src/lab4/csv_sang_jsonl.py 500000` (các con số ở Bước 15–19 sẽ khác).

> ⚠️ Tệp `retail_transactions.jsonl` (1,25 GB) chiếm đĩa của codespace; xoá ở Bước 21 khi không dùng nữa. Tệp do container (chạy bằng `root`) tạo ra nên thuộc sở hữu `root` nhưng có quyền `rw-rw-rw-`: user `vscode` của codespace vẫn sửa/xoá được mà không cần `sudo`.

> 💡 Trong lúc `mongoimport` chạy (khoảng 2 phút), đừng để codespace rảnh quá lâu. Nếu codespace bị dừng giữa chừng, mở lại, chạy `lab nosql` rồi chạy lại đúng lệnh `mongoimport` — nhờ `--drop` nên không bị trùng `_id`.

### Bước 15: Chỉ mục và đọc kết quả explain

Mở lại `mongosh` (`docker exec -it mongodb mongosh retail`) và dán hàm tóm tắt dưới đây; hàm gọi `explain("executionStats")` rồi chỉ giữ lại các chỉ số quan trọng.

<!-- shell: mongosh -->
```bash
function tomTat(q) {
  const e = db.transactions.find(q).explain("executionStats");
  const wp = e.queryPlanner.winningPlan.queryPlan || e.queryPlanner.winningPlan;
  const st = JSON.stringify(wp);
  return {
    cac_stage: st.match(/"stage":"(\w+)"/g).map(x => x.slice(9, -1)).join(" > "),
    chi_muc: (st.match(/"indexName":"([^"]+)"/) || [null, "-"])[1],
    so_ket_qua: e.executionStats.nReturned,
    so_doc_da_quet: e.executionStats.totalDocsExamined,
    so_khoa_da_quet: e.executionStats.totalKeysExamined,
    thoi_gian_ms: e.executionStats.executionTimeMillis
  };
}
```

Đo truy vấn trước khi có chỉ mục, tạo ba chỉ mục (cả khối lệnh dưới mất khoảng 25 giây với 5 triệu document trên máy 2 nhân), rồi đo lại:

<!-- shell: mongosh -->
```bash
tomTat({ "store.region": "CanTho" })
db.transactions.createIndex({ "store.region": 1 })
db.transactions.createIndex({ ts: 1 })
db.transactions.createIndex({ "store.region": 1, ts: -1 })
db.transactions.getIndexes().map(i => i.name)
tomTat({ "store.region": "CanTho" })
tomTat({ "store.region": "CanTho", ts: { $gte: ISODate("2025-03-01T00:00:00+07:00"), $lt: ISODate("2025-03-02T00:00:00+07:00") } })
tomTat({ category: "sach" })
```

**Kết quả mong đợi (`thoi_gian_ms` là ví dụ trên máy 2 nhân; mongosh in mỗi kết quả trên nhiều dòng, ở đây gộp lại một dòng):**

```text
retail> tomTat({ "store.region": "CanTho" })
{ cac_stage: 'COLLSCAN', chi_muc: '-', so_ket_qua: 298941, so_doc_da_quet: 5000000, so_khoa_da_quet: 0, thoi_gian_ms: 1582 }
retail> ...
[ '_id_', 'store.region_1', 'ts_1', 'store.region_1_ts_-1' ]
retail> tomTat({ "store.region": "CanTho" })
{ cac_stage: 'FETCH > IXSCAN', chi_muc: 'store.region_1', so_ket_qua: 298941, so_doc_da_quet: 298941, so_khoa_da_quet: 298941, thoi_gian_ms: 873 }
retail> tomTat({ "store.region": "CanTho", ts: { ... } })
{ cac_stage: 'FETCH > IXSCAN', chi_muc: 'store.region_1_ts_-1', so_ket_qua: 1325, so_doc_da_quet: 1325, so_khoa_da_quet: 1325, thoi_gian_ms: 94 }
retail> tomTat({ category: "sach" })
{ cac_stage: 'COLLSCAN', chi_muc: '-', so_ket_qua: 714852, so_doc_da_quet: 5000000, so_khoa_da_quet: 0, thoi_gian_ms: 1402 }
```

Nhận xét:

- Không có chỉ mục, MongoDB phải quét toàn bộ 5.000.000 document (`COLLSCAN`) để trả 298.941 document của CanTho.
- Có chỉ mục `store.region_1`, kế hoạch là `IXSCAN` (duyệt chỉ mục) rồi `FETCH` (lấy document): số document phải đọc bằng đúng số kết quả.
- Truy vấn kết hợp khu vực + khoảng thời gian dùng chỉ mục kết hợp `store.region_1_ts_-1`, chỉ đọc 1.325 document (một ngày của CanTho) và nhanh hơn khoảng 17 lần so với quét toàn bộ (1582 ms so với 94 ms trong lần kiểm thử).
- Trường `category` chưa có chỉ mục nên vẫn `COLLSCAN`. Chỉ mục không miễn phí: mỗi lần ghi phải cập nhật thêm các cây chỉ mục, và chỉ mục chiếm RAM/đĩa.

> 💡 Giá trị vùng trong bộ dữ liệu là `HCM`, `Hanoi`, `DaNang`, `CanTho`, `HaiPhong`, `NgheAn`, `LamDong` (phân biệt chữ hoa/thường). Truy vấn với giá trị không tồn tại (ví dụ `"mienbac"` của bản bài cũ) trả về 0 document chứ không báo lỗi.

## PHẦN 6: AGGREGATION VÀ TRUY CẬP MONGODB TỪ PYTHON, SPARK

### Bước 16: Aggregation pipeline — doanh thu theo khu vực và theo tháng

Pipeline là một dãy stage; mỗi stage nhận luồng document từ stage trước. Doanh thu thực của một giao dịch là `quantity × unit_price × (1 − discount)`. Trong `mongosh retail`:

<!-- shell: mongosh -->
```bash
db.transactions.aggregate([
  { $project: { region: "$store.region",
                revenue: { $multiply: ["$quantity", "$unit_price", { $subtract: [1, "$discount"] }] } } },
  { $group: { _id: "$region",
              tong_doanh_thu: { $sum: "$revenue" },
              so_don_hang: { $sum: 1 },
              doanh_thu_trung_binh: { $avg: "$revenue" } } },
  { $sort: { tong_doanh_thu: -1 } }
])
```

**Kết quả mong đợi (toàn bộ 3 pipeline của bước này chạy khoảng 9 giây trên máy 2 nhân; mongosh in mỗi document trên nhiều dòng, ở đây gộp lại):**

```text
[
  { _id: 'HCM', tong_doanh_thu: 76689681595950, so_don_hang: 2252467, doanh_thu_trung_binh: 34046972.317885235 },
  { _id: 'Hanoi', tong_doanh_thu: 51023353014350, so_don_hang: 1499922, doanh_thu_trung_binh: 34017337.57778738 },
  { _id: 'DaNang', tong_doanh_thu: 13604231803050, so_don_hang: 399549, doanh_thu_trung_binh: 34048969.72098541 },
  { _id: 'CanTho', tong_doanh_thu: 10178397790200, so_don_hang: 298941, doanh_thu_trung_binh: 34048182.71899806 },
  { _id: 'HaiPhong', tong_doanh_thu: 8507582848950, so_don_hang: 249216, doanh_thu_trung_binh: 34137386.23904565 },
  { _id: 'NgheAn', tong_doanh_thu: 5105528945900, so_don_hang: 150174, doanh_thu_trung_binh: 33997422.629083596 },
  { _id: 'LamDong', tong_doanh_thu: 5083454187550, so_don_hang: 149731, doanh_thu_trung_binh: 33950579.289191954 }
]
```

Kiểm chứng bằng một pipeline gom toàn bộ collection (`_id: null`):

<!-- shell: mongosh -->
```bash
db.transactions.aggregate([
  { $group: { _id: null,
              tong: { $sum: { $multiply: ["$quantity", "$unit_price", { $subtract: [1, "$discount"] }] } },
              n: { $sum: 1 } } }
])
```

**Kết quả mong đợi:**

```text
[ { _id: null, tong: 170192230185950, n: 5000000 } ]
```

Tổng 7 giá trị `tong_doanh_thu` ở trên đúng bằng 170192230185950 và tổng `so_don_hang` bằng 5.000.000. Dữ liệu được sinh lệch có chủ đích: HCM và Hà Nội chiếm 75% số giao dịch (hiện tượng lệch dữ liệu — data skew — sẽ gặp lại khi tối ưu Spark).

Doanh thu theo tháng của HCM — `$match` đặt **đầu** pipeline để dùng được chỉ mục `store.region_1`, và `$dateToString` với `timezone` để nhóm theo giờ Việt Nam:

<!-- shell: mongosh -->
```bash
db.transactions.aggregate([
  { $match: { "store.region": "HCM" } },
  { $group: { _id: { $dateToString: { format: "%Y-%m", date: "$ts", timezone: "+07:00" } },
              doanh_thu: { $sum: { $multiply: ["$quantity", "$unit_price", { $subtract: [1, "$discount"] }] } } } },
  { $sort: { _id: 1 } }
])
```

**Kết quả mong đợi:**

```text
[
  { _id: '2025-01', doanh_thu: 9895995515450 },
  { _id: '2025-02', doanh_thu: 8900430204000 },
  { _id: '2025-03', doanh_thu: 9920623165950 },
  { _id: '2025-04', doanh_thu: 9580286163050 },
  { _id: '2025-05', doanh_thu: 9919881336200 },
  { _id: '2025-06', doanh_thu: 9580727675000 },
  { _id: '2025-07', doanh_thu: 9920357995100 },
  { _id: '2025-08', doanh_thu: 8971379541200 }
]
```

> 💡 Nếu ở Bước 14 lưu `ts` dạng chuỗi thay vì `{"$date": ...}`, `$dateToString` sẽ báo lỗi vì trường không phải kiểu Date — đây là lý do nên chuẩn hoá kiểu dữ liệu ngay khi nhập.

### Bước 17: Kết nối hai collection bằng $lookup

Nhập bảng sản phẩm trực tiếp từ CSV (`--headerline` lấy dòng đầu làm tên trường, MongoDB tự nhận kiểu số):

<!-- shell: Terminal codespace -->
```bash
docker exec -it mongodb mongoimport --db retail --collection products --drop \
  --type csv --headerline --file /import/products.csv
```

**Kết quả mong đợi:**

```text
2026-11-05T16:30:36.544+0000	500 document(s) imported successfully. 0 document(s) failed to import.
```

Tìm 3 thương hiệu bán nhiều đơn nhất tại cửa hàng S001 trong ngày 01/03/2025 (`$match` trước để giảm số document phải `$lookup`):

<!-- shell: mongosh -->
```bash
db.products.findOne()
db.transactions.aggregate([
  { $match: { "store.store_id": "S001", ts: { $gte: ISODate("2025-03-01T00:00:00+07:00"), $lt: ISODate("2025-03-02T00:00:00+07:00") } } },
  { $lookup: { from: "products", localField: "product_id", foreignField: "product_id", as: "sp" } },
  { $unwind: "$sp" },
  { $group: { _id: "$sp.brand", so_don: { $sum: 1 }, tong_sl: { $sum: "$quantity" } } },
  { $sort: { so_don: -1 } },
  { $limit: 3 }
])
```

**Kết quả mong đợi (`_id` của sản phẩm là ObjectId tự sinh, sẽ khác trên máy bạn):**

```text
{
  _id: ObjectId('6aecaf2c9cc928a60696f485'),
  product_id: 'P00003',
  product_name: 'San pham 3',
  category: 'the_thao',
  brand: 'Brand07',
  base_price: 16924000
}
[
  { _id: 'Brand39', so_don: 16, tong_sl: 47 },
  { _id: 'Brand21', so_don: 16, tong_sl: 46 },
  { _id: 'Brand16', so_don: 15, tong_sl: 39 }
]
```

> 💡 `findOne()` không bảo đảm thứ tự; `mongoimport` dùng nhiều luồng nên document đầu tiên bạn nhận có thể khác. `$lookup` tương đương LEFT OUTER JOIN; trên collection lớn nên có chỉ mục ở `foreignField` (`db.products.createIndex({product_id: 1})`).

### Bước 18: Truy cập MongoDB từ Python bằng pymongo

<!-- file: src/lab4/pymongo_demo.py -->
```python
"""Truy cập MongoDB từ Python bằng pymongo."""
import time
from datetime import datetime, timezone, timedelta
from pymongo import MongoClient, ASCENDING

VN = timezone(timedelta(hours=7))
# tz_aware + tzinfo: trả datetime theo giờ Việt Nam (mặc định pymongo trả giờ UTC "naive")
client = MongoClient("mongodb://mongodb:27017", tz_aware=True, tzinfo=VN)   # từ host: mongodb://localhost:27017
db = client["retail"]
tx = db["transactions"]
print("MongoDB", client.server_info()["version"], "- so document:", tx.estimated_document_count())

# 1) find có điều kiện + projection + sort + limit
q = {"store.region": "DaNang", "ts": {"$gte": datetime(2025, 8, 1, tzinfo=VN)}}
for d in tx.find(q, {"_id": 1, "category": 1, "ts": 1}).sort("ts", ASCENDING).limit(3):
    print(d)

# 2) aggregate: doanh thu theo danh mục (category)
t0 = time.time()
pipeline = [
    {"$group": {"_id": "$category",
                "doanh_thu": {"$sum": {"$multiply": ["$quantity", "$unit_price",
                                                     {"$subtract": [1, "$discount"]}]}},
                "so_don": {"$sum": 1}}},
    {"$sort": {"doanh_thu": -1}},
]
for r in tx.aggregate(pipeline, allowDiskUse=True):
    print(f"{r['_id']:<12} {r['doanh_thu']:>20,.0f} {r['so_don']:>9,}")
print(f"aggregate mat {time.time() - t0:.1f} giay")

# 3) ghi: insert_many + update_many + delete_many trên collection tạm
tmp = db["tmp_demo"]
tmp.drop()
tmp.insert_many([{"sku": f"P{i:05d}", "ton_kho": i * 10} for i in range(1, 6)])
r = tmp.update_many({"ton_kho": {"$lt": 30}}, {"$set": {"can_nhap": True}})
print("update_many matched/modified:", r.matched_count, r.modified_count)
print("delete_many deleted:", tmp.delete_many({"can_nhap": True}).deleted_count,
      "- con lai:", tmp.count_documents({}))
client.close()
```

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master python3 /src/lab4/pymongo_demo.py
```

**Kết quả mong đợi:**

```text
MongoDB 8.0.32 - so document: 5000000
{'_id': 'T0004576767', 'category': 'thuc_pham', 'ts': datetime.datetime(2025, 8, 1, 0, 0, 36, tzinfo=datetime.timezone(datetime.timedelta(seconds=25200)))}
{'_id': 'T0002671770', 'category': 'dien_tu', 'ts': datetime.datetime(2025, 8, 1, 0, 0, 37, tzinfo=datetime.timezone(datetime.timedelta(seconds=25200)))}
{'_id': 'T0004996124', 'category': 'thoi_trang', 'ts': datetime.datetime(2025, 8, 1, 0, 1, 47, tzinfo=datetime.timezone(datetime.timedelta(seconds=25200)))}
gia_dung       24,372,296,414,100   714,852
dien_tu        24,361,361,256,650   714,649
sach           24,319,316,680,500   714,852
the_thao       24,306,974,849,550   713,600
thuc_pham      24,302,788,049,600   714,206
thoi_trang     24,298,821,245,400   714,576
me_va_be       24,230,671,690,150   713,265
aggregate mat 2.7 giay
update_many matched/modified: 2 2
delete_many deleted: 2 - con lai: 3
```

> 💡 MongoDB luôn lưu Date theo UTC. Nếu bỏ `tz_aware=True, tzinfo=VN`, pymongo trả `datetime(2025, 7, 31, 17, 0, 36)` (giờ UTC, không kèm múi giờ) cho cùng giao dịch — dễ gây hiểu nhầm là dữ liệu tháng 7.

### Bước 19: Spark 4 đọc/ghi MongoDB bằng MongoDB Spark Connector 11

MongoDB Spark Connector từ bản 11.0 hỗ trợ Spark 4 (Scala 2.13). Connector không có sẵn trong image nên `spark-submit` tải từ Maven Central qua `--packages` (codespace luôn có Internet nên tải được ngay; lần đầu tải 5 tệp jar khoảng 2,5 MB trong vài giây, các lần sau dùng bộ nhớ đệm trong container `spark-master` và báo `5 already retrieved`).

<!-- file: src/lab4/spark_mongo.py -->
```python
"""Spark 4.1 đọc/ghi MongoDB bằng MongoDB Spark Connector 11.x (Scala 2.13)."""
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("Buoi4_Spark_MongoDB")
         .config("spark.mongodb.read.connection.uri", "mongodb://mongodb:27017")
         .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017")
         .getOrCreate())

df = (spark.read.format("mongodb")
      .option("database", "retail").option("collection", "transactions")
      .load())
df.printSchema()

# Bộ lọc được đẩy xuống MongoDB (predicate pushdown) -> chỉ đọc document của CanTho
cantho = df.filter(F.col("store.region") == "CanTho")
cantho.select("_id", "category").explain()
print("So giao dich CanTho:", cantho.count())

kq = (cantho.groupBy("category")
      .agg(F.round(F.sum(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount"))), 0)
           .alias("doanh_thu"), F.count("*").alias("so_don"))
      .orderBy(F.desc("doanh_thu")))
kq.show()

(kq.write.format("mongodb").mode("overwrite")
   .option("database", "retail").option("collection", "doanh_thu_cantho_theo_danh_muc")
   .save())
print("Da ghi", kq.count(), "dong vao retail.doanh_thu_cantho_theo_danh_muc")
spark.stop()
```

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.log.level=WARN \
  --packages org.mongodb.spark:mongo-spark-connector_2.13:11.0.2 \
  /src/lab4/spark_mongo.py
```

**Kết quả mong đợi (rút gọn; cả lệnh khoảng 40 giây trên máy 2 nhân, lần đầu có thêm phần tải gói khoảng 5 giây):**

```text
downloading https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.13/11.0.2/mongo-spark-connector_2.13-11.0.2.jar ...
...
:: resolution report :: resolve 3428ms :: artifacts dl 1233ms
...
	org.mongodb#mongodb-driver-sync;5.1.4 from central in [default]
	org.mongodb.spark#mongo-spark-connector_2.13;11.0.2 from central in [default]
...
root
 |-- _id: string (nullable = true)
 |-- category: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- discount: double (nullable = true)
 |-- product_id: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- store: struct (nullable = true)
 |    |-- store_id: string (nullable = true)
 |    |-- region: string (nullable = true)
 |-- ts: timestamp (nullable = true)
 |-- unit_price: double (nullable = true)

== Physical Plan ==
*(1) Project [_id#0, category#1]
+- BatchScan MongoTable()[_id#0, category#1] MongoScan{namespaceDescription=retail.transactions} RuntimeFilters: []

So giao dich CanTho: 298941
+----------+----------------+------+
|  category|       doanh_thu|so_don|
+----------+----------------+------+
|thoi_trang| 1.4683465195E12| 43132|
|      sach|1.46697866195E12| 42974|
|  gia_dung|1.46369705085E12| 42739|
|  the_thao|  1.452390486E12| 42543|
|   dien_tu| 1.4490364589E12| 42668|
|  me_va_be|1.44322852535E12| 42360|
| thuc_pham|1.43472008765E12| 42525|
+----------+----------------+------+

Da ghi 7 dong vao retail.doanh_thu_cantho_theo_danh_muc
```

Nhận xét: connector tự suy ra schema (kể cả struct lồng `store`) bằng cách lấy mẫu document. Kế hoạch vật lý **không còn** toán tử `Filter` phía Spark vì điều kiện `store.region = 'CanTho'` đã được đẩy xuống MongoDB thành stage `$match`. Số giao dịch 298.941 khớp với Bước 15. Kiểm tra collection kết quả:

<!-- shell: Terminal codespace -->
```bash
docker exec -it mongodb mongosh retail --quiet --eval 'db.doanh_thu_cantho_theo_danh_muc.find({}, {_id: 0}).sort({doanh_thu: -1}).limit(3)'
```

**Kết quả mong đợi:**

```text
[
  { category: 'thoi_trang', doanh_thu: 1468346519500, so_don: Long('43132') },
  { category: 'sach', doanh_thu: 1466978661950, so_don: Long('42974') },
  { category: 'gia_dung', doanh_thu: 1463697050850, so_don: Long('42739') }
]
```

> 💡 Lệnh `spark-submit` ở trên là lúc RAM của codespace cao nhất trong buổi (đo trên bản sao 2 nhân / 8 GB: khoảng 6–7,5 GB, gồm cả bộ nhớ đệm tệp; không container nào bị OOM). Đừng mở thêm job Spark khác hay `lab hive`/`lab kafka` trong lúc chạy.

> ⚠️ Đừng dùng `mongo-spark-connector_2.12` hoặc bản 10.x: chúng dành cho Spark 3 và sẽ lỗi lớp Scala tương tự connector `hbase-spark`.

## PHẦN 7: SO SÁNH VÀ DỌN DẸP

### Bước 20: So sánh mô hình dữ liệu HBase và MongoDB

Điền bảng sau vào báo cáo, bổ sung số liệu đo được của chính mình (thời gian nạp, kích thước, thời gian truy vấn):

| Tiêu chí | HBase | MongoDB |
|---|---|---|
| Mô hình dữ liệu | Wide-column: row key → column family → column → timestamp; mọi giá trị là byte | Document BSON: trường lồng nhau, mảng, kiểu dữ liệu phong phú (Date, số, chuỗi) |
| Thiết kế khoá | Row key quyết định cả phân bố dữ liệu và truy vấn nhanh (Bước 5–10) | `_id` là khoá chính; truy vấn linh hoạt qua chỉ mục phụ trên nhiều trường (Bước 15) |
| Truy vấn | `get`/`scan` theo khoảng row key; lọc theo cột khác phải quét | `find`/`aggregate` với điều kiện trên mọi trường, chỉ mục kết hợp |
| Tổng hợp | Không có engine tổng hợp; dùng Spark/MapReduce (Bước 12) | Aggregation pipeline chạy phía server (Bước 16–17) |
| Ghi tốc độ cao | Rất mạnh nếu row key phân tán tốt (salting, pre-split) | Tốt; mở rộng ngang bằng sharding với shard key thiết kế cẩn thận |
| Lưu trữ nền | HFile trên HDFS, nhân bản do HDFS đảm nhiệm | Engine WiredTiger trên đĩa cục bộ, nhân bản bằng replica set |
| Bài toán phù hợp | Chuỗi thời gian khối lượng lớn, log, dữ liệu cảm biến, tra cứu theo khoá | Catalog sản phẩm, hồ sơ người dùng, dữ liệu bán cấu trúc cần truy vấn và báo cáo linh hoạt |

### Bước 21: Dọn dẹp

Giữ nguyên dữ liệu nếu định làm bài tập mở rộng. Khi đã xong, xoá bảng đối chứng trong HBase shell (`docker exec -it hbase hbase shell`; chạy khi `hbase` còn bật):

<!-- shell: HBase shell -->
```bash
disable 'iot_time_first'
drop 'iot_time_first'
list
exit
```

**Kết quả mong đợi:**

```text
hbase:002:0> drop 'iot_time_first'
hbase:003:0> list
TABLE
iot_sensor_data
1 row(s)
=> ["iot_sensor_data"]
```

Xoá các tệp trung gian lớn để tiết kiệm đĩa của codespace (không cần `sudo`):

<!-- shell: Terminal codespace -->
```bash
rm -f data/retail_transactions.jsonl src/lab4/load_iot.hbase
rm -rf src/lab4/__pycache__
```

Các lệnh không in gì khi thành công (đã kiểm thử: user `vscode` xoá được cả tệp lẫn thư mục `__pycache__` do container tạo ra).

> 💡 Bảng `iot_sensor_data` và collection `retail.transactions` có thể giữ lại để dùng trong đồ án: dữ liệu HBase nằm trên HDFS, dữ liệu MongoDB nằm trong volume `mongodb`, cả hai còn nguyên sau khi tắt dịch vụ hoặc dừng codespace (đã kiểm thử: sau khi dừng và mở lại codespace, `count 'iot_sensor_data'` vẫn là 500000 và `transactions` vẫn có 5000000 document). Lệnh `lab reset` xoá **toàn bộ** volume (HDFS, HBase, MongoDB), chỉ dùng khi muốn làm lại từ đầu.

## Kết thúc buổi

Tắt HBase và MongoDB để giải phóng khoảng 1,5 GB RAM (dữ liệu vẫn giữ nguyên):

<!-- shell: Terminal codespace -->
```bash
lab stop-extras
lab status
```

**Kết quả mong đợi (dòng RAM là ví dụ; hai lần kiểm thử in 2.0 GB và 2.2 GB):**

```text
 Container mongodb  Stopped
 Container hbase  Stopped
NAME              STATUS
datanode1         Up 14 minutes (healthy)
...
spark-master      Up 14 minutes (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 2.0 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

> ⚠️ Nếu quên `lab stop-extras` mà codespace tự dừng (sau 30 phút không thao tác) hoặc bạn dừng codespace, thì lần mở sau `hbase` và `mongodb` **tự bật lại** cùng cụm lõi (chính sách `restart: unless-stopped`), chiếm thêm RAM ở buổi sau. Gõ `lab stop-extras` khi thấy chúng trong `lab status`.

Lưu bài làm lên GitHub (chỉ các tệp mã nguồn; dữ liệu trong `data/` đã bị `.gitignore` loại trừ):

<!-- shell: Terminal codespace -->
```bash
git add src/lab4/*.py
git commit -m "Buoi 4: HBase va MongoDB"
git push
```

> ⚠️ Đừng dùng `git add src/lab4` khi còn thư mục `src/lab4/__pycache__`: thư mục này (do Python trong container tạo ra) sẽ bị đưa lên repo. Xoá nó như ở Bước 21 hoặc chỉ `git add` các tệp `.py`.

Cuối cùng dừng codespace để không tốn quota: trên https://github.com/codespaces bấm "..." cạnh codespace → "Stop codespace" (hoặc trong VS Code: Command Palette → "Codespaces: Stop Current Codespace"). Không chọn "Delete" — xoá codespace sẽ mất image, dữ liệu HDFS/HBase/MongoDB và các tệp chưa push.

## Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `ERROR: Table ... is enabled. Disable it first.` khi `drop` | HBase chỉ cho xoá bảng đã tắt | Chạy `disable 'ten_bang'` rồi mới `drop 'ten_bang'` |
| `java.lang.NoClassDefFoundError: scala/Serializable` khi dùng `--packages org.apache.hbase.connectors.spark:hbase-spark:1.0.0` | Connector `hbase-spark` chỉ có bản Scala 2.12/Spark 3; Spark 4.1 dùng Scala 2.13 | Không dùng connector này; đọc HBase bằng happybase như Bước 11–12 |
| `MongoServerError: E11000 duplicate key error ... dup key: { _id: ... }` | Chèn document trùng `_id`, hoặc chạy lại `mongoimport` không có `--drop` | Dùng `--drop` khi nhập lại, hoặc `--mode upsert`; với `insertOne` đổi `_id` |
| Truy vấn MongoDB theo khu vực trả về 0 document | Dùng giá trị không có trong dữ liệu (ví dụ `"mienbac"` của bài cũ) hoặc sai hoa/thường | Xem các giá trị thật bằng `db.transactions.distinct("store.region")` |
| `can't convert from BSON type string to Date` trong `$dateToString` | Trường thời gian lưu dạng chuỗi | Nhập lại với `{"$date": ...}` như Bước 14, hoặc chuyển bằng `$toDate` trong pipeline |
| pymongo in giờ lệch 7 tiếng (giao dịch 00:00 ngày 01/08 hiện thành 17:00 ngày 31/07) | MongoDB lưu UTC, pymongo mặc định trả `datetime` UTC không múi giờ | Tạo client với `tz_aware=True, tzinfo=timezone(timedelta(hours=7))` |
| `mongodb` khởi động lại liên tục, log `Linux kernel versions 6.19 and newer has a known incompatibility` | Lỗi MongoDB 8 với kernel Linux mới (SERVER-121912) | Compose đã đặt `GLIBC_TUNABLES=glibc.pthread.rseq=1`; nếu vẫn lỗi, cập nhật kernel (bản ≥ 7.0.14 đã sửa) |
| Đầu `mongosh` có cảnh báo `Please set the environment variable GLIBC_TUNABLES=glibc.pthread.rseq=0` | Cảnh báo khởi động chuẩn của MongoDB 8; compose cố ý đặt `rseq=1` để mongod chạy được trên kernel 6.19–7.0.13 | Bỏ qua cảnh báo, **không** đổi về `rseq=0` (trên kernel mới mongod sẽ từ chối khởi động) |
| `count`/`scan` trong HBase shell báo `NotServingRegionException` ngay sau `create` | Region mới tạo đang được gán cho RegionServer | Chờ vài giây rồi chạy lại; `status 'simple'` để xem số region online |
| `TTransportException(type=1, message="Could not connect to ('localhost', 9090)")` | Script chạy **trong container** nhưng kết nối `localhost`, hoặc `hbase` chưa bật/chưa healthy (ví dụ vừa mở lại codespace) | Trong container dùng `happybase.Connection("hbase", 9090)`; chạy `lab nosql` rồi `lab status` để chắc `hbase` đã `healthy` |
| `python3: command not found` hoặc `No module named 'happybase'` khi chạy script ngay trong terminal codespace | Terminal codespace không có Python của bài; Python, `happybase`, `pymongo` nằm trong image của cụm | Chạy qua container: `docker exec -it -w /src/lab4 spark-master python3 <script>.py` |
| `ModuleNotFoundError: No module named 'rowkey'` khi chạy `python3 -c "from rowkey import ..."` | Thư mục làm việc của container không phải `/src/lab4` | Thêm `-w /src/lab4` vào `docker exec` như trong bài |
| `Error response from daemon: No such container: hbase` (hoặc `mongodb`) | Chưa bật dịch vụ bổ sung trong phiên này (codespace mới mở chỉ tự bật cụm lõi) | Chạy `lab nosql` |
| `git status` liệt kê `src/lab4/__pycache__/...` | Python trong container tạo tệp `.pyc` khi `import rowkey`; repo tạo trước 06/11/2026 có `.gitignore` chưa loại `__pycache__` | `rm -rf src/lab4/__pycache__` (không cần `sudo`) rồi `git add src/lab4/*.py` |
| Job Spark hoặc `mongoimport` chậm hẳn, `hbase` bị `OOMKilled` | Bật thêm dịch vụ khác (`lab hive`, `lab kafka`, `lab extras`) hoặc chạy nhiều job cùng lúc trên máy 8 GB (cụm lõi + HBase + MongoDB đã dùng tới 6–7 GB khi chạy job) | `lab stop-extras` rồi chỉ bật `lab nosql`; chạy từng job một. Nếu vẫn thiếu, tạo codespace 4 nhân / 16 GB (tốn gấp đôi quota) |
| Codespace tự dừng giữa chừng (mất kết nối, terminal báo "Codespace stopped") | Quá 30 phút không thao tác | Mở lại codespace, chờ cụm lõi bật, `lab nosql`, rồi chạy lại bước đang dở (`mongoimport --drop` và `nap_happybase.py` chạy lại an toàn; bảng HBase và collection MongoDB vẫn còn) |
| Sau khi mở lại codespace thấy `hbase`, `mongodb` tự chạy dù hôm nay không học NoSQL | Buổi trước không `lab stop-extras` trước khi codespace dừng; compose đặt `restart: unless-stopped` | `lab stop-extras` |
| Không mở được HBase Master/Spark UI bằng `http://localhost:16010` trên trình duyệt, hoặc bấm liên kết `http://hbase:16030` trong Web UI không vào được | Trình duyệt chạy trên máy bạn, không phải trong codespace; tên container chỉ phân giải trong mạng Docker | Dùng đường dẫn của `lab ui` hoặc tab PORTS (biểu tượng quả địa cầu); cổng 16030 mở từ tab PORTS. Nếu cổng không có trong tab PORTS, bấm "Add Port" và nhập số cổng |
| `spark-submit --packages` treo lâu hoặc báo lỗi tải gói | Mạng tới Maven Central tạm lỗi | Chạy lại lệnh; gói đã tải được lưu đệm. Không đổi sang `mongo-spark-connector_2.12` hoặc bản 10.x |

## Câu hỏi kiểm tra

1. Vì sao dùng row key bắt đầu bằng thời gian (`yyyyMMddHHmmss#device_id`) cho dữ liệu cảm biến ghi liên tục gây hotspot? Dẫn chứng bằng kết quả `list_regions 'iot_time_first'` của bạn.
   *Gợi ý: bản ghi mới luôn có khoá lớn nhất nên mọi lần ghi dồn vào region chứa khoảng khoá cuối (trong bài: region `[2, 4)` nhận 109 MB và toàn bộ request); region đó và RegionServer phụ trách trở thành điểm nghẽn, các region khác rảnh.*
2. Bảng tạo với `SPLITS => ['2','4','6','8','a','c','e']` có bao nhiêu region? Vì sao các điểm chia này phù hợp với tiền tố salt 1 ký tự hexa, và nếu muốn 16 region thì khai báo thế nào?
   *Gợi ý: 7 điểm chia tạo 8 region; mỗi region phủ 2 trong 16 giá trị hexa nên nhận khoảng 1/8 dữ liệu; 16 region cần 15 điểm chia `'1','2',...,'f'`.*
3. Salting làm truy vấn nào chậm đi? Với bảng `iot_sensor_data`, cần bao nhiêu lệnh `scan` để lấy mọi bản ghi trong khoảng 12:00:00–12:00:05 ngày 03/06/2025, và có cách thiết kế nào giảm chi phí đó?
   *Gợi ý: 16 lệnh scan (một cho mỗi bucket) kèm lọc thời gian; có thể giảm số bucket, hoặc thêm bảng chỉ mục phụ có khoá theo thời gian, hoặc dùng khoá `salt#yyyyMMddHH#device_id` nếu truy vấn chủ yếu theo giờ.*
4. Trong kết quả `explain` ở Bước 15, hai trường `totalDocsExamined` và `nReturned` cho biết điều gì về hiệu quả của chỉ mục? Vì sao chỉ mục kết hợp `{store.region: 1, ts: -1}` phục vụ được cả truy vấn chỉ lọc `store.region`?
   *Gợi ý: tỉ lệ docsExamined/nReturned càng gần 1 thì càng ít đọc thừa; theo quy tắc tiền tố (prefix) chỉ mục kết hợp dùng được cho truy vấn trên trường đứng đầu, nhưng không dùng được cho truy vấn chỉ lọc `ts`.*
5. Vì sao đặt `$match` ở đầu pipeline (Bước 16, 17) thay vì sau `$group`/`$lookup`?
   *Gợi ý: `$match` đầu pipeline dùng được chỉ mục và giảm số document đi qua các stage sau, đặc biệt `$lookup` tốn chi phí cho từng document đầu vào.*
6. Bài cũ đọc HBase vào Spark bằng connector `hbase-spark`. Vì sao trên Spark 4.1 không dùng được, và cách đọc ở Bước 12 đã tận dụng thiết kế row key như thế nào để song song hoá?
   *Gợi ý: connector biên dịch cho Scala 2.12 (lỗi `scala/Serializable`); mỗi bucket salt là một khoảng khoá độc lập nên ánh xạ 1 bucket = 1 partition = 1 task quét `row_prefix`.*

## Bài tập mở rộng

1. Tạo bảng thứ ba `iot_no_split` dùng khoá có salt nhưng **không** pre-split, nạp 500.000 hàng bằng `nap_happybase.py` (sửa tên bảng) và so sánh số region, `SIZE`, `REQ` với `iot_sensor_data`. Giải thích vì sao bảng vẫn chỉ có 1 region (gợi ý: ngưỡng tự chia `hbase.hregion.max.filesize`), rồi thử `split 'iot_no_split', '8'`.
2. Thiết kế bảng HBase phục vụ truy vấn "10 bản ghi **mới nhất** của một thiết bị" bằng kỹ thuật đảo thời gian (`Long.MAX_VALUE - timestamp` hoặc `99999999999999 - yyyyMMddHHmmss`) trong row key; viết script nạp và lệnh `scan ... LIMIT => 10` chứng minh kết quả.
3. Viết pipeline MongoDB tính top 3 danh mục theo doanh thu **trong từng khu vực** (gợi ý: `$group` theo `{region, category}`, `$sort`, rồi `$group` theo region với `$push` và `$slice`, hoặc dùng `$setWindowFields`). Đối chiếu kết quả của một khu vực bằng Spark ở Bước 19.
4. Đo ảnh hưởng của chỉ mục lên tốc độ ghi: nhập lại 500.000 document vào một collection mới (`csv_sang_jsonl.py 500000` + `mongoimport`) hai lần — không chỉ mục và sau khi tạo 3 chỉ mục ở Bước 15 — rồi so sánh thời gian.

## Yêu cầu nộp bài và tiêu chí chấm

Nộp một báo cáo (PDF hoặc Markdown) gồm: ảnh chụp hoặc log của các lệnh ở từng bước, thư mục `src/lab4` (các script đã chạy, kể cả script viết thêm — đã `git push` lên repo của bạn, kèm đường dẫn repo trong báo cáo), bảng so sánh ở Bước 20 có số liệu đo của chính mình, và trả lời các câu hỏi kiểm tra.

| Tiêu chí | Điểm |
|---|---|
| Thao tác HBase shell đúng (Bước 3–4): CRUD, VERSIONS, disable/drop, giải thích kết quả | 1.0 |
| Thiết kế row key có salt, bảng phân bố bucket và giải thích (Bước 5) | 1.5 |
| Tạo bảng pre-split, nạp đủ 500.000 hàng bằng hai cách, `count` khớp (Bước 6–8) | 1.5 |
| So sánh `list_regions` / Web UI của hai bảng, chỉ ra hotspot bằng số liệu (Bước 9) | 1.0 |
| Truy vấn theo row key và đọc HBase từ Python/Spark, số dòng khớp (Bước 10–12) | 1.0 |
| MongoDB: CRUD, `mongoimport` đủ document, chỉ mục và bảng so sánh `explain` trước/sau (Bước 13–15) | 1.5 |
| Aggregation và kiểm chứng tổng, `$lookup`, pymongo (Bước 16–18) | 1.0 |
| Bảng so sánh HBase – MongoDB có số liệu đo thực tế và trả lời câu hỏi kiểm tra | 1.0 |
| Hoàn thành ít nhất một bài tập mở rộng | 0.5 |
| **Tổng** | **10** |
