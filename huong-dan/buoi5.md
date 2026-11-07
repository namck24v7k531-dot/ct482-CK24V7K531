# Buổi 5 — Phân tích dữ liệu bán lẻ với Spark RDD, DataFrame và Spark SQL (GitHub Codespaces)

Thời lượng: 4 tiết, chia hai tuần (Tuần 10: PHẦN 1–3 — RDD, cache, Spark UI; Tuần 11: PHẦN 4–6 — chế độ ANSI của Spark 4, DataFrame, Spark SQL, Parquet) · Chuẩn đầu ra: CLO4 · Máy khuyến nghị: codespace mặc định **2 nhân / 8 GB** — đủ cho toàn bài (mọi `spark-submit` của buổi tổng khoảng 6 phút, RAM đỉnh khoảng 5,7 GB); máy 4 nhân / 16 GB nhanh hơn khoảng 1,5 lần nhưng tốn gấp đôi quota, chỉ cần khi bạn phải bật thêm dịch vụ khác hoặc job báo thiếu bộ nhớ · Dịch vụ cần bật: chỉ cụm lõi 8 container (tự bật khi mở codespace; nếu chưa: `lab start`), không cần `lab hive`, `lab nosql`, `lab kafka` — nếu còn bật từ buổi trước thì `lab stop-extras` · Quota ước tính: khoảng 2 giờ mở codespace mỗi tuần × 2 tuần × 2 nhân ≈ 8 core-hour (máy 4 nhân ≈ 16 core-hour); dữ liệu của buổi chiếm thêm khoảng 2,1 GB đĩa. Môi trường dùng Spark 4.1.3 (Scala 2.13, Python 3.12): Spark Master chạy trong container `spark-master`, hai Spark Worker nằm trong `nodemanager1` và `nodemanager2` (mỗi worker 2 core, 1 GB). Dữ liệu dùng: `web_access.log` (1.000.000 dòng log Apache, 121 MB) cho phần RDD và `retail_transactions.csv` (5.000.000 giao dịch, 405 MB) cùng `products.csv` (500 sản phẩm) cho phần DataFrame/SQL, sinh bằng `lab data 5000000`. Mọi kết quả mong đợi được lấy từ một lần chạy thật trên bản sao Dev Container của codespace (đúng thư mục `.devcontainer` của repo, giới hạn 2 nhân / 8 GB) với bộ dữ liệu đầy đủ; các số đo thời gian phụ thuộc máy và chỉ là ví dụ.

## Mục tiêu

Sau khi hoàn thành bài thực hành, sinh viên có khả năng:

- Kiểm tra tình trạng cụm HDFS/Spark, mở PySpark shell kết nối Spark Master và đọc các thông tin `sc.master`, `applicationId`, cấu hình bộ nhớ executor.
- Phân tích `web_access.log` bằng Spark RDD thuần: đếm lượt truy cập theo mã trạng thái HTTP, tìm URL lỗi 5xx nhiều nhất, đếm lượt theo giờ; phân biệt transformation (lười) và action (kích hoạt tính toán).
- Đo bằng số liệu cụ thể tác dụng của `cache()`/`persist()`, đọc tab Storage, Stages của Spark UI; so sánh `reduceByKey` với `groupByKey` qua chỉ số Shuffle Write/Shuffle Read.
- Giải thích chế độ ANSI SQL bật mặc định trong Spark 4, nhận diện các lỗi `CAST_INVALID_INPUT`, `DIVIDE_BY_ZERO`, `ARITHMETIC_OVERFLOW` và xử lý dữ liệu bẩn bằng họ hàm `try_*`.
- Nạp CSV vào DataFrame với schema tường minh, tính doanh thu theo tháng và khu vực, ghi kết quả lên HDFS.
- Chuyển dữ liệu sang Parquet (có và không phân vùng), so sánh dung lượng và thời gian truy vấn, đọc `explain()` để nhận ra column pruning, predicate pushdown và partition pruning.
- Viết cùng một phân tích RFM bằng DataFrame API và bằng Spark SQL, chứng minh hai cách cho cùng kết quả và cùng kế hoạch thực thi; dùng JOIN và hàm cửa sổ trong Spark SQL.

## Chuẩn bị

### Yêu cầu

- Codespace của học phần đã tạo ở Buổi 0 (repo của bạn tạo từ template hoặc GitHub Classroom). Chỉ cần trình duyệt và Internet; không cài gì trên máy phòng lab.
- Máy codespace mặc định 2 nhân / 8 GB là đủ. Cụm lõi dùng khoảng 2,3 GB RAM khi rảnh và khoảng 4,7 GB khi chạy job (dòng `RAM các container` của `lab status`); VS Code chạy trong codespace chiếm thêm khoảng 1,8 GB. Vì vậy chỉ bật cụm lõi, tắt dịch vụ bổ sung của buổi trước và không mở nhiều phiên Spark cùng lúc.
- Mọi lệnh "Terminal codespace" chạy trong terminal của VS Code trên codespace, đứng ở **thư mục gốc của repo** (terminal mới mở luôn đứng sẵn ở đó; tên repo của mỗi bạn khác nhau nên bài chỉ dùng đường dẫn tương đối như `data/`, `src/lab5/`). Thư mục `data/` và `src/` của repo được mount thành `/data` và `/src` trong `namenode`, `spark-master`, `nodemanager1`, `nodemanager2`, nên không cần `docker cp`.
- Bài này dùng hai terminal: **terminal A** cho PySpark shell, **terminal B** cho các lệnh khác khi shell đang mở (mở thêm bằng biểu tượng `+` trên khung TERMINAL).

### Mở codespace và kiểm tra cụm

1. Vào https://github.com/codespaces → bấm tên codespace của học phần. Khi mở lại, cụm lõi tự khởi động (postStartCommand); chờ đến khi terminal hiện `[khoi-dong] Cụm đã sẵn sàng`.
2. Nếu giảng viên báo có cập nhật repo, lấy về trước khi làm bài; sau đó kiểm tra cụm:

<!-- shell: Terminal codespace -->
```bash
git pull
lab status
```

**Kết quả mong đợi:** 8 container `healthy`, HDFS có 2 DataNode, YARN có 2 NodeManager (thời gian `Up ...` và con số RAM trên máy bạn khác):

```text
NAME              STATUS
datanode1         Up About a minute (healthy)
datanode2         Up About a minute (healthy)
historyserver     Up About a minute (healthy)
namenode          Up About a minute (healthy)
nodemanager1      Up About a minute (healthy)
nodemanager2      Up About a minute (healthy)
resourcemanager   Up About a minute (healthy)
spark-master      Up About a minute (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 2.3 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

> 💡 Nếu `lab status` báo `HDFS: chưa chạy` hoặc thiếu container, gõ `lab start` (chờ tối đa 5 phút) rồi kiểm tra lại. Nếu danh sách còn `hiveserver2`, `hbase`, `mongodb` hoặc `kafka` từ buổi trước, gõ `lab stop-extras` để giải phóng RAM.

### Giao diện Web dùng trong bài

<!-- shell: Terminal codespace -->
```bash
lab ui
```

**Kết quả mong đợi (ví dụ với codespace tên `ten-codespace-abc123`; trên máy bạn là tên codespace của bạn):**

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

| Giao diện | Cổng (link trong `lab ui` hoặc tab PORTS) | Dùng ở bước |
|---|---|---|
| HDFS NameNode | 9870 | Bước 2 |
| Spark Master (danh sách worker, ứng dụng) | 8080 | Bước 1, 3 |
| Spark UI của ứng dụng đang chạy trong `spark-master` (PySpark shell, `spark-submit`) | 4040 | Bước 3–15 |
| Spark History Server (ứng dụng đã kết thúc) | 18080 | Bước 16 |
| YARN ResourceManager | 8088 | Bước 16 |

> 💡 Liên kết bên trong Web UI dùng tên container (ví dụ `http://nodemanager1:8042`, `historyserver:18080`); trình duyệt không mở được. Hãy dùng link có cùng số cổng trong `lab ui`. Spark Worker UI (cổng 8081) không được forward.

> ⚠️ Cổng 4040 chỉ có trang khi đang có một ứng dụng Spark chạy (PySpark shell đang mở hoặc `spark-submit` đang chạy). Mở link 4040 lúc không có ứng dụng nào sẽ báo lỗi kết nối — đó không phải lỗi forward cổng.

### Dữ liệu 5 triệu dòng

Codespace mới tạo chỉ có dữ liệu mẫu 200.000 dòng. Kiểm tra số dòng:

<!-- shell: Terminal codespace -->
```bash
wc -l data/retail_transactions.csv data/web_access.log
```

**Kết quả mong đợi khi còn dữ liệu mẫu:**

```text
  200001 data/retail_transactions.csv
   50000 data/web_access.log
  250001 total
```

Nếu thấy `200001` (hoặc tệp không tồn tại), sinh bộ dữ liệu đầy đủ — chỉ cần **một lần cho cả học kỳ**, mất khoảng 30 giây và thêm khoảng 0,5 GB vào đĩa codespace:

<!-- shell: Terminal codespace -->
```bash
lab data 5000000
wc -l data/retail_transactions.csv data/web_access.log
```

**Kết quả mong đợi:**

```text
...
  giao dich: 4,500,000
  products.csv: 0.0 MB
  retail_transactions.csv: 405.2 MB
  web_access.log: 121.4 MB
  sensor_readings.csv: 22.7 MB
  5000001 data/retail_transactions.csv
  1000000 data/web_access.log
  6000001 total
```

> 💡 Thư mục `data/` nằm trong `.gitignore`, nên dữ liệu không bị đẩy lên GitHub. Nếu đã có 5 triệu dòng từ buổi trước thì bỏ qua lệnh `lab data`. Sau khi sinh lại dữ liệu, phải `put -f` lại lên HDFS (Bước 2).

> ⚠️ Kiểm tra dung lượng đĩa trước khi làm bài: `df -h /workspaces` (cột `Avail`) và `docker system df`. Buổi này cần thêm khoảng 2,1 GB (0,5 GB tệp trong `data/` và khoảng 1,6 GB trên HDFS do nhân bản 2). Nếu còn dưới 3 GB, xoá image dịch vụ không còn dùng (ví dụ `docker image rm apache/hive:4.2.1` sau Lab 3).

Tạo thư mục mã nguồn của bài (tức `/src/lab5` trong container). Các tệp mã nguồn cho ở từng bước dưới dạng khối tệp có ghi chú `file:`; có thể giải nén gói mã nguồn Buổi 5 của giảng viên vào thư mục `src/` (kéo thả tệp zip vào khung Explorer của VS Code rồi giải nén) thay cho việc gõ lại.

<!-- shell: Terminal codespace -->
```bash
mkdir -p src/lab5
```

## PHẦN 1: CHUẨN BỊ CỤM VÀ DỮ LIỆU

### Bước 1: Kiểm tra HDFS, YARN và Spark Master

Mục đích: xác nhận HDFS đã thoát safe mode và Spark Master đã có đủ worker trước khi đưa dữ liệu vào xử lý; nếu bỏ qua, các lỗi ở bước sau (`Connection refused`, `SafeModeException`, job đứng chờ tài nguyên) sẽ khó chẩn đoán.

<!-- shell: Terminal codespace -->
```bash
docker exec -it namenode hdfs dfsadmin -report
docker exec -it namenode hdfs dfsadmin -safemode get
docker exec -it resourcemanager yarn node -list
docker exec -it spark-master bash -lc 'curl -s localhost:8080/json/ | jq "{status, aliveworkers, cores, memory}"'
```

**Kết quả mong đợi (rút gọn):**

```text
...
Live datanodes (2):
Name: 172.19.0.5:9866 (datanode2.bigdata-lab_default)
Hostname: datanode2
...
Safe mode is OFF
...
Total Nodes:2
         Node-Id	     Node-State	Node-Http-Address	Number-of-Running-Containers
nodemanager1:39625	        RUNNING	nodemanager1:8042	                           0
nodemanager2:43711	        RUNNING	nodemanager2:8042	                           0
{
  "status": "ALIVE",
  "aliveworkers": 2,
  "cores": 4,
  "memory": 2048
}
```

Spark Master có 2 worker, tổng 4 core và 2048 MB (mỗi worker 2 core, 1024 MB). Mở giao diện Spark Master (link cổng 8080 trong `lab ui`, hoặc tab PORTS → dòng 8080 → biểu tượng quả địa cầu) để thấy hai worker `nodemanager1`, `nodemanager2` ở trạng thái `ALIVE`. Địa chỉ IP (`172.19.0.x`) và số cổng sau tên NodeManager trên máy bạn có thể khác.

> ⚠️ Nếu `Safe mode is ON`, chờ 30–60 giây rồi kiểm tra lại. Chỉ trong môi trường học mới ép thoát bằng `docker exec -it namenode hdfs dfsadmin -safemode leave`. Ngay sau khi mở lại codespace, cụm cần thêm khoảng 1–2 phút sau thông báo `Cụm đã sẵn sàng` để NameNode thoát safe mode.

### Bước 2: Đưa dữ liệu lên HDFS

Thư mục `data/` của repo (đã có dữ liệu 5 triệu dòng sau phần Chuẩn bị) có sẵn ở `/data` trong `namenode`, nên `put` trực tiếp (không cần `docker cp`). Bước này mất khoảng 6 giây trên máy 2 nhân:

<!-- shell: Terminal codespace -->
```bash
docker exec -it namenode hdfs dfs -mkdir -p /data/raw
docker exec -it namenode hdfs dfs -put -f /data/web_access.log /data/retail_transactions.csv /data/products.csv /data/raw/
docker exec -it namenode hdfs dfs -ls -h /data/raw
docker exec -it namenode hdfs dfs -du -s -h /data/raw
```

**Kết quả mong đợi:**

```text
Found 3 items
-rw-r--r--   2 root supergroup     22.1 K 2026-11-05 23:22 /data/raw/products.csv
-rw-r--r--   2 root supergroup    386.4 M 2026-11-05 23:22 /data/raw/retail_transactions.csv
-rw-r--r--   2 root supergroup    115.8 M 2026-11-05 23:22 /data/raw/web_access.log
502.2 M  1004.4 M  /data/raw
```

`hdfs dfs -ls -h` hiển thị theo đơn vị nhị phân (115.8 M = 121.399.073 byte, đúng kích thước tệp gốc). Cột thứ hai của `-du` (1004.4 M) là dung lượng thực chiếm trên đĩa với hệ số nhân bản 2 — trên Codespaces, con số này tính vào dung lượng đĩa (và quota lưu trữ) của codespace.

> ⚠️ Nếu kích thước `retail_transactions.csv` chỉ khoảng 15.5 M, bạn đang dùng dữ liệu mẫu 200.000 dòng: chạy `lab data 5000000` (phần Chuẩn bị) rồi chạy lại lệnh `put -f` ở trên.

### Bước 3: Mở PySpark shell kết nối tới Spark Master

<!-- shell: Terminal codespace (terminal A) -->
```bash
docker exec -it spark-master pyspark --master spark://spark-master:7077
```

**Kết quả mong đợi (phần cuối của thông báo khởi động):**

```text
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 4.1.3
      /_/

Using Python version 3.12.3 (main, Aug 31 2026 10:18:26)
Spark context Web UI available at http://spark-master:4040
Spark context available as 'sc' (master = spark://spark-master:7077, app id = app-20261105232242-0000).
SparkSession available as 'spark'.
>>>
```

Các biến `sc` (SparkContext) và `spark` (SparkSession) đã được tạo sẵn. Kiểm tra:

<!-- shell: Python (pyspark) -->
```python
print(sc.master)
print(sc.applicationId)
print(spark.version, sc.defaultParallelism)
print(spark.conf.get("spark.executor.memory"), spark.conf.get("spark.driver.memory"))
```

**Kết quả mong đợi:**

```text
spark://spark-master:7077
app-20261105232242-0000
4.1.3 4
768m 768m
```

Mở Spark Master (cổng 8080): mục "Running Applications" có ứng dụng `PySparkShell` đang `RUNNING` với 4 core. Mở link "Spark UI (job đang chạy)" của `lab ui` (cổng 4040, dạng `https://<tên-codespace>-4040.app.github.dev`) để xem Spark UI của chính phiên shell này (chưa có job nào). Dòng `Spark context Web UI available at http://spark-master:4040` trong thông báo khởi động là địa chỉ nội bộ, trình duyệt không mở được. **Giữ nguyên phiên shell** cho đến hết PHẦN 4 (Bước 11).

> ⚠️ Bài cũ dùng `--conf spark.executor.memory=1g --conf spark.driver.memory=1g`. Môi trường mới đặt mặc định 768m cho vừa worker 1 GB. Giá trị lớn hơn bộ nhớ worker (ví dụ `spark.executor.memory=2g`) khiến job đứng mãi với cảnh báo `Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources`.

> 💡 Mỗi phiên PySpark/`spark-submit` chạy trong `spark-master` chiếm một cổng UI: phiên đầu là 4040, phiên thứ hai chạy song song sẽ nhận 4041 (codespace tạo từ 06/11/2026 forward sẵn cổng này; codespace cũ phải thêm bằng tab PORTS → *Forward a Port*). Hơn nữa phiên shell đang mở giữ cả 4 core của cụm, nên `spark-submit` ở terminal khác sẽ đứng chờ (xem Lỗi thường gặp). Hãy thoát phiên cũ (`exit()` hoặc Ctrl+D) trước khi mở phiên mới.

## PHẦN 2: XỬ LÝ LOG WEB BẰNG RDD

### Bước 4: Đọc log, parse bằng biểu thức chính quy; transformation và action

Mỗi dòng log có dạng Apache combined: `IP - - [thời gian] "PHƯƠNG_THỨC URL GIAO_THỨC" MÃ_TRẠNG_THÁI SỐ_BYTE "referer" "user-agent"`.

<!-- shell: Python (pyspark) -->
```python
import re

log_path = "hdfs://namenode:9000/data/raw/web_access.log"
raw_rdd = sc.textFile(log_path, minPartitions=4)     # transformation: chưa đọc file
print(raw_rdd.getNumPartitions())
raw_rdd.take(2)                                       # action nhỏ: đọc 2 dòng đầu
```

**Kết quả mong đợi:**

```text
4
['211.50.159.75 - - [03/Mar/2025:22:20:45 +0700] "GET /api/v1/orders HTTP/1.1" 404 82868 "-" "curl/8.4.0"', '98.134.188.103 - - [05/Mar/2025:04:52:37 +0700] "GET /api/v1/orders HTTP/1.1" 200 77327 "-" "Mozilla/5.0 (X11; Linux x86_64)"']
```

> 💡 Tệp 115,8 MiB nằm gọn trong một block HDFS 128 MB, nên mặc định `textFile` chỉ tạo 2 partition và chỉ 2 trong 4 core làm việc. `minPartitions=4` chia thành 4 split để tận dụng cả 4 core của cụm.

<!-- shell: Python (pyspark) -->
```python
pattern = re.compile(
    r'(?P<ip>\S+) \S+ \S+ \[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<url>\S+) \S+" '
    r'(?P<status>\d{3}) (?P<size>\S+)'
)

def parse_line(line):
    m = pattern.match(line)
    return m.groupdict() if m else None

parsed_rdd = raw_rdd.map(parse_line).filter(lambda x: x is not None)   # transformation
total_lines = raw_rdd.count()       # action: đọc toàn bộ file
parsed_lines = parsed_rdd.count()   # action: đọc lại file lần 2 (chưa cache)
print("Tổng dòng:", total_lines, "- Parse hợp lệ:", parsed_lines)
```

**Kết quả mong đợi:**

```text
Tổng dòng: 1000000 - Parse hợp lệ: 1000000
```

Dòng `parsed_rdd = ...map(...).filter(...)` chạy tức thì vì `map` và `filter` chỉ mở rộng đồ thị DAG; chỉ khi gặp `count()` Spark mới gửi job, đọc từng split trên HDFS và áp toàn bộ chuỗi biến đổi. Vào Spark UI (cổng 4040) → tab "Jobs": mỗi lệnh `count()` là một job riêng, và `take(2)` cũng là một job (rất nhỏ).

> 💡 Khi gõ hàm hoặc vòng `for` trong PySpark shell, dòng bắt đầu bằng `...` là dòng tiếp nối; nhấn Enter thêm một lần ở dòng trống để kết thúc khối lệnh trước khi gõ lệnh tiếp theo.

### Bước 5: Đếm lượt truy cập theo mã trạng thái bằng reduceByKey

<!-- shell: Python (pyspark) -->
```python
status_counts = (parsed_rdd
                 .map(lambda d: (d["status"], 1))     # transformation
                 .reduceByKey(lambda a, b: a + b))    # transformation (có shuffle)
for status, cnt in sorted(status_counts.collect()):   # action
    print(status, cnt)

print(sum(c for _, c in status_counts.collect()) == parsed_lines)
```

**Kết quả mong đợi:**

```text
200 799709
301 50391
404 79827
500 39984
503 30089
True
```

Kiểm chứng chéo bằng `grep` trên tệp gốc (container `namenode` thấy tệp ở `/data`). PySpark shell đang chiếm terminal A, nên mở **terminal B** (biểu tượng `+` trên khung TERMINAL; terminal mới đứng sẵn ở gốc repo). Mẫu `'" 404 '` gồm cả dấu nháy đóng của phần request nên chỉ khớp trường mã trạng thái:

<!-- shell: Terminal codespace (terminal B) -->
```bash
docker exec -it namenode bash -c "grep -c '\" 404 ' /data/web_access.log"
docker exec -it namenode bash -c "grep -c ' 404 ' /data/web_access.log"
```

**Kết quả mong đợi:**

```text
79827
79837
```

Số đầu khớp chính xác kết quả Spark cho mã 404. Mẫu lỏng `' 404 '` (của bài cũ) đếm dư 10 dòng vì còn khớp các dòng có **số byte** trả về bằng 404 — minh hoạ vì sao nên parse theo cấu trúc thay vì tìm chuỗi.

### Bước 6: Top URL lỗi 5xx và số lượt truy cập theo giờ

<!-- shell: Python (pyspark) -->
```python
error5xx_rdd = parsed_rdd.filter(lambda d: d["status"].startswith("5"))
top_urls = (error5xx_rdd
            .map(lambda d: (d["url"], 1))
            .reduceByKey(lambda a, b: a + b)
            .takeOrdered(10, key=lambda kv: -kv[1]))   # action
for url, cnt in top_urls:
    print(cnt, url)

top_url = top_urls[0][0]
recheck = error5xx_rdd.filter(lambda d: d["url"] == top_url).count()
print(recheck, "==", top_urls[0][1])
```

**Kết quả mong đợi:**

```text
7126 /san-pham
7092 /tim-kiem
7090 /api/v1/orders
7067 /san-pham/dien-tu
7041 /gio-hang
7041 /dang-nhap
6960 /thanh-toan
6951 /api/v1/products
6876 /khuyen-mai
6829 /
7126 == 7126
```

Tổng 10 giá trị bằng 70.073 = 39.984 (mã 500) + 30.089 (mã 503). Bộ dữ liệu chỉ có 10 URL nên "top 10" chứa toàn bộ URL.

Đếm theo giờ — ép giờ về `int` để `sortByKey()` sắp theo thứ tự số (khoá chuỗi sẽ sắp `"10"` trước `"2"`):

<!-- shell: Python (pyspark) -->
```python
def extract_hour(time_str):
    # "01/Mar/2025:14:23:01 +0700" -> 14 (ép về int để sắp xếp đúng thứ tự số)
    return int(time_str.split(":")[1])

hourly_counts = (parsed_rdd
                 .map(lambda d: (extract_hour(d["time"]), 1))
                 .reduceByKey(lambda a, b: a + b)
                 .sortByKey())
hourly = hourly_counts.collect()
for hour, cnt in hourly[:3] + hourly[-2:]:
    print(f"{hour:02d}h -> {cnt}")

print(len(hourly), sum(c for _, c in hourly) == parsed_lines)
```

**Kết quả mong đợi:**

```text
00h -> 41467
01h -> 41803
02h -> 41384
22h -> 42151
23h -> 41867
24 True
```

`filter`, `map`, `reduceByKey`, `sortByKey` là transformation; `collect`, `count`, `take`, `takeOrdered` là action. Trong tab "Jobs" của Spark UI (cổng 4040), mỗi action là một job; mở một job có `reduceByKey` sẽ thấy hai stage ngăn cách bởi ranh giới shuffle.

> ⚠️ Chỉ dùng `collect()` khi kết quả đã được tổng hợp nhỏ (24 dòng theo giờ). `collect()` trên RDD 1 triệu dòng kéo toàn bộ dữ liệu về driver (768 MB) và có thể làm driver hết bộ nhớ; lúc gỡ lỗi hãy dùng `take(n)`.

## PHẦN 3: CACHE, SPARK UI VÀ SHUFFLE

### Bước 7: Đo thời gian khi chưa cache

Mỗi action trên `parsed_rdd` hiện phải đọc lại tệp từ HDFS và chạy lại regex cho 1 triệu dòng.

<!-- shell: Python (pyspark) -->
```python
import time

parsed_rdd.setName("parsed_rdd")
parsed_rdd.count()                  # lượt "làm nóng" JVM/Python worker, bỏ kết quả
t0 = time.time()
for _ in range(3):
    parsed_rdd.count()              # mỗi lần đều đọc lại HDFS và parse lại regex

print("Không cache - 3 lần count():", round(time.time() - t0, 2), "giây")
```

**Kết quả mong đợi (thời gian là ví dụ trên codespace 2 nhân / 8 GB):**

```text
parsed_rdd PythonRDD[27] at RDD at PythonRDD.scala:59
1000000
1000000
1000000
1000000
Không cache - 3 lần count(): 3.92 giây
```

### Bước 8: persist() và tab Storage của Spark UI

<!-- shell: Python (pyspark) -->
```python
parsed_rdd.persist()                # chỉ đánh dấu, chưa vật chất hoá
t0 = time.time()
parsed_rdd.count()                  # lần đầu: đọc, parse và lưu vào bộ nhớ executor
print("Lần count() đầu sau persist():", round(time.time() - t0, 2), "giây")
t0 = time.time()
for _ in range(3):
    parsed_rdd.count()              # đọc từ cache

print("Có cache - 3 lần count():", round(time.time() - t0, 2), "giây")
print(parsed_rdd.getStorageLevel())
```

**Kết quả mong đợi (thời gian là ví dụ trên codespace 2 nhân / 8 GB):**

```text
parsed_rdd PythonRDD[27] at RDD at PythonRDD.scala:59
1000000
Lần count() đầu sau persist(): 2.77 giây
1000000
1000000
1000000
Có cache - 3 lần count(): 1.92 giây
Memory Serialized 1x Replicated
```

Mở Spark UI (cổng 4040) → tab "Storage": có dòng `parsed_rdd` với "Cached Partitions" 4, "Fraction Cached" 100%. Cũng có thể đọc cùng thông tin qua REST API của Spark UI ngay trong shell (shell chạy trong `spark-master`, nên ở đây `localhost:4040` là cổng của chính container đó và dùng được):

<!-- shell: Python (pyspark) -->
```python
import json, urllib.request
UI = "http://localhost:4040/api/v1/applications/" + sc.applicationId
for r in json.load(urllib.request.urlopen(UI + "/storage/rdd")):
    print(r["name"], r["numCachedPartitions"], "/", r["numPartitions"],
          r["storageLevel"], round(r["memoryUsed"] / 2**20, 1), "MiB")

```

**Kết quả mong đợi:**

```text
parsed_rdd 4 / 4 Memory Serialized 1x Replicated 34.0 MiB
```

Nhận xét: với 1 triệu dòng (khoảng 116 MiB), chênh lệch tuyệt đối nhỏ (3,92 s so với 1,92 s, giảm khoảng 51% trên máy 2 nhân; máy 4 nhân đo được 2,38 s so với 0,59 s) vì dữ liệu nhỏ so với cụm; với tệp vài GB hoặc pipeline parse phức tạp hơn, khoảng cách sẽ lớn hơn nhiều. Trong PySpark, RDD được cache ở dạng **đã tuần tự hoá** (pickle, nén theo lô) nên chỉ chiếm 34 MiB.

> 💡 Lần `count()` đầu sau `persist()` vẫn phải đọc HDFS và parse, đồng thời ghi vào bộ nhớ — đó là chi phí "vật chất hoá" cache. Cache chỉ có lợi khi RDD được dùng lại ít nhất hai lần.

> ⚠️ Executor chỉ có 768 MB. Nếu cache RDD lớn hơn bộ nhớ dành cho lưu trữ, các partition không vừa sẽ bị bỏ và tính lại mỗi lần ("Fraction Cached" < 100%). Khi đó dùng `persist(StorageLevel.MEMORY_AND_DISK)` (`from pyspark import StorageLevel`) để phần dư được ghi xuống đĩa.

### Bước 9: So sánh reduceByKey và groupByKey qua Shuffle Write

Tính tổng số byte trả về theo URL bằng hai cách trên **toàn bộ** 1 triệu bản ghi (đang được cache):

<!-- shell: Python (pyspark) -->
```python
pairs_rdd = parsed_rdd.map(lambda d: (d["url"], int(d["size"])))   # (url, số byte trả về)
t0 = time.time()
result_reduce = pairs_rdd.reduceByKey(lambda a, b: a + b).collect()
print("reduceByKey:", round(time.time() - t0, 2), "giây, số key:", len(result_reduce))
t0 = time.time()
result_group = pairs_rdd.groupByKey().mapValues(sum).collect()
print("groupByKey:", round(time.time() - t0, 2), "giây, số key:", len(result_group))
print(sorted(result_reduce) == sorted(result_group))
```

**Kết quả mong đợi (thời gian là ví dụ):**

```text
reduceByKey: 1.36 giây, số key: 10
groupByKey: 1.5 giây, số key: 10
True
```

Thời gian gần như nhau vì dữ liệu đã nằm trong cache và chỉ có 10 khoá; khác biệt thực sự nằm ở **lượng dữ liệu shuffle**. Xem tab "Stages" của Spark UI cổng 4040 (cột "Shuffle Write" và "Shuffle Read"), hoặc đọc qua REST API:

<!-- shell: Python (pyspark) -->
```python
stages = json.load(urllib.request.urlopen(UI + "/stages"))
for s in sorted(stages, key=lambda s: s["stageId"])[-4:]:
    print(s["stageId"], s["name"][:22], "| Shuffle Write:", s["shuffleWriteBytes"],
          "B | Shuffle Read:", s["shuffleReadBytes"], "B")

parsed_rdd.unpersist()
```

**Kết quả mong đợi (số hiệu stage trên máy bạn có thể khác):**

```text
25 reduceByKey at <stdin> | Shuffle Write: 1864 B | Shuffle Read: 0 B
26 collect at <stdin>:1 | Shuffle Write: 0 B | Shuffle Read: 1864 B
27 groupByKey at <stdin>: | Shuffle Write: 3445384 B | Shuffle Read: 0 B
28 collect at <stdin>:1 | Shuffle Write: 0 B | Shuffle Read: 3445384 B
parsed_rdd PythonRDD[27] at RDD at PythonRDD.scala:59
```

`reduceByKey` cộng dồn cục bộ trong từng partition trước khi shuffle (map-side combine), nên mỗi partition chỉ gửi 10 cặp (url, tổng) — tổng cộng 1.864 byte. `groupByKey` phải chuyển **mọi** giá trị (1 triệu số nguyên) qua mạng rồi mới cộng ở phía nhận — 3.445.384 byte, gấp khoảng 1.850 lần. Ghi ba số này (Shuffle Write của hai cách và tỉ lệ) vào báo cáo.

> 💡 Đọc cột byte, đừng đọc cột số bản ghi shuffle: PySpark gửi dữ liệu theo lô đã pickle, nên "Shuffle Write Records" đếm số lô chứ không phải số cặp khoá–giá trị.

## PHẦN 4: SPARK 4 VÀ CHẾ ĐỘ ANSI SQL

> 💡 Bắt đầu Tuần 11 bằng một lần mở codespace mới: cụm tự bật, dữ liệu trên HDFS (Bước 2) vẫn còn, nhưng phiên PySpark shell của tuần trước đã mất. Chạy `lab status`, rồi mở lại PySpark shell như Bước 3 (terminal A) trước khi làm Bước 10; PHẦN 4 không cần các biến của PHẦN 2–3.

### Bước 10: ANSI mode bật mặc định — các lỗi điển hình và họ hàm try_*

Từ Spark 4.0, cấu hình `spark.sql.ansi.enabled` mặc định là `true`: Spark SQL tuân theo chuẩn ANSI, nghĩa là phép ép kiểu sai, chia cho 0, tràn số… **báo lỗi ngay** thay vì âm thầm trả `NULL` hoặc giá trị sai như Spark 3. Điều này giúp phát hiện dữ liệu bẩn sớm, nhưng mã viết cho Spark 3 có thể bị dừng giữa chừng. Tiếp tục trong PySpark shell:

<!-- shell: Python (pyspark) -->
```python
print(spark.conf.get("spark.sql.ansi.enabled"))
spark.sql("SELECT CAST('abc' AS INT) AS x").show()
```

**Kết quả mong đợi (rút gọn traceback):**

```text
true
{"ts": "2026-11-05 23:23:28.535", "level": "ERROR", "logger": "SQLQueryContextLogger", "msg": "[CAST_INVALID_INPUT] The value 'abc' of the type \"STRING\" cannot be cast to \"INT\" because it is malformed. ...
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  ...
pyspark.errors.exceptions.captured.NumberFormatException: [CAST_INVALID_INPUT] The value 'abc' of the type "STRING" cannot be cast to "INT" because it is malformed. Correct the value as per the syntax, or change its target type. Use `try_cast` to tolerate malformed input ...
== SQL (line 1, position 8) ==
SELECT CAST('abc' AS INT) AS x
       ^^^^^^^^^^^^^^^^^^
```

Thông báo lỗi của Spark 4 có **mã lỗi** trong ngoặc vuông (`CAST_INVALID_INPUT`), chỉ đúng vị trí gây lỗi trong câu SQL và gợi ý cách sửa (`Use try_cast ...`). Dòng JSON phía trên là log ERROR của PySpark, có thể bỏ qua. Thử tiếp các trường hợp còn lại; mỗi cặp gồm một lệnh gây lỗi và phiên bản `try_*` an toàn:

<!-- shell: Python (pyspark) -->
```python
spark.sql("SELECT TRY_CAST('abc' AS INT) AS x, CAST('123' AS INT) AS y").show()
spark.sql("SELECT 10 / 0 AS x").show()
spark.sql("SELECT TRY_DIVIDE(10, 0) AS x, 10 DIV 3 AS y").show()
spark.sql("SELECT 2147483647 + 1 AS x").show()
spark.sql("SELECT TRY_ADD(2147483647, 1) AS x, CAST(2147483647 AS BIGINT) + 1 AS y").show()
spark.sql("SELECT TO_TIMESTAMP('2025-02-30 08:00:00') AS x").show()
spark.sql("SELECT TRY_TO_TIMESTAMP('2025-02-30 08:00:00') AS x").show()
spark.sql("SELECT ELEMENT_AT(ARRAY(1, 2, 3), 5) AS x").show()
spark.sql("SELECT TRY_ELEMENT_AT(ARRAY(1, 2, 3), 5) AS x").show()
```

**Kết quả mong đợi (mỗi lỗi chỉ giữ dòng thông báo chính):**

```text
+----+---+
|   x|  y|
+----+---+
|NULL|123|
+----+---+
pyspark.errors.exceptions.captured.ArithmeticException: [DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error. ...
+----+---+
|   x|  y|
+----+---+
|NULL|  3|
+----+---+
pyspark.errors.exceptions.captured.ArithmeticException: [ARITHMETIC_OVERFLOW] integer overflow. Use 'try_add' to tolerate overflow and return NULL instead. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error. ...
+----+----------+
|   x|         y|
+----+----------+
|NULL|2147483648|
+----+----------+
pyspark.errors.exceptions.captured.DateTimeException: [CAST_INVALID_INPUT] The value '2025-02-30 08:00:00' of the type "STRING" cannot be cast to "TIMESTAMP" because it is malformed. ...
+----+
|   x|
+----+
|NULL|
+----+
pyspark.errors.exceptions.captured.ArrayIndexOutOfBoundsException: [INVALID_ARRAY_INDEX_IN_ELEMENT_AT] The index 5 is out of bounds. The array has 3 elements. Use `try_element_at` to tolerate accessing element at invalid index and ...
+----+
|   x|
+----+
|NULL|
+----+
```

Bảng tóm tắt (điền thêm vào báo cáo):

| Biểu thức | ANSI tắt (mặc định của Spark 3) | ANSI bật (mặc định của Spark 4) | Cách viết an toàn |
|---|---|---|---|
| `CAST('abc' AS INT)` | `NULL` | lỗi `CAST_INVALID_INPUT` | `TRY_CAST('abc' AS INT)` |
| `10 / 0` | `NULL` | lỗi `DIVIDE_BY_ZERO` | `TRY_DIVIDE(10, 0)` |
| `2147483647 + 1` (INT) | `-2147483648` (tràn vòng) | lỗi `ARITHMETIC_OVERFLOW` | `TRY_ADD(...)` hoặc ép sang `BIGINT` trước |
| `TO_TIMESTAMP('2025-02-30 ...')` | `NULL` | lỗi `CAST_INVALID_INPUT` | `TRY_TO_TIMESTAMP(...)` |
| `ELEMENT_AT(ARRAY(1,2,3), 5)` | `NULL` | lỗi `INVALID_ARRAY_INDEX_IN_ELEMENT_AT` | `TRY_ELEMENT_AT(...)` |

Kiểm chứng cột "ANSI tắt" bằng cách tạm tắt ANSI **trong phiên hiện tại** rồi bật lại:

<!-- shell: Python (pyspark) -->
```python
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.sql("SELECT CAST('abc' AS INT) AS x, 10 / 0 AS y, 2147483647 + 1 AS z").show()
spark.conf.set("spark.sql.ansi.enabled", "true")
```

**Kết quả mong đợi:**

```text
+----+----+-----------+
|   x|   y|          z|
+----+----+-----------+
|NULL|NULL|-2147483648|
+----+----+-----------+
```

> ⚠️ Tắt ANSI (`--conf spark.sql.ansi.enabled=false`) chỉ nên dùng tạm thời khi chạy lại mã cũ. Giá trị `-2147483648` ở cột `z` là kết quả **sai** mà không có cảnh báo nào — chính là loại lỗi âm thầm mà ANSI mode được bật để ngăn.

### Bước 11: Dữ liệu bẩn — đọc CSV với schema, PERMISSIVE/FAILFAST, cast và try_cast

Tạo một tệp CSV nhỏ có lỗi cố ý: số lượng là chữ (`abc`), đơn giá dùng dấu phẩy thập phân (`1,5`), ngày không tồn tại (30/02), số lượng bằng 0. Trong **terminal B** (mở ở Bước 5), chạy lệnh sau để tạo tệp `src/lab5/du_lieu_ban.csv`, tức `/src/lab5/du_lieu_ban.csv` trong mọi container của cụm. Cũng có thể tạo tệp bằng trình soạn thảo VS Code (chuột phải thư mục `src/lab5` → New File) rồi dán nội dung:

<!-- shell: Terminal codespace (terminal B) -->
```bash
cat > src/lab5/du_lieu_ban.csv <<'EOF'
transaction_id,quantity,unit_price,discount,ts
T1,2,150000,0.1,2025-03-01 10:00:00
T2,abc,200000,0,2025-03-01 11:00:00
T3,1,"1,5",0,2025-03-01 12:00:00
T4,3,99000,0.2,2025-02-30 08:00:00
T5,0,50000,0,2025-03-02 09:15:00
EOF
```

Quay lại PySpark shell ở terminal A. Đọc không schema (mọi cột là chuỗi), rồi thử ép kiểu bằng `cast`:

<!-- shell: Python (pyspark) -->
```python
from pyspark.sql import functions as F
raw = spark.read.option("header", True).csv("file:///src/lab5/du_lieu_ban.csv")
raw.printSchema()
raw.select("transaction_id", F.col("quantity").cast("int").alias("q")).show()
```

**Kết quả mong đợi (rút gọn):**

```text
root
 |-- transaction_id: string (nullable = true)
 |-- quantity: string (nullable = true)
 |-- unit_price: string (nullable = true)
 |-- discount: string (nullable = true)
 |-- ts: string (nullable = true)
26/11/05 23:23:33 WARN TaskSetManager: Lost task 0.0 in stage 36.0 (TID 108) (nodemanager1 executor 1): org.apache.spark.SparkNumberFormatException: [CAST_INVALID_INPUT] The value 'abc' of the type "STRING" cannot be cast to "INT" ...
26/11/05 23:23:34 ERROR TaskSetManager: Task 0 in stage 36.0 failed 4 times; aborting job
...
pyspark.errors.exceptions.captured.NumberFormatException: [CAST_INVALID_INPUT] The value 'abc' of the type "STRING" cannot be cast to "INT" because it is malformed. ...
== DataFrame ==
"cast" was called from
<stdin>:1
```

Khác với Bước 10 (lỗi ngay khi phân tích hằng số), ở đây lỗi xảy ra **trong task trên executor** (`nodemanager1 executor 1`; số stage, TID và tên executor trên máy bạn có thể khác); Spark thử lại task 4 lần rồi mới huỷ job. Với DataFrame API, thông báo chỉ ra dòng mã Python gây lỗi (`"cast" was called from <stdin>:1`). Làm sạch bằng `try_cast`, `regexp_replace` và `try_to_timestamp`:

<!-- shell: Python (pyspark) -->
```python
clean = raw.select(
    "transaction_id",
    F.expr("try_cast(quantity AS INT)").alias("quantity"),
    F.expr("try_cast(regexp_replace(unit_price, ',', '.') AS DOUBLE)").alias("unit_price"),
    F.expr("try_to_timestamp(ts, 'yyyy-MM-dd HH:mm:ss')").alias("ts"))
clean.show()
clean.select("transaction_id", (F.lit(100) / F.col("quantity")).alias("x")).show()
clean.select("transaction_id", F.try_divide(F.lit(100), F.col("quantity")).alias("x")).show()
```

**Kết quả mong đợi (rút gọn):**

```text
+--------------+--------+----------+-------------------+
|transaction_id|quantity|unit_price|                 ts|
+--------------+--------+----------+-------------------+
|            T1|       2|  150000.0|2025-03-01 10:00:00|
|            T2|    NULL|  200000.0|2025-03-01 11:00:00|
|            T3|       1|       1.5|2025-03-01 12:00:00|
|            T4|       3|   99000.0|               NULL|
|            T5|       0|   50000.0|2025-03-02 09:15:00|
+--------------+--------+----------+-------------------+
26/11/05 23:23:36 WARN TaskSetManager: Lost task 0.0 in stage 38.0 (TID 113) (nodemanager1 executor 1): org.apache.spark.SparkArithmeticException: [DIVIDE_BY_ZERO] Division by zero. ...
...
pyspark.errors.exceptions.captured.ArithmeticException: [DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. ...
== DataFrame ==
"__truediv__" was called from
<stdin>:1
+--------------+------------------+
|transaction_id|                 x|
+--------------+------------------+
|            T1|              50.0|
|            T2|              NULL|
|            T3|             100.0|
|            T4|33.333333333333336|
|            T5|              NULL|
+--------------+------------------+
```

Cách thứ hai: khai báo schema ngay khi đọc. Bộ đọc CSV **không** áp dụng quy tắc ANSI: ở chế độ mặc định `PERMISSIVE`, giá trị không parse được thành `NULL`; ở chế độ `FAILFAST`, gặp bản ghi lỗi đầu tiên là dừng.

<!-- shell: Python (pyspark) -->
```python
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType, TimestampType)
sch = StructType([StructField("transaction_id", StringType()),
                  StructField("quantity", IntegerType()),
                  StructField("unit_price", DoubleType()),
                  StructField("discount", DoubleType()),
                  StructField("ts", TimestampType())])
path = "file:///src/lab5/du_lieu_ban.csv"
spark.read.option("header", True).schema(sch).csv(path).show()
spark.read.option("header", True).option("mode", "FAILFAST").schema(sch).csv(path).show()
```

**Kết quả mong đợi (rút gọn):**

```text
+--------------+--------+----------+--------+-------------------+
|transaction_id|quantity|unit_price|discount|                 ts|
+--------------+--------+----------+--------+-------------------+
|            T1|       2|  150000.0|     0.1|2025-03-01 10:00:00|
|            T2|    NULL|  200000.0|     0.0|2025-03-01 11:00:00|
|            T3|       1|      NULL|     0.0|2025-03-01 12:00:00|
|            T4|       3|   99000.0|     0.2|               NULL|
|            T5|       0|   50000.0|     0.0|2025-03-02 09:15:00|
+--------------+--------+----------+--------+-------------------+
...
py4j.protocol.Py4JJavaError: An error occurred while calling o484.showString.
: org.apache.spark.SparkException: [FAILED_READ_FILE.NO_HINT] Encountered error while reading file file:///src/lab5/du_lieu_ban.csv.  SQLSTATE: KD001
Caused by: org.apache.spark.SparkException: [MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION] Malformed records are detected in record parsing: [T2,null,200000.0,0.0,1740801600000000].
Parse Mode: FAILFAST. To process malformed records as null result, try setting the option 'mode' as 'PERMISSIVE'.  SQLSTATE: 22023
Caused by: java.lang.NumberFormatException: For input string: "abc"
```

So sánh với cách `try_cast` + `regexp_replace`: đọc thẳng bằng schema làm mất giá trị `1,5` của T3 (thành `NULL`), còn cách làm sạch từ chuỗi giữ lại được 1.5. Thoát PySpark shell bằng `exit()` trước khi sang PHẦN 5 — trên Codespaces điều này **bắt buộc**: shell đang mở giữ cả 4 core của cụm (các `spark-submit` sau sẽ đứng chờ) và giữ RAM của driver cùng hai executor.

> 💡 Quy tắc thực hành với Spark 4: đọc dữ liệu thô bằng schema tường minh (hoặc toàn chuỗi), làm sạch bằng `try_cast`/`try_to_timestamp`, rồi **đếm số `NULL` sinh ra** để biết bao nhiêu bản ghi hỏng, thay vì tắt ANSI.

> 💡 Đường dẫn `file:///src/...` đọc tệp cục bộ; khi chạy trên cụm, **mọi** executor phải thấy cùng đường dẫn. Trong môi trường này `nodemanager1/2` đều mount `./src` nên đọc được; trên cụm thật hãy đưa tệp lên HDFS.

## PHẦN 5: DATAFRAME VỚI SCHEMA TƯỜNG MINH VÀ PARQUET

Từ phần này, mã được viết thành script và chạy bằng `spark-submit` (chương trình hoàn chỉnh, chạy lại được, phù hợp nộp bài). Schema dùng chung đặt trong một module riêng. Tạo các tệp trong thư mục `src/lab5` của repo bằng trình soạn thảo VS Code (hoặc giải nén gói mã nguồn của giảng viên vào `src/`).

> ⚠️ Trên máy 2 nhân / 8 GB, các job của PHẦN 5–6 đẩy RAM các container (`lab status`) lên khoảng 4,7 GB; đo trong bản sao, đỉnh của cả codespace là 5,7 GB khi chạy Bước 14, chưa kể VS Code server (khoảng 1,8 GB). Trước khi chạy: đã `exit()` PySpark shell, đã `lab stop-extras` nếu còn Hive/HBase/MongoDB/Kafka từ buổi trước, và chỉ chạy **một** `spark-submit` mỗi lần.

### Bước 12: Doanh thu theo tháng và khu vực bằng DataFrame

Không dùng `inferSchema=true`: Spark phải đọc thêm một lượt toàn bộ 405 MB chỉ để đoán kiểu và có thể đoán sai. Khai báo `StructType` giúp kiểm soát kiểu và đọc nhanh hơn.

<!-- file: src/lab5/schema_retail.py -->
```python
"""Schema tường minh dùng chung cho các script Buổi 5."""
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType, TimestampType)

RETAIL_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("store_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("ts", TimestampType(), True),
])
CSV_PATH = "hdfs://namenode:9000/data/raw/retail_transactions.csv"
PARQUET_PATH = "hdfs://namenode:9000/user/student/parquet/transactions"
```

<!-- file: src/lab5/buoc_doanh_thu.py -->
```python
"""Buổi 5: DataFrame với schema tường minh, doanh thu theo tháng và khu vực."""
import time
from pyspark.sql import SparkSession, functions as F
from schema_retail import RETAIL_SCHEMA, CSV_PATH

spark = SparkSession.builder.appName("Buoi5_DoanhThu").getOrCreate()
print("ANSI mode:", spark.conf.get("spark.sql.ansi.enabled"))

df = (spark.read.option("header", True)
      .option("mode", "PERMISSIVE")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(RETAIL_SCHEMA)
      .csv(CSV_PATH)
      .withColumn("region", F.trim("region")))
df.printSchema()

t0 = time.time()
df.cache()
print("Tong so dong:", df.count(), f"({time.time() - t0:.1f} s, lan dau doc CSV + cache)")
print("So dong co ts NULL:", df.filter(F.col("ts").isNull()).count())

revenue = (df.withColumn("net_amount",
                         F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")))
             .withColumn("month", F.date_format("ts", "yyyy-MM")))

by_month_region = (revenue.groupBy("month", "region")
    .agg(F.round(F.sum("net_amount"), 0).cast("long").alias("total_revenue"),
         F.count("transaction_id").alias("so_giao_dich"))
    .orderBy("month", F.desc("total_revenue")))
by_month_region.show(10, truncate=False)

out = "hdfs://namenode:9000/user/student/output/revenue_by_month_region"
by_month_region.coalesce(1).write.mode("overwrite").option("header", True).csv(out)

# Kiểm chứng: tổng các khu vực trong tháng 2025-03 == tổng tính trực tiếp
a = by_month_region.filter("month = '2025-03'").agg(F.sum("total_revenue")).first()[0]
b = revenue.filter("month = '2025-03'").agg(F.round(F.sum("net_amount"), 0).cast("long")).first()[0]
print(f"Kiem chung 2025-03: {a:,.0f} vs {b:,.0f} -> chenh lech {abs(a - b):,.0f}")
spark.stop()
```

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.log.level=WARN \
  --py-files /src/lab5/schema_retail.py /src/lab5/buoc_doanh_thu.py
```

**Kết quả mong đợi (toàn bộ job khoảng 48 giây trên codespace 2 nhân / 8 GB, 29 giây trên máy 4 nhân):**

```text
Setting Spark log level to "WARN".
ANSI mode: true
root
 |-- transaction_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- category: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- unit_price: double (nullable = true)
 |-- discount: double (nullable = true)
 |-- store_id: string (nullable = true)
 |-- region: string (nullable = true)
 |-- ts: timestamp (nullable = true)
Tong so dong: 5000000 (22.1 s, lan dau doc CSV + cache)
So dong co ts NULL: 0
+-------+--------+-------------+------------+
|month  |region  |total_revenue|so_giao_dich|
+-------+--------+-------------+------------+
|2025-01|HCM     |9895995515450|290297      |
|2025-01|Hanoi   |6595625912650|194050      |
|2025-01|DaNang  |1762320891450|51563       |
|2025-01|CanTho  |1319904354000|38720       |
|2025-01|HaiPhong|1094099087250|32006       |
|2025-01|NgheAn  |660250594600 |19393       |
|2025-01|LamDong |646341465550 |19264       |
|2025-02|HCM     |8900430204000|261841      |
|2025-02|Hanoi   |5975254991950|175198      |
|2025-02|DaNang  |1567857500950|46519       |
+-------+--------+-------------+------------+
only showing top 10 rows
Kiem chung 2025-03: 22,012,826,011,400 vs 22,012,826,011,400 -> chenh lech 0
```

Kiểm tra kết quả đã ghi lên HDFS:

<!-- shell: Terminal codespace -->
```bash
docker exec -it namenode hdfs dfs -ls /user/student/output/revenue_by_month_region
docker exec -it namenode bash -c "hdfs dfs -cat /user/student/output/revenue_by_month_region/part-00000* | head -5"
```

**Kết quả mong đợi:**

```text
Found 2 items
-rw-r--r--   2 root supergroup          0 2026-11-05 23:24 /user/student/output/revenue_by_month_region/_SUCCESS
-rw-r--r--   2 root supergroup       1991 2026-11-05 23:24 /user/student/output/revenue_by_month_region/part-00000-10b9527f-f39c-4161-bb2d-6604ebec354c-c000.csv
month,region,total_revenue,so_giao_dich
2025-01,HCM,9895995515450,290297
2025-01,Hanoi,6595625912650,194050
2025-01,DaNang,1762320891450,51563
2025-01,CanTho,1319904354000,38720
```

Nhận xét:

- `printSchema()` báo mọi cột `nullable = true`, kể cả `transaction_id` đã khai báo `False`: nguồn CSV luôn coi cột là có thể NULL, ràng buộc NOT NULL phải tự kiểm tra (ví dụ `df.filter(F.col("transaction_id").isNull()).count()`).
- Doanh thu HCM tháng 01/2025 (9.895.995.515.450) trùng khớp với kết quả tính bằng MongoDB ở Buổi 4 — hai hệ thống, cùng dữ liệu, cùng con số.
- `coalesce(1)` gom kết quả nhỏ (56 dòng) thành một tệp `part-00000-...csv` để dễ đọc; không dùng `coalesce(1)` cho dữ liệu lớn vì khi đó chỉ một task ghi toàn bộ.
- `F.round(...).cast("long")` cho số nguyên dễ đọc thay cho dạng `9.89599551545E12` của kiểu `double`. Phép ép `double → long` vẫn chịu kiểm tra ANSI: nếu giá trị vượt phạm vi `long`, Spark báo `CAST_OVERFLOW` thay vì cắt số.

> 💡 `--py-files` gửi `schema_retail.py` tới mọi executor; ở script này schema chỉ dùng trên driver nên không bắt buộc, nhưng nên giữ thói quen khi module được gọi bên trong hàm chạy trên executor. `--conf spark.log.level=WARN` ẩn các dòng `INFO`.

> 💡 Trong lúc job chạy, mở link cổng 4040 của `lab ui` (hoặc tab PORTS → 4040) để theo dõi tab Jobs/Stages/SQL. Cũng có thể kiểm tra trong một terminal codespace khác: `curl -s localhost:4040/api/v1/applications` trả về tên ứng dụng `Buoi5_...` đang chạy; khi job kết thúc, cổng 4040 không còn trả lời — xem lại ở Spark History (cổng 18080, Bước 16).

### Bước 13: Chuyển sang Parquet — dung lượng, tốc độ và kế hoạch thực thi

Parquet là định dạng **cột** có nén (mặc định snappy) và lưu sẵn thống kê min/max cho từng nhóm hàng, nên Spark chỉ đọc các cột cần (column pruning) và bỏ qua nhóm hàng không thoả điều kiện (predicate pushdown). Khi ghi với `partitionBy("month")`, mỗi tháng là một thư mục con và điều kiện trên `month` loại bỏ cả thư mục (partition pruning).

<!-- file: src/lab5/buoc_parquet.py -->
```python
"""Buổi 5: chuyển CSV -> Parquet, so sánh dung lượng, thời gian, pushdown."""
import time
from pyspark.sql import SparkSession, functions as F
from schema_retail import RETAIL_SCHEMA, CSV_PATH, PARQUET_PATH

spark = SparkSession.builder.appName("Buoi5_Parquet").getOrCreate()
csv_df = (spark.read.option("header", True).schema(RETAIL_SCHEMA)
          .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(CSV_PATH))

# 1) Ghi Parquet (nén snappy mặc định) và Parquet phân vùng theo tháng
t0 = time.time()
csv_df.write.mode("overwrite").parquet(PARQUET_PATH)
print(f"Ghi Parquet: {time.time() - t0:.1f} s")
t0 = time.time()
(csv_df.withColumn("month", F.date_format("ts", "yyyy-MM"))
       .write.mode("overwrite").partitionBy("month").parquet(PARQUET_PATH + "_by_month"))
print(f"Ghi Parquet partitionBy(month): {time.time() - t0:.1f} s")

pq_df = spark.read.parquet(PARQUET_PATH)
pqm_df = spark.read.parquet(PARQUET_PATH + "_by_month")
print("So dong CSV / Parquet:", csv_df.count(), "/", pq_df.count())


def truy_van(df):
    return (df.filter(F.col("region") == "CanTho")
              .groupBy("category").agg(F.sum("quantity").alias("sl")))


def do_thoi_gian(ten, df, lan=3):
    truy_van(df).collect()                         # lượt làm nóng
    t0 = time.time()
    for _ in range(lan):
        truy_van(df).collect()                     # mỗi lần tạo DataFrame mới (xem 💡 bên dưới)
    print(f"{ten:<28} {(time.time() - t0) / lan:6.2f} s/lan")


do_thoi_gian("CSV (schema tuong minh)", csv_df)
do_thoi_gian("Parquet", pq_df)

# 2) Kế hoạch thực thi: column pruning + predicate pushdown + partition pruning
pq_df.filter(F.col("region") == "CanTho").select("category", "quantity").explain()
pqm_df.filter("month = '2025-03' AND region = 'CanTho'").select("category").explain()
print("Dong thang 2025-03:", pqm_df.filter("month = '2025-03'").count())
spark.stop()
```

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.log.level=WARN \
  --py-files /src/lab5/schema_retail.py /src/lab5/buoc_parquet.py
```

**Kết quả mong đợi (toàn bộ job khoảng 1 phút 22 giây trên codespace 2 nhân, 46 giây trên máy 4 nhân; thời gian là ví dụ; kế hoạch thực thi rút gọn):**

```text
Ghi Parquet: 22.7 s
Ghi Parquet partitionBy(month): 21.9 s
So dong CSV / Parquet: 5000000 / 5000000
CSV (schema tuong minh)        2.77 s/lan
Parquet                        0.68 s/lan
== Physical Plan ==
*(1) Project [category#36, quantity#37]
+- *(1) Filter (isnotnull(region#41) AND (region#41 = CanTho))
   +- *(1) ColumnarToRow
      +- FileScan parquet [category#36,quantity#37,region#41] Batched: true, ..., PartitionFilters: [], PushedFilters: [IsNotNull(region), EqualTo(region,CanTho)], ReadSchema: struct<category:string,quantity:int,region:string>
== Physical Plan ==
*(1) Project [category#46]
+- *(1) Filter (isnotnull(region#51) AND (region#51 = CanTho))
   +- *(1) ColumnarToRow
      +- FileScan parquet [category#46,region#51,month#53] Batched: true, ..., PartitionFilters: [isnotnull(month#53), (month#53 = 2025-03)], PushedFilters: [IsNotNull(region), EqualTo(region,CanTho)], ReadSchema: struct<category:string,region:string>
Dong thang 2025-03: 646616
```

So sánh dung lượng trên HDFS:

<!-- shell: Terminal codespace -->
```bash
docker exec -it namenode hdfs dfs -du -s -h /data/raw/retail_transactions.csv /user/student/parquet/transactions /user/student/parquet/transactions_by_month
docker exec -it namenode hdfs dfs -ls /user/student/parquet/transactions_by_month
```

**Kết quả mong đợi (rút gọn):**

```text
386.4 M  772.8 M  /data/raw/retail_transactions.csv
118.0 M  236.0 M  /user/student/parquet/transactions
121.6 M  243.2 M  /user/student/parquet/transactions_by_month
Found 9 items
-rw-r--r--   2 root supergroup          0 2026-11-05 23:26 /user/student/parquet/transactions_by_month/_SUCCESS
drwxr-xr-x   - root supergroup          0 2026-11-05 23:26 /user/student/parquet/transactions_by_month/month=2025-01
drwxr-xr-x   - root supergroup          0 2026-11-05 23:26 /user/student/parquet/transactions_by_month/month=2025-02
...
```

Đọc kết quả:

- Parquet chỉ chiếm 118 MiB, bằng khoảng 31% CSV (386 MiB); truy vấn lọc theo khu vực nhanh hơn khoảng 4 lần (0,68 s so với 2,77 s mỗi lần trên máy 2 nhân; máy 4 nhân: 0,49 s so với 1,44 s).
- `ReadSchema` chỉ gồm 3 cột cần dùng trên tổng 10 cột (column pruning); `PushedFilters` cho thấy điều kiện `region = 'CanTho'` được đẩy xuống bộ đọc Parquet.
- Với bảng phân vùng, `PartitionFilters: [... (month#53 = 2025-03)]` nghĩa là Spark chỉ mở thư mục `month=2025-03` (646.616 dòng thay vì 5 triệu). `ReadSchema` không chứa `month` vì giá trị cột phân vùng lấy từ tên thư mục.

> 💡 Vì sao hàm `do_thoi_gian` tạo DataFrame mới (`truy_van(df)`) ở mỗi lần đo? Nếu gọi `collect()` nhiều lần trên **cùng một** DataFrame có shuffle, Spark tái sử dụng tệp shuffle của lần trước và bỏ qua stage đọc dữ liệu (Spark UI ghi "skipped"), khiến số đo gần bằng 0 và sai lệch. Trong lần thử đầu của tài liệu này, cách đo sai đó cho kết quả 0,04 s/lần với CSV.

## PHẦN 6: SPARK SQL VÀ CATALYST

### Bước 14: Phân tích RFM bằng DataFrame API và bằng Spark SQL

RFM chấm điểm khách hàng theo Recency (số ngày từ lần mua gần nhất tới ngày mốc), Frequency (số giao dịch) và Monetary (tổng tiền). Ta viết cùng một phép tính bằng hai API, rồi chứng minh Catalyst Optimizer sinh ra **cùng** kế hoạch. Script đọc từ Parquet đã tạo ở Bước 13.

<!-- file: src/lab5/buoc_rfm.py -->
```python
"""Buổi 5: RFM bằng DataFrame API và Spark SQL, đối chiếu kết quả và kế hoạch."""
from pyspark.sql import SparkSession, functions as F
from schema_retail import PARQUET_PATH

spark = SparkSession.builder.appName("Buoi5_RFM").getOrCreate()
revenue = (spark.read.parquet(PARQUET_PATH)
           .withColumn("net_amount",
                       F.col("quantity") * F.col("unit_price") * (1 - F.col("discount"))))

ref_date = revenue.agg(F.to_date(F.max("ts"))).first()[0]      # datetime.date
print("Ngay moc:", ref_date)

# Cách 1: DataFrame API
rfm_df = (revenue.groupBy("customer_id")
          .agg(F.datediff(F.lit(ref_date), F.max("ts")).alias("recency"),
               F.countDistinct("transaction_id").alias("frequency"),
               F.round(F.sum("net_amount"), 0).cast("long").alias("monetary")))

# Cách 2: Spark SQL trên temp view
revenue.createOrReplaceTempView("giao_dich")
rfm_sql = spark.sql(f"""
    SELECT customer_id,
           DATEDIFF(DATE'{ref_date}', MAX(ts))   AS recency,
           COUNT(DISTINCT transaction_id)      AS frequency,
           CAST(ROUND(SUM(net_amount), 0) AS BIGINT) AS monetary
    FROM giao_dich
    GROUP BY customer_id
""")

rfm_df.orderBy(F.desc("monetary")).show(5)
rfm_sql.orderBy(F.desc("monetary")).show(5)

print("So khach hang:", rfm_df.count(), "/", rfm_sql.count())
diff = rfm_df.exceptAll(rfm_sql).count() + rfm_sql.exceptAll(rfm_df).count()
print("So dong khac nhau giua 2 cach:", diff)
print("Cung ngu nghia (sameSemantics):", rfm_df.sameSemantics(rfm_sql))
rfm_df.explain()
rfm_sql.explain()

# Chấm điểm R, F, M (1..4) theo tứ phân vị xấp xỉ (không cần cửa sổ toàn cục)
qs = rfm_df.approxQuantile(["recency", "frequency", "monetary"], [0.25, 0.5, 0.75], 0.01)
print("Tu phan vi recency/frequency/monetary:", qs)


def diem(cot, q, dao_nguoc=False):
    d = (F.when(F.col(cot) <= q[0], 1).when(F.col(cot) <= q[1], 2)
          .when(F.col(cot) <= q[2], 3).otherwise(4))
    return (5 - d) if dao_nguoc else d       # recency càng nhỏ càng tốt -> đảo điểm


seg = (rfm_df.withColumn("R", diem("recency", qs[0], dao_nguoc=True))
             .withColumn("F", diem("frequency", qs[1]))
             .withColumn("M", diem("monetary", qs[2])))
(seg.groupBy("R").agg(F.count("*").alias("so_kh"),
                      F.round(F.avg("frequency"), 1).alias("frequency_tb"),
                      F.round(F.avg("monetary"), 0).cast("long").alias("monetary_tb"))
    .orderBy(F.desc("R")).show())
seg.write.mode("overwrite").parquet("hdfs://namenode:9000/user/student/output/rfm")
print("Da ghi /user/student/output/rfm")
spark.stop()
```

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.log.level=WARN \
  --py-files /src/lab5/schema_retail.py /src/lab5/buoc_rfm.py
```

**Kết quả mong đợi (khoảng 2 phút 6 giây trên codespace 2 nhân, 1 phút 26 giây trên máy 4 nhân — job dài nhất của buổi; kế hoạch thứ hai rút gọn vì giống kế hoạch thứ nhất):**

```text
Ngay moc: 2025-08-28
+-----------+-------+---------+----------+
|customer_id|recency|frequency|  monetary|
+-----------+-------+---------+----------+
|   C0013719|      1|      114|4587098800|
|   C0007566|      1|      115|4585773550|
|   C0000119|      4|      100|4563143350|
|   C0030000|      0|      116|4487218550|
|   C0002348|      7|      110|4460548200|
+-----------+-------+---------+----------+
only showing top 5 rows
+-----------+-------+---------+----------+
|customer_id|recency|frequency|  monetary|
+-----------+-------+---------+----------+
|   C0013719|      1|      114|4587098800|
...
So khach hang: 199995 / 199995
So dong khac nhau giua 2 cach: 0
Cung ngu nghia (sameSemantics): True
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[customer_id#1], functions=[max(ts#9), sum(net_amount#11), count(distinct transaction_id#0)])
   +- Exchange hashpartitioning(customer_id#1, 200), ENSURE_REQUIREMENTS, [plan_id=1331]
      +- HashAggregate(keys=[customer_id#1], functions=[merge_max(ts#9), merge_sum(net_amount#11), partial_count(distinct transaction_id#0)])
         +- HashAggregate(keys=[customer_id#1, transaction_id#0], functions=[merge_max(ts#9), merge_sum(net_amount#11)])
            +- Exchange hashpartitioning(customer_id#1, transaction_id#0, 200), ENSURE_REQUIREMENTS, [plan_id=1327]
               +- HashAggregate(keys=[customer_id#1, transaction_id#0], functions=[partial_max(ts#9), partial_sum(net_amount#11)])
                  +- Project [transaction_id#0, customer_id#1, ts#9, ((cast(quantity#4 as double) * unit_price#5) * (1.0 - discount#6)) AS net_amount#11]
                     +- FileScan parquet [transaction_id#0,customer_id#1,quantity#4,unit_price#5,discount#6,ts#9] Batched: true, DataFilters: [], ..., PushedFilters: [], ReadSchema: struct<transaction_i...
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
...
Tu phan vi recency/frequency/monetary: [[3.0, 11.0, 27.0], [8.0, 11.0, 15.0], [261911900.0, 370770000.0, 554922800.0]]
+---+-----+------------+-----------+
|  R|so_kh|frequency_tb|monetary_tb|
+---+-----+------------+-----------+
|  4|54759|        52.1| 1773979159|
|  3|47827|        24.7|  839506837|
|  2|47631|        11.1|  377052688|
|  1|49778|         8.8|  300140933|
+---+-----+------------+-----------+
Da ghi /user/student/output/rfm
```

Nhận xét:

- `exceptAll` hai chiều bằng 0 và `sameSemantics` trả `True`: hai cách viết cho cùng kết quả và cùng kế hoạch logic sau tối ưu; kế hoạch vật lý chỉ khác số `plan_id`. Tứ phân vị của `monetary` là giá trị **xấp xỉ** (sai số tương đối 0,01) nên có thể lệch nhẹ giữa các lần chạy (lần chạy trên máy phòng lab cho `262262500.0, 370825600.0`); bảng điểm R bên dưới không đổi. DataFrame API và SQL chỉ là hai "mặt tiền" của cùng một bộ máy Catalyst.
- `countDistinct` khiến kế hoạch có **hai** lần `Exchange` (shuffle): lần đầu theo `(customer_id, transaction_id)` để khử trùng lặp, lần sau theo `customer_id`. `Exchange hashpartitioning(..., 200)` dùng 200 partition mặc định (`spark.sql.shuffle.partitions`); `AdaptiveSparkPlan` (AQE) sẽ gộp bớt các partition nhỏ lúc chạy.
- Nhóm R = 4 (mua gần nhất) có tần suất và chi tiêu trung bình cao nhất — đúng với dữ liệu sinh theo quy luật Pareto (20% khách tạo khoảng 60% giao dịch). Các nhóm không đều 25% vì nhiều khách có cùng `recency` (giá trị nguyên).

> 💡 Bài cũ đọc lại CSV với `inferSchema=True` và gọi `spark.read.table("giao_dich")` để lấy temp view của job trước. Temp view chỉ sống trong **một** SparkSession: khi job Bước 12 kết thúc (`spark.stop()`), view biến mất. Muốn chia sẻ dữ liệu giữa các job, ghi ra Parquet trên HDFS như Bước 13.

> 💡 Chấm điểm bằng `approxQuantile` thay vì `ntile(4).over(Window.orderBy(...))`: cửa sổ không có `partitionBy` buộc Spark dồn toàn bộ dữ liệu về **một** partition (Spark in cảnh báo `No Partition Defined for Window operation! Moving all data to a single partition`).

### Bước 15: Spark SQL — JOIN với bảng sản phẩm và hàm cửa sổ

<!-- file: src/lab5/buoc_sql.py -->
```python
"""Buổi 5: Spark SQL — temp view, JOIN với bảng sản phẩm, hàm cửa sổ, ANSI-safe."""
from pyspark.sql import SparkSession
from schema_retail import PARQUET_PATH

spark = SparkSession.builder.appName("Buoi5_SparkSQL").getOrCreate()
spark.read.parquet(PARQUET_PATH).createOrReplaceTempView("giao_dich")
(spark.read.option("header", True).option("inferSchema", True)
      .csv("hdfs://namenode:9000/data/raw/products.csv")
      .createOrReplaceTempView("san_pham"))
spark.sql("SHOW VIEWS").show()
spark.sql("DESCRIBE san_pham").show()

top_brand = spark.sql("""
    WITH dt AS (
        SELECT g.region, p.brand,
               SUM(g.quantity * g.unit_price * (1 - g.discount)) AS doanh_thu
        FROM giao_dich g JOIN san_pham p ON g.product_id = p.product_id
        GROUP BY g.region, p.brand
    )
    SELECT region, brand, CAST(ROUND(doanh_thu) AS BIGINT) AS doanh_thu, hang
    FROM (SELECT *, RANK() OVER (PARTITION BY region ORDER BY doanh_thu DESC) AS hang FROM dt)
    WHERE hang <= 3
    ORDER BY region, hang
""")
top_brand.show(21, truncate=False)
top_brand.explain()

# Tỉ lệ giảm giá trung bình theo danh mục: dùng try_divide để an toàn với ANSI
spark.sql("""
    SELECT category,
           ROUND(TRY_DIVIDE(SUM(quantity * unit_price * discount),
                            SUM(quantity * unit_price)) * 100, 2) AS pct_giam_gia,
           COUNT_IF(discount > 0) AS so_don_co_giam_gia
    FROM giao_dich GROUP BY category ORDER BY pct_giam_gia DESC
""").show()
spark.stop()
```

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.log.level=WARN \
  --py-files /src/lab5/schema_retail.py /src/lab5/buoc_sql.py
```

**Kết quả mong đợi (khoảng 29 giây trên codespace 2 nhân, 21 giây trên máy 4 nhân; kế hoạch rút gọn):**

```text
+---------+---------+-----------+
|namespace| viewName|isTemporary|
+---------+---------+-----------+
|         |giao_dich|       true|
|         | san_pham|       true|
+---------+---------+-----------+
+------------+---------+-------+
|    col_name|data_type|comment|
+------------+---------+-------+
|  product_id|   string|   NULL|
|product_name|   string|   NULL|
|    category|   string|   NULL|
|       brand|   string|   NULL|
|  base_price|      int|   NULL|
+------------+---------+-------+
+--------+-------+-------------+----+
|region  |brand  |doanh_thu    |hang|
+--------+-------+-------------+----+
|CanTho  |Brand16|448264484750 |1   |
|CanTho  |Brand36|406780337350 |2   |
|CanTho  |Brand30|383084008050 |3   |
|DaNang  |Brand16|593450089850 |1   |
...
|NgheAn  |Brand30|193617532800 |3   |
+--------+-------+-------------+----+
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [region#8 ASC NULLS FIRST, hang#58 ASC NULLS FIRST], true, 0
   ...
            +- Window [rank(doanh_thu#60) windowspecdefinition(region#8, doanh_thu#60 DESC NULLS LAST, ...)) AS hang#58], [region#8], [doanh_thu#60 DESC NULLS LAST]
               +- WindowGroupLimit [region#8], [doanh_thu#60 DESC NULLS LAST], rank(doanh_thu#60), 3, Final
                  ...
                        +- WindowGroupLimit [region#8], [doanh_thu#60 DESC NULLS LAST], rank(doanh_thu#60), 3, Partial
                              ...
                                          +- BroadcastHashJoin [product_id#2], [product_id#27], Inner, BuildRight, false
                                             :- Filter isnotnull(product_id#2)
                                             :  +- FileScan parquet [product_id#2,quantity#4,unit_price#5,discount#6,region#8] ...
                                             +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=435]
                                                +- Filter isnotnull(product_id#27)
                                                   +- FileScan csv [product_id#27,brand#30] ...
+----------+------------+------------------+
|  category|pct_giam_gia|so_don_co_giam_gia|
+----------+------------+------------------+
|      sach|        9.33|            409361|
|  me_va_be|        9.29|            407626|
|   dien_tu|        9.28|            408676|
| thuc_pham|        9.28|            407667|
|  gia_dung|        9.28|            408761|
|  the_thao|        9.28|            407754|
|thoi_trang|        9.26|            408229|
+----------+------------+------------------+
```

Đọc kế hoạch thực thi:

- `BroadcastHashJoin ... BuildRight`: bảng `san_pham` (500 dòng, nhỏ hơn ngưỡng `spark.sql.autoBroadcastJoinThreshold` 10 MB) được gửi nguyên vẹn tới mọi executor, nên bảng 5 triệu dòng **không** phải shuffle để JOIN.
- `WindowGroupLimit ... 3, Partial` rồi `Final`: Spark (từ bản 3.5) nhận ra mẫu "RANK() ≤ 3" và cắt bớt ngay trong từng partition trước khi shuffle, thay vì xếp hạng toàn bộ rồi mới lọc.
- Câu truy vấn thứ hai dùng `TRY_DIVIDE` để an toàn với ANSI (nếu một danh mục có tổng tiền bằng 0, kết quả là `NULL` thay vì lỗi `DIVIDE_BY_ZERO`) và `COUNT_IF` để đếm có điều kiện.

### Bước 16: Chạy trên YARN và xem lại job ở Spark History Server

Cùng một script có thể chạy trên YARN chỉ bằng cách đổi `--master` — mã nguồn không đổi:

<!-- shell: Terminal codespace -->
```bash
time docker exec -it spark-master spark-submit --master yarn \
  --conf spark.log.level=WARN \
  --py-files /src/lab5/schema_retail.py /src/lab5/buoc_doanh_thu.py
docker exec -it resourcemanager yarn application -list -appStates FINISHED
```

**Kết quả mong đợi (rút gọn; thời gian là ví dụ):**

```text
...
Tong so dong: 5000000 (21.5 s, lan dau doc CSV + cache)
...
Kiem chung 2025-03: 22,012,826,011,400 vs 22,012,826,011,400 -> chenh lech 0

real	1m5.942s
...
                Application-Id	    Application-Name	    Application-Type	      User	     Queue	             State	       Final-State	       Progress	                       Tracking-URL
application_1793895631263_0001	      Buoi5_DoanhThu	               SPARK	      root	root.default	          FINISHED	         SUCCEEDED	           100%	historyserver:18080/history/application_1793895631263_0001/1
```

Kết quả tính toán giống hệt lần chạy standalone, nhưng tổng thời gian dài hơn (khoảng 66 giây so với 48 giây trên máy 2 nhân; 56 giây so với 29 giây trên máy 4 nhân) vì YARN phải khởi động ApplicationMaster và cấp container cho từng executor trước khi job bắt đầu (xem lại lý thuyết YARN ở Chương 5).

Mở Spark History Server (link "Spark History" của `lab ui`, cổng 18080): danh sách có mọi ứng dụng đã chạy trong buổi (`PySparkShell`, `Buoi5_DoanhThu`, `Buoi5_Parquet`, `Buoi5_RFM`, `Buoi5_SparkSQL`), cả standalone (`app-...`) lẫn YARN (`application_...`). Bấm vào `Buoi5_RFM` → tab "SQL / DataFrame" để xem đồ thị kế hoạch thực thi trực quan, hoặc tab "Stages" để so sánh Shuffle Read/Write của từng stage. Trên YARN UI (cổng 8088), liên kết "History" của ứng dụng trỏ tới `historyserver:18080` — trình duyệt không mở được; hãy mở link cổng 18080 của `lab ui` rồi chọn ứng dụng `application_...`. Có thể kiểm tra nhanh danh sách ứng dụng ngay trong terminal codespace:

<!-- shell: Terminal codespace -->
```bash
curl -s localhost:18080/api/v1/applications | jq -r '.[] | [.id, .name, .attempts[0].completed, .attempts[0].duration] | @tsv'
```

**Kết quả mong đợi (mã ứng dụng và thời lượng tính bằng mili giây trên máy bạn khác; số liệu từ codespace 2 nhân):**

```text
application_1793895631263_0001	Buoi5_DoanhThu	true	62997
app-20261105232850-0004	Buoi5_SparkSQL	true	25612
app-20261105232640-0003	Buoi5_RFM	true	122321
app-20261105232509-0002	Buoi5_Parquet	true	76388
app-20261105232412-0001	Buoi5_DoanhThu	true	44939
app-20261105232242-0000	PySparkShell	true	56272
```

> 💡 Spark UI cổng 4040 chỉ tồn tại khi ứng dụng còn chạy; History Server đọc lại event log mà mọi job ghi vào `hdfs:///spark-logs`, nên vẫn xem được sau khi job kết thúc — kể cả sau khi dừng và mở lại codespace.

## Kết thúc buổi

Làm sau mỗi tuần (cuối Tuần 10 và cuối Tuần 11). Dữ liệu trên HDFS (`/data/raw`, Parquet, kết quả) được giữ trong codespace khi dừng, nên Tuần 11 không phải đưa dữ liệu lên lại.

Thoát PySpark shell nếu còn mở (`exit()`), và tắt dịch vụ bổ sung nếu đã lỡ bật (buổi này không cần):

<!-- shell: Terminal codespace -->
```bash
lab stop-extras
```

Lưu bài làm lên GitHub. Thư mục `src/lab5/__pycache__` do `spark-submit` sinh ra (chủ sở hữu `root`) không cần nộp, xoá trước khi commit:

<!-- shell: Terminal codespace -->
```bash
rm -rf src/lab5/__pycache__
git add src/lab5
git commit -m "Buoi 5: Spark RDD, DataFrame, SQL"
git push
```

**Kết quả mong đợi (rút gọn; mã commit và số tệp trên máy bạn khác):**

```text
[main fdbd964] Buoi 5: Spark RDD, DataFrame, SQL
 ...
 create mode 100644 src/lab5/buoc_doanh_thu.py
 ...
```

Cuối cùng, dừng codespace để không tốn quota: github.com/codespaces → `...` cạnh tên codespace → **Stop codespace** (hoặc Command Palette → `Codespaces: Stop Current Codespace`). Không cần `lab stop`: lần mở sau cụm tự bật lại.

> ⚠️ Codespace tự dừng sau 30 phút không thao tác. Các job của buổi này đều ngắn (dài nhất khoảng 2 phút), nhưng nếu để PySpark shell mở rồi bỏ đi lâu, codespace dừng sẽ làm mất phiên shell và dữ liệu đã cache — khi quay lại chỉ cần mở lại shell (Bước 3) và chạy lại các khối lệnh cần thiết.

## Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Job đứng ở `(0 + 0) / 4`, log lặp lại `WARN TaskSchedulerImpl: Initial job has not accepted any resources` | Yêu cầu `spark.executor.memory` (ví dụ `2g`) lớn hơn bộ nhớ worker (1 GB) | Dùng mặc định 768m hoặc tối đa `1g`; trên YARN tổng executor + overhead 384 MB không vượt 1792 MB |
| `[CAST_INVALID_INPUT] The value 'abc' of the type "STRING" cannot be cast to "INT"` | Spark 4 bật ANSI mode; dữ liệu có giá trị không hợp lệ | Dùng `try_cast`, làm sạch bằng `regexp_replace`; đếm số `NULL` sau khi làm sạch |
| `[DIVIDE_BY_ZERO] Division by zero` | Chia cho cột có giá trị 0 dưới ANSI mode | `try_divide(a, b)` hoặc `F.when(b != 0, a / b)` |
| `[ARITHMETIC_OVERFLOW] integer overflow` | Phép cộng/nhân kiểu `INT` vượt 2.147.483.647 | Ép sang `BIGINT`/`DOUBLE` trước khi tính, hoặc `try_add`/`try_multiply` |
| `[CAST_INVALID_INPUT] ... cannot be cast to "TIMESTAMP"` | Chuỗi ngày giờ sai (30/02) hoặc sai định dạng | `try_to_timestamp(col, 'yyyy-MM-dd HH:mm:ss')`; khi đọc CSV dùng `timestampFormat` |
| `[MALFORMED_RECORD_IN_PARSING...] Parse Mode: FAILFAST` | Đọc CSV với `mode=FAILFAST` gặp bản ghi không khớp schema | Dùng `PERMISSIVE` (mặc định) rồi kiểm tra `NULL`, hoặc đọc toàn chuỗi rồi `try_cast` |
| Dòng JSON `{"ts": ..., "level": "ERROR", "logger": "SQLQueryContextLogger", ...}` xuất hiện trước traceback | Logger Python của PySpark 4 ghi log lỗi dạng JSON | Không phải lỗi thêm; đọc dòng `pyspark.errors.exceptions...` bên dưới |
| Lỗi trong task được thử lại, log `Task 0 in stage ... failed 4 times; aborting job` | Lỗi dữ liệu (ANSI) xảy ra trên executor; Spark thử lại 4 lần mặc định trước khi huỷ job | Sửa dữ liệu/biểu thức; thử lại không giúp được với lỗi xác định |
| Mở link cổng 4040 báo lỗi kết nối hoặc trang trống | Không có ứng dụng Spark nào đang chạy (phiên PySpark đã thoát, `spark-submit` đã xong), hoặc ứng dụng đang dùng cổng 4041 (codespace cũ chưa forward cổng này) | Thoát các phiên cũ rồi chạy lại; xem job đã xong ở Spark History (cổng 18080 trong `lab ui`) |
| Đo thời gian lần 2, 3 gần bằng 0 khi `collect()` lại cùng DataFrame | Spark tái sử dụng kết quả shuffle cũ, stage đọc dữ liệu bị "skipped" | Tạo DataFrame mới cho mỗi lần đo (như `truy_van(df)` ở Bước 13) |
| `grep -c ' 404 '` cho số lớn hơn Spark | Mẫu lỏng khớp cả trường số byte bằng 404 | Dùng mẫu `'" 404 '` |
| `sortByKey()` trả khoá theo thứ tự `['1', '10', '2', '23']` | Khoá là chuỗi nên so sánh theo từ điển | Ép khoá về `int` như hàm `extract_hour` |
| `AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] ... giao_dich` khi chạy script RFM của bài cũ | Temp view chỉ tồn tại trong SparkSession đã tạo ra nó | Đọc lại từ Parquet trên HDFS (Bước 13–14) |
| Nhiều dòng `INFO` che khuất kết quả khi `spark-submit` | Mức log mặc định của `spark-submit` là INFO | Thêm `--conf spark.log.level=WARN` |
| `spark-submit` đứng mãi, log có `Service 'SparkUI' could not bind on port 4040. Attempting port 4041.` rồi lặp `Initial job has not accepted any resources` | PySpark shell ở terminal khác vẫn mở và giữ cả 4 core của cụm (Spark Master: `coresused: 4`) | Ctrl+C job đang chờ, `exit()` PySpark shell rồi chạy lại `spark-submit` |
| Link trong Web UI (`http://nodemanager1:8042/...`, `historyserver:18080/history/...`, `http://spark-master:4040`) không mở được | Tên container chỉ phân giải được bên trong cụm, trình duyệt ở ngoài codespace | Dùng link cùng số cổng trong `lab ui` hoặc tab PORTS |
| Cổng không có trong tab PORTS hoặc link `lab ui` không mở | Cổng chưa được forward (cửa sổ VS Code mới tải lại, hoặc cụm chưa khởi động xong) | Tab PORTS → Forward a Port → nhập số cổng (4040, 8080, 18080...); chờ `lab status` đủ 8 container `healthy` |
| Kết quả nhỏ hơn bài nhiều (ví dụ `Tổng dòng: 50000`, `Tong so dong: 200000`) | Vẫn dùng dữ liệu mẫu 200.000 dòng của codespace tạo trước 06/11/2026 | `lab data 5000000` rồi `hdfs dfs -put -f` lại (Bước 2) |
| Job chậm bất thường, terminal/VS Code phản hồi chậm, executor bị mất (`ExecutorLostFailure`) | Codespace 8 GB cạn RAM: còn bật dịch vụ bổ sung, PySpark shell và `spark-submit` chạy cùng lúc, hoặc nhiều job song song | `lab stop-extras`, thoát PySpark shell, chạy từng job một; kiểm tra `lab status` (RAM các container khoảng 4,7 GB khi chạy job là bình thường); nếu vẫn lỗi, tạo codespace 4 nhân / 16 GB |
| Mở lại codespace thì PySpark shell, biến `parsed_rdd` và cache không còn | Codespace tự dừng sau 30 phút không thao tác (hoặc đã Stop) | Mở lại shell (Bước 3) và chạy lại các khối lệnh; dữ liệu HDFS vẫn còn |
| `git status` thấy `src/lab5/__pycache__/...` | `spark-submit` (chạy bằng `root` trong `spark-master`) biên dịch `schema_retail.py` vào thư mục mount | `rm -rf src/lab5/__pycache__` trước khi `git add` |

## Câu hỏi kiểm tra

1. Trong đoạn mã Bước 4, dòng nào là transformation, dòng nào là action? Vì sao lệnh `parsed_rdd = raw_rdd.map(...).filter(...)` chạy xong ngay lập tức dù tệp có 1 triệu dòng?
   *Gợi ý: `textFile`, `map`, `filter` chỉ thêm nút vào DAG (lazy); `count`, `take` buộc Spark lập kế hoạch stage, gửi task tới executor và đọc HDFS.*
2. Từ số liệu của bạn ở Bước 7–8 (trên codespace của bạn), cache giúp giảm bao nhiêu phần trăm thời gian 3 lần `count()`? Vì sao lần `count()` đầu tiên sau `persist()` không nhanh hơn?
   *Gợi ý: lần đầu vẫn đọc HDFS, parse và tốn thêm chi phí ghi vào bộ nhớ; chỉ các lần sau mới đọc từ cache.*
3. Giải thích vì sao Shuffle Write của `groupByKey` lớn hơn `reduceByKey` khoảng 1.850 lần ở Bước 9. Khi nào bắt buộc phải dùng `groupByKey`?
   *Gợi ý: `reduceByKey` gộp cục bộ trước khi shuffle nên mỗi partition chỉ gửi 10 cặp; `groupByKey` gửi mọi giá trị. Chỉ dùng `groupByKey` khi cần toàn bộ danh sách giá trị (ví dụ tính trung vị), không phải khi chỉ cần tổng/đếm.*
4. Spark 4 bật ANSI mode mặc định. Nêu hai lợi ích và một bất lợi khi chuyển mã từ Spark 3 sang. Vì sao `spark.read.schema(...).csv(...)` ở chế độ `PERMISSIVE` không báo lỗi với giá trị `abc` trong cột số nguyên?
   *Gợi ý: lợi ích — phát hiện dữ liệu bẩn sớm, không có kết quả sai âm thầm (như tràn số thành `-2147483648`); bất lợi — job cũ có thể dừng giữa chừng. Bộ đọc CSV có cơ chế xử lý bản ghi lỗi riêng (`mode`), độc lập với quy tắc ép kiểu ANSI của biểu thức SQL.*
5. Trong `explain()` ở Bước 13, `ReadSchema`, `PushedFilters` và `PartitionFilters` cho biết điều gì? Vì sao Parquet nhỏ hơn CSV khoảng 3 lần dù cùng nội dung?
   *Gợi ý: cột cần đọc, điều kiện đẩy xuống bộ đọc, thư mục phân vùng được giữ lại; Parquet lưu theo cột nên giá trị cùng kiểu đứng cạnh nhau, nén (dictionary, run-length, snappy) hiệu quả, và số được lưu nhị phân thay vì chuỗi.*
6. Vì sao truy vấn RFM viết bằng DataFrame API và bằng Spark SQL có cùng hiệu năng? Dựa vào kế hoạch ở Bước 14, giải thích vì sao có hai `Exchange`.
   *Gợi ý: cả hai được phân tích thành cùng kế hoạch logic và tối ưu bởi Catalyst (`sameSemantics = True`); `COUNT(DISTINCT)` cần một lần shuffle để khử trùng lặp theo `(customer_id, transaction_id)` và một lần để gộp theo `customer_id`.*

## Bài tập mở rộng

1. Viết lại Bước 5–6 (đếm theo mã trạng thái, top URL 5xx, lượt theo giờ) bằng **DataFrame API** với `F.regexp_extract` thay cho RDD; so sánh thời gian và kế hoạch `explain()` với phiên bản RDD, giải thích vì sao DataFrame thường nhanh hơn trong PySpark (gợi ý: không phải chuyển dữ liệu sang tiến trình Python).
2. Thử `persist(StorageLevel.MEMORY_AND_DISK)` và `persist(StorageLevel.DISK_ONLY)` cho `parsed_rdd`; ghi lại dung lượng trong tab Storage và thời gian 3 lần `count()` cho mỗi mức lưu trữ.
3. Đặt `spark.sql.shuffle.partitions` bằng 8, 200, 1000 (qua `--conf`) khi chạy `buoc_rfm.py`, tắt/bật AQE (`spark.sql.adaptive.enabled`); lập bảng thời gian và số task của stage sau shuffle, rút ra nhận xét.
4. Ghi bảng giao dịch ra Parquet phân vùng theo **hai** cấp `region`, `month`; đo dung lượng, số tệp và thời gian truy vấn "doanh thu CanTho tháng 2025-03" so với Bước 13. Thảo luận về vấn đề "quá nhiều tệp nhỏ" khi phân vùng quá mịn.
5. Dữ liệu lệch: HCM chiếm 45% giao dịch. Viết truy vấn JOIN `giao_dich` với một bảng tổng hợp theo `region`, quan sát trên History Server thời gian task lớn nhất so với trung vị của stage JOIN, rồi thử kỹ thuật salting khoá JOIN để cân bằng.

## Yêu cầu nộp bài và tiêu chí chấm

Đẩy thư mục `src/lab5` lên repo GitHub của bạn (mục Kết thúc buổi) và nộp đường dẫn commit cùng một báo cáo (PDF hoặc Markdown, có thể đặt trong repo, ví dụ `src/lab5/bao-cao.md`) gồm: log/ảnh chụp kết quả của từng bước, ảnh chụp tab Storage và Stages của Spark UI mở qua cổng forward 4040 (Bước 8–9), bảng ANSI ở Bước 10 có bổ sung ít nhất một trường hợp tự tìm, bảng so sánh CSV/Parquet có số đo của chính codespace mình (ghi rõ loại máy 2 hay 4 nhân), và trả lời các câu hỏi kiểm tra.

| Tiêu chí | Điểm |
|---|---|
| Kiểm tra cụm, đưa dữ liệu lên HDFS, mở PySpark kết nối Spark Master đúng cấu hình bộ nhớ (Bước 1–3) | 0.5 |
| Pipeline RDD: parse log, đếm theo mã trạng thái, top URL 5xx, theo giờ; kiểm chứng chéo khớp (Bước 4–6) | 1.5 |
| Đo cache có số liệu và ảnh tab Storage; so sánh Shuffle Write `reduceByKey`/`groupByKey`, giải thích đúng (Bước 7–9) | 1.5 |
| ANSI mode: tái hiện các lỗi, bảng so sánh, làm sạch dữ liệu bẩn bằng `try_*`, giải thích PERMISSIVE/FAILFAST (Bước 10–11) | 1.5 |
| DataFrame với schema tường minh, doanh thu theo tháng/khu vực ghi lên HDFS, kiểm chứng tổng (Bước 12) | 1.0 |
| Parquet: so sánh dung lượng, thời gian, đọc đúng `ReadSchema`/`PushedFilters`/`PartitionFilters` (Bước 13) | 1.5 |
| RFM hai cách cho kết quả trùng khớp, giải thích kế hoạch; Spark SQL JOIN + cửa sổ (Bước 14–15) | 1.0 |
| Chạy trên YARN và tra cứu History Server (Bước 16), trả lời câu hỏi kiểm tra | 1.0 |
| Hoàn thành ít nhất một bài tập mở rộng | 0.5 |
| **Tổng** | **10** |
