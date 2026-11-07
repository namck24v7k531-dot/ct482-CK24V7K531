# Buổi 3 — Kho dữ liệu với Hive: phân vùng, ORC/Parquet và tối ưu truy vấn (GitHub Codespaces)

Thời lượng 4 tiết (180 phút thực hành), ứng với CLO3 (thiết kế và truy vấn kho dữ liệu trên Hadoop). Máy khuyến nghị: **2 nhân / 8 GB** (mặc định) là đủ — đo trên bản sao codespace 2 nhân/8 GB với dữ liệu 5 triệu dòng, RAM đỉnh khoảng 4,6–5,2 GB, còn trống khoảng 3 GB; máy 4 nhân không cần thiết. Dịch vụ cần bật: cụm lõi (tự bật khi mở codespace) + `lab hive` (`hive-metastore-db` PostgreSQL 16, `hive-metastore`, `hiveserver2` — Hive 4.2.1, engine Tez chạy cục bộ trong HiveServer2). Dữ liệu: `data/retail_transactions.csv` (5 000 000 dòng, 405 MB) và `data/products.csv` (500 dòng). Quota ước tính: khoảng 3,5 giờ mở codespace × 2 nhân ≈ **7 core-hour** (máy 4 nhân: ≈ 14 core-hour); đĩa codespace lúc cao nhất ≈ 16 GB (xem mục Chuẩn bị). Mọi lệnh và kết quả mẫu được chạy thật trên bản sao Dev Container dùng đúng cấu hình `.devcontainer` của repo, giới hạn 2 nhân/8 GB; thời gian truy vấn chỉ là ví dụ và phụ thuộc máy.

## Mục tiêu

Sau khi hoàn thành buổi thực hành, sinh viên có khả năng:

- Khởi động Hive (Metastore + HiveServer2), kết nối bằng `beeline` và giải thích vai trò của Metastore trong việc quản lý schema.
- Tạo bảng EXTERNAL trên tệp CSV có sẵn trong HDFS và minh hoạ khái niệm *schema-on-read*.
- Thiết kế bảng phân vùng theo ngày, nạp dữ liệu bằng *dynamic partition insert*, kiểm chứng cấu trúc thư mục phân vùng trên HDFS và đo tác động của *partition pruning*.
- So sánh dung lượng và thời gian truy vấn giữa ba định dạng Text, ORC và Parquet (nén Snappy).
- Đọc kế hoạch thực thi (`EXPLAIN`) của Hive trên Tez, so sánh Merge Join với Map Join và thu thập thống kê bằng `ANALYZE TABLE`.
- Viết truy vấn phân tích dùng hàm cửa sổ (`RANK`, `SUM ... OVER`).

## Chuẩn bị

**1. Mở codespace.** Vào https://github.com/codespaces, chọn codespace của repo môn học (tạo từ template ở Buổi 0) và bấm mở. Khi codespace khởi động, cụm lõi 8 container tự bật (khoảng 1 phút; cửa sổ *Terminal* hiện dòng `[khoi-dong] Cụm đã sẵn sàng.`). Terminal mặc định đứng ở **gốc repo** (`/workspaces/<tên-repo>`); mọi lệnh dưới đây dùng đường dẫn tương đối tính từ thư mục này.

**2. Cập nhật tài liệu của giảng viên** (nếu giảng viên thông báo có cập nhật) và kiểm tra cụm:

<!-- shell: Terminal codespace -->
```bash
git pull
lab status
```

**Kết quả mong đợi** (`git pull` tuỳ repo; phần `lab status`):

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
RAM các container: 2.2 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

> ⚠️ Nếu có container chưa `healthy` hoặc `lab status` báo `HDFS: chưa chạy`, gõ `lab start` và chờ lệnh kết thúc.

**3. Dữ liệu 5 triệu dòng.** Codespace tạo trước 06/11/2026 chỉ có bộ dữ liệu mẫu 200 000 dòng (codespace tạo sau ngày này đã có sẵn 5 triệu dòng). Kiểm tra:

<!-- shell: Terminal codespace -->
```bash
wc -l data/retail_transactions.csv
```

Nếu kết quả là `200001 data/retail_transactions.csv` (chưa sinh bộ đầy đủ), sinh lại bộ 5 triệu dòng — chỉ cần làm **một lần cho cả học kỳ**, các buổi sau dùng lại:

<!-- shell: Terminal codespace -->
```bash
time lab data 5000000
wc -l data/retail_transactions.csv
du -sh data
```

**Kết quả mong đợi:**

```text
Sinh du lieu vao ./data (5,000,000 giao dich)...
  giao dich: 500,000
  ...
  giao dich: 4,500,000
  products.csv: 0.0 MB
  retail_transactions.csv: 405.2 MB
  web_access.log: 121.4 MB
  sensor_readings.csv: 22.7 MB

real	0m28.233s
5000001 data/retail_transactions.csv
524M	data
```

Lệnh ghi đè cả bốn tệp trong `./data` (mất khoảng 30 giây, chiếm 524 MB). Thư mục `data/` đã nằm trong `.gitignore` nên không bị đẩy lên GitHub.

**4. Bật Hive** — xem Bước 1.

**Tài nguyên (đo trên bản sao codespace 2 nhân / 8 GB, dữ liệu 5 triệu dòng):**

| Thời điểm | RAM các container (`lab status`) | Ghi chú |
|---|---|---|
| Cụm lõi vừa bật | 2,2 GB | 8 container |
| Sau `lab hive` | 3,0 GB | `hiveserver2` 380 MB, `hive-metastore` 311 MB, `hive-metastore-db` 119 MB |
| Đỉnh khi nạp ba bảng ở Bước 11 | 5,0–5,2 GB | `hiveserver2` 1,82 GiB / giới hạn 2 GiB; bộ nhớ ẩn danh toàn codespace 4,6 GiB / 8 GiB |
| Sau `lab stop-extras` | 2,6 GB | |

Máy 8 GB còn trống khoảng 3 GB trong lúc chạy buổi này, nên **không** bật thêm `lab nosql`, `lab kafka` hay `lab extras` cùng lúc. Có thể tự xem RAM của codespace bằng `free -h` (dòng `Mem:`, cột `available`).

**Đĩa:** lần đầu `lab hive` tải image `apache/hive:4.2.1` (3,9 GB) và `postgres:16` (0,6 GB). Đo sau buổi: image ≈ 9,5 GB (`bigdata-lab` 4,9 GB + Hive 3,9 GB + PostgreSQL 0,6 GB), volume HDFS + Metastore ≈ 2,7 GB, thư mục tạm của Tez trong `hiveserver2` ≈ 1,8 GB (dọn được, xem mục Kết thúc buổi), `./data` 0,5 GB; cộng nền dev container ≈ 1,7 GB → khoảng **16 GB** lúc cao nhất. Tài khoản GitHub Free chỉ có 15 GB lưu trữ/tháng — nên xác minh GitHub Student Developer Pack (20 GB) và làm đủ bước dọn ở cuối buổi.

**Web UI dùng trong buổi này.** Trong codespace không mở được `http://localhost:...` từ trình duyệt máy bạn; lấy đường dẫn bằng lệnh `lab ui` (hoặc tab **PORTS** cạnh TERMINAL → biểu tượng quả địa cầu ở cổng tương ứng):

<!-- shell: Terminal codespace -->
```bash
lab ui
```

**Kết quả mong đợi** (tên codespace của bạn khác):

```text
HDFS NameNode            https://ten-codespace-cua-ban-x7g9-9870.app.github.dev
YARN ResourceManager     https://ten-codespace-cua-ban-x7g9-8088.app.github.dev
...
HiveServer2 UI           https://ten-codespace-cua-ban-x7g9-10002.app.github.dev
HBase Master             https://ten-codespace-cua-ban-x7g9-16010.app.github.dev
HBase RegionServer       https://ten-codespace-cua-ban-x7g9-16030.app.github.dev
NodeManager 1            https://ten-codespace-cua-ban-x7g9-8042.app.github.dev
NodeManager 2            https://ten-codespace-cua-ban-x7g9-8043.app.github.dev
DataNode 1               https://ten-codespace-cua-ban-x7g9-9864.app.github.dev
DataNode 2               https://ten-codespace-cua-ban-x7g9-9865.app.github.dev
(Hoặc mở tab PORTS bên cạnh TERMINAL, bấm biểu tượng quả địa cầu.)
```

| Giao diện | Cổng (dòng trong `lab ui`) | Dùng để |
|---|---|---|
| HiveServer2 Web UI | 10002 (`HiveServer2 UI`) | Phiên kết nối, truy vấn đang chạy/đã chạy, cấu hình |
| HDFS NameNode | 9870 (`HDFS NameNode`) | Duyệt `/user/hive/warehouse/retail_dw.db/...` và `/data/raw/...` |
| YARN ResourceManager | 8088 (`YARN ResourceManager`) | Đối chiếu: truy vấn Hive **không** xuất hiện ở đây vì Tez chạy cục bộ trong HiveServer2 |

> 💡 Liên kết nội bộ bên trong Web UI (ví dụ `http://nodemanager1:8042/...`, `http://hiveserver2:10002/...`) không mở được từ trình duyệt. Dùng đường dẫn tương ứng từ `lab ui`. Bên trong codespace, `curl localhost:10002` vẫn dùng được để kiểm tra nhanh.

**Khác biệt quan trọng so với môi trường cũ (Hive 2.3, MapReduce):**

| Nội dung | Môi trường mới |
|---|---|
| Container | `hiveserver2` (không còn `hive-server`), `hive-metastore`, `hive-metastore-db` |
| Bật/tắt | `lab hive` / `lab stop-extras` (thay cho `docker compose -f ... -f ... up/stop`) |
| Client | Chỉ dùng `beeline` (không còn `hive` CLI) |
| Engine | Tez ở chế độ local bên trong HiveServer2 — truy vấn không tạo ứng dụng YARN |
| `CREATE TABLE` không ACID | Hive 4 tự chuyển thành `EXTERNAL_TABLE` với `TRANSLATED_TO_EXTERNAL=TRUE`, `external.table.purge=TRUE` |
| Thống kê | Hive 4 tự thu thập thống kê khi `INSERT` (`hive.stats.autogather`) |
| Bộ đệm kết quả truy vấn | Bật mặc định nhưng chỉ áp dụng cho bảng transactional (`hive.query.results.cache.nontransactional.tables.enabled=false`); khi đo thời gian vẫn nên tắt hẳn cho chắc chắn |
| Hệ số nhân bản tệp Hive ghi | Client HDFS trong image Hive mặc định ghi 3 bản sao (cụm chỉ có 2 DataNode) → đặt `SET dfs.replication=2;` ở đầu mỗi phiên |

## PHẦN 1: KHỞI ĐỘNG HIVE VÀ KẾT NỐI BEELINE

### Bước 1: Bật các dịch vụ Hive

<!-- shell: Terminal codespace -->
```bash
time lab hive
lab status
```

`lab hive` tự bật các dịch vụ phụ thuộc theo thứ tự `hive-metastore-db` → `hive-metastore` → `hiveserver2` và chờ đến khi cả ba `healthy`.

**Kết quả mong đợi** (rút gọn):

```text
 Volume bigdata-lab_hive-metastore-db  Creating
 Volume bigdata-lab_hive-metastore-db  Created
 Container namenode  Running
 Container hive-metastore-db  Creating
 ...
 Container hive-metastore-db  Healthy
 Container namenode  Healthy
 Container hive-metastore  Healthy
 Container hiveserver2  Healthy

real	0m22.092s
NAME                STATUS
datanode1           Up 2 minutes (healthy)
datanode2           Up 2 minutes (healthy)
historyserver       Up 2 minutes (healthy)
hive-metastore      Up 16 seconds (healthy)
hive-metastore-db   Up 22 seconds (healthy)
hiveserver2         Up 5 seconds (healthy)
namenode            Up 2 minutes (healthy)
nodemanager1        Up 2 minutes (healthy)
nodemanager2        Up 2 minutes (healthy)
resourcemanager     Up 2 minutes (healthy)
spark-master        Up 2 minutes (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 3.0 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

> ⚠️ **Lần đầu** chạy `lab hive` trên một codespace, Docker phải tải image `apache/hive:4.2.1` (~3,9 GB) và `postgres:16` (~0,6 GB) — dự kiến thêm khoảng 3–6 phút (in nhiều dòng `Pulling` / `Downloading`). Đừng tắt terminal. Từ lần thứ hai, `lab hive` chỉ mất khoảng 20–25 giây. Dòng `Volume ... Created` chỉ xuất hiện lần đầu.

> ⚠️ Healthcheck của `hiveserver2` chỉ kiểm tra cổng 10000 đã mở. Nếu `beeline` báo `Connection refused` ngay sau khi bật, chờ thêm 10–20 giây rồi thử lại.

### Bước 2: Kết nối beeline vào HiveServer2

<!-- shell: Terminal codespace (terminal A) -->
```bash
docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000/default -n hive
```

Tại dấu nhắc `0: jdbc:hive2://localhost:10000/default>` gõ:

<!-- shell: beeline (hiveserver2) -->
```sql
SHOW DATABASES;
SELECT current_date(), version();
SET hive.execution.engine;
SET hive.exec.dynamic.partition.mode;
```

**Kết quả mong đợi:**

```text
Connected to: Apache Hive (version 4.2.1)
Driver: Hive JDBC (version 4.2.1)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 4.2.1 by Apache Hive
+----------------+
| database_name  |
+----------------+
| default        |
+----------------+
1 row selected (1.567 seconds)
+-------------+--------------------------------------------------+
|     _c0     |                       _c1                        |
+-------------+--------------------------------------------------+
| 2026-11-05  | 4.2.1 r0f4761a83b40a279560359fa611a65774e42324b  |
+-------------+--------------------------------------------------+
1 row selected (0.939 seconds)
+----------------------------+
|            set             |
+----------------------------+
| hive.execution.engine=tez  |
+----------------------------+
+---------------------------------------------+
|                     set                     |
+---------------------------------------------+
| hive.exec.dynamic.partition.mode=nonstrict  |
+---------------------------------------------+
```

Beeline còn in nhiều dòng `INFO  : Compiling command ...`, `INFO  : Completed executing ...` — đó là log của HiveServer2 gửi về, có thể bỏ qua. Thoát beeline bằng `!quit`.

> 💡 Nên mở **hai terminal** trong codespace (biểu tượng `+` ở khung TERMINAL): terminal A giữ phiên beeline suốt buổi, terminal B dùng cho các lệnh HDFS trong container `namenode`. Các lệnh `SET ...` chỉ có hiệu lực trong phiên beeline hiện tại; nếu thoát hoặc codespace bị dừng, phải chạy lại `USE retail_dw;` và các lệnh `SET` cần thiết.

> 💡 **Chạy tệp SQL không tương tác** (ví dụ khi viết báo cáo): tạo tệp trong `src/lab03/` (khung Explorer bên trái → chuột phải thư mục `src` → *New Folder* `lab03` → *New File*) rồi chuyển hướng vào stdin. Trong Hive 4.2.1 trên môi trường này, `beeline -f tep.sql` **không** chạy được (không có TTY thì báo `Unable to create a terminal`), nên dùng cách sau (đứng ở gốc repo):

<!-- shell: Terminal codespace -->
```bash
docker exec -i -e HADOOP_CLIENT_OPTS=-Dorg.jline.terminal.provider=dumb hiveserver2 \
  beeline -u jdbc:hive2://localhost:10000/default -n hive --silent=true < src/lab03/thu_f.sql
```

Ví dụ với tệp `src/lab03/thu_f.sql` dưới đây (chạy sau khi đã làm xong Phần 2–4):

<!-- file: src/lab03/thu_f.sql -->
```sql
USE retail_dw;
SELECT COUNT(*) AS n_dim FROM product_dim;
SHOW TABLES;
```

Kết quả chạy thử:

```text
+--------+
| n_dim  |
+--------+
| 500    |
+--------+
+---------------------------+
|         tab_name          |
+---------------------------+
| product_dim               |
| retail_transactions_part  |
| retail_transactions_raw   |
| retail_txn_part_orc       |
| retail_txn_part_parquet   |
| retail_txn_part_text      |
+---------------------------+
```

Các dòng `SLF4J: ...` in ra kèm theo là cảnh báo thư viện, bỏ qua được.

## PHẦN 2: BẢNG EXTERNAL VÀ SCHEMA-ON-READ

### Bước 3: Đưa dữ liệu vào thư mục HDFS riêng

`LOCATION` của bảng Hive là một **thư mục**; Hive đọc mọi tệp trong thư mục đó. Vì vậy mỗi bảng cần một thư mục riêng chỉ chứa dữ liệu của nó. Thư mục `./data` của repo được gắn thành `/data` trong container `namenode`.

<!-- shell: Terminal codespace (terminal B) -->
```bash
docker exec -it namenode bash
```

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -mkdir -p /data/raw/retail /data/raw/products
hdfs dfs -put -f /data/retail_transactions.csv /data/raw/retail/
hdfs dfs -put -f /data/products.csv /data/raw/products/
hdfs dfs -ls -R /data/raw/retail /data/raw/products
```

**Kết quả mong đợi** (lệnh `put` 405 MB mất khoảng 7 giây):

```text
-rw-r--r--   2 root supergroup  405181403 2026-11-05 23:23 /data/raw/retail/retail_transactions.csv
-rw-r--r--   2 root supergroup      22590 2026-11-05 23:23 /data/raw/products/products.csv
```

> ⚠️ Không đặt `LOCATION '/data/raw'`: thư mục này chứa cả tệp giao dịch (từ Buổi 2) lẫn các thư mục con khác, bảng sẽ đọc lẫn dữ liệu.

> ⚠️ Kích thước phải là `405181403` byte. Nếu thấy khoảng 16 MB, bạn đang dùng bộ dữ liệu 200 000 dòng — quay lại mục Chuẩn bị, chạy `lab data 5000000` rồi `put -f` lại.

### Bước 4: Tạo database và bảng EXTERNAL

Quay lại terminal A (beeline, hoặc kết nối lại như Bước 2):

<!-- shell: beeline (hiveserver2) -->
```sql
CREATE DATABASE IF NOT EXISTS retail_dw COMMENT 'Kho du lieu ban le - Buoi 3';
USE retail_dw;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_transactions_raw (
  transaction_id STRING,
  customer_id    STRING,
  product_id     STRING,
  category       STRING,
  quantity       INT,
  unit_price     DOUBLE,
  discount       DOUBLE,
  store_id       STRING,
  region         STRING,
  ts             STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/retail'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS product_dim (
  product_id   STRING,
  product_name STRING,
  category     STRING,
  brand        STRING,
  base_price   DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/products'
TBLPROPERTIES ('skip.header.line.count'='1');

SHOW TABLES;
```

**Kết quả mong đợi:**

```text
No rows affected (0.415 seconds)
No rows affected (0.026 seconds)
No rows affected (0.158 seconds)
No rows affected (0.067 seconds)
+--------------------------+
|         tab_name         |
+--------------------------+
| product_dim              |
| retail_transactions_raw  |
+--------------------------+
2 rows selected (0.081 seconds)
```

`CREATE EXTERNAL TABLE` chỉ ghi metadata (tên cột, kiểu, vị trí, định dạng) vào Metastore (lưu trong PostgreSQL); tệp CSV không bị đọc, di chuyển hay chuyển đổi.

### Bước 5: Kiểm tra bằng SELECT và DESCRIBE FORMATTED

<!-- shell: beeline (hiveserver2) -->
```sql
SELECT COUNT(*) AS so_dong FROM retail_transactions_raw;
SELECT transaction_id, customer_id, category, unit_price, region, ts FROM retail_transactions_raw LIMIT 5;
SELECT * FROM product_dim LIMIT 3;
DESCRIBE FORMATTED retail_transactions_raw;
```

**Kết quả mong đợi** (rút gọn):

```text
----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      2          2        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      1          1        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 3.02 s
----------------------------------------------------------------------------------------------
+----------+
| so_dong  |
+----------+
| 5000000  |
+----------+
1 row selected (6.536 seconds)
+-----------------+--------------+-----------+-------------+---------+----------------------+
| transaction_id  | customer_id  | category  | unit_price  | region  |          ts          |
+-----------------+--------------+-----------+-------------+---------+----------------------+
| T0003312504     | C0029214     | sach      | 8419000.0   | Hanoi   | 2025-07-25 08:14:09  |
| T0003312505     | C0084676     | me_va_be  | 1267000.0   | Hanoi   | 2025-02-28 04:09:33  |
| T0003312506     | C0022127     | gia_dung  | 1.6457E7    | DaNang  | 2025-06-21 01:54:32  |
| T0003312507     | C0038605     | dien_tu   | 1.0036E7    | DaNang  | 2025-01-22 01:14:12  |
| T0003312508     | C0111034     | gia_dung  | 1785000.0   | DaNang  | 2025-07-30 04:49:27  |
+-----------------+--------------+-----------+-------------+---------+----------------------+
+-------------------------+---------------------------+-----------------------+--------------------+-------------------------+
| product_dim.product_id  | product_dim.product_name  | product_dim.category  | product_dim.brand  | product_dim.base_price  |
+-------------------------+---------------------------+-----------------------+--------------------+-------------------------+
| P00001                  | San pham 1                | the_thao              | Brand08            | 645000.0                |
| P00002                  | San pham 2                | gia_dung              | Brand16            | 5596000.0               |
| P00003                  | San pham 3                | the_thao              | Brand07            | 1.6924E7                |
+-------------------------+---------------------------+-----------------------+--------------------+-------------------------+
...
| Location:                     | hdfs://namenode:9000/data/raw/retail               | NULL                  |
| Table Type:                   | EXTERNAL_TABLE                                     | NULL                  |
| Table Parameters:             | NULL                                               | NULL                  |
|                               | EXTERNAL                                           | TRUE                  |
|                               | bucketing_version                                  | 2                     |
|                               | numFiles                                           | 1                     |
|                               | skip.header.line.count                             | 1                     |
|                               | totalSize                                          | 405181403             |
...
| SerDe Library:                | org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe | NULL                  |
| InputFormat:                  | org.apache.hadoop.mapred.TextInputFormat           | NULL                  |
...
```

Nhận xét:

- Bảng tiến độ `VERTICES` cho thấy truy vấn chạy trên Tez: `Map 1` (2 task đọc tệp) → `Reducer 2` (cộng số đếm). Truy vấn đầu tiên của phiên chậm hơn (6,5 s) vì phải khởi tạo phiên Tez.
- `LIMIT 5` trả về 5 dòng **bất kỳ** (ở đây từ split thứ hai, `T0003312504`...), không phải 5 dòng đầu tệp — bảng không có thứ tự nếu không `ORDER BY`.
- Giá trị `DOUBLE` lớn được hiển thị dạng khoa học (`1.6457E7` = 16 457 000).

Đối chiếu với số dòng của tệp (terminal B, trong container `namenode`):

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -cat /data/raw/retail/retail_transactions.csv | wc -l
```

**Kết quả mong đợi:**

```text
5000001
```

Chênh đúng 1 dòng — dòng header được bỏ qua nhờ `skip.header.line.count=1`.

### Bước 6: Minh hoạ schema-on-read

Thêm vào thư mục của bảng một tệp nhỏ có giá trị `quantity` không phải số, **không** sửa định nghĩa bảng:

<!-- shell: Bên trong container namenode -->
```bash
printf 'transaction_id,customer_id,product_id,category,quantity,unit_price,discount,store_id,region,ts\nT9999999999,C0000001,P00001,sach,abc,100000,0,S001,HCM,2025-03-01 10:00:00\n' > /tmp/dong_loi.csv
hdfs dfs -put -f /tmp/dong_loi.csv /data/raw/retail/
```

<!-- shell: beeline (hiveserver2) -->
```sql
SELECT COUNT(*) AS so_dong FROM retail_transactions_raw;
SELECT transaction_id, quantity, unit_price, region FROM retail_transactions_raw WHERE quantity IS NULL;
```

**Kết quả mong đợi:**

```text
+----------+
| so_dong  |
+----------+
| 5000001  |
+----------+
1 row selected (2.395 seconds)
+-----------------+-----------+-------------+---------+
| transaction_id  | quantity  | unit_price  | region  |
+-----------------+-----------+-------------+---------+
| T9999999999     | NULL      | 100000.0    | HCM     |
+-----------------+-----------+-------------+---------+
1 row selected (2.584 seconds)
```

Tệp mới được "nhận" ngay mà không cần lệnh nạp; chuỗi `abc` không ép được sang `INT` nên Hive trả `NULL` lúc **đọc**, không báo lỗi lúc ghi — đó là *schema-on-read*. Ngược lại, RDBMS (*schema-on-write*) sẽ từ chối dòng này ngay khi `INSERT`. Xoá tệp lỗi để các bước sau đúng số liệu:

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -rm /data/raw/retail/dong_loi.csv
```

**Kết quả mong đợi:**

```text
Deleted /data/raw/retail/dong_loi.csv
```

> ⚠️ `skip.header.line.count` áp dụng cho **từng tệp**. Nếu tệp `dong_loi.csv` chỉ có một dòng dữ liệu (không có header), dòng đó bị coi là header và bỏ qua: `COUNT(*)` vẫn là 5 000 000 và truy vấn `quantity IS NULL` không trả dòng nào. Mọi tệp đặt vào thư mục của bảng phải có cùng cấu trúc, kể cả dòng header.

## PHẦN 3: PHÂN VÙNG ĐỘNG THEO NGÀY

### Bước 7: Tạo bảng phân vùng ORC và nạp bằng dynamic partition

Bảng đích phân vùng theo `dt` (`yyyy-MM-dd`, lấy từ 10 ký tự đầu của `ts`). Với *dynamic partition*, Hive tự tạo một thư mục cho mỗi giá trị `dt` xuất hiện trong dữ liệu.

<!-- shell: beeline (hiveserver2) -->
```sql
USE retail_dw;
SET dfs.replication=2;

CREATE TABLE IF NOT EXISTS retail_transactions_part (
  transaction_id STRING,
  customer_id    STRING,
  product_id     STRING,
  category       STRING,
  quantity       INT,
  unit_price     DOUBLE,
  discount       DOUBLE,
  store_id       STRING,
  region         STRING,
  ts             STRING
)
PARTITIONED BY (dt STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.max.dynamic.partitions.pernode=500;

INSERT OVERWRITE TABLE retail_transactions_part PARTITION (dt)
SELECT transaction_id, customer_id, product_id, category, quantity,
       unit_price, discount, store_id, region, ts,
       SUBSTR(ts, 1, 10) AS dt
FROM retail_transactions_raw
WHERE ts IS NOT NULL;
```

**Kết quả mong đợi:**

```text
----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      2          2        0        0       0       0
Reducer 2 ...... container     SUCCEEDED     21         21        0        0       0       0
Reducer 3 ...... container     SUCCEEDED     21         21        0        0       0       0
----------------------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 53.25 s
----------------------------------------------------------------------------------------------
5,000,000 rows affected (53.965 seconds)
```

Giải thích:

- `SET dfs.replication=2;` — HiveServer2 ghi tệp bằng client HDFS riêng của image Hive, mặc định 3 bản sao; cụm chỉ có 2 DataNode nên nếu thiếu dòng này, mọi tệp Hive ghi ra đều *under-replicated* (`hdfs fsck /user/hive/warehouse` báo `Under-replicated blocks: 240 (100.0 %)`). Lệnh `SET` chỉ có hiệu lực trong phiên beeline hiện tại.
- `nonstrict` cho phép mọi cột phân vùng đều động (môi trường đã bật sẵn, đặt lại cho rõ). Cột phân vùng `dt` phải đứng **cuối** danh sách `SELECT`.
- Dữ liệu trải trên 240 ngày; mặc định `hive.exec.max.dynamic.partitions.pernode=100` nên nâng giới hạn để tránh lỗi khi một task ghi nhiều phân vùng.
- Thời gian nạp phụ thuộc máy: khoảng 54 giây trên codespace 2 nhân (máy phòng lab: 34–45 giây). Đây là bước nặng nhất buổi — `hiveserver2` dùng tới khoảng 1,1 GiB RAM.

Nếu quên chế độ `nonstrict` (thử `SET hive.exec.dynamic.partition.mode=strict;` rồi chạy lại `INSERT`), Hive báo:

```text
Error: Error while compiling statement: FAILED: SemanticException [Error 10096]: Dynamic partition strict mode requires at least one static partition column. To turn this off set hive.exec.dynamic.partition.mode=nonstrict; ... (state=42000,code=10096)
```

### Bước 8: Kiểm chứng phân vùng trong Hive và trên HDFS

<!-- shell: beeline (hiveserver2) -->
```sql
SET hive.exec.dynamic.partition.mode=nonstrict;
SELECT COUNT(*) FROM retail_transactions_part;
SHOW PARTITIONS retail_transactions_part;
DESCRIBE FORMATTED retail_transactions_part;
```

**Kết quả mong đợi** (rút gọn):

```text
+----------+
|   _c0    |
+----------+
| 5000000  |
+----------+
1 row selected (2.359 seconds)
+----------------+
|   partition    |
+----------------+
| dt=2025-01-01  |
| dt=2025-01-02  |
| dt=2025-01-03  |
...
| dt=2025-08-27  |
| dt=2025-08-28  |
+----------------+
240 rows selected (0.03 seconds)
...
| # Partition Information       | NULL                                               | NULL                        |
| Location:                     | hdfs://namenode:9000/user/hive/warehouse/retail_dw.db/retail_transactions_part | NULL                        |
| Table Type:                   | EXTERNAL_TABLE                                     | NULL                        |
|                               | EXTERNAL                                           | TRUE                        |
|                               | TRANSLATED_TO_EXTERNAL                             | TRUE                        |
|                               | external.table.purge                               | TRUE                        |
|                               | numPartitions                                      | 240                         |
|                               | orc.compress                                       | SNAPPY                      |
| SerDe Library:                | org.apache.hadoop.hive.ql.io.orc.OrcSerde          | NULL                        |
```

Bảng tạo bằng `CREATE TABLE` (không ACID) được Hive 4 chuyển thành `EXTERNAL_TABLE` với `external.table.purge=TRUE`: vẫn nằm trong warehouse và `DROP TABLE` vẫn xoá dữ liệu như bảng managed (xem Bước 20). Trên lần chạy kiểm thử, `COUNT(*)` chạy một DAG Tez rất nhỏ (1 task `Map 1`, 1 task `Reducer 2`) và xong trong khoảng 2 giây; mỗi phân vùng đã có thống kê `numRows` do Hive 4 tự thu thập khi `INSERT` (ví dụ `DESCRIBE FORMATTED retail_transactions_part PARTITION (dt='2025-03-15')` cho `numRows 20913`). Kiểm tra thư mục trên HDFS (terminal B, container `namenode`):

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -ls /user/hive/warehouse/retail_dw.db/retail_transactions_part | head -n 5
hdfs dfs -ls /user/hive/warehouse/retail_dw.db/retail_transactions_part | grep -c 'dt='
hdfs dfs -ls -h /user/hive/warehouse/retail_dw.db/retail_transactions_part/dt=2025-03-15
hdfs dfs -du -s -h /user/hive/warehouse/retail_dw.db/retail_transactions_part
hdfs fsck /user/hive/warehouse 2>/dev/null | grep -E "Under-replicated"
```

**Kết quả mong đợi:**

```text
Found 240 items
drwxr-xr-x   - hive supergroup          0 2026-11-05 23:25 /user/hive/warehouse/retail_dw.db/retail_transactions_part/dt=2025-01-01
drwxr-xr-x   - hive supergroup          0 2026-11-05 23:24 /user/hive/warehouse/retail_dw.db/retail_transactions_part/dt=2025-01-02
drwxr-xr-x   - hive supergroup          0 2026-11-05 23:24 /user/hive/warehouse/retail_dw.db/retail_transactions_part/dt=2025-01-03
drwxr-xr-x   - hive supergroup          0 2026-11-05 23:24 /user/hive/warehouse/retail_dw.db/retail_transactions_part/dt=2025-01-04
240
Found 1 items
-rw-r--r--   2 hive supergroup    599.6 K 2026-11-05 23:24 /user/hive/warehouse/retail_dw.db/retail_transactions_part/dt=2025-03-15/000007_0
140.0 M  280.0 M  /user/hive/warehouse/retail_dw.db/retail_transactions_part
 Under-replicated blocks:	0 (0.0 %)
```

Mỗi phân vùng là một thư mục `dt=giá_trị`; tệp `000007_0` là tệp ORC do reduce task số 7 ghi. Toàn bộ 5 triệu dòng chỉ chiếm 140 MB ở dạng ORC + Snappy (CSV gốc 386 MB). Cột `2` xác nhận tệp có đúng 2 bản sao nhờ `SET dfs.replication=2`.

> 💡 Nếu lỡ nạp dữ liệu khi chưa `SET dfs.replication=2`, sửa các tệp đã ghi bằng `hdfs dfs -setrep -R 2 /user/hive/warehouse` trong container `namenode` (in `Replication 2 set: ...` cho từng tệp, sau đó `fsck` báo `Under-replicated blocks: 0`).

### Bước 9: Partition pruning với EXPLAIN

So sánh kế hoạch của truy vấn lọc theo **cột phân vùng** (`dt`) với truy vấn lọc theo **cột dữ liệu** (`region`):

<!-- shell: beeline (hiveserver2) -->
```sql
EXPLAIN
SELECT category, SUM(quantity) FROM retail_transactions_part
WHERE dt = '2025-03-15'
GROUP BY category;
```

**Kết quả mong đợi:**

```text
+----------------------------------------------------+
|                      Explain                       |
+----------------------------------------------------+
| Plan optimized by CBO.                             |
|                                                    |
| Vertex dependency in root stage                    |
| Reducer 2 <- Map 1 (SIMPLE_EDGE)                   |
|                                                    |
| Stage-0                                            |
|   Fetch Operator                                   |
|     limit:-1                                       |
|     Stage-1                                        |
|       Reducer 2 vectorized                         |
|       File Output Operator [FS_13]                 |
|         Group By Operator [GBY_12] (rows=7 width=100) |
|           Output:["_col0","_col1"],aggregations:["sum(VALUE._col0)"],keys:KEY._col0 |
|         <-Map 1 [SIMPLE_EDGE] vectorized           |
|           SHUFFLE [RS_11]                          |
|             PartitionCols:_col0                    |
|             Group By Operator [GBY_10] (rows=7 width=100) |
|               Output:["_col0","_col1"],aggregations:["sum(quantity)"],keys:category |
|               Select Operator [SEL_9] (rows=20913 width=96) |
|                 Output:["category","quantity"]     |
|                 TableScan [TS_0] (rows=20913 width=96) |
|                   retail_dw@retail_transactions_part,retail_transactions_part,Tbl:COMPLETE,Col:COMPLETE,Output:["category","quantity"] |
+----------------------------------------------------+
```

<!-- shell: beeline (hiveserver2) -->
```sql
EXPLAIN
SELECT category, SUM(quantity) FROM retail_transactions_part
WHERE region = 'HCM'
GROUP BY category;
```

**Kết quả mong đợi** (phần khác biệt):

```text
|             Group By Operator [GBY_11] (rows=28 width=100) |
|               Output:["_col0","_col1"],aggregations:["sum(quantity)"],keys:category |
|               Select Operator [SEL_10] (rows=714286 width=185) |
|                 Output:["category","quantity"]     |
|                 Filter Operator [FIL_9] (rows=714286 width=185) |
|                   predicate:(region = 'HCM')       |
|                   TableScan [TS_0] (rows=5000000 width=185) |
|                     retail_dw@retail_transactions_part,retail_transactions_part,Tbl:COMPLETE,Col:COMPLETE,Output:["category","quantity","region"] |
```

Với `dt = '2025-03-15'`, `TableScan` chỉ ước lượng 20 913 dòng (một ngày) và **không có** `Filter Operator`: điều kiện đã được dùng để chọn thư mục ngay lúc lập kế hoạch. Với `region = 'HCM'`, Hive phải quét toàn bộ 5 000 000 dòng rồi lọc. Xem chính xác các phân vùng được đọc bằng `EXPLAIN DEPENDENCY`:

<!-- shell: beeline (hiveserver2) -->
```sql
EXPLAIN DEPENDENCY
SELECT category, SUM(quantity) FROM retail_transactions_part WHERE dt = '2025-03-15' GROUP BY category;
```

**Kết quả mong đợi:**

```text
| {"input_tables":[{"tablename":"retail_dw@retail_transactions_part","tabletype":"EXTERNAL_TABLE"}],"input_partitions":[{"partitionName":"retail_dw@retail_transactions_part@dt=2025-03-15"}]} |
```

Chạy lại `EXPLAIN DEPENDENCY` với điều kiện `region = 'HCM'`: danh sách `input_partitions` có đủ **240** phân vùng (`dt=2025-01-01` … `dt=2025-08-28`). Tỷ lệ cắt tỉa của truy vấn theo `dt` = 239/240 ≈ 99,6 %.

### Bước 10: Đo thời gian truy vấn có và không có cắt tỉa phân vùng

Hive 4 có bộ đệm kết quả truy vấn (bật mặc định, nhưng chỉ áp dụng cho bảng transactional — bảng trong buổi này không phải transactional). Vẫn tắt hẳn trước khi đo để chắc chắn mỗi lần chạy đều thực thi thật:

<!-- shell: beeline (hiveserver2) -->
```sql
SET hive.query.results.cache.enabled=false;
SELECT category, SUM(quantity) AS sl FROM retail_transactions_part WHERE dt = '2025-03-15' GROUP BY category;
SELECT category, SUM(quantity) AS sl FROM retail_transactions_part WHERE region = 'HCM' GROUP BY category;
```

**Kết quả mong đợi:**

```text
+-------------+-------+
|  category   |  sl   |
+-------------+-------+
| dien_tu     | 9105  |
| gia_dung    | 8867  |
| me_va_be    | 9010  |
| sach        | 8764  |
| the_thao    | 8934  |
| thoi_trang  | 9075  |
| thuc_pham   | 9058  |
+-------------+-------+
7 rows selected (0.743 seconds)
+-------------+---------+
|  category   |   sl    |
+-------------+---------+
| dien_tu     | 970086  |
| gia_dung    | 964623  |
| me_va_be    | 964000  |
| sach        | 964858  |
| the_thao    | 963479  |
| thoi_trang  | 963906  |
| thuc_pham   | 966170  |
+-------------+---------+
7 rows selected (2.767 seconds)
```

Chạy mỗi câu 3 lần, ghi thời gian ở dòng `rows selected (... seconds)`. Ví dụ trên codespace 2 nhân:

| Truy vấn | Lần 1 (s) | Lần 2 (s) | Lần 3 (s) |
|---|---|---|---|
| `WHERE dt = '2025-03-15'` (1/240 phân vùng) | 0,743 | 1,458 | 0,752 |
| `WHERE region = 'HCM'` (240/240 phân vùng) | 2,767 | 2,648 | 1,946 |

Thời gian dao động giữa các lần (máy ảo dùng chung, khởi động JIT, nạp metadata), nên lấy trung vị. Chênh lệch giữa hai truy vấn chỉ khoảng 2–4 lần dù lượng dữ liệu đọc chênh 240 lần, vì chi phí cố định (biên dịch, khởi tạo Tez) chiếm phần lớn thời gian khi dữ liệu nhỏ; trên cụm thật với dữ liệu lớn, chênh lệch sẽ lớn hơn nhiều.

## PHẦN 4: SO SÁNH TEXT, ORC VÀ PARQUET

### Bước 11: Tạo ba bảng phân vùng theo region

Ba bảng có cùng nội dung, khác định dạng lưu trữ, cùng phân vùng theo `region` (7 phân vùng):

<!-- shell: beeline (hiveserver2) -->
```sql
USE retail_dw;
SET dfs.replication=2;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS retail_txn_part_text (
  transaction_id STRING, customer_id STRING, product_id STRING, category STRING,
  quantity INT, unit_price DOUBLE, discount DOUBLE, store_id STRING, ts STRING)
PARTITIONED BY (region STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

CREATE TABLE IF NOT EXISTS retail_txn_part_orc (
  transaction_id STRING, customer_id STRING, product_id STRING, category STRING,
  quantity INT, unit_price DOUBLE, discount DOUBLE, store_id STRING, ts STRING)
PARTITIONED BY (region STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

CREATE TABLE IF NOT EXISTS retail_txn_part_parquet (
  transaction_id STRING, customer_id STRING, product_id STRING, category STRING,
  quantity INT, unit_price DOUBLE, discount DOUBLE, store_id STRING, ts STRING)
PARTITIONED BY (region STRING)
STORED AS PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE retail_txn_part_text PARTITION (region)
SELECT transaction_id, customer_id, product_id, category,
       quantity, unit_price, discount, store_id, ts, region
FROM retail_transactions_raw;

INSERT OVERWRITE TABLE retail_txn_part_orc PARTITION (region)
SELECT transaction_id, customer_id, product_id, category,
       quantity, unit_price, discount, store_id, ts, region
FROM retail_txn_part_text;

INSERT OVERWRITE TABLE retail_txn_part_parquet PARTITION (region)
SELECT transaction_id, customer_id, product_id, category,
       quantity, unit_price, discount, store_id, ts, region
FROM retail_txn_part_text;
```

**Kết quả mong đợi:** ba lệnh `INSERT` mỗi lệnh chạy một DAG Tez 3 vertex, tổng cộng khoảng 70 giây trên codespace 2 nhân, ví dụ:

```text
Map 1 .......... container     SUCCEEDED      2          2        0        0       0       0
Reducer 2 ...... container     SUCCEEDED     22         22        0        0       0       0
Reducer 3 ...... container     SUCCEEDED     22         22        0        0       0       0
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 25.96 s
5,000,000 rows affected (26.307 seconds)
...
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 19.84 s
5,000,000 rows affected (20.024 seconds)
...
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 21.89 s
5,000,000 rows affected (22.313 seconds)
```

> ⚠️ Đây là lúc RAM cao nhất của buổi (codespace đo được 5,0–5,2 GB cho các container, `hiveserver2` gần chạm giới hạn 2 GB). Chạy lần lượt từng `INSERT` trong **một** phiên beeline, không mở nhiều phiên chạy song song.

### Bước 12: Kiểm tra số dòng và định dạng tệp

<!-- shell: beeline (hiveserver2) -->
```sql
SHOW PARTITIONS retail_txn_part_orc;
SELECT COUNT(*) FROM retail_txn_part_text;
SELECT COUNT(*) FROM retail_txn_part_orc;
SELECT COUNT(*) FROM retail_txn_part_parquet;
```

**Kết quả mong đợi:**

```text
+------------------+
|    partition     |
+------------------+
| region=CanTho    |
| region=DaNang    |
| region=HaiPhong  |
| region=Hanoi     |
| region=HCM       |
| region=LamDong   |
| region=NgheAn    |
+------------------+
7 rows selected (0.062 seconds)
+----------+
|   _c0    |
+----------+
| 5000000  |
+----------+
1 row selected (2.476 seconds)
+----------+
|   _c0    |
+----------+
| 5000000  |
+----------+
1 row selected (0.533 seconds)
+----------+
|   _c0    |
+----------+
| 5000000  |
+----------+
1 row selected (1.243 seconds)
```

Ba giá trị bằng nhau. Trên lần chạy kiểm thử, `COUNT(*)` của bảng ORC được trả lời ngay từ thống kê trong Metastore (không có bảng `VERTICES`, 0,5 s), còn bảng Text và Parquet vẫn chạy một DAG Tez nhỏ. Kiểm tra định dạng thực trên HDFS bằng *magic bytes* (terminal B, container `namenode`):

<!-- shell: Bên trong container namenode -->
```bash
W=/user/hive/warehouse/retail_dw.db
hdfs dfs -ls -h $W/retail_txn_part_orc/region=HCM
F=$(hdfs dfs -ls $W/retail_txn_part_orc/region=HCM | awk '/000/ {print $8; exit}')
hdfs dfs -cat $F 2>/dev/null | head -c 3; echo
P=$(hdfs dfs -ls $W/retail_txn_part_parquet/region=HCM | awk '/000/ {print $8; exit}')
hdfs dfs -cat $P 2>/dev/null | head -c 4; echo
hdfs dfs -cat $W/retail_txn_part_text/region=HCM/* 2>/dev/null | head -n 2
```

**Kết quả mong đợi:**

```text
Found 1 items
-rw-r--r--   2 hive supergroup     62.3 M 2026-11-05 23:26 /user/hive/warehouse/retail_dw.db/retail_txn_part_orc/region=HCM/000005_0
ORC
PAR1
T0003583128,C0001770,P00484,sach,4,8765000.0,0.3,S024,2025-03-26 14:58:37
T0003583130,C0032555,P00111,gia_dung,3,1874000.0,0.2,S008,2025-04-11 00:36:09
```

Tệp ORC bắt đầu bằng `ORC`, tệp Parquet bắt đầu (và kết thúc) bằng `PAR1`. Tệp Text không còn cột `region` trong dữ liệu — giá trị đó chỉ nằm trong tên thư mục `region=HCM`.

### Bước 13: So sánh dung lượng và thời gian ba truy vấn

Dung lượng (container `namenode`):

<!-- shell: Bên trong container namenode -->
```bash
W=/user/hive/warehouse/retail_dw.db
hdfs dfs -du -s -h $W/retail_txn_part_text $W/retail_txn_part_orc $W/retail_txn_part_parquet
```

**Kết quả mong đợi:**

```text
367.9 M  735.7 M  /user/hive/warehouse/retail_dw.db/retail_txn_part_text
139.4 M  278.8 M  /user/hive/warehouse/retail_dw.db/retail_txn_part_orc
117.4 M  234.8 M  /user/hive/warehouse/retail_dw.db/retail_txn_part_parquet
```

Ba truy vấn dùng chung (thay `retail_txn_part_orc` bằng `retail_txn_part_text` và `retail_txn_part_parquet` để đo hai bảng còn lại):

<!-- shell: beeline (hiveserver2) -->
```sql
SET hive.query.results.cache.enabled=false;
-- Truy vấn 1: doanh thu theo region (quét toàn bảng, GROUP BY)
SELECT region, CAST(SUM(quantity * unit_price * (1 - discount)) AS BIGINT) AS revenue
FROM retail_txn_part_orc GROUP BY region;
-- Truy vấn 2: lọc theo điều kiện trên cột dữ liệu
SELECT COUNT(*) AS so_gd FROM retail_txn_part_orc WHERE category = 'dien_tu' AND quantity > 3;
-- Truy vấn 3: join với bảng chiều product_dim
SELECT p.brand, CAST(SUM(t.quantity * t.unit_price) AS BIGINT) AS revenue
FROM retail_txn_part_orc t JOIN product_dim p ON t.product_id = p.product_id
GROUP BY p.brand ORDER BY revenue DESC LIMIT 5;
```

**Kết quả mong đợi** (giống nhau cho cả ba định dạng):

```text
+-----------+-----------------+
|  region   |     revenue     |
+-----------+-----------------+
| CanTho    | 10178397790200  |
| DaNang    | 13604231803050  |
| HCM       | 76689681595950  |
| HaiPhong  | 8507582848950   |
| Hanoi     | 51023353014350  |
| LamDong   | 5083454187550   |
| NgheAn    | 5105528945900   |
+-----------+-----------------+
+---------+
|  so_gd  |
+---------+
| 286038  |
+---------+
+----------+----------------+
| p.brand  |    revenue     |
+----------+----------------+
| Brand16  | 8260961635000  |
| Brand36  | 7507728050000  |
| Brand30  | 7089892670000  |
| Brand17  | 6770981310000  |
| Brand04  | 6385132385000  |
+----------+----------------+
```

Tổng doanh thu 7 khu vực = 170 192 230 185 950, khớp với kết quả MapReduce ở Buổi 2. Ví dụ bảng số liệu trên codespace 2 nhân (trung vị của 3 lần chạy; tự điền số đo của bạn):

| Định dạng | Dung lượng logic | So với Text | TG truy vấn 1 (s) | TG truy vấn 2 (s) | TG truy vấn 3 (s) |
|---|---|---|---|---|---|
| Text | 367,9 MB | 100 % | 3,53 | 2,32 | 4,04 |
| ORC + Snappy | 139,4 MB | 37,9 % | 1,51 | 0,63 | 2,22 |
| Parquet + Snappy | 117,4 MB | 31,9 % | 1,17 | 0,81 | 2,13 |

Nhận xét: ORC/Parquet chỉ chiếm khoảng 1/3 dung lượng Text nhờ lưu theo cột + mã hoá (dictionary, RLE) + nén; truy vấn 1 và 2 nhanh hơn khoảng 2–4 lần (ORC: 2,3 và 3,7 lần; Parquet: 3,0 và 2,9 lần) vì chỉ đọc các cột cần thiết và dùng thực thi vector hoá. Truy vấn 3 cải thiện ít hơn (khoảng 1,8–1,9 lần) vì một phần thời gian dành cho join và sắp xếp. Trong lần đo này Parquet vừa nhỏ hơn vừa nhanh hơn ORC ở truy vấn 1, còn ORC nhanh hơn ở truy vấn 2 (lọc) — không có định dạng "luôn tốt nhất", và thứ tự có thể đảo giữa các lần đo trên máy ảo dùng chung.

## PHẦN 5: EXPLAIN, MAP JOIN VÀ THỐNG KÊ

### Bước 14: EXPLAIN khi tắt map join

<!-- shell: beeline (hiveserver2) -->
```sql
SET hive.auto.convert.join=false;
EXPLAIN
SELECT p.brand, SUM(t.quantity * t.unit_price) AS revenue
FROM retail_txn_part_orc t
JOIN product_dim p ON t.product_id = p.product_id
GROUP BY p.brand;
```

**Kết quả mong đợi** (rút gọn):

```text
| Vertex dependency in root stage                    |
| Reducer 2 <- Map 1 (SIMPLE_EDGE), Map 4 (SIMPLE_EDGE) |
| Reducer 3 <- Reducer 2 (SIMPLE_EDGE)               |
...
|               Merge Join Operator [MERGEJOIN_19] (rows=5500000 width=98) |
|                 Conds:RS_22._col0=RS_25._col0(Inner),Output:["_col1","_col3"] |
|               <-Map 1 [SIMPLE_EDGE] vectorized     |
|                 SHUFFLE [RS_22]                    |
|                   PartitionCols:_col0              |
...
|                       TableScan [TS_0] (rows=5000000 width=102) |
|                         retail_dw@retail_txn_part_orc,t,Tbl:COMPLETE,Col:COMPLETE,Output:["product_id","quantity","unit_price"] |
|               <-Map 4 [SIMPLE_EDGE] vectorized     |
|                 SHUFFLE [RS_25]                    |
|                   PartitionCols:_col0              |
...
|                       TableScan [TS_3] (rows=379 width=342) |
|                         retail_dw@product_dim,p,Tbl:COMPLETE,Col:NONE,Output:["product_id","brand"] |
```

*Common join* (trên Tez gọi là **Merge Join**): cả hai bảng đều được **SHUFFLE** theo `product_id` sang `Reducer 2` để ghép, tức là 5 triệu dòng giao dịch phải đi qua shuffle. Lưu ý `product_dim` đang được ước lượng `rows=379` và `Col:NONE` (chưa có thống kê cột) — giá trị thật là 500.

### Bước 15: EXPLAIN khi bật map join

<!-- shell: beeline (hiveserver2) -->
```sql
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask.size=20000000;
EXPLAIN
SELECT p.brand, SUM(t.quantity * t.unit_price) AS revenue
FROM retail_txn_part_orc t
JOIN product_dim p ON t.product_id = p.product_id
GROUP BY p.brand;
```

**Kết quả mong đợi** (rút gọn):

```text
| Vertex dependency in root stage                    |
| Map 1 <- Map 3 (BROADCAST_EDGE)                    |
| Reducer 2 <- Map 1 (SIMPLE_EDGE)                   |
...
|               Map Join Operator [MAPJOIN_26] (rows=5500000 width=98) |
|                 Conds:SEL_25._col0=RS_23._col0(Inner),Output:["_col1","_col3"] |
|               <-Map 3 [BROADCAST_EDGE] vectorized  |
|                 BROADCAST [RS_23]                  |
|                   PartitionCols:_col0              |
...
|                       TableScan [TS_3] (rows=379 width=342) |
|                         retail_dw@product_dim,p,Tbl:COMPLETE,Col:NONE,Output:["product_id","brand"] |
|               <-Select Operator [SEL_25] (rows=5000000 width=98) |
...
|                     TableScan [TS_0] (rows=5000000 width=102) |
```

Khác biệt chính: `product_dim` (nhỏ hơn ngưỡng 20 MB) được **BROADCAST** tới vertex `Map 1`, nơi `Map Join Operator` ghép trực tiếp từng dòng giao dịch với bảng băm trong bộ nhớ. Không còn bước shuffle bảng lớn cho join; DAG giảm từ 3 xuống 2 tầng xử lý (`Map 1 → Reducer 2`).

### Bước 16: Đo thời gian join trước và sau khi bật map join

<!-- shell: beeline (hiveserver2) -->
```sql
SET hive.query.results.cache.enabled=false;
SET hive.auto.convert.join=false;
SELECT p.brand, CAST(SUM(t.quantity * t.unit_price) AS BIGINT) AS revenue
FROM retail_txn_part_orc t JOIN product_dim p ON t.product_id = p.product_id
GROUP BY p.brand ORDER BY revenue DESC LIMIT 3;
SET hive.auto.convert.join=true;
SELECT p.brand, CAST(SUM(t.quantity * t.unit_price) AS BIGINT) AS revenue
FROM retail_txn_part_orc t JOIN product_dim p ON t.product_id = p.product_id
GROUP BY p.brand ORDER BY revenue DESC LIMIT 3;
```

**Kết quả mong đợi** (bảng tiến độ của hai lần chạy, rút gọn):

```text
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Map 5 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      2          2        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      3          3        0        0       0       0
Reducer 4 ...... container     SUCCEEDED      1          1        0        0       0       0
VERTICES: 05/05  [==========================>>] 100%  ELAPSED TIME: 7.18 s
+----------+----------------+
| p.brand  |    revenue     |
+----------+----------------+
| Brand16  | 8260961635000  |
| Brand36  | 7507728050000  |
| Brand30  | 7089892670000  |
+----------+----------------+
3 rows selected (7.496 seconds)
...
Map 4 .......... container     SUCCEEDED      1          1        0        0       0       0
Map 1 .......... container     SUCCEEDED      1          1        0        0       0       0
Reducer 2 ...... container     SUCCEEDED      3          3        0        0       0       0
Reducer 3 ...... container     SUCCEEDED      1          1        0        0       0       0
VERTICES: 04/04  [==========================>>] 100%  ELAPSED TIME: 1.73 s
3 rows selected (1.891 seconds)
```

Ví dụ thời gian trên codespace 2 nhân (3 lần mỗi chế độ): tắt map join 7,50 / 5,88 / 7,55 s; bật map join 1,89 / 1,97 / 2,20 s — nhanh hơn khoảng 3,8 lần, kết quả giống hệt. Trên máy ít nhân, lợi ích của việc bỏ shuffle càng rõ.

### Bước 17: Thu thập thống kê bằng ANALYZE TABLE

Hive 4 đã tự thu thập thống kê cho các bảng được nạp bằng `INSERT` (như `retail_txn_part_orc`), nhưng **không** có thống kê cho bảng EXTERNAL trỏ vào tệp có sẵn (`product_dim`, `retail_transactions_raw`). Xem trước:

<!-- shell: beeline (hiveserver2) -->
```sql
DESCRIBE FORMATTED retail_txn_part_orc PARTITION (region='HCM');
DESCRIBE FORMATTED product_dim;
```

**Kết quả mong đợi** (các dòng thống kê):

```text
| Partition Parameters:             | NULL                                               | NULL                                               |
|                                   | COLUMN_STATS_ACCURATE                              | {\"BASIC_STATS\":\"true\",\"COLUMN_STATS\":{\"category\":\"true\",\"customer_id\":\"true\",\"discount\":\"true\",\"product_id\":\"true\",\"quantity\":\"true\",\"store_id\":\"true\",\"transaction_id\":\"true\",\"ts\":\"true\",\"unit_price\":\"true\"}} |
|                                   | numFiles                                           | 1                                                  |
|                                   | numRows                                            | 2252467                                            |
|                                   | rawDataSize                                        | 1304178393                                         |
|                                   | totalSize                                          | 65378422                                           |
...
| Table Parameters:             | NULL                                               | NULL                  |
|                               | numFiles                                           | 1                     |
|                               | totalSize                                          | 22590                 |
```

Phân vùng `region=HCM` đã có `numRows=2252467` (45 % giao dịch — dữ liệu lệch có chủ đích), còn `product_dim` chỉ có `numFiles`, `totalSize`, **không có** `numRows`. Thu thập thống kê:

<!-- shell: beeline (hiveserver2) -->
```sql
ANALYZE TABLE retail_txn_part_orc PARTITION (region) COMPUTE STATISTICS;
ANALYZE TABLE retail_txn_part_orc PARTITION (region) COMPUTE STATISTICS FOR COLUMNS product_id, category, quantity, unit_price;
ANALYZE TABLE product_dim COMPUTE STATISTICS;
ANALYZE TABLE product_dim COMPUTE STATISTICS FOR COLUMNS product_id, brand;
DESCRIBE FORMATTED product_dim;
DESCRIBE FORMATTED product_dim brand;
```

**Kết quả mong đợi** (rút gọn):

```text
No rows affected (0.202 seconds)
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 6.33 s
No rows affected (6.59 seconds)
No rows affected (0.276 seconds)
No rows affected (0.477 seconds)
...
|                               | COLUMN_STATS_ACCURATE                              | {\"BASIC_STATS\":\"true\",\"COLUMN_STATS\":{\"brand\":\"true\",\"product_id\":\"true\"}} |
|                               | numFiles                                           | 1                                                  |
|                               | numRows                                            | 500                                                |
|                               | rawDataSize                                        | 22040                                              |
|                               | totalSize                                          | 22590                                              |
...
| col_name               | brand                                              |
| data_type              | string                                             |
| num_nulls              | 0                                                  |
| distinct_count         | 40                                                 |
| avg_col_len            | 7.0                                                |
| max_col_len            | 7                                                  |
```

Chạy lại `EXPLAIN` của Bước 15: ước lượng cho `product_dim` đổi thành `TableScan [TS_3] (rows=500 width=181)` với `Col:COMPLETE`, và số dòng ước lượng sau join đổi từ `rows=5500000` thành `rows=5030181` — bộ tối ưu chi phí (CBO) dùng số liệu thật thay cho ước lượng thô từ kích thước tệp.

> 💡 `ANALYZE TABLE ... COMPUTE STATISTICS NOSCAN` chỉ đọc metadata tệp (`numFiles`, `totalSize`), không cho `numRows`; muốn có số dòng thật phải bỏ `NOSCAN`.

## PHẦN 6: HÀM CỬA SỔ

### Bước 18: Xếp hạng sản phẩm theo doanh thu trong từng khu vực

<!-- shell: beeline (hiveserver2) -->
```sql
WITH revenue_by_product AS (
  SELECT region, product_id,
         SUM(quantity * unit_price * (1 - discount)) AS revenue
  FROM retail_txn_part_orc
  GROUP BY region, product_id
),
ranked AS (
  SELECT region, product_id, revenue,
         RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS rank_in_region,
         ROUND(revenue / SUM(revenue) OVER (PARTITION BY region), 4) AS ty_trong
  FROM revenue_by_product
)
SELECT region, product_id, CAST(revenue AS BIGINT) AS revenue, rank_in_region, ty_trong
FROM ranked
WHERE rank_in_region <= 3
ORDER BY region, rank_in_region;
```

**Kết quả mong đợi:**

```text
+-----------+-------------+---------------+-----------------+-----------+
|  region   | product_id  |    revenue    | rank_in_region  | ty_trong  |
+-----------+-------------+---------------+-----------------+-----------+
| CanTho    | P00005      | 23015929600   | 1               | 0.0023    |
| CanTho    | P00377      | 22936564500   | 2               | 0.0023    |
| CanTho    | P00380      | 22875097800   | 3               | 0.0022    |
| DaNang    | P00169      | 30818678150   | 1               | 0.0023    |
| DaNang    | P00379      | 30617077400   | 2               | 0.0023    |
| DaNang    | P00041      | 30408410700   | 3               | 0.0022    |
| HCM       | P00177      | 161694246100  | 1               | 0.0021    |
| HCM       | P00408      | 161481725850  | 2               | 0.0021    |
| HCM       | P00297      | 160961670150  | 3               | 0.0021    |
| HaiPhong  | P00279      | 19773332350   | 1               | 0.0023    |
| HaiPhong  | P00191      | 19550912350   | 2               | 0.0023    |
| HaiPhong  | P00249      | 19364120250   | 3               | 0.0023    |
| Hanoi     | P00480      | 108661687050  | 1               | 0.0021    |
| Hanoi     | P00357      | 108436273900  | 2               | 0.0021    |
| Hanoi     | P00079      | 107943931700  | 3               | 0.0021    |
| LamDong   | P00134      | 13441708450   | 1               | 0.0026    |
| LamDong   | P00499      | 12395718600   | 2               | 0.0024    |
| LamDong   | P00369      | 11945342400   | 3               | 0.0023    |
| NgheAn    | P00117      | 12544568600   | 1               | 0.0025    |
| NgheAn    | P00291      | 12436748100   | 2               | 0.0024    |
| NgheAn    | P00499      | 12184348100   | 3               | 0.0024    |
+-----------+-------------+---------------+-----------------+-----------+
21 rows selected (1.978 seconds)
```

`RANK() OVER (PARTITION BY region ORDER BY revenue DESC)` đánh số lại từ 1 trong từng khu vực; `SUM(revenue) OVER (PARTITION BY region)` là tổng doanh thu khu vực, đặt cạnh từng dòng mà không gộp dòng như `GROUP BY`. Không thể viết `WHERE rank_in_region <= 3` ngay trong câu chứa hàm cửa sổ vì `WHERE` được xử lý trước — phải bọc trong CTE/truy vấn con như trên.

### Bước 19: Kiểm chứng kết quả xếp hạng

<!-- shell: beeline (hiveserver2) -->
```sql
-- Tổng tỷ trọng mỗi khu vực phải bằng 1
WITH revenue_by_product AS (
  SELECT region, product_id, SUM(quantity * unit_price * (1 - discount)) AS revenue
  FROM retail_txn_part_orc GROUP BY region, product_id
),
r AS (
  SELECT region, revenue / SUM(revenue) OVER (PARTITION BY region) AS ty_trong
  FROM revenue_by_product
)
SELECT region, ROUND(SUM(ty_trong), 6) AS tong_ty_trong FROM r GROUP BY region ORDER BY region;
-- Sản phẩm doanh thu cao nhất mỗi khu vực, tính độc lập không dùng hàm cửa sổ
WITH rp AS (
  SELECT region, product_id, SUM(quantity * unit_price * (1 - discount)) AS revenue
  FROM retail_txn_part_orc GROUP BY region, product_id
),
mx AS (SELECT region, MAX(revenue) AS max_revenue FROM rp GROUP BY region)
SELECT rp.region, rp.product_id, CAST(rp.revenue AS BIGINT) AS revenue
FROM rp JOIN mx ON rp.region = mx.region AND rp.revenue = mx.max_revenue
ORDER BY rp.region;
```

**Kết quả mong đợi:**

```text
+-----------+----------------+
|  region   | tong_ty_trong  |
+-----------+----------------+
| CanTho    | 1.0            |
| DaNang    | 1.0            |
| HCM       | 1.0            |
| HaiPhong  | 1.0            |
| Hanoi     | 1.0            |
| LamDong   | 1.0            |
| NgheAn    | 1.0            |
+-----------+----------------+
+------------+----------------+---------------+
| rp.region  | rp.product_id  |    revenue    |
+------------+----------------+---------------+
| CanTho     | P00005         | 23015929600   |
| DaNang     | P00169         | 30818678150   |
| HCM        | P00177         | 161694246100  |
| HaiPhong   | P00279         | 19773332350   |
| Hanoi      | P00480         | 108661687050  |
| LamDong    | P00134         | 13441708450   |
| NgheAn     | P00117         | 12544568600   |
+------------+----------------+---------------+
```

Sản phẩm hạng 1 ở Bước 18 trùng với kết quả tính độc lập cho cả 7 khu vực.

## PHẦN 7: DỌN DẸP

### Bước 20: Xoá bảng thử nghiệm

So sánh hành vi `DROP TABLE` của bảng EXTERNAL thật (`product_dim`) với bảng tạo bằng `CREATE TABLE` đã được chuyển thành external có `purge` (`retail_txn_part_text`):

<!-- shell: beeline (hiveserver2) -->
```sql
USE retail_dw;
DROP TABLE retail_txn_part_text;
DROP TABLE product_dim;
SHOW TABLES;
```

**Kết quả mong đợi:**

```text
+---------------------------+
|         tab_name          |
+---------------------------+
| retail_transactions_part  |
| retail_transactions_raw   |
| retail_txn_part_orc       |
| retail_txn_part_parquet   |
+---------------------------+
4 rows selected (0.031 seconds)
```

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -ls /user/hive/warehouse/retail_dw.db
hdfs dfs -ls /data/raw/products
```

**Kết quả mong đợi:**

```text
Found 3 items
drwxr-xr-x   - hive supergroup          0 2026-11-05 23:25 /user/hive/warehouse/retail_dw.db/retail_transactions_part
drwxr-xr-x   - hive supergroup          0 2026-11-05 23:30 /user/hive/warehouse/retail_dw.db/retail_txn_part_orc
drwxr-xr-x   - hive supergroup          0 2026-11-05 23:27 /user/hive/warehouse/retail_dw.db/retail_txn_part_parquet
Found 1 items
-rw-r--r--   2 root supergroup      22590 2026-11-05 23:23 /data/raw/products/products.csv
```

Thư mục `retail_txn_part_text` đã bị xoá cùng bảng (`external.table.purge=TRUE`, giải phóng 736 MB trên HDFS), còn `products.csv` của bảng EXTERNAL `product_dim` vẫn nguyên. Muốn dùng lại `product_dim`, chạy lại lệnh `CREATE EXTERNAL TABLE` ở Bước 4 — dữ liệu không mất. Thoát beeline bằng `!quit` và thoát container `namenode` bằng `exit`.

## Kết thúc buổi

**1. Dọn thư mục tạm của Tez.** Tez chạy cục bộ trong `hiveserver2` và giữ lại dữ liệu trung gian của mọi phiên beeline trong `/tmp/hive/hive/_tez_session_dir` (sau buổi này khoảng 1,7 GB, không tự xoá kể cả khi tắt Hive). Sau khi đã `!quit` mọi phiên beeline:

<!-- shell: Terminal codespace -->
```bash
docker exec hiveserver2 du -sh /tmp/hive/hive/_tez_session_dir
docker exec hiveserver2 bash -c 'rm -rf /tmp/hive/hive/_tez_session_dir/*'
docker exec hiveserver2 du -sh /tmp/hive/hive/_tez_session_dir
```

**Kết quả mong đợi** (con số đầu tuỳ số phiên bạn đã mở):

```text
1.7G	/tmp/hive/hive/_tez_session_dir
4.0K	/tmp/hive/hive/_tez_session_dir
```

**2. Tắt các dịch vụ Hive** để giải phóng RAM (metadata trong PostgreSQL và dữ liệu HDFS được giữ nguyên):

<!-- shell: Terminal codespace -->
```bash
lab stop-extras
lab status
```

**Kết quả mong đợi:**

```text
 Container hiveserver2  Stopping
 Container hiveserver2  Stopped
 Container hive-metastore  Stopping
 Container hive-metastore  Stopped
 Container hive-metastore-db  Stopping
 Container hive-metastore-db  Stopped
NAME              STATUS
datanode1         Up 11 minutes (healthy)
...
spark-master      Up 11 minutes (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 2.6 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

Lần sau bật lại bằng `lab hive`, `SHOW TABLES` trong `retail_dw` vẫn thấy 4 bảng và `SELECT COUNT(*) FROM retail_txn_part_orc` vẫn trả 5000000 (đã kiểm tra, kể cả sau khi dừng rồi mở lại codespace). **Không** chạy `lab reset` nếu muốn giữ các bảng cho các buổi sau.

**3. Lưu bài làm lên GitHub.** Lưu các tệp `.sql` và báo cáo của bạn trong `src/lab03/`, rồi:

<!-- shell: Terminal codespace -->
```bash
git add src/lab03
git commit -m "Buoi 3: Hive phan vung, ORC/Parquet"
git push
```

Lệnh `commit` in một dòng dạng `[main ...] Buoi 3: Hive phan vung, ORC/Parquet` kèm số tệp thay đổi; `push` kết thúc bằng dòng `main -> main`. Thư mục `data/` đã được `.gitignore` loại trừ nên không bị đẩy lên.

**4. Dừng codespace** để không tốn quota: github.com/codespaces → dấu `...` cạnh codespace → *Stop codespace* (hoặc Command Palette `Ctrl+Shift+P` → `Codespaces: Stop Current Codespace`). Codespace cũng tự dừng sau 30 phút không thao tác; dữ liệu HDFS, bảng Hive và tệp trong repo vẫn còn khi mở lại.

> ⚠️ Khi mở lại codespace, chỉ **cụm lõi** tự bật (khoảng 1 phút). Hive phải bật lại bằng `lab hive`, và mọi lệnh `SET` của phiên beeline cũ đã mất.

## Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `beeline` báo `Unable to create a terminal` | Chạy không có TTY (thiếu `-it`, chạy trong script, hoặc dùng `-f`) | Tương tác: `docker exec -it hiveserver2 beeline ...`; chạy tệp: `docker exec -i -e HADOOP_CLIENT_OPTS=-Dorg.jline.terminal.provider=dumb hiveserver2 beeline ... < src/lab03/tep.sql` |
| `beeline -f tep.sql` chỉ in câu lệnh, không thực thi | Hành vi của beeline 4.2.1 trong môi trường này | Dùng chuyển hướng stdin như trên |
| `bash: src/lab03/thu_f.sql: No such file or directory` | Terminal không đứng ở gốc repo | `cd /workspaces/<tên-repo>` (hoặc mở terminal mới — terminal mới luôn đứng ở gốc repo) |
| `Could not open client transport ... Connection refused` | HiveServer2 chưa sẵn sàng, đã bị `lab stop-extras`, hoặc codespace vừa mở lại (Hive không tự bật) | `lab hive`; chờ 10–20 giây; `lab status`; `docker logs hiveserver2 --tail 50` |
| `lab hive` lần đầu chạy rất lâu, in nhiều dòng `Pulling` | Đang tải image Hive (~3,9 GB) và PostgreSQL (~0,6 GB) | Chờ (vài phút); nếu báo hết thời gian chờ, chạy lại `lab hive` — phần đã tải được giữ lại |
| `No such container: hive-server` | Tên container cũ | Dùng `hiveserver2` |
| `fsck` báo mọi tệp trong `/user/hive/warehouse` under-replicated | Hive ghi 3 bản sao theo cấu hình mặc định của image Hive | `SET dfs.replication=2;` đầu mỗi phiên; sửa tệp cũ: `hdfs dfs -setrep -R 2 /user/hive/warehouse` |
| `SemanticException [Error 10096]: Dynamic partition strict mode requires at least one static partition column` | Chế độ `strict` | `SET hive.exec.dynamic.partition.mode=nonstrict;` |
| Lỗi vượt `hive.exec.max.dynamic.partitions(.pernode)` | Quá nhiều giá trị phân vùng (ví dụ `ts` sai định dạng sinh giá trị rác) | Tăng giới hạn hợp lý; kiểm tra `SELECT SUBSTR(ts,1,10) d, COUNT(*) FROM retail_transactions_raw GROUP BY SUBSTR(ts,1,10) ORDER BY 2 DESC LIMIT 20;` |
| `COUNT(*)` ra 200000 thay vì 5000000 | Đang dùng bộ dữ liệu 200 000 dòng của codespace tạo trước 06/11/2026 | `lab data 5000000`, rồi `hdfs dfs -put -f` lại ở Bước 3 |
| Lần chạy đầu của mỗi truy vấn chậm hơn hẳn các lần sau; thời gian dao động | Khởi động JIT, nạp metadata; máy ảo Codespaces dùng chung | Chạy mỗi truy vấn 3 lần, lấy trung vị; `SET hive.query.results.cache.enabled=false;` để loại trừ bộ đệm kết quả |
| `COUNT(*)` thiếu hoặc thừa đúng số tệp | `skip.header.line.count` bỏ dòng đầu của **mỗi** tệp | Mọi tệp trong thư mục bảng phải có header (hoặc đều không có và bỏ thuộc tính) |
| Kết quả toàn `NULL` | Sai ký tự phân cách hoặc thứ tự cột | `hdfs dfs -cat ... | head -3` để kiểm tra dữ liệu thực tế; sửa `FIELDS TERMINATED BY` |
| `hiveserver2` tự khởi động lại khi chạy truy vấn lớn, beeline mất kết nối | Thiếu RAM (giới hạn 2 GB của container; máy 8 GB khi bật thêm dịch vụ khác) | `lab status` xem RAM; `lab stop-extras` rồi chỉ bật `lab hive`; không mở nhiều phiên beeline chạy song song; nếu vẫn lỗi, tạo codespace 4 nhân/16 GB |
| Codespace tự dừng giữa buổi, mở lại thì beeline mất | Không thao tác 30 phút (hoặc đóng tab trình duyệt) | Mở lại codespace, `lab hive`, kết nối lại beeline, chạy lại `USE retail_dw;` và các `SET` cần thiết; `INSERT OVERWRITE` dở dang có thể chạy lại an toàn |
| Link HiveServer2 UI không mở được | Hive chưa bật, cổng 10002 chưa được forward, hoặc dùng `http://localhost:10002` từ trình duyệt máy mình | `lab hive`; lấy URL bằng `lab ui` hoặc tab PORTS (thêm cổng 10002 nếu chưa có); kiểm tra trong codespace bằng `curl -s -o /dev/null -w '%{http_code}' localhost:10002` (mong đợi `200`) |
| Link nội bộ trong Web UI (`http://hiveserver2:...`, `http://nodemanager1:8042/...`) không mở được | Tên máy nội bộ của mạng Docker | Dùng URL tương ứng từ `lab ui` |
| `No space left on device` hoặc cảnh báo đĩa codespace gần đầy | Image Hive + dữ liệu HDFS + thư mục tạm Tez (~1,7 GB mỗi buổi) | Dọn `/tmp/hive/hive/_tez_session_dir` (mục Kết thúc buổi); `docker system df` để xem; xoá bảng không dùng (`DROP TABLE`) |
| Truy vấn Hive không thấy trên YARN (8088) | Tez chạy ở chế độ local trong HiveServer2 | Bình thường; theo dõi ở HiveServer2 UI (cổng 10002, lấy URL bằng `lab ui`) |

## Câu hỏi kiểm tra

1. Vì sao bảng ORC/Parquet nén Snappy chỉ chiếm khoảng 1/3 dung lượng bảng Text cho cùng dữ liệu, và vì sao truy vấn 2 ở Bước 13 (lọc theo `category`, `quantity`) nhanh hơn nhiều trên ORC?
   *Gợi ý: lưu theo cột, mã hoá dictionary/RLE trước khi nén, chỉ đọc cột cần dùng, chỉ số min/max theo stripe/row group cho phép bỏ qua khối dữ liệu.*
2. Giải thích vì sao `EXPLAIN` của truy vấn `WHERE dt = '2025-03-15'` không có `Filter Operator`, còn truy vấn `WHERE region = 'HCM'` thì có. Nếu thường xuyên lọc theo khu vực, nên thiết kế phân vùng như thế nào, và nhược điểm của việc phân vùng theo cột có quá nhiều giá trị là gì?
   *Gợi ý: partition pruning ở bước lập kế hoạch; quá nhiều phân vùng → nhiều tệp nhỏ, tải cho Metastore và NameNode.*
3. `hive.auto.convert.join` dựa vào tiêu chí nào để chuyển Merge Join thành Map Join? Trong `EXPLAIN` của Tez, dấu hiệu nào cho biết đang dùng Map Join?
   *Gợi ý: kích thước ước lượng của bảng nhỏ so với `hive.auto.convert.join.noconditionaltask.size`; `BROADCAST_EDGE`, `Map Join Operator`.*
4. Vì sao `ANALYZE TABLE` vẫn cần thiết trong Hive 4 dù thống kê đã tự thu thập khi `INSERT`? Ước lượng nào trong `EXPLAIN` thay đổi sau khi chạy `ANALYZE` ở Bước 17?
   *Gợi ý: bảng EXTERNAL trỏ vào tệp nạp ngoài Hive không có thống kê; `rows=379 Col:NONE` → `rows=500 Col:COMPLETE`.*
5. Phân biệt `RANK()`, `DENSE_RANK()` và `ROW_NUMBER()` khi có nhiều sản phẩm cùng doanh thu. Ngoài ra, giải thích sự khác nhau khi `DROP TABLE` giữa `product_dim` và `retail_txn_part_text` trong Bước 20.
   *Gợi ý: nhảy số / không nhảy số / luôn duy nhất; EXTERNAL thật giữ dữ liệu, bảng có `external.table.purge=TRUE` xoá dữ liệu.*

## Bài tập mở rộng

1. **ORC ZLIB so với Snappy.** Tạo bảng `retail_txn_part_orc_zlib` giống `retail_txn_part_orc` nhưng `TBLPROPERTIES ('orc.compress'='ZLIB')`, nạp dữ liệu, so sánh dung lượng (`hdfs dfs -du -s -h`) và thời gian ba truy vấn ở Bước 13. Nhận xét đánh đổi giữa tỷ lệ nén và tốc độ giải nén.
2. **LAG/LEAD theo tháng.** Viết truy vấn tính doanh thu theo `(region, thang)` với `thang = SUBSTR(ts, 1, 7)`, dùng `LAG()` để tính mức tăng/giảm so với tháng trước, rồi xếp hạng các tháng sụt giảm mạnh nhất trong mỗi khu vực. Lưu ý dữ liệu tháng 2025-08 chỉ đến ngày 28.
3. **Thực thi vector hoá.** Chạy lại ba truy vấn ở Bước 13 trên ORC với `SET hive.vectorized.execution.enabled=false;` rồi `=true;` (mặc định). So sánh thời gian và tìm từ khoá `vectorized` trong `EXPLAIN`. Giải thích vì sao xử lý theo lô cột (vector 1024 dòng) nhanh hơn xử lý từng dòng.
4. **Bucketing.** Tạo bảng ORC `CLUSTERED BY (customer_id) INTO 8 BUCKETS`, nạp dữ liệu, kiểm tra số tệp trong mỗi phân vùng trên HDFS và thử `TABLESAMPLE(BUCKET 1 OUT OF 8 ON customer_id)`. So sánh với lấy mẫu `LIMIT`.

> 💡 Mỗi bảng mới 5 triệu dòng ở bài tập mở rộng chiếm thêm khoảng 280 MB trên HDFS (2 bản sao) và khoảng 1 phút nạp trên codespace 2 nhân. Xoá bảng sau khi đo xong (`DROP TABLE ...`) để tiết kiệm đĩa codespace.

## Yêu cầu nộp bài và tiêu chí chấm

Nộp một tệp PDF hoặc Markdown `MSSV_Buoi03_BigData.pdf` (hoặc `.md`) gồm toàn bộ câu lệnh HiveQL đã chạy (đồng thời commit các tệp `.sql` trong `src/lab03/` lên repo GitHub của bạn), log/ảnh chụp kết quả các lệnh `EXPLAIN`, `EXPLAIN DEPENDENCY`, `DESCRIBE FORMATTED`, bảng so sánh dung lượng/thời gian đã điền số liệu đo trên codespace của bạn (ghi rõ cấu hình máy 2 hay 4 nhân), ảnh chụp HiveServer2 Web UI (mở bằng đường dẫn cổng 10002 từ `lab ui`, phần danh sách truy vấn) và trả lời 5 câu hỏi kiểm tra.

| Tiêu chí | Điểm |
|---|---|
| Kết nối beeline, tạo bảng EXTERNAL, đối chiếu số dòng và minh hoạ schema-on-read (Phần 1–2) | 1,5 |
| Bảng phân vùng động theo ngày: nạp đúng 240 phân vùng, kiểm chứng trên HDFS, có `SET dfs.replication=2` (Bước 7–8) | 1,5 |
| Partition pruning: so sánh `EXPLAIN`/`EXPLAIN DEPENDENCY` và thời gian, giải thích đúng (Bước 9–10) | 1,5 |
| Ba bảng Text/ORC/Parquet khớp số dòng; bảng dung lượng và thời gian 3 truy vấn có số đo thật và nhận xét (Phần 4) | 2,0 |
| So sánh `EXPLAIN` Merge Join/Map Join, đo thời gian, `ANALYZE TABLE` và đọc thống kê (Phần 5) | 2,0 |
| Truy vấn hàm cửa sổ chạy đúng, có kiểm chứng; trả lời câu hỏi và trình bày (Phần 6, câu hỏi) | 1,5 |
| Bài tập mở rộng (điểm thưởng) | +1,0 (tổng không vượt quá 10) |
