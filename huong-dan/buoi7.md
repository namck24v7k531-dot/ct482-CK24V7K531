# Buổi 7 — Học máy phân tán với Spark MLlib: dự đoán khách hàng rời bỏ (GitHub Codespaces)

Bài thực hành kéo dài 2 tiết (khoảng 90–100 phút, tuần 14), phục vụ CLO5, chạy trong codespace của học phần (GitHub Codespaces). Sinh viên xây dựng bài toán dự đoán khách hàng rời bỏ (churn) từ 5 triệu giao dịch bán lẻ: tạo nhãn theo cửa sổ thời gian, tính đặc trưng RFM, đóng gói tiền xử lý và `RandomForestClassifier` trong một `Pipeline`, đánh giá bằng AUC/F1, xử lý mất cân bằng lớp, tinh chỉnh bằng `CrossValidator`, lưu `PipelineModel` lên HDFS và chấm điểm theo lô (Spark standalone và YARN). Máy khuyến nghị: **4 nhân / 16 GB** — toàn bộ phiên PySpark mất khoảng 7 phút, đỉnh RAM khoảng 7 GB; máy mặc định 2 nhân / 8 GB vẫn chạy được nhưng sát trần RAM, phải mở PySpark với tham số một executor (Bước 2) và `CrossValidator` mất 9–12 phút. Dịch vụ cần bật: chỉ cụm lõi (tự bật khi mở codespace, kiểm tra bằng `lab status`); không cần `lab hive`, `lab nosql`, `lab kafka` — nếu đang bật thì tắt bằng `lab stop-extras`. Dữ liệu: `data/retail_transactions.csv` bản đầy đủ 5 000 000 dòng (405 MB), sinh một lần bằng `lab data 5000000`. Quota ước tính cho buổi (khoảng 2 giờ mở codespace): 8 core-hour trên máy 4 nhân, 4 core-hour trên máy 2 nhân.

## Mục tiêu

Sau khi hoàn thành bài thực hành, sinh viên có khả năng:

- Xây dựng nhãn churn từ dữ liệu giao dịch thô bằng cửa sổ quan sát (đặc trưng) và cửa sổ gán nhãn tách biệt theo thời gian.
- Tính đặc trưng cấp khách hàng (recency, frequency, monetary, …) bằng DataFrame API trên dữ liệu phân tán.
- Thiết kế `Pipeline` gồm `StringIndexer`, `OneHotEncoder`, `VectorAssembler`, `StandardScaler` và `RandomForestClassifier`.
- Chia train/test theo thời gian để tránh rò rỉ dữ liệu (data leakage).
- Đánh giá mô hình bằng AUC-ROC, AUC-PR, F1 tổng thể và F1 của lớp churn; nhận diện và xử lý mất cân bằng lớp bằng trọng số mẫu.
- Tinh chỉnh siêu tham số bằng `CrossValidator` + `ParamGridBuilder` trong giới hạn tài nguyên của cụm.
- Lưu và nạp lại `PipelineModel` để suy luận theo lô trong một ứng dụng độc lập, chạy được trên cả Spark standalone và YARN.

## Chuẩn bị

**1. Chọn cấu hình máy cho buổi này.** Bài nặng nhất học phần: Spark giữ 2 executor, driver và hai bảng đặc trưng đã cache trong bộ nhớ. Đo trên bản sao codespace (cùng `.devcontainer` của repo, giới hạn tài nguyên như máy Codespaces):

| Cấu hình máy | Cách mở PySpark | Toàn bộ phiên PySpark (Bước 2–12) | `CrossValidator` (Bước 10) | Đỉnh RAM tiến trình (chưa tính VS Code) |
|---|---|---|---|---|
| 4 nhân / 16 GB (khuyến nghị) | mặc định | khoảng 6,5 phút | khoảng 4 phút (235 s) | 6,8 GB, không dùng swap |
| 2 nhân / 8 GB | mặc định (2 executor) | 13–17 phút | 9–12 phút (524–709 s) | 6,6 GB và phải đẩy thêm khoảng 0,9 GB ra swap; RAM chạm trần 8 GB |
| 2 nhân / 8 GB | một executor: `--executor-cores 2 --total-executor-cores 2` | khoảng 14 phút | khoảng 11 phút (677 s) | 4,9 GB, không phát sinh swap mới |

VS Code và các extension chạy trong codespace thật dùng thêm khoảng 1,5–2 GB, nên trên máy 2 nhân / 8 GB **bắt buộc** dùng cách mở một executor ở Bước 2. Kết quả mô hình (AUC, F1, ma trận nhầm lẫn) giống nhau ở cả ba cách.

Đổi máy cho codespace đã có (giữ nguyên dữ liệu HDFS và bài làm): vào https://github.com/codespaces → bấm `...` cạnh codespace → **Change machine type** → **4-core** → codespace khởi động lại với máy mới. Cuối buổi có thể đổi lại 2-core để tiết kiệm quota (máy 4 nhân tốn gấp đôi core-hour).

**2. Mở codespace và chờ cụm tự bật.** Vào https://github.com/codespaces → bấm tên codespace của mình. Cụm lõi (8 container) tự khởi động khi codespace mở (30–90 giây); terminal hiện `Cụm đã sẵn sàng` là xong. Mọi lệnh `Terminal codespace` trong bài gõ ở terminal của VS Code, thư mục hiện hành là gốc repo (`/workspaces/<tên-repo>`).

Nếu giảng viên thông báo có cập nhật repo, lấy về trước khi làm:

<!-- shell: Terminal codespace -->
```bash
git pull
```

**3. Kiểm tra cụm:**

<!-- shell: Terminal codespace -->
```bash
lab status
```

**Kết quả mong đợi:** 8 container `healthy`, HDFS có 2 DataNode, YARN có 2 node:

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

Nếu danh sách có thêm `hiveserver2`, `hive-metastore`, `kafka`, `hbase`, `mongodb` (còn bật từ buổi trước), tắt chúng để giải phóng RAM:

<!-- shell: Terminal codespace -->
```bash
lab stop-extras
```

**4. Dữ liệu 5 triệu dòng.** Codespace tạo trước 06/11/2026 chỉ có dữ liệu mẫu 200 000 dòng (codespace tạo sau ngày này đã có sẵn 5 triệu dòng). Kiểm tra:

<!-- shell: Terminal codespace -->
```bash
wc -l data/retail_transactions.csv
```

Nếu kết quả là `200001 data/retail_transactions.csv` (hoặc tệp chưa có), sinh bản đầy đủ — chỉ cần làm **một lần cho cả học kỳ**, dữ liệu được giữ lại trong codespace:

<!-- shell: Terminal codespace -->
```bash
lab data 5000000
wc -l data/retail_transactions.csv
```

**Kết quả mong đợi** (khoảng 25–30 giây):

```text
Sinh du lieu vao ./data (5,000,000 giao dich)...
  giao dich: 500,000
...
  giao dich: 4,500,000
  products.csv: 0.0 MB
  retail_transactions.csv: 405.2 MB
  web_access.log: 121.4 MB
  sensor_readings.csv: 22.7 MB
5000001 data/retail_transactions.csv
```

> ⚠️ Không dùng bộ 200 000 dòng cho bài này: mỗi khách hàng chỉ có khoảng 1 giao dịch nên gần như toàn bộ khách hàng bị gán nhãn churn và bài toán mất ý nghĩa. `gen_data.py` dùng `--seed 42` mặc định nên mọi sinh viên có cùng dữ liệu và cùng các con số đếm trong bài.

> 💡 Dung lượng: thư mục `data/` khoảng 524 MB; sau khi đưa lên HDFS (hệ số nhân bản 2), hai volume DataNode tăng thêm khoảng 0,9 GB. Tổng đĩa codespace dùng cho buổi này (image `bigdata-lab` 4,9 GB + dữ liệu + HDFS) khoảng 6,5 GB, chưa tính nền dev container — còn dư nhiều so với 32 GB của máy. Thư mục `data/` đã được loại trừ trong `.gitignore` của repo nên không bị đưa lên GitHub khi commit.

**5. Tài nguyên của cụm** — ràng buộc quan trọng nhất của bài, giống hệt cụm trên máy phòng lab (cấu hình nằm trong `docker-compose.yml`, không phụ thuộc máy codespace 2 hay 4 nhân):

| Thành phần | Giới hạn | Hệ quả khi viết lệnh |
|---|---|---|
| Spark Worker (trong `nodemanager1`, `nodemanager2`) | 2 core, 1 GB mỗi worker; tổng 4 core, 2 GB | Executor standalone tối đa khoảng 1 GB; mặc định `spark.executor.memory=768m` |
| Driver (chạy trong container `spark-master`, giới hạn 2 GB) | Mặc định `spark.driver.memory=768m` | Đủ cho bài; không đặt `--driver-memory 2g` (container sẽ hết RAM) |
| YARN container | Tối đa 1792 MB kể cả overhead 384 MB | Trên YARN dùng `--executor-memory` ≤ 1g |
| Máy codespace 2 nhân / 8 GB | 4 core Spark chia nhau 2 nhân thật; RAM chạm trần khi chạy 2 executor | Mở PySpark một executor (Bước 2), không chạy song song job khác |

Lệnh của tài liệu cũ `--driver-memory 2g --executor-memory 2g` **không chạy được** trong môi trường này (xem Bước 1 và mục Lỗi thường gặp).

**6. Các Web UI dùng trong bài.** Trong codespace không mở `http://localhost:<cổng>` từ trình duyệt máy mình được; lấy đường dẫn bằng lệnh `lab ui` (dạng `https://<tên-codespace>-<cổng>.app.github.dev`) hoặc mở tab **PORTS** cạnh TERMINAL → bấm biểu tượng quả địa cầu ở cổng tương ứng:

<!-- shell: Terminal codespace -->
```bash
lab ui
```

| Giao diện | Cổng (tab PORTS) | Dùng để |
|---|---|---|
| Spark Master | 8080 | Xem worker, core/bộ nhớ đã cấp cho phiên PySpark |
| Spark UI của driver | 4040 | Tab Jobs/Stages/Storage: theo dõi `CrossValidator`, kiểm tra DataFrame đã cache (chỉ có khi phiên PySpark đang mở) |
| Spark History Server | 18080 | Xem lại phiên đã kết thúc |
| YARN ResourceManager | 8088 | Theo dõi ứng dụng chấm điểm chạy trên YARN (Bước 13) |
| HDFS NameNode | 9870 | Duyệt `/models/churn_pipeline_v1`, `/output/churn_scores_v1` |

> 💡 Bên trong terminal codespace, `curl localhost:<cổng>` vẫn dùng được (ví dụ `curl -s localhost:4040/api/v1/applications`). Liên kết nội bộ trong Web UI dạng `http://spark-master:4040` hay `http://nodemanager1:8042/...` không mở được từ trình duyệt — đổi sang đường dẫn tương ứng từ `lab ui`.

## PHẦN 1: CHUẨN BỊ DỮ LIỆU VÀ TÀI NGUYÊN

### Bước 1: Kiểm tra tài nguyên Spark và đưa dữ liệu lên HDFS

Cụm đã tự bật khi mở codespace (nếu `lab status` báo chưa chạy, gõ `lab start`). Kiểm tra tài nguyên Spark Master cấp cho các ứng dụng:

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master bash -lc "curl -s localhost:8080/json/ | jq -c '{aliveworkers, cores, memory, workers: [.workers[] | {cores, memory}]}'"
```

**Kết quả mong đợi:** Spark Master có 2 worker, tổng 4 core và 2048 MB (trên cả máy 2 nhân lẫn 4 nhân — số core này là cấu hình của worker, không phải số nhân thật của máy).

```text
{"aliveworkers":2,"cores":4,"memory":2048,"workers":[{"cores":2,"memory":1024},{"cores":2,"memory":1024}]}
```

Thư mục `data/` của repo đã được mount thành `/data` trong `namenode`, nên đưa thẳng tệp lên HDFS, không cần `docker cp`:

<!-- shell: Terminal codespace -->
```bash
docker exec -it namenode hdfs dfs -mkdir -p /data/raw
docker exec -it namenode hdfs dfs -put -f /data/retail_transactions.csv /data/raw/
docker exec -it namenode hdfs dfs -ls -h /data/raw
```

**Kết quả mong đợi** (lệnh `-put` mất khoảng 5 giây):

```text
Found 1 items
-rw-r--r--   2 root supergroup    386.4 M 2026-11-05 23:22 /data/raw/retail_transactions.csv
```

Tệp có hệ số nhân bản 2 (cụm có 2 DataNode). HDFS hiển thị 386,4 M vì dùng đơn vị MiB (405,2 MB = 386,4 MiB). Nếu kích thước chỉ khoảng 15 M, bạn đang đưa lên bộ 200 000 dòng — quay lại mục Chuẩn bị, chạy `lab data 5000000` rồi `-put -f` lại.

Để thấy vì sao phải giữ bộ nhớ executor dưới 1 GB, thử yêu cầu executor 2 GB trên Spark standalone (Ctrl+C sau khoảng 30 giây):

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master bash -lc 'spark-submit --master spark://spark-master:7077 \
  --executor-memory 2g --class org.apache.spark.examples.SparkPi $SPARK_EXAMPLES_JAR 10'
```

**Kết quả mong đợi:** job treo, cứ 15 giây lặp lại cảnh báo:

```text
26/11/05 23:22:43 WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources
26/11/05 23:22:58 WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources
```

Trên YARN, yêu cầu tương tự bị từ chối ngay:

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master bash -lc 'spark-submit --master yarn \
  --executor-memory 2g --class org.apache.spark.examples.SparkPi $SPARK_EXAMPLES_JAR 10'
```

**Kết quả mong đợi:**

```text
Exception in thread "main" java.lang.IllegalArgumentException: Required executor memory (2048 MB), offHeap memory (0) MB, overhead (384 MB), and PySpark memory (0 MB) is above the max threshold (1792 MB) of this cluster! Please check the values of 'yarn.scheduler.maximum-allocation-mb' and/or 'yarn.nodemanager.resource.memory-mb'.
```

> 💡 Không có worker nào đủ 2 GB nên Spark standalone chờ vô hạn chứ không báo lỗi; YARN biết trước giới hạn tối đa của container nên từ chối ngay khi submit. Chọn máy codespace 16 GB không thay đổi kết quả này: giới hạn nằm ở cấu hình worker và YARN của cụm, không phải ở RAM của máy.

### Bước 2: Mở PySpark và khảo sát dữ liệu

Mở PySpark shell trên Spark standalone với cấu hình bộ nhớ mặc định (driver 768m, executor 768m, mỗi worker một executor 2 core). Trên **máy 4 nhân / 16 GB**:

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master pyspark --master spark://spark-master:7077
```

Trên **máy 2 nhân / 8 GB**, dùng một executor duy nhất 2 core (bỏ executor thứ hai, tiết kiệm khoảng 1,5 GB RAM; máy chỉ có 2 nhân thật nên tốc độ gần như không đổi):

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master pyspark --master spark://spark-master:7077 --executor-cores 2 --total-executor-cores 2
```

**Kết quả mong đợi** (app id khác nhau trên mỗi máy):

```text
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 4.1.3
      /_/

Using Python version 3.12.3 (main, Aug 31 2026 10:18:26)
Spark context Web UI available at http://spark-master:4040
Spark context available as 'sc' (master = spark://spark-master:7077, app id = app-20261105232323-0001).
SparkSession available as 'spark'.
>>>
```

Dòng `Spark context Web UI available at http://spark-master:4040` dùng tên container; trên trình duyệt hãy mở cổng 4040 từ `lab ui` hoặc tab PORTS.

Đọc CSV với schema khai báo tường minh thay vì `inferSchema=True`: Spark không phải quét toàn bộ 405 MB thêm một lần chỉ để đoán kiểu, và cột `ts` có ngay kiểu `timestamp`.

<!-- shell: Python (pyspark) -->
```python
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               DoubleType, TimestampType)

schema = StructType([
    StructField("transaction_id", StringType()), StructField("customer_id", StringType()),
    StructField("product_id", StringType()),     StructField("category", StringType()),
    StructField("quantity", IntegerType()),      StructField("unit_price", DoubleType()),
    StructField("discount", DoubleType()),       StructField("store_id", StringType()),
    StructField("region", StringType()),         StructField("ts", TimestampType()),
])

df = (spark.read.option("header", True).schema(schema)
      .csv("hdfs://namenode:9000/data/raw/retail_transactions.csv"))

df.printSchema()
print("so giao dich:", df.count(), "| so khach hang:", df.select("customer_id").distinct().count())
df.select(F.min("ts").alias("ts_min"), F.max("ts").alias("ts_max")).show(truncate=False)
df.groupBy(F.date_format("ts", "yyyy-MM").alias("thang")).count().orderBy("thang").show()
```

**Kết quả mong đợi** (khoảng 15–40 giây):

```text
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

so giao dich: 5000000 | so khach hang: 199995
+-------------------+-------------------+
|ts_min             |ts_max             |
+-------------------+-------------------+
|2025-01-01 00:00:05|2025-08-28 23:59:56|
+-------------------+-------------------+

+-------+------+
|  thang| count|
+-------+------+
|2025-01|645293|
|2025-02|582257|
|2025-03|646616|
|2025-04|625022|
|2025-05|646212|
|2025-06|625431|
|2025-07|645511|
|2025-08|583658|
+-------+------+
```

Dữ liệu trải đều 240 ngày, từ 01/01/2025 đến hết 28/08/2025, khoảng 20 000 giao dịch mỗi ngày cho gần 200 000 khách hàng. Trong lúc job chạy, terminal hiện thanh tiến độ dạng `[Stage 3:====>    (24 + 4) / 200]`; đó là hành vi bình thường.

> 💡 Khi gõ (hoặc dán) một khối lệnh nhiều dòng vào PySpark shell, nhấn Enter thêm một lần sau khối `def`/`for` để shell kết thúc khối lệnh. Không để dòng trống *bên trong* thân hàm: shell Python sẽ coi dòng trống là kết thúc hàm.

### Bước 3: Xác định các mốc thời gian

Bài toán: "khách hàng có quay lại mua trong 60 ngày tới không?". Đặc trưng chỉ được tính từ dữ liệu **trước** mốc cắt; nhãn được tính từ 60 ngày **sau** mốc cắt. Có hai snapshot: snapshot train (mốc 01/05/2025) và snapshot test (mốc 30/06/2025, sau khi cửa sổ nhãn train đã kết thúc).

<!-- shell: Python (pyspark) -->
```python
# Mốc thời gian (nửa khoảng [bắt đầu, kết thúc) để không bỏ sót/đếm trùng ngày ranh giới)
TRAIN_FEATURE_END = "2025-05-01"   # đặc trưng train: ts <  2025-05-01
TRAIN_LABEL_END   = "2025-06-30"   # nhãn train:     2025-05-01 <= ts < 2025-06-30 (60 ngày)
TEST_FEATURE_END  = "2025-06-30"   # đặc trưng test: ts <  2025-06-30
TEST_LABEL_END    = "2025-08-29"   # nhãn test:      2025-06-30 <= ts < 2025-08-29 (60 ngày)

assert TRAIN_LABEL_END <= TEST_FEATURE_END, "nhãn train phải kết thúc trước khi cửa sổ nhãn test bắt đầu"
```

Không có kết quả in ra; nếu điều kiện sai, `assert` dừng với `AssertionError`.

| Snapshot | Cửa sổ đặc trưng | Cửa sổ nhãn (60 ngày) |
|---|---|---|
| train | 01/01/2025 ≤ ts < 01/05/2025 | 01/05/2025 ≤ ts < 30/06/2025 |
| test | 01/01/2025 ≤ ts < 30/06/2025 | 30/06/2025 ≤ ts < 29/08/2025 |

> 💡 Cửa sổ đặc trưng của test được phép bao trùm cửa sổ nhãn của train, vì tại thời điểm dự đoán (30/06) đó là dữ liệu quá khứ. Điều kiện bắt buộc là nhãn test nằm hoàn toàn sau mọi dữ liệu dùng để huấn luyện.

## PHẦN 2: ĐẶC TRƯNG VÀ NHÃN THEO CỬA SỔ THỜI GIAN

### Bước 4: Hàm tạo snapshot đặc trưng và nhãn

Hàm `build_snapshot` sinh bảng một-khách-hàng-một-dòng: các đặc trưng tính từ dữ liệu trước `feature_end`, nhãn `churn_label = 1` nếu khách **không có** giao dịch nào trong cửa sổ nhãn. Đặc trưng gồm:

- `recency_days`: số ngày từ giao dịch gần nhất đến mốc cắt; `tenure_days`: số ngày từ giao dịch đầu tiên đến mốc cắt.
- `frequency`, `monetary`, `avg_basket_value`, `distinct_categories`: số giao dịch, tổng chi tiêu, giá trị trung bình mỗi giao dịch, số danh mục đã mua.
- `last_category`, `last_region`, `last_store`: danh mục, khu vực, cửa hàng của giao dịch gần nhất (`max_by(cột, "ts")`, có từ Spark 3.3). Tài liệu cũ dùng `F.first()`, hàm này trả về một dòng bất kỳ của nhóm nên kết quả thay đổi giữa các lần chạy; `max_by` cho kết quả xác định.

<!-- shell: Python (pyspark) -->
```python
def build_snapshot(df, feature_end, label_end, snapshot_name):
    fe = F.lit(feature_end).cast("timestamp")
    le = F.lit(label_end).cast("timestamp")
    feat_df  = df.filter(F.col("ts") < fe)
    label_df = df.filter((F.col("ts") >= fe) & (F.col("ts") < le))
    revenue = F.col("quantity") * F.col("unit_price") * (1 - F.col("discount"))
    features = feat_df.groupBy("customer_id").agg(
        F.datediff(fe, F.max("ts")).alias("recency_days"),
        F.datediff(fe, F.min("ts")).alias("tenure_days"),
        F.count("transaction_id").alias("frequency"),
        F.sum(revenue).alias("monetary"),
        (F.sum(revenue) / F.count("transaction_id")).alias("avg_basket_value"),
        F.countDistinct("category").alias("distinct_categories"),
        F.max_by("category", "ts").alias("last_category"),   # danh mục của giao dịch gần nhất
        F.max_by("region", "ts").alias("last_region"),
        F.max_by("store_id", "ts").alias("last_store"),
    )
    active_next = label_df.select("customer_id").distinct().withColumn("is_active_next", F.lit(1))
    return (features.join(active_next, "customer_id", "left")
            .withColumn("churn_label", F.when(F.col("is_active_next").isNull(), 1).otherwise(0))
            .drop("is_active_next")
            .withColumn("snapshot", F.lit(snapshot_name)))
```

Không có kết quả in ra (chỉ định nghĩa hàm). Các phép biến đổi ở đây là *lazy*: chưa có job Spark nào chạy cho đến khi gọi một action.

### Bước 5: Tạo snapshot train/test và kiểm tra phân phối nhãn

`cache()` giữ hai bảng đặc trưng (khoảng 200 000 dòng mỗi bảng) trong bộ nhớ executor. Nếu không cache, mỗi lần huấn luyện ở Bước 7–10 (CrossValidator huấn luyện 13 mô hình) Spark lại đọc 405 MB CSV và tính lại toàn bộ phép tổng hợp.

<!-- shell: Python (pyspark) -->
```python
train_df = build_snapshot(df, TRAIN_FEATURE_END, TRAIN_LABEL_END, "train").cache()
test_df  = build_snapshot(df, TEST_FEATURE_END,  TEST_LABEL_END,  "test").cache()

print("train:", train_df.count(), "khach hang | test:", test_df.count(), "khach hang")
train_df.groupBy("churn_label").count().orderBy("churn_label").show()
test_df.groupBy("churn_label").count().orderBy("churn_label").show()
train_df.show(3)
print("recency am:", train_df.filter(F.col("recency_days") < 0).count())
```

**Kết quả mong đợi** (khoảng 45 giây trên máy 4 nhân, 50–100 giây trên máy 2 nhân):

```text
train: 198931 khach hang | test: 199908 khach hang
+-----------+------+
|churn_label| count|
+-----------+------+
|          0|185673|
|          1| 13258|
+-----------+------+

+-----------+------+
|churn_label| count|
+-----------+------+
|          0|186786|
|          1| 13122|
+-----------+------+

+-----------+------------+-----------+---------+------------+--------------------+-------------------+-------------+-----------+----------+-----------+--------+
|customer_id|recency_days|tenure_days|frequency|    monetary|    avg_basket_value|distinct_categories|last_category|last_region|last_store|churn_label|snapshot|
+-----------+------------+-----------+---------+------------+--------------------+-------------------+-------------+-----------+----------+-----------+--------+
|   C0000399|           3|        120|       52|  1.594534E9|3.0664115384615384E7|                  7|     the_thao|      Hanoi|      S026|          0|   train|
|   C0000568|           1|        120|       44|1.33163335E9|3.0264394318181816E7|                  7|   thoi_trang|     CanTho|      S046|          0|   train|
|   C0000631|           1|        114|       56| 1.9468656E9| 3.476545714285714E7|                  7|    thuc_pham|      Hanoi|      S006|          0|   train|
+-----------+------------+-----------+---------+------------+--------------------+-------------------+-------------+-----------+----------+-----------+--------+
only showing top 3 rows
recency am: 0
```

- Tỉ lệ churn chỉ khoảng **6,7%** (13 258 / 198 931): dữ liệu **mất cân bằng lớp** nặng. Điều này quyết định cách đánh giá ở Bước 8–9.
- `recency am: 0` xác nhận không có giao dịch nào sau mốc cắt lọt vào đặc trưng.
- Mở cổng 4040 (từ `lab ui` hoặc tab PORTS) → tab **Storage** để xem các DataFrame đã được cache và dung lượng chúng chiếm trong bộ nhớ executor.

> 💡 Dữ liệu sinh bởi `gen_data.py` không mô phỏng hành vi rời bỏ thực sự: mỗi giao dịch chọn khách hàng ngẫu nhiên (20% khách hàng tạo khoảng 60% giao dịch). Khách "churn" chủ yếu là khách mua ít, tình cờ không có giao dịch nào trong 60 ngày. Vì vậy tín hiệu dự đoán yếu và AUC thu được chỉ quanh 0,61 — đây là đặc điểm của dữ liệu, không phải lỗi của pipeline.

## PHẦN 3: PIPELINE, HUẤN LUYỆN VÀ ĐÁNH GIÁ

### Bước 6: Xây dựng Pipeline tiền xử lý và mô hình

Pipeline gồm 5 stage: `StringIndexer` (chuỗi → chỉ số, `handleInvalid="keep"` để giá trị mới ở tập test không gây lỗi), `OneHotEncoder` (chỉ số → vector thưa), `VectorAssembler` (ghép 6 đặc trưng số và 3 vector one-hot thành `features_raw`), `StandardScaler` (`withMean=False` để giữ vector thưa; với `withMean=True` Spark 4 không báo lỗi nhưng chuyển mọi vector sang dạng dày, tốn bộ nhớ hơn nhiều khi có hàng trăm cột one-hot) và `RandomForestClassifier`.

<!-- shell: Python (pyspark) -->
```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier

categorical_cols = ["last_category", "last_region", "last_store"]
indexed_cols = [c + "_idx" for c in categorical_cols]
encoded_cols = [c + "_enc" for c in categorical_cols]
numeric_cols = ["recency_days", "tenure_days", "frequency", "monetary",
                "avg_basket_value", "distinct_categories"]

indexer   = StringIndexer(inputCols=categorical_cols, outputCols=indexed_cols, handleInvalid="keep")
encoder   = OneHotEncoder(inputCols=indexed_cols, outputCols=encoded_cols)
assembler = VectorAssembler(inputCols=numeric_cols + encoded_cols, outputCol="features_raw")
scaler    = StandardScaler(inputCol="features_raw", outputCol="features", withMean=False, withStd=True)
rf        = RandomForestClassifier(featuresCol="features", labelCol="churn_label", seed=42)

pipeline = Pipeline(stages=[indexer, encoder, assembler, scaler, rf])
print([type(s).__name__ for s in pipeline.getStages()])
```

**Kết quả mong đợi:**

```text
['StringIndexer', 'OneHotEncoder', 'VectorAssembler', 'StandardScaler', 'RandomForestClassifier']
```

> 💡 Random Forest dựa trên các ngưỡng chia nên không bị ảnh hưởng bởi thang đo; `StandardScaler` được giữ lại để pipeline dùng được ngay khi thay mô hình bằng `LogisticRegression` (xem Bài tập mở rộng).

### Bước 7: Chia train/test theo thời gian và huấn luyện mô hình cơ sở

Tập train là snapshot có nhãn kết thúc trước khi cửa sổ nhãn test bắt đầu, nên mô hình không thể "nhìn thấy" tương lai của đối tượng nó dự đoán. Đây là *chia theo thời gian*, khác với `randomSplit`.

<!-- shell: Python (pyspark) -->
```python
import time
train_final = train_df.na.fill(0.0, subset=numeric_cols)
test_final  = test_df.na.fill(0.0, subset=numeric_cols)
train_final.select("snapshot").distinct().show()
test_final.select("snapshot").distinct().show()

t0 = time.time()
model = pipeline.fit(train_final)
print(f"thoi gian huan luyen: {time.time() - t0:.1f} s")

predictions = model.transform(test_final)
predictions.select("customer_id", "churn_label", "prediction", "probability").show(5, truncate=False)
```

**Kết quả mong đợi** (thời gian huấn luyện là ví dụ: khoảng 30 giây trên máy 4 nhân, 30–55 giây trên máy 2 nhân):

```text
+--------+
|snapshot|
+--------+
|   train|
+--------+

+--------+
|snapshot|
+--------+
|    test|
+--------+

thoi gian huan luyen: 50.1 s
+-----------+-----------+----------+----------------------------------------+
|customer_id|churn_label|prediction|probability                             |
+-----------+-----------+----------+----------------------------------------+
|C0000399   |0          |0.0       |[0.9399307882751058,0.06006921172489412]|
|C0000568   |0          |0.0       |[0.9399307882751058,0.06006921172489412]|
|C0000631   |0          |0.0       |[0.9399307882751058,0.06006921172489412]|
|C0000701   |0          |0.0       |[0.9399307882751058,0.06006921172489412]|
|C0001377   |0          |0.0       |[0.9399307882751058,0.06006921172489412]|
+-----------+-----------+----------+----------------------------------------+
only showing top 5 rows
```

Có thể xuất hiện thêm một dòng `WARN SparkStringUtils: Truncated the string representation of a plan since it was too large...` — chỉ là cảnh báo khi in kế hoạch thực thi, bỏ qua được. Cột `probability` là vector 2 chiều `[P(không churn), P(churn)]` có tổng bằng 1. Năm khách hàng đầu đều là khách mua thường xuyên, rơi vào cùng các lá của cây nên có cùng xác suất.

### Bước 8: Đánh giá bằng AUC và F1

AUC-ROC đo khả năng xếp hạng (khách churn có xác suất cao hơn khách không churn hay không), không phụ thuộc ngưỡng. AUC-PR nhạy hơn với lớp hiếm. `MulticlassClassificationEvaluator(metricName="f1")` trả về F1 **có trọng số theo số mẫu của từng lớp**, nên bị lớp đa số chi phối; F1 của riêng lớp churn lấy bằng `fMeasureByLabel` với `metricLabel=1.0`.

<!-- shell: Python (pyspark) -->
```python
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

auc_evaluator = BinaryClassificationEvaluator(labelCol="churn_label", rawPredictionCol="rawPrediction",
                                              metricName="areaUnderROC")
pr_evaluator  = BinaryClassificationEvaluator(labelCol="churn_label", rawPredictionCol="rawPrediction",
                                              metricName="areaUnderPR")
f1_weighted   = MulticlassClassificationEvaluator(labelCol="churn_label", predictionCol="prediction",
                                                  metricName="f1")
f1_churn      = MulticlassClassificationEvaluator(labelCol="churn_label", predictionCol="prediction",
                                                  metricName="fMeasureByLabel", metricLabel=1.0)

print(f"AUC-ROC        = {auc_evaluator.evaluate(predictions):.4f}")
print(f"AUC-PR         = {pr_evaluator.evaluate(predictions):.4f}")
print(f"F1 (weighted)  = {f1_weighted.evaluate(predictions):.4f}")
print(f"F1 (lop churn) = {f1_churn.evaluate(predictions):.4f}")
predictions.groupBy("churn_label", "prediction").count().orderBy("churn_label", "prediction").show()
```

**Kết quả mong đợi:**

```text
AUC-ROC        = 0.6088
AUC-PR         = 0.0842
F1 (weighted)  = 0.9027
F1 (lop churn) = 0.0000
+-----------+----------+------+
|churn_label|prediction| count|
+-----------+----------+------+
|          0|       0.0|186786|
|          1|       0.0| 13122|
+-----------+----------+------+
```

Mô hình **dự đoán mọi khách hàng là không churn**: với ngưỡng 0,5 và 93% mẫu thuộc lớp 0, không lá nào có đa số churn. F1 có trọng số 0,90 trông rất tốt nhưng hoàn toàn vô dụng cho mục tiêu giữ chân khách hàng; F1 của lớp churn bằng 0 và AUC-PR (0,084) chỉ nhỉnh hơn tỉ lệ churn ngẫu nhiên (0,066). Đây là lý do không được đánh giá bài toán mất cân bằng chỉ bằng accuracy hay F1 có trọng số.

### Bước 9: Xử lý mất cân bằng lớp bằng trọng số mẫu

Gán cho mỗi mẫu churn trọng số `n_neg / n_pos` (khoảng 14), mẫu còn lại trọng số 1, rồi khai báo cột trọng số cho Random Forest (`weightCol`, hỗ trợ từ Spark 3.0). Vì `rf` là cùng một đối tượng nằm trong `pipeline`, thay đổi có hiệu lực ngay cho pipeline.

<!-- shell: Python (pyspark) -->
```python
# Trọng số lớp: mỗi mẫu churn "nặng" bằng (số mẫu không churn / số mẫu churn)
n_pos = train_final.filter("churn_label = 1").count()
n_neg = train_final.filter("churn_label = 0").count()
w_pos = n_neg / n_pos
print(f"n_pos={n_pos}, n_neg={n_neg}, trong so lop churn = {w_pos:.2f}")

train_final = train_final.withColumn(
    "class_weight", F.when(F.col("churn_label") == 1, F.lit(w_pos)).otherwise(F.lit(1.0))).cache()
rf.setWeightCol("class_weight")

t0 = time.time()
model_w = pipeline.fit(train_final)
print(f"thoi gian huan luyen (co trong so): {time.time() - t0:.1f} s")
pred_w = model_w.transform(test_final)
print(f"AUC-ROC        = {auc_evaluator.evaluate(pred_w):.4f}")
print(f"AUC-PR         = {pr_evaluator.evaluate(pred_w):.4f}")
print(f"F1 (weighted)  = {f1_weighted.evaluate(pred_w):.4f}")
print(f"F1 (lop churn) = {f1_churn.evaluate(pred_w):.4f}")
pred_w.groupBy("churn_label", "prediction").count().orderBy("churn_label", "prediction").show()
```

**Kết quả mong đợi** (thời gian là ví dụ: khoảng 16 giây trên máy 4 nhân, 19–36 giây trên máy 2 nhân):

```text
n_pos=13258, n_neg=185673, trong so lop churn = 14.00
RandomForestClassifier_979b164e82d4
thoi gian huan luyen (co trong so): 28.2 s
AUC-ROC        = 0.6075
AUC-PR         = 0.0826
F1 (weighted)  = 0.4224
F1 (lop churn) = 0.1503
+-----------+----------+------+
|churn_label|prediction| count|
+-----------+----------+------+
|          0|       0.0| 53251|
|          0|       1.0|133535|
|          1|       0.0|  1201|
|          1|       1.0| 11921|
+-----------+----------+------+
```

Dòng `RandomForestClassifier_...` là giá trị trả về của `rf.setWeightCol(...)` mà shell tự in ra (mã định danh khác nhau trên mỗi máy). AUC-ROC có thể là 0,6075 hoặc 0,6076 tùy lần chạy; ma trận nhầm lẫn giống hệt. Mô hình có trọng số bắt được 91% khách churn (recall = 11 921 / 13 122) nhưng đánh nhầm nhiều khách không churn (precision khoảng 8%); F1 lớp churn tăng từ 0 lên 0,15. AUC hầu như không đổi vì trọng số chủ yếu dịch chuyển ngưỡng quyết định chứ không làm thứ hạng tốt hơn.

## PHẦN 4: TINH CHỈNH, DIỄN GIẢI VÀ TRIỂN KHAI

### Bước 10: Tinh chỉnh siêu tham số với CrossValidator

`CrossValidator` chia `train_final` thành 3 fold ngẫu nhiên, với mỗi tổ hợp tham số huấn luyện trên 2 fold và đánh giá AUC trên fold còn lại, rồi huấn luyện lại mô hình tốt nhất trên toàn bộ `train_final`: tổng cộng 2 × 2 × 3 + 1 = 13 lần huấn luyện. Chia ngẫu nhiên **bên trong** tập train là an toàn về rò rỉ, vì toàn bộ `train_final` đã kết thúc trước cửa sổ nhãn test. `parallelism=2` cho phép huấn luyện 2 mô hình đồng thời — phù hợp với 2 executor của cụm; đặt cao hơn chỉ làm các mô hình tranh nhau 4 core.

> ⚠️ Đây là bước dài nhất: khoảng 4 phút trên máy 4 nhân, 9–12 phút trên máy 2 nhân. Codespace **tự dừng sau 30 phút không thao tác** và khi dừng thì phiên PySpark (cùng các DataFrame đã cache) mất hết. Trong lúc chờ, theo dõi tiến độ ở cổng 4040 (tab Jobs) hoặc đọc trước Bước 11; không bỏ máy đi quá lâu.

<!-- shell: Python (pyspark) -->
```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

param_grid = (ParamGridBuilder()
              .addGrid(rf.numTrees, [20, 50])
              .addGrid(rf.maxDepth, [5, 10])
              .build())

cv = CrossValidator(estimator=pipeline, estimatorParamMaps=param_grid,
                    evaluator=auc_evaluator, numFolds=3, parallelism=2, seed=42)

t0 = time.time()
cv_model = cv.fit(train_final)
print(f"thoi gian CrossValidator: {time.time() - t0:.1f} s")

for params, metric in zip(param_grid, cv_model.avgMetrics):
    print({p.name: v for p, v in params.items()}, f"AUC trung binh 3 fold = {metric:.4f}")

best_rf = cv_model.bestModel.stages[-1]
print("numTrees tot nhat:", best_rf.getNumTrees, "| maxDepth tot nhat:", best_rf.getOrDefault("maxDepth"))

cv_predictions = cv_model.transform(test_final)
print(f"AUC-ROC sau tinh chinh = {auc_evaluator.evaluate(cv_predictions):.4f}")
print(f"F1 lop churn sau tinh chinh = {f1_churn.evaluate(cv_predictions):.4f}")
```

**Kết quả mong đợi** (lần chạy trên máy 2 nhân / 8 GB, cách mở mặc định; trong lúc chạy có thể xuất hiện vài dòng `WARN DAGScheduler: Broadcasting large task binary with size ... KiB` — bình thường):

```text
thoi gian CrossValidator: 524.1 s
{'numTrees': 20, 'maxDepth': 5} AUC trung binh 3 fold = 0.6109
{'numTrees': 20, 'maxDepth': 10} AUC trung binh 3 fold = 0.6109
{'numTrees': 50, 'maxDepth': 5} AUC trung binh 3 fold = 0.6093
{'numTrees': 50, 'maxDepth': 10} AUC trung binh 3 fold = 0.6080
numTrees tot nhat: 20 | maxDepth tot nhat: 10
AUC-ROC sau tinh chinh = 0.6087
F1 lop churn sau tinh chinh = 0.1506
```

Bốn cấu hình chênh nhau dưới 0,003 AUC: với dữ liệu này, tăng số cây hay độ sâu không cải thiện đáng kể, nên mô hình nhỏ (20 cây) là lựa chọn hợp lý về chi phí. Ba lần chạy kiểm thử khác (máy 2 nhân mở lại, máy 2 nhân một executor, máy 4 nhân) đều cho AUC của cấu hình (20, 10) là 0,6099 và cấu hình tốt nhất là **(20, 5)**, kéo theo `AUC-ROC sau tinh chinh = 0.6075`–`0.6076` và `F1 lop churn sau tinh chinh = 0.1503`; thời gian CV lần lượt 709,3 s, 677,3 s và 235,3 s. Các giá trị gần bằng nhau nên thứ tự có thể đổi giữa các lần chạy do cách chia dữ liệu theo partition; chữ số thứ ba trở đi của AUC trên máy bạn có thể khác.

> ⚠️ Nếu `cv.fit()` quá chậm hoặc executor bị mất (`ExecutorLostFailure`, container bị OOMKilled, terminal VS Code bị ngắt), giảm lưới tham số còn một tham số hai giá trị và `numFolds=2`; trên máy 2 nhân, mở lại PySpark theo cách một executor ở Bước 2. Không tăng `--executor-memory` vượt quá 768m trên Spark standalone.

### Bước 11: Diễn giải mô hình bằng độ quan trọng đặc trưng

`featureImportances` của Random Forest cho biết mức đóng góp (giảm độ bất thuần) của từng chiều trong vector đặc trưng. Tên của từng chiều lấy từ metadata mà `VectorAssembler` gắn vào cột `features_raw`.

<!-- shell: Python (pyspark) -->
```python
attrs = cv_predictions.schema["features_raw"].metadata["ml_attr"]["attrs"]
names = {a["idx"]: a["name"] for group in attrs.values() for a in group}
importances = best_rf.featureImportances.toArray()
top = sorted(enumerate(importances), key=lambda x: -x[1])[:8]
for idx, imp in top:
    print(f"{names[idx]:<28s} {imp:.4f}")
```

**Kết quả mong đợi** (khi cấu hình tốt nhất là (20, 10)):

```text
monetary                     0.3282
frequency                    0.2815
distinct_categories          0.2537
tenure_days                  0.0416
recency_days                 0.0413
avg_basket_value             0.0185
last_region_enc_Hanoi        0.0019
last_region_enc_HCM          0.0015
```

Khi CV chọn (20, 5), ba dòng đầu là `frequency 0.3205`, `distinct_categories 0.2711`, `monetary 0.2544` — thứ tự đổi nhưng vẫn là ba đặc trưng đó. Ba đặc trưng mức độ mua sắm (`monetary`, `frequency`, `distinct_categories`, tương quan chặt với nhau) chiếm khoảng 85% độ quan trọng; các đặc trưng phân loại gần như không đóng góp — đúng với cách dữ liệu được sinh (khu vực và cửa hàng chọn ngẫu nhiên cho từng giao dịch).

### Bước 12: Lưu PipelineModel và lô dữ liệu cần chấm điểm

`cv_model.bestModel` là một `PipelineModel` chứa cả 5 stage đã học (bảng ánh xạ của `StringIndexer`, độ lệch chuẩn của `StandardScaler`, các cây của Random Forest). Lưu mô hình lên HDFS, đồng thời lưu đặc trưng của snapshot test **không kèm nhãn** để mô phỏng lô khách hàng mới cần chấm điểm.

<!-- shell: Python (pyspark) -->
```python
model_path = "hdfs://namenode:9000/models/churn_pipeline_v1"
cv_model.bestModel.write().overwrite().save(model_path)

# Lô khách hàng "mới" cần chấm điểm: đặc trưng của snapshot test, KHÔNG có nhãn
(test_final.drop("churn_label")
 .write.mode("overwrite").parquet("hdfs://namenode:9000/user/student/lab7/new_customers"))
print("da luu mo hinh va lo du lieu moi")
```

**Kết quả mong đợi:**

```text
da luu mo hinh va lo du lieu moi
```

Thoát PySpark shell (`exit()`) để trả lại core và RAM cho các bước sau, rồi kiểm tra trên HDFS:

<!-- shell: Terminal codespace -->
```bash
docker exec -it namenode hdfs dfs -ls /models/churn_pipeline_v1/stages
docker exec -it namenode bash -lc "hdfs dfs -ls -R /models/churn_pipeline_v1 | head -n 8"
docker exec -it namenode hdfs dfs -du -s -h /models/churn_pipeline_v1 /user/student/lab7/new_customers
```

**Kết quả mong đợi** (mã định danh của stage và thời gian khác nhau trên mỗi máy):

```text
Found 5 items
drwxr-xr-x   - root supergroup          0 2026-11-05 23:36 /models/churn_pipeline_v1/stages/0_StringIndexer_a54179e77548
drwxr-xr-x   - root supergroup          0 2026-11-05 23:36 /models/churn_pipeline_v1/stages/1_OneHotEncoder_d0b92dcb3d6d
drwxr-xr-x   - root supergroup          0 2026-11-05 23:36 /models/churn_pipeline_v1/stages/2_VectorAssembler_ec7f9898a773
drwxr-xr-x   - root supergroup          0 2026-11-05 23:36 /models/churn_pipeline_v1/stages/3_StandardScaler_a7390851e564
drwxr-xr-x   - root supergroup          0 2026-11-05 23:36 /models/churn_pipeline_v1/stages/4_RandomForestClassifier_979b164e82d4
drwxr-xr-x   - root supergroup          0 2026-11-05 23:36 /models/churn_pipeline_v1/metadata
-rw-r--r--   2 root supergroup          0 2026-11-05 23:36 /models/churn_pipeline_v1/metadata/_SUCCESS
-rw-r--r--   2 root supergroup        335 2026-11-05 23:36 /models/churn_pipeline_v1/metadata/part-00000-a6a18425-65fe-4d73-8c51-5ac5ef227f75-c000.txt
drwxr-xr-x   - root supergroup          0 2026-11-05 23:36 /models/churn_pipeline_v1/stages
drwxr-xr-x   - root supergroup          0 2026-11-05 23:36 /models/churn_pipeline_v1/stages/0_StringIndexer_a54179e77548
drwxr-xr-x   - root supergroup          0 2026-11-05 23:36 /models/churn_pipeline_v1/stages/0_StringIndexer_a54179e77548/data
-rw-r--r--   2 root supergroup          0 2026-11-05 23:36 /models/churn_pipeline_v1/stages/0_StringIndexer_a54179e77548/data/_SUCCESS
-rw-r--r--   2 root supergroup       1122 2026-11-05 23:36 /models/churn_pipeline_v1/stages/0_StringIndexer_a54179e77548/data/part-00000-dc800357-f452-4fcc-b4a3-bc213f7656b9-c000.snappy.parquet
149.4 K  298.7 K  /models/churn_pipeline_v1
5.3 M    10.7 M   /user/student/lab7/new_customers
```

Cả mô hình chỉ chiếm khoảng 149 KB (khoảng 37 KB nếu CV chọn `maxDepth=5`), trong khi lô dữ liệu cần chấm điểm là 5,3 MB. Mỗi stage là một thư mục con có `metadata/` (tham số dạng JSON) và `data/` (tham số đã học, dạng Parquet) — tên thư mục kèm mã định danh ngẫu nhiên của stage. Có thể duyệt cùng cấu trúc trên Web UI HDFS (cổng 9870 → *Utilities* → *Browse the file system*).

### Bước 13: Suy luận theo lô trong một ứng dụng độc lập

Trong thực tế, mô hình được huấn luyện một lần và được một ứng dụng khác nạp lại để chấm điểm định kỳ. Tạo tệp `src/lab7/churn_score.py` trong repo: trong khung Explorer của VS Code bấm chuột phải vào thư mục `src` → *New Folder* `lab7` → *New File* `churn_score.py`, hoặc từ terminal:

<!-- shell: Terminal codespace -->
```bash
mkdir -p src/lab7
code src/lab7/churn_score.py
```

Dán nội dung sau rồi lưu (Ctrl+S). Thư mục `src/` của repo được mount thành `/src` trong container `spark-master`, nên tệp có ngay tại `/src/lab7/churn_score.py`:

<!-- file: src/lab7/churn_score.py -->
```python
"""Suy luận theo lô (batch inference) bằng PipelineModel đã lưu — Buổi 7, CT482.

Chạy như một ứng dụng ĐỘC LẬP với phiên huấn luyện:
  spark-submit --master spark://spark-master:7077 /src/lab7/churn_score.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

MODEL_PATH = "hdfs://namenode:9000/models/churn_pipeline_v1"
INPUT_PATH = "hdfs://namenode:9000/user/student/lab7/new_customers"
OUTPUT_PATH = "hdfs://namenode:9000/output/churn_scores_v1"

spark = SparkSession.builder.appName("ChurnBatchScoring").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

model = PipelineModel.load(MODEL_PATH)
print("cac stage:", [type(s).__name__ for s in model.stages])

new_batch = spark.read.parquet(INPUT_PATH)
print("so khach hang can cham diem:", new_batch.count(), "| co cot churn_label?", "churn_label" in new_batch.columns)

scores = (model.transform(new_batch)
          .withColumn("churn_prob", F.round(vector_to_array("probability")[1], 4))
          .select("customer_id", "recency_days", "frequency", "prediction", "churn_prob"))

scores.write.mode("overwrite").parquet(OUTPUT_PATH)

result = spark.read.parquet(OUTPUT_PATH)
result.groupBy("prediction").count().orderBy("prediction").show()
print("10 khach hang co nguy co roi bo cao nhat:")
result.orderBy(F.desc("churn_prob")).show(10)
spark.stop()
```

`vector_to_array` (Spark 3.0+) chuyển cột `probability` kiểu vector sang mảng để lấy xác suất churn ở chỉ số 1.

Chạy trên Spark standalone (đã thoát PySpark shell ở Bước 12; nếu shell còn mở, ứng dụng sẽ phải chờ core):

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /src/lab7/churn_score.py
```

**Kết quả mong đợi** (khoảng 30 giây trên máy 4 nhân, 45 giây trên máy 2 nhân; các dòng `INFO` lúc khởi động trước khi `setLogLevel("WARN")` có hiệu lực được lược bỏ):

```text
cac stage: ['StringIndexerModel', 'OneHotEncoderModel', 'VectorAssembler', 'StandardScalerModel', 'RandomForestClassificationModel']
so khach hang can cham diem: 199908 | co cot churn_label? False
+----------+------+
|prediction| count|
+----------+------+
|       0.0| 58342|
|       1.0|141566|
+----------+------+

10 khach hang co nguy co roi bo cao nhat:
+-----------+------------+---------+----------+----------+
|customer_id|recency_days|frequency|prediction|churn_prob|
+-----------+------------+---------+----------+----------+
|   C0128812|         100|        3|       1.0|    0.6197|
|   C0126165|          16|        3|       1.0|    0.6185|
|   C0082555|          84|        3|       1.0|    0.6178|
|   C0082635|          39|        3|       1.0|    0.6178|
|   C0192343|         150|        3|       1.0|    0.6159|
...
+-----------+------------+---------+----------+----------+
only showing top 10 rows
```

- Mô hình nạp lại chạy được trên dữ liệu **không có** cột `churn_label`: pipeline đã lưu đủ trạng thái của mọi stage.
- Vì mô hình được huấn luyện có trọng số, 141 566 khách (71%) bị gắn nhãn churn ở ngưỡng 0,5. Khi triển khai, thay vì dùng `prediction`, nên xếp hạng theo `churn_prob` và chọn top-K khách hàng phù hợp với ngân sách chăm sóc.
- Nếu Bước 10 chọn (20, 5), số liệu là `54452` / `145456` và xác suất cao nhất khoảng 0,585 (khách `C0126165`) — cũng là kết quả đúng.

Chạy cùng ứng dụng trên YARN (executor 1 GB, nằm trong giới hạn 1792 MB):

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master spark-submit --master yarn --executor-memory 1g --num-executors 2 /src/lab7/churn_score.py
docker exec -it resourcemanager yarn application -list -appStates FINISHED
```

**Kết quả mong đợi:** cùng số liệu như trên (58 342 / 141 566) và ứng dụng `ChurnBatchScoring` ở trạng thái `FINISHED / SUCCEEDED` (mất khoảng 40–50 giây, phần lớn là thời gian YARN cấp container):

```text
so khach hang can cham diem: 199908 | co cot churn_label? False
|       0.0| 58342|
|       1.0|141566|
...
application_1793895637868_0002	   ChurnBatchScoring	               SPARK	      root	root.default	          FINISHED	         SUCCEEDED	           100%	historyserver:18080/history/application_1793895637868_0002/1
```

Kết quả được ghi tại `/output/churn_scores_v1` (7 tệp Parquet và `_SUCCESS`). Xem lại hai lần chạy ở Spark History Server (cổng 18080) và YARN ResourceManager (cổng 8088) — mở bằng đường dẫn từ `lab ui`; cột Tracking-URL `historyserver:18080/...` là tên container, thay phần `historyserver:18080` bằng đường dẫn cổng 18080 của `lab ui`.

## Kết thúc buổi

**1.** Thoát PySpark shell nếu còn mở (`exit()`); kiểm tra không còn ứng dụng nào giữ core:

<!-- shell: Terminal codespace -->
```bash
docker exec spark-master curl -s localhost:8080/json/ | jq -c '{activeapps: [.activeapps[].name]}'
```

**Kết quả mong đợi:**

```text
{"activeapps":[]}
```

**2.** Buổi này không bật dịch vụ bổ sung; nếu đã bật từ trước thì tắt: `lab stop-extras`. Dữ liệu HDFS (`/data/raw`, `/models/churn_pipeline_v1`, `/output/churn_scores_v1`) được giữ lại trong codespace, không cần xoá.

**3.** Lưu bài làm lên GitHub (mã các bước trong PySpark shell chép vào `src/lab7/churn_train.py` theo yêu cầu nộp bài):

<!-- shell: Terminal codespace -->
```bash
git add src/lab7
git commit -m "Buổi 7: MLlib churn"
git push
```

**4.** Dừng codespace để không tốn quota: https://github.com/codespaces → `...` → **Stop codespace**. Nếu đã đổi sang máy 4 nhân cho buổi này, có thể đổi lại 2-core (`...` → **Change machine type**) cho các buổi sau.

## Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `Initial job has not accepted any resources` lặp lại mãi | `--executor-memory` (ví dụ `2g` như tài liệu cũ) lớn hơn bộ nhớ 1 GB của mỗi Spark Worker, hoặc một ứng dụng khác (phiên `pyspark` thứ hai, `spark-submit` đang chạy) đang giữ hết 4 core | Bỏ tùy chọn (dùng mặc định 768m) hoặc đặt ≤ 768m; đóng phiên/job khác (xem cổng 8080 từ `lab ui`) |
| `IllegalArgumentException: Required executor memory (2048 MB) ... is above the max threshold (1792 MB) of this cluster!` | Trên YARN, executor + overhead 384 MB vượt giới hạn container | Dùng `--executor-memory 1g` (hoặc nhỏ hơn) khi `--master yarn` |
| Phiên PySpark chậm dần hoặc mất kết nối khi đặt `--driver-memory 2g` | Container `spark-master` chỉ có 2 GB cho cả Spark Master, JVM driver và tiến trình Python; heap 2 GB không còn chỗ cho phần còn lại | Giữ driver mặc định 768m (tối đa khoảng 1g); không `collect()` dữ liệu lớn về driver |
| Máy 2 nhân: `CrossValidator` chạy quá 12 phút, terminal/VS Code giật hoặc mất kết nối, `ExecutorLostFailure`, `lab status` thấy container khởi động lại | Hai executor + driver + cụm lõi dùng khoảng 6,6 GB, cộng VS Code vượt 8 GB RAM của máy | Mở lại PySpark một executor (`--executor-cores 2 --total-executor-cores 2`, Bước 2), tắt dịch vụ bổ sung (`lab stop-extras`), hoặc đổi máy sang 4-core / 16 GB |
| Codespace tự dừng giữa lúc đang chạy `CrossValidator`; mở lại thì `NameError: name 'train_final' is not defined` | Codespace dừng sau 30 phút không thao tác, phiên PySpark và cache bị mất | Mở lại codespace (cụm tự bật, dữ liệu HDFS còn nguyên), mở lại PySpark và chạy lại từ Bước 2; tăng *Default idle timeout* tại github.com/settings/codespaces nếu cần |
| Mở `http://spark-master:4040` hoặc `http://localhost:4040` trên trình duyệt không được | Tên container/localhost chỉ có nghĩa bên trong codespace | Dùng đường dẫn cổng 4040 từ `lab ui` hoặc tab PORTS → quả địa cầu; cổng 4040 chỉ có khi phiên PySpark/`spark-submit` đang chạy |
| Không thấy cổng 4041 trong tab PORTS | Mở phiên Spark thứ hai trong khi phiên đầu còn chạy, driver chuyển sang cổng 4041 (codespace tạo trước 06/11/2026 không forward sẵn cổng này) | Đóng phiên cũ; hoặc thêm cổng 4041 bằng *Forward a Port* trong tab PORTS |
| `F1 (lop churn) = 0.0000`, mọi dự đoán là 0 | Mất cân bằng lớp (6,7% churn) với ngưỡng 0,5 | Đánh giá thêm AUC-PR, F1 lớp churn; dùng trọng số lớp (Bước 9) hoặc chọn ngưỡng theo xác suất |
| `NameError: name 'spark' is not defined` | Chạy `pyspark < script.py` hoặc `docker exec -i` không có TTY: shell không khởi tạo biến `spark` | Dùng `docker exec -it ... pyspark` trong terminal codespace và gõ lệnh; hoặc chạy script bằng `spark-submit` với `SparkSession.builder.getOrCreate()` như `churn_score.py` |
| `IndentationError: unexpected indent` (hoặc `expected an indented block`) khi dán hàm vào PySpark shell | Thân hàm chứa dòng trống, shell coi đó là kết thúc khối | Dán khối lệnh không có dòng trống bên trong hàm/vòng lặp (như mã trong bài) |
| `Path ... already exists` khi lưu mô hình | Ghi đè thư mục đã có | Dùng `.write().overwrite().save(...)` hoặc `hdfs dfs -rm -r` thư mục cũ |
| `[CAST_INVALID_INPUT]` hoặc `[DIVIDE_BY_ZERO]` | Spark 4 bật ANSI SQL mặc định | Khai báo schema đúng kiểu, dùng `try_cast`/`try_divide`; hoặc tạm `--conf spark.sql.ansi.enabled=false` |
| AUC gần 1,0 bất thường | Rò rỉ dữ liệu: đặc trưng tính từ cùng khoảng thời gian với nhãn, hoặc chia ngẫu nhiên trên dữ liệu gộp | Kiểm tra lại các mốc ở Bước 3 và hàm `build_snapshot` |
| Kết quả đếm (số khách, số churn) khác bảng trong bài; `hdfs dfs -ls -h` báo khoảng 15 M | Dùng bộ 200 000 dòng của codespace tạo trước 06/11/2026, hoặc seed khác | `lab data 5000000`, kiểm tra `wc -l data/retail_transactions.csv` = 5000001, rồi `hdfs dfs -put -f` lại tệp |

## Câu hỏi kiểm tra

1. Vì sao không dùng `randomSplit([0.8, 0.2])` trực tiếp trên dữ liệu giao dịch để chia train/test cho bài toán churn?
   *Gợi ý: giao dịch của cùng một khách hàng và giao dịch sau mốc gán nhãn có thể rơi vào cả hai tập, mô hình "nhìn thấy" tương lai — AUC/F1 cao giả tạo.*
2. `StringIndexer(handleInvalid="keep")` giải quyết vấn đề gì so với giá trị mặc định `"error"`? Cho ví dụ trong dữ liệu bài này.
3. Tại sao `StandardScaler` dùng `withMean=False`? Bước chuẩn hóa có ảnh hưởng tới Random Forest không, và khi nào nó trở nên cần thiết?
4. Ở Bước 8, F1 có trọng số bằng 0,90 nhưng F1 lớp churn bằng 0. Giải thích sự khác nhau và cho biết chỉ số nào phù hợp với mục tiêu kinh doanh "tìm khách sắp rời bỏ".
5. Trọng số lớp ở Bước 9 thay đổi những ô nào của ma trận nhầm lẫn? Vì sao AUC-ROC gần như không đổi?
6. `CrossValidator` với 4 tổ hợp tham số và `numFolds=3` huấn luyện bao nhiêu mô hình? `parallelism=2` có tác dụng gì và vì sao không nên đặt bằng 8 trên cụm này?
7. Vì sao phải `cache()` hai bảng snapshot trước khi chạy `CrossValidator`? Điều gì xảy ra nếu bỏ `cache()`?
8. `PipelineModel` lưu trên HDFS gồm những gì? Vì sao ứng dụng chấm điểm không cần biết bảng ánh xạ của `StringIndexer` mà vẫn cho kết quả nhất quán với lúc huấn luyện?
9. Với tài nguyên 2 worker × (2 core, 1 GB), hãy giải thích vì sao lệnh `pyspark --executor-memory 2g` treo trên standalone còn trên YARN bị từ chối ngay. Đổi codespace sang máy 16 GB có làm lệnh này chạy được không? Vì sao?
10. Theo Bước 11, ba đặc trưng quan trọng nhất là gì? Liên hệ với cách `gen_data.py` sinh dữ liệu để giải thích vì sao AUC chỉ khoảng 0,61.

## Bài tập mở rộng

1. **Minh họa rò rỉ dữ liệu**: tính đặc trưng trên **toàn bộ** dữ liệu (không cắt theo mốc), gộp với nhãn của snapshot train rồi `randomSplit([0.8, 0.2])`. So sánh AUC với Bước 8 và giải thích.
2. **Chọn ngưỡng**: dùng `vector_to_array("probability")[1]` để tính precision, recall, F1 của lớp churn với các ngưỡng 0,3; 0,5; 0,6; 0,7 trên mô hình có trọng số. Chọn ngưỡng sao cho số khách cần chăm sóc không vượt 20 000.
3. **So sánh mô hình**: thay `RandomForestClassifier` bằng `LogisticRegression` (cùng `weightCol`) và `GBTClassifier`; so sánh AUC-ROC, AUC-PR và thời gian huấn luyện.
4. **Thêm đặc trưng**: tỉ lệ chi tiêu theo từng danh mục (pivot `category`), số ngày có giao dịch trong 30 ngày cuối, mức giảm giá trung bình. Đặc trưng nào làm tăng AUC?
5. **Huấn luyện trên YARN**: chuyển toàn bộ mã Bước 2–12 thành một script `src/lab7/churn_train.py` (thêm `spark = SparkSession.builder.appName("ChurnTrain").getOrCreate()` ở đầu), chạy bằng `spark-submit --master yarn --executor-memory 1g --num-executors 2` và so sánh thời gian `CrossValidator` với Spark standalone. Trên máy 2 nhân, đóng mọi phiên PySpark khác trước khi chạy.
6. **TrainValidationSplit**: thay `CrossValidator` bằng `TrainValidationSplit(trainRatio=0.8)` và so sánh thời gian chạy và tham số tốt nhất.
7. **Ảnh hưởng của cấu hình máy** (dành cho sinh viên dùng Codespaces): ghi thời gian `CrossValidator` khi mở PySpark mặc định và khi mở một executor (`--executor-cores 2 --total-executor-cores 2`), quan sát RAM bằng `lab status` trong lúc chạy; giải thích kết quả dựa trên số nhân thật của máy codespace.

## Yêu cầu nộp bài và tiêu chí chấm

Nộp bài theo hai cách song song: (1) `git push` thư mục `src/lab7/` lên repo GitHub của mình (gồm `churn_train.py` — toàn bộ lệnh đã chạy trong PySpark shell, theo thứ tự các bước — và `churn_score.py`); (2) nộp tệp `MSSV_buoi7.zip` gồm hai tệp mã trên và báo cáo `MSSV_buoi7.pdf` có ảnh chụp/đoạn log cho từng mục dưới đây cùng phần trả lời câu hỏi kiểm tra. Ghi rõ trong báo cáo cấu hình máy codespace đã dùng (2 hay 4 nhân) và cách mở PySpark.

| Tiêu chí | Minh chứng cần có | Điểm |
|---|---|---|
| Chuẩn bị dữ liệu và tài nguyên | `wc -l data/retail_transactions.csv`; `hdfs dfs -ls -h /data/raw`; thông tin worker của Spark Master; giải thích lỗi khi yêu cầu executor 2 GB | 1,0 |
| Khảo sát và mốc thời gian | Schema, khoảng thời gian dữ liệu, bảng mốc train/test đúng nguyên tắc không chồng lấp nhãn | 1,0 |
| Đặc trưng và nhãn | Phân phối `churn_label` của hai snapshot; kiểm tra `recency am: 0`; giải thích từng đặc trưng | 1,5 |
| Pipeline và huấn luyện | Danh sách 5 stage; kết quả dự đoán mẫu có `probability` | 1,0 |
| Đánh giá và mất cân bằng lớp | AUC-ROC, AUC-PR, F1 có trọng số, F1 lớp churn và ma trận nhầm lẫn trước/sau khi dùng trọng số; nhận xét | 2,0 |
| Tinh chỉnh và diễn giải | Bảng AUC của 4 cấu hình, tham số tốt nhất (chấp nhận (20, 5) hoặc (20, 10)), độ quan trọng đặc trưng và nhận xét | 1,5 |
| Lưu/nạp mô hình và chấm điểm | Cấu trúc `/models/churn_pipeline_v1`; kết quả `churn_score.py` trên standalone và YARN | 1,0 |
| Câu hỏi kiểm tra và trình bày | Trả lời đúng, dẫn chứng bằng số liệu của chính mình; mã đã `git push` lên repo | 1,0 |
| **Tổng** | | **10** |

Bài tập mở rộng (tùy chọn) được cộng tối đa 1 điểm, không vượt quá 10.
