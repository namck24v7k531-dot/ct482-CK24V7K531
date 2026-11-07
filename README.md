# Môi trường thực hành CT482 – Xử lý dữ liệu lớn (bản 2026.10)

Bộ môi trường Docker dùng chung cho các buổi thực hành 1–7 của học phần CT482. Toàn bộ cụm Hadoop/Spark chạy từ **một image tự build `bigdata-lab:2026.10`**; Hive, Kafka và MongoDB dùng image chính thức.

| Thành phần | Phiên bản | Ghi chú |
|---|---|---|
| Hệ điều hành image | Ubuntu 24.04 | |
| Java | OpenJDK 17 (17.0.20) | `JAVA_HOME=/usr/lib/jvm/java-17` |
| Python | 3.12 | numpy 2.5.3, pandas 2.3.3, pyarrow 21.0.0, kafka-python 2.3.2, confluent-kafka 2.15.1, pymongo 4.18.2, happybase 1.2.0 |
| Hadoop (HDFS, YARN, MapReduce) | 3.4.3 | |
| Spark | 4.1.3 (Scala 2.13, bản bin-hadoop3) | PySpark lấy từ `/opt/spark/python` (đúng phiên bản 4.1.3) |
| HBase | 2.6.7 (bản hadoop3) | chế độ standalone, dữ liệu trên HDFS |
| Hive | 4.2.1 (image `apache/hive:4.2.1`) | Metastore dùng PostgreSQL 16, engine Tez chạy cục bộ trong HiveServer2 |
| Kafka | 4.3.1 (image `apache/kafka:4.3.1`) | KRaft, không cần ZooKeeper |
| MongoDB | 8.0 (image `mongo:8.0`, 8.0.32) | không bật xác thực |

---

## 1. Kiến trúc

### Cụm lõi – `docker-compose.yml` (8 container, cùng image `bigdata-lab:2026.10`)

| Container | Vai trò | Cổng (host) | Mô tả |
|---|---|---|---|
| namenode | HDFS NameNode | 9870, 9000 | Web UI HDFS và cổng RPC `hdfs://namenode:9000` |
| datanode1 | HDFS DataNode | 9864 | DataNode thứ nhất |
| datanode2 | HDFS DataNode | 9865 | DataNode thứ hai (cổng 9864 trong container) |
| resourcemanager | YARN ResourceManager | 8088 | Web UI YARN |
| nodemanager1 | YARN NodeManager + Spark Worker | 8042 | Chạy container MapReduce/Spark-on-YARN và executor Spark standalone |
| nodemanager2 | YARN NodeManager + Spark Worker | 8043 | Như trên (cổng 8042 trong container) |
| spark-master | Spark Master (standalone) | 8080, 7077, 4040 | Web UI Master, cổng submit, Spark UI của driver chạy trong container này |
| historyserver | Spark History + MapReduce JobHistory | 18080, 19888 | Xem lại job đã chạy xong |

### Dịch vụ bổ sung – `docker-compose.extras.yml`

| Container | Vai trò | Cổng (host) | Dùng ở bài |
|---|---|---|---|
| hive-metastore-db | PostgreSQL 16 lưu metadata Hive | – (không mở) | Lab 3 |
| hive-metastore | Hive Metastore (Thrift) | 9083 | Lab 3 |
| hiveserver2 | HiveServer2 + beeline | 10000 (JDBC), 10002 (Web UI) | Lab 3 |
| kafka | Kafka broker + controller (KRaft) | 9092 | Lab 6 |
| hbase | HBase Master + RegionServer + ZooKeeper nhúng + Thrift | 16010 (Master UI), 16030 (RegionServer UI), 9090 (Thrift) | Lab 4 |
| mongodb | MongoDB 8.0 | 27017 | Lab 4 |

Không có cổng nào của extras trùng với cụm lõi.

---

## 2. Yêu cầu

- Docker Engine ≥ 24 (Linux) hoặc Docker Desktop ≥ 4.30 (Windows/macOS), **Docker Compose ≥ 2.24** (`docker compose version`).
- RAM máy ≥ 8 GB. Với Docker Desktop/WSL2 cần cấp cho Docker **tối thiểu 6 GB** (khuyến nghị 8 GB nếu bật extras).
- Ổ đĩa trống ≥ 15 GB (image `bigdata-lab` ~4,9 GB, `apache/hive` ~3,9 GB, cộng dữ liệu HDFS).
- Kết nối Internet khi build lần đầu (tải Hadoop/Spark/HBase ~1,5 GB; thử lần lượt `downloads.apache.org` → `mirrors.aliyun.com/apache` → `archive.apache.org`, xem mục 4).
- Máy Apple Silicon (arm64): Dockerfile tự chọn bản Hadoop aarch64; bản amd64 đã được kiểm thử đầy đủ.

## 3. Cấu trúc thư mục

```
moi-truong-bigdata-lab/
├── Dockerfile                     # image bigdata-lab:2026.10
├── docker-compose.yml             # cụm lõi 8 container
├── docker-compose.extras.yml      # Hive, Kafka, HBase, MongoDB
├── docker-compose.noports.yml     # (chỉ để kiểm thử) bỏ cổng host của cụm lõi
├── docker-compose.extras.noports.yml
├── scripts/entrypoint.sh          # khởi động từng vai trò (namenode, datanode, ...)
├── scripts-host/smoke_test.sh     # kiểm tra nhanh toàn cụm, chạy trên máy host
├── conf/hadoop/  conf/spark/  conf/hbase/  conf/hive/
├── hive/ext-jars/                 # JDBC driver PostgreSQL cho Hive Metastore
├── downloads/                     # (tuỳ chọn) đặt sẵn tệp .tgz để build không cần tải lại
├── .env.example                   # (tuỳ chọn) đổi mirror tải Apache: cp .env.example .env
├── data/                          # dữ liệu dùng chung, mount thành /data trong container
├── src/                           # mã nguồn bài lab, mount thành /src (có sẵn src/kiem-tra/)
├── gen_data.py                    # sinh dữ liệu mẫu
├── README.md   TESTING.md
```

## 4. Build image

```bash
cd moi-truong-bigdata-lab
docker compose build
# hoặc:
docker build --network host -t bigdata-lab:2026.10 -t bigdata-lab:latest .
```

- Lần đầu mất khoảng 5–15 phút tùy mạng. Các lần sau dùng cache.
- **Máy chủ tải (mirror)**: Dockerfile thử lần lượt các mirror trong biến `APACHE_MIRROR`
  (mặc định `https://downloads.apache.org https://mirrors.aliyun.com/apache https://archive.apache.org/dist`).
  Mirror nào không kết nối được thì sau khoảng 95 giây chuyển sang mirror kế tiếp.
  `archive.apache.org` giữ mọi phiên bản nhưng hay **chặn hoặc giới hạn IP tải nhiều**: cả phòng máy
  ra Internet chung một IP nên dễ bị chặn, vì vậy chỉ dùng làm phương án cuối. Đổi danh sách mirror:
  ```bash
  docker compose build --build-arg APACHE_MIRROR="https://mirrors.aliyun.com/apache https://downloads.apache.org"
  # hoặc lâu dài: cp .env.example .env, sửa dòng APACHE_MIRROR, rồi docker compose build
  ```
- **Mạng chậm/hay đứt**: tải trước các tệp sau và đặt vào `downloads/` (giữ nguyên tên), Dockerfile sẽ dùng bản có sẵn
  (tệp `.tar.gz`/`.tgz` hỏng, tải dở sẽ bị phát hiện bằng `gzip -t` và tải lại):
  ```bash
  cd downloads
  M=https://downloads.apache.org      # hoặc https://mirrors.aliyun.com/apache, https://archive.apache.org/dist
  curl -fL -C - --retry 5 -O $M/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz   # máy arm64: hadoop-3.4.3-aarch64.tar.gz
  curl -fL -C - --retry 5 -O $M/spark/spark-4.1.3/spark-4.1.3-bin-hadoop3.tgz
  curl -fL -C - --retry 5 -O $M/hbase/2.6.7/hbase-2.6.7-hadoop3-bin.tar.gz
  for f in *.tar.gz *.tgz; do gzip -t "$f" && echo "OK $f"; done
  cd .. && docker compose build
  ```
  `downloads.apache.org` và các mirror chỉ giữ bản phát hành hiện hành; khi Apache ra bản mới, bản cũ chỉ còn trên `archive.apache.org`.
- **Cả phòng máy (giảng viên)**: build một lần trên máy có mạng tốt, rồi phát image qua LAN/USB để sinh viên không phải build:
  ```bash
  # máy giảng viên
  docker save bigdata-lab:2026.10 bigdata-lab:latest | gzip > bigdata-lab_2026.10.tar.gz
  # máy sinh viên
  docker load -i bigdata-lab_2026.10.tar.gz
  docker compose up -d      # KHÔNG chạy docker compose build; pull_policy: missing sẽ dùng image đã nạp
  ```
- Image Hive, Kafka, PostgreSQL, MongoDB được tải tự động khi chạy extras lần đầu.

## 5. Sinh dữ liệu mẫu

```bash
# Có Python 3 trên máy host:
python3 gen_data.py --rows 5000000 --out ./data            # ~500 MB giao dịch
python3 gen_data.py --rows 200000  --out ./data --small     # bản nhỏ cho máy yếu

# Không có Python trên host: dùng chính image của cụm
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD":/w -w /w bigdata-lab:2026.10 \
  python3 gen_data.py --rows 200000 --small --out ./data
```

Sinh ra `retail_transactions.csv`, `products.csv`, `web_access.log`, `sensor_readings.csv` trong `./data`. Thư mục này đã được mount thành `/data` trong `namenode`, `spark-master`, `nodemanager1/2`, `hiveserver2`, `hbase` (và `/import` trong `mongodb`), nên **không cần `docker cp`**:

```bash
docker exec -it namenode hdfs dfs -mkdir -p /data/raw
docker exec -it namenode hdfs dfs -put -f /data/retail_transactions.csv /data/raw/
```

## 6. Khởi động và dừng

```bash
# Cụm lõi (Lab 1, 2, 5, 7)
docker compose up -d
docker compose ps                      # chờ tất cả "healthy" (~30–90 giây)

# Thêm dịch vụ bổ sung: LUÔN ghi cả hai tệp -f
docker compose -f docker-compose.yml -f docker-compose.extras.yml up -d hiveserver2      # Lab 3
docker compose -f docker-compose.yml -f docker-compose.extras.yml up -d hbase mongodb    # Lab 4
docker compose -f docker-compose.yml -f docker-compose.extras.yml up -d kafka            # Lab 6
docker compose -f docker-compose.yml -f docker-compose.extras.yml up -d                  # tất cả

# Dừng một dịch vụ bổ sung để giải phóng RAM
docker compose -f docker-compose.yml -f docker-compose.extras.yml stop hiveserver2 hive-metastore hive-metastore-db

# Dừng toàn bộ (GIỮ dữ liệu HDFS, Hive, Kafka, HBase, MongoDB trong volume)
docker compose -f docker-compose.yml -f docker-compose.extras.yml down

# Xoá sạch, làm lại từ đầu (MẤT toàn bộ dữ liệu trong volume)
docker compose -f docker-compose.yml -f docker-compose.extras.yml down -v
```

Mẹo: để khỏi gõ hai `-f`, tạo tệp `.env` cạnh `docker-compose.yml` với dòng
`COMPOSE_FILE=docker-compose.yml:docker-compose.extras.yml` (Windows dùng dấu `;`). Khi đó `docker compose up -d kafka` là đủ.

## 7. Web UI

| Giao diện | Địa chỉ |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| DataNode 1 / 2 | http://localhost:9864 · http://localhost:9865 |
| YARN ResourceManager | http://localhost:8088 |
| NodeManager 1 / 2 | http://localhost:8042 · http://localhost:8043 |
| Spark Master | http://localhost:8080 |
| Spark UI của job đang chạy (driver trong spark-master) | http://localhost:4040 |
| Spark History Server | http://localhost:18080 |
| MapReduce JobHistory | http://localhost:19888 |
| HiveServer2 Web UI | http://localhost:10002 |
| HBase Master / RegionServer | http://localhost:16010 · http://localhost:16030 |

Liên kết bên trong Web UI dùng tên container (ví dụ `http://nodemanager1:8042/...`). Trình duyệt trên host không phân giải được các tên này: thay tên bằng `localhost` và cổng host tương ứng ở bảng trên. Spark Worker UI (cổng 8081) không mở ra host.

## 8. Kiểm tra sức khoẻ cụm

```bash
docker compose ps                                                     # mọi container "healthy"
docker exec -it namenode hdfs dfsadmin -report | grep "Live datanodes"   # Live datanodes (2)
docker exec -it namenode hdfs dfsadmin -safemode get                   # Safe mode is OFF
docker exec -it resourcemanager yarn node -list                        # Total Nodes:2
docker exec -it spark-master curl -s localhost:8080/json/ | grep -o '"aliveworkers" : [0-9]*'   # 2

# Chạy thử nhanh
docker exec -it namenode bash -lc 'hadoop jar $HADOOP_EXAMPLES_JAR pi 2 10'
docker exec -it spark-master bash -lc 'spark-submit --master spark://spark-master:7077 --class org.apache.spark.examples.SparkPi $SPARK_EXAMPLES_JAR 10'
docker exec -it spark-master bash -lc 'spark-submit --master yarn --class org.apache.spark.examples.SparkPi $SPARK_EXAMPLES_JAR 10'
```

Kiểm tra toàn bộ một lần (chạy trên host, sau khi đã sinh dữ liệu): `./scripts-host/smoke_test.sh` — in `PASS/FAIL` cho HDFS, YARN, MapReduce, Streaming, Spark standalone/YARN và các dịch vụ extras đang chạy.

Thư mục `src/kiem-tra/` có sẵn các script kiểm tra: `mapper.py`/`reducer.py` (Hadoop Streaming), `WordCount.java`, `spark_hdfs_parquet.py`, `kafka_python_test.py`, `kafka_streaming_test.py`, `happybase_test.py`, `pymongo_test.py`, `mllib_test.py`.

## 9. Cách chạy từng loại công việc

### HDFS
Chạy trong bất kỳ container nào của image `bigdata-lab` (thường là `namenode`): `docker exec -it namenode bash`, sau đó `hdfs dfs ...`. Người dùng trong container là `root`; HDFS tắt kiểm tra quyền (`dfs.permissions.enabled=false`) để tránh lỗi quyền khi học. Có sẵn các thư mục `/user/student`, `/data`, `/tmp`, `/user/hive/warehouse`, `/spark-logs`, `/hbase`.

### MapReduce (Java) — Lab 2
JDK 17 (có `javac`, `jar`) đã cài trong image, **không cần Maven**:
```bash
docker exec -it namenode bash
mkdir -p /root/lab2 && cd /root/lab2         # hoặc đặt mã nguồn trong ./src trên host → /src
javac -classpath "$(hadoop classpath)" -d classes WordCount.java   # (tạo thư mục classes trước: mkdir -p classes)
jar cf wordcount.jar -C classes .
hadoop jar wordcount.jar WordCount /user/student/text_input /user/student/output/wc
```
Nếu muốn dùng Maven, chạy container một lần trên host: `docker run --rm -v "$PWD/src":/src -w /src/<du-an> maven:3.9-eclipse-temurin-17 mvn -q package`, rồi `hadoop jar /src/<du-an>/target/<ten>.jar ...` trong container `namenode`. Khai báo `hadoop-client` phiên bản `3.4.3` với `<scope>provided</scope>`.

### Hadoop Streaming (Python) — Lab 2
```bash
docker exec -it namenode bash -lc '
hadoop jar $STREAM_JAR \
  -files /src/kiem-tra/mapper.py,/src/kiem-tra/reducer.py \
  -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input /data/raw/retail_transactions.csv -output /user/student/output/revenue_by_category'
```
- `$STREAM_JAR` = `$HADOOP_STREAMING_JAR` = `/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar`
- `$HADOOP_EXAMPLES_JAR` = `/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.3.jar`
- Python trên mọi node: `/usr/bin/python3` (3.12); dùng `-mapper "python3 mapper.py"` để không phụ thuộc dòng shebang.

### Spark — Lab 5, 6, 7
```bash
# Standalone (Spark Master + 2 Worker trong nodemanager1/2, mỗi worker 2 core, 1 GB)
docker exec -it spark-master pyspark --master spark://spark-master:7077
docker exec -it spark-master spark-submit --master spark://spark-master:7077 /src/bai_lab.py

# Trên YARN (client mode mặc định; thêm --deploy-mode cluster nếu muốn driver chạy trong YARN)
docker exec -it spark-master spark-submit --master yarn /src/bai_lab.py
```
- Không ghi `--master` thì Spark chạy `local[*]` trong container đó.
- Đường dẫn HDFS: `hdfs://namenode:9000/...` (hoặc ngắn gọn `/data/raw/...`, vì mặc định `fs.defaultFS` là HDFS). Đọc tệp cục bộ phải ghi `file:///data/...`.
- Mặc định `spark.driver.memory=768m`, `spark.executor.memory=768m` (vừa với Worker 1 GB). Đặt `--executor-memory` lớn hơn 1 GB thì executor standalone **không được cấp** (job đứng ở "Initial job has not accepted any resources"); trên YARN giới hạn là 1792 MB/container kể cả overhead 384 MB.
- Connector Kafka (`spark-sql-kafka-0-10_2.13-4.1.3` và phụ thuộc) **đã có sẵn trong `/opt/spark/jars`**, không cần `--packages`. Nếu vẫn ghi `--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3` thì vẫn chạy (tải từ Maven Central, mất ~20 giây lần đầu).
- Event log ghi vào `hdfs:///spark-logs`, xem ở Spark History Server (cổng 18080).

### Hive — Lab 3
```bash
docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000/default -n hive
```
- Chạy tệp SQL không tương tác: `docker exec -i -e HADOOP_CLIENT_OPTS=-Dorg.jline.terminal.provider=dumb hiveserver2 beeline -u jdbc:hive2://localhost:10000/default -n hive < /duong/dan/script.sql` (hoặc dùng `-e "câu lệnh; câu lệnh;"`).
- Warehouse: `hdfs://namenode:9000/user/hive/warehouse` (`retail_dw.db/...`).
- `hive.exec.dynamic.partition.mode=nonstrict` đã bật sẵn.

### Kafka — Lab 6
```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic retail-transactions --partitions 3 --replication-factor 1
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic retail-transactions --from-beginning
docker exec -it kafka kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic retail-transactions
```
- Từ container khác (Spark, Python trong cụm): `kafka:9092`. Từ máy host (script Python chạy trên host): `localhost:9092`.
- Thư viện Python có sẵn trong image: `kafka-python` và `confluent-kafka`.

### HBase — Lab 4
```bash
docker exec -it hbase hbase shell
docker exec -i hbase hbase shell -n /src/load_iot.hbase      # chạy tệp lệnh (đặt tệp trong ./src)
```
Python qua Thrift: `happybase.Connection("hbase", 9090)` (từ container trong cụm) hoặc `happybase.Connection("localhost", 9090)` (từ host).

### MongoDB — Lab 4
```bash
docker exec -it mongodb mongosh retail
docker exec -it mongodb mongoimport --db retail --collection transactions --file /import/retail_transactions.jsonl --type json
```
Python: `MongoClient("mongodb://mongodb:27017")` (trong cụm) hoặc `mongodb://localhost:27017` (từ host).

## 10. Tài nguyên (đo thực tế, `docker stats`)

| Trạng thái | RAM dùng |
|---|---|
| Cụm lõi, rảnh | ~2,3 GB |
| Cụm lõi, chạy đồng thời 1 job MapReduce trên YARN + 1 job Spark MLlib standalone | ~4,7 GB (đỉnh) |
| Cụm lõi + toàn bộ extras, rảnh | ~4,8 GB (extras ~2,5 GB, riêng HiveServer2 ~1 GB) |

Tổng `mem_limit` (giới hạn trần) của cụm lõi là 11 GB, của extras là 6,75 GB; đây là mức trần để một container không chiếm hết máy, không phải mức dùng thường xuyên. Máy 8 GB nên chạy cụm lõi và chỉ bật extras cần cho bài đang học.

## 11. Lỗi thường gặp và cách xử lý

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Container bị `OOMKilled`, máy treo, WSL2 chậm | Docker Desktop/WSL2 được cấp quá ít RAM | Windows: tạo `%UserProfile%\.wslconfig` với `[wsl2]` / `memory=8GB` / `processors=4`, chạy `wsl --shutdown` rồi mở lại Docker Desktop. macOS: Settings → Resources → Memory ≥ 6 GB. Tắt extras không dùng. |
| `Bind for 0.0.0.0:8080 failed: port is already allocated` | Cổng đang bị chương trình/container khác dùng | `docker ps` tìm container chiếm cổng (Linux/macOS: `sudo lsof -i :8080`, Windows: `netstat -ano \| findstr 8080`), dừng nó; hoặc sửa vế trái của cổng trong compose, ví dụ `"18081:8080"`. |
| `SafeModeException` / `Name node is in safe mode` | NameNode vừa khởi động, chưa nhận đủ block report | Chờ 30–60 giây; kiểm tra `hdfs dfsadmin -safemode get`; nếu cần: `docker exec -it namenode hdfs dfsadmin -safemode leave`. |
| DataNode không lên, log có `Incompatible clusterIDs` | Đã xoá volume NameNode (format lại) nhưng còn volume DataNode cũ | Cụm dùng `clusterId` cố định nên hiếm gặp. Nếu vẫn bị: `docker compose down -v` (xoá cả volume datanode) rồi `up -d` lại. |
| Job đứng ở `ACCEPTED` | NodeManager chưa đăng ký hoặc hết RAM YARN | `yarn node -list` phải thấy 2 node; dừng job khác (`yarn application -kill <id>`); giảm bộ nhớ yêu cầu. |
| `InaccessibleObjectException` / `module java.base does not "opens" ...` | Java 17 chặn reflection | Image đã thêm sẵn `--add-opens` cho daemon Hadoop (`hadoop-env.sh`) và cho container MapReduce (`mapreduce.map/reduce.java.opts`, `yarn.app.mapreduce.am.command-opts`). Nếu tự đặt `-Dmapreduce.map.java.opts=...` khi submit thì phải **giữ lại** các tuỳ chọn `--add-opens` (xem `conf/hadoop/mapred-site.xml`). Spark tự thêm các tuỳ chọn này. |
| Spark job đứng ở `Initial job has not accepted any resources` | `--executor-memory`/`--total-executor-cores` vượt khả năng Worker (1 GB, 2 core) | Dùng `--executor-memory 768m` hoặc nhỏ hơn. |
| `[CAST_INVALID_INPUT]`, `[DIVIDE_BY_ZERO]`... trong Spark SQL | Spark 4 bật ANSI SQL mặc định | Làm sạch dữ liệu, dùng `try_cast`, `try_divide`; hoặc tạm tắt: `--conf spark.sql.ansi.enabled=false`. |
| `beeline` báo `Unable to create a terminal` | Chạy beeline không có TTY (thiếu `-it`, hoặc trong script) | Dùng `docker exec -it ...`; khi chạy tệp SQL dùng cách ở mục Hive (biến `HADOOP_CLIENT_OPTS=-Dorg.jline.terminal.provider=dumb` và `<` chuyển hướng). |
| `mongodb` khởi động lại liên tục, log: `Linux kernel versions 6.19 and newer has a known incompatibility` | Lỗi MongoDB 8 với kernel Linux mới (SERVER-121912) | Compose đã đặt `GLIBC_TUNABLES=glibc.pthread.rseq=1` cho `mongodb`. Nếu vẫn lỗi, cập nhật kernel của máy (MongoDB ghi nhận kernel ≥ 7.0.14 đã sửa lỗi). |
| `NodeManager unhealthy` trong `yarn node -list` | Ổ đĩa host gần đầy | Giải phóng ổ đĩa (`docker system df` để xem dung lượng Docker); ngưỡng đã nâng lên 98,5%. |
| Build báo `curl: (28) Failed to connect to archive.apache.org port 443 ... Couldn't connect to server`, kết thúc `exit code: 28` | `archive.apache.org` chặn/giới hạn IP của phòng máy (nhiều máy tải cùng lúc qua một IP) hoặc tường lửa chặn | Tải bản `Dockerfile` mới (đã tự thử nhiều mirror). Với bản cũ: `docker compose build --build-arg APACHE_MIRROR=https://downloads.apache.org`; hoặc tải trước vào `downloads/`, hoặc `docker load` image do giảng viên phát (mục 4). Kiểm tra nhanh: `curl -sI -m 15 https://downloads.apache.org/ \| head -1`. |
| Build báo `Temporary failure resolving 'archive.ubuntu.com'` | DNS của BuildKit | Compose đã đặt `network: host` khi build; nếu build tay, thêm `--network host`. |
| Tệp sinh trong `./data` thuộc `root` (Linux) | Container chạy bằng root | `sudo chown -R $USER: data` hoặc chạy `docker run --user "$(id -u):$(id -g)" ...` như mục 5. |

## 12. Khác biệt so với môi trường cũ (bde2020)

| Nội dung | Môi trường cũ | Môi trường mới | Ảnh hưởng tới bài lab |
|---|---|---|---|
| Image | Nhiều image bde2020 (Hadoop 3.2.1, Java 8), Spark 3.3 riêng | Một image `bigdata-lab:2026.10` cho 8 container; Spark Worker nằm trong nodemanager1/2 | Tên container `spark-worker1` không còn; executor chạy trong `nodemanager1/2` |
| Java | 8 | 17 | Dùng `JAVA_HOME=/usr/lib/jvm/java-17`; MapReduce cần `--add-opens` (đã cấu hình) |
| Python | 3.7/3.8 | 3.12 | Các thư viện cài sẵn như bảng đầu trang |
| Hadoop | 3.2.1 | 3.4.3 | Lệnh `hdfs`, `yarn`, `mapred` giữ nguyên; jar ví dụ/streaming ở đường dẫn mới (mục 9) |
| Cấu hình | `hadoop.env` (biến `CORE_CONF_...`) | Tệp XML trong `conf/hadoop/` | Muốn đổi cấu hình thì sửa XML rồi `docker compose build` |
| HDFS | Dead node sau ~10 phút | Dead node sau ~60 giây | Thuận tiện cho Lab 1 (thử dừng `datanode2`) |
| Spark | 3.3/3.4, Scala 2.12 | 4.1.3, Scala 2.13 | `--packages ..._2.12:3.x` phải đổi thành `..._2.13:4.1.3` (hoặc bỏ hẳn vì đã có sẵn); **ANSI SQL bật mặc định**; cần Java 17+; pandas ≥ 2.2 |
| Spark History | Không có | `historyserver:18080`, event log trên HDFS | Xem lại job đã xong |
| Hive | 2.3.2, engine MapReduce, container `hive-server` | 4.2.1, **engine Tez chạy cục bộ** trong `hiveserver2`, Metastore PostgreSQL 16 | Đổi `hive-server` → `hiveserver2`; dùng `beeline` (không còn `hive` CLI); bảng tạo bằng `CREATE TABLE` (không ACID) được Hive 4 chuyển thành `EXTERNAL_TABLE` — kiểm tra bằng `DESCRIBE FORMATTED` |
| Kafka | bitnami/kafka 3.7 | apache/kafka 4.3.1 KRaft, không ZooKeeper | `kafka.tools.GetOffsetShell` không còn → dùng `kafka-get-offsets.sh`; trong cụm dùng `kafka:9092` |
| HBase | harisekhon/hbase 2.1, container `hbase-master` | 2.6.7 trên HDFS (`/hbase`), container `hbase` | Đổi tên container; Thrift 9090 cho happybase. Connector `hbase-spark` chưa có bản cho Spark 4 |
| MongoDB | 7, có user/password | 8.0, không xác thực | `mongosh`, `mongoimport` dùng trực tiếp, không cần `-u/-p` |
| Mạng Docker | Tên cố định `bigdata-lab`, extras khai báo `external` | Mạng mặc định của project | Luôn chạy extras bằng hai `-f` |

## 13. Đường dẫn quan trọng trong image

| Biến | Giá trị |
|---|---|
| `JAVA_HOME` | `/usr/lib/jvm/java-17` |
| `HADOOP_HOME`, `HADOOP_CONF_DIR` | `/opt/hadoop`, `/opt/hadoop/etc/hadoop` |
| `SPARK_HOME`, `SPARK_CONF_DIR` | `/opt/spark`, `/opt/spark/conf` |
| `HBASE_HOME` | `/opt/hbase` |
| `STREAM_JAR` / `HADOOP_STREAMING_JAR` | `/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar` |
| `HADOOP_EXAMPLES_JAR` | `/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.3.jar` |
| `SPARK_EXAMPLES_JAR` | `/opt/spark/examples/jars/spark-examples_2.13-4.1.3.jar` |
| `PYTHONPATH` | `/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip` |
| Python | `/usr/bin/python3` (3.12) — `PYSPARK_PYTHON=python3` |

## Ghi chú phiên bản bộ môi trường (2026.10, bản cập nhật 03/10/2026)

- `namenode` dùng `pull_policy: missing`: `docker compose up` chỉ build khi máy chưa có image `bigdata-lab:2026.10`; các lần sau khởi động ngay, không build lại. Muốn build lại sau khi sửa cấu hình: `docker compose build`.
- Thêm `conf/hive/hdfs-site.xml` (`dfs.replication=2`) để tệp do Hive ghi chỉ nhân bản 2 lần, khớp với cụm 2 DataNode.
- `mem_limit` của mỗi DataNode tăng từ 512 MB lên 768 MB.
