# Buổi 1 — Triển khai cụm Hadoop bằng Docker và thao tác HDFS (GitHub Codespaces)

Thời lượng 4 tiết (khoảng 180 phút thực hành, phần còn lại viết báo cáo), ứng với CLO2 (triển khai và vận hành hạ tầng lưu trữ phân tán). **Máy khuyến nghị: 2 nhân / 8 GB** (mặc định của codespace). Buổi này chỉ dùng HDFS nên không cần máy 4 nhân. Chỉ cần **cụm lõi** (8 container dùng chung image `bigdata-lab:2026.10`: Hadoop 3.4.3, Spark 4.1.3, JDK 17, Python 3.12). Cụm tự bật khi mở codespace, **không cần** `lab hive`, `lab nosql`, `lab kafka`. Dữ liệu: `retail_transactions.csv` 5 000 000 giao dịch (khoảng 405 MB), sinh một lần bằng `lab data 5000000`. **Quota ước tính:** codespace mở khoảng 3–3,5 giờ trên máy 2 nhân, tức 6–7 core-hour (máy 4 nhân tốn gấp đôi). Lúc cao nhất (Phần 5) buổi này dùng thêm khoảng 5 GB đĩa; sau khi dọn ở Bước 19 thì trả lại gần hết. Mọi kết quả mẫu trong tài liệu được chạy thật trên bản sao codespace 2 nhân / 8 GB với bộ dữ liệu đầy đủ; số đo thời gian chỉ là ví dụ và phụ thuộc máy.

## Mục tiêu

Sau khi hoàn thành buổi thực hành, sinh viên có khả năng:

- Mở codespace của học phần, kiểm tra cụm Hadoop gồm NameNode, 2 DataNode, ResourceManager, 2 NodeManager, Spark Master và History Server (chạy bằng Docker Compose bên trong codespace) bằng `lab status` và `docker compose ps`.
- Mở Web UI của HDFS (cổng 9870), YARN (8088), Spark Master (8080) và History Server (18080, 19888) qua tab **PORTS** hoặc lệnh `lab ui` để theo dõi trạng thái cụm.
- Thao tác thành thạo các lệnh `hdfs dfs` cơ bản: `mkdir`, `put`, `get`, `ls`, `du`, `cat`, `tail`, `rm`; tải một tệp từ codespace về máy cá nhân và kiểm tra toàn vẹn.
- Thay đổi hệ số nhân bản bằng `setrep`, đọc phân bố block trên các DataNode bằng `hdfs fsck` và giải thích trạng thái under-replicated.
- Mô phỏng sự cố mất một DataNode, quan sát NameNode phát hiện node chết và trạng thái bản sao trước/sau khi node quay lại.
- Đo ảnh hưởng của kích thước block (64 MB so với 128 MB) tới số block và thời gian ghi tệp 1 GB, từ đó nhận xét về đánh đổi giữa số block và chi phí quản lý.
- Chứng minh dữ liệu HDFS được giữ lại khi dừng cụm và khi dừng/mở lại codespace.

## Chuẩn bị

**Yêu cầu**

- Tài khoản GitHub (nên xác minh GitHub Student Developer Pack để có 180 core-hour/tháng) và **codespace của học phần** đã tạo ở Buổi 0 từ repo `bigdatalab` (repo template của giảng viên hoặc repo GitHub Classroom).
- Trình duyệt (Chrome, Edge, Firefox) và Internet ổn định. Không cần cài Docker, WSL hay Java trên máy cá nhân.

**Phiên bản thực tế trong image `bigdata-lab:2026.10`**

| Thành phần | Phiên bản |
|---|---|
| Hệ điều hành image | Ubuntu 24.04 |
| Java | OpenJDK 17 |
| Python | 3.12 |
| Hadoop (HDFS, YARN, MapReduce) | 3.4.3 |
| Spark | 4.1.3 (Scala 2.13) |

**Các Web UI dùng trong buổi này** (mở bằng link từ `lab ui`, hoặc tab **PORTS** → biểu tượng quả địa cầu ở dòng có nhãn tương ứng)

| Giao diện | Cổng (nhãn trong tab PORTS) | Dùng để |
|---|---|---|
| HDFS NameNode | 9870 (HDFS NameNode) | Live/Dead DataNode, dung lượng, duyệt tệp (Utilities → Browse the file system) |
| DataNode 1 / DataNode 2 | 9864 (DataNode 1) · 9865 (DataNode 2) | Thông tin từng DataNode |
| YARN ResourceManager | 8088 (YARN ResourceManager) | Danh sách NodeManager, ứng dụng |
| NodeManager 1 / 2 | 8042 (NodeManager 1) · 8043 (NodeManager 2) | Container đang chạy trên từng node |
| Spark Master | 8080 (Spark Master) | 2 Spark Worker (chạy trong nodemanager1/2) |
| Spark History Server | 18080 (Spark History) | Lịch sử job Spark |
| MapReduce JobHistory | 19888 (MapReduce JobHistory) | Lịch sử job MapReduce |

Địa chỉ trên trình duyệt có dạng `https://<tên-codespace>-<cổng>.app.github.dev`. Bên trong terminal codespace vẫn gọi được `http://localhost:<cổng>`.

**Mở codespace và chờ cụm tự bật**

1. Vào https://github.com/codespaces, bấm tên codespace của học phần (trạng thái *Stopped* hoặc *Active*). Codespace mở VS Code trong trình duyệt.
2. Lệnh khởi động (postStartCommand) tự bật cụm lõi. Mất khoảng 1–1,5 phút; trong bản sao kiểm thử, `khoi-dong.sh` mất 72 giây. Khi xong, cửa sổ log hiện dòng `[khoi-dong] Cụm đã sẵn sàng. Gõ 'lab ui' để lấy đường dẫn Web UI.`
3. Mở terminal (menu ☰ → Terminal → New Terminal, hoặc Ctrl+`). Terminal mặc định đứng ở **thư mục gốc của repo** (`/workspaces/<tên-repo>`, tên repo của mỗi bạn khác nhau). Mọi lệnh "Terminal codespace" trong tài liệu chạy tại đây, dùng đường dẫn tương đối.

Nếu giảng viên có cập nhật repo (thông báo trên lớp), lấy bản mới trước khi làm bài:

<!-- shell: Terminal codespace -->
```bash
git pull
```

> 💡 Repo tạo từ template là repo riêng của bạn, nên `git pull` chỉ lấy được thay đổi của chính repo đó. Nếu giảng viên phát tệp mới qua Notion, làm theo hướng dẫn kèm theo.

Kiểm tra cụm:

<!-- shell: Terminal codespace -->
```bash
lab status
```

**Kết quả mong đợi:**

```text
NAME              STATUS
datanode1         Up About a minute (healthy)
datanode2         Up About a minute (healthy)
historyserver     Up About a minute (healthy)
namenode          Up About a minute (healthy)
nodemanager1      Up 57 seconds (healthy)
nodemanager2      Up 57 seconds (healthy)
resourcemanager   Up About a minute (healthy)
spark-master      Up About a minute (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 2.2 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

Nếu có container chưa `healthy`, chờ 30 giây rồi chạy lại `lab status`; nếu vẫn chưa chạy thì gõ `lab start`.

**Dữ liệu 5 triệu dòng** (chỉ làm một lần cho cả học kỳ). Codespace tạo trước 06/11/2026 chỉ có dữ liệu mẫu 200 000 dòng (codespace tạo sau ngày này đã có sẵn 5 triệu dòng). Kiểm tra số dòng:

<!-- shell: Terminal codespace -->
```bash
wc -l data/retail_transactions.csv
```

Nếu kết quả là `200001 data/retail_transactions.csv`, sinh lại bộ đầy đủ (khoảng 30 giây, thêm khoảng 500 MB trên đĩa codespace):

<!-- shell: Terminal codespace -->
```bash
lab data 5000000
wc -l data/retail_transactions.csv
ls -lh data
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
5000001 data/retail_transactions.csv
total 524M
-rw-r--r-- 1 vscode vscode  23K Nov  5 16:21 products.csv
-rw-r--r-- 1 vscode vscode 387M Nov  5 16:22 retail_transactions.csv
-rw-r--r-- 1 vscode vscode  22M Nov  5 16:22 sensor_readings.csv
-rw-r--r-- 1 vscode vscode 116M Nov  5 16:22 web_access.log
```

Dữ liệu sinh với `--seed 42` mặc định nên mọi codespace cho ra **cùng một bộ dữ liệu**; các số đếm, tổng và mã MD5 trong tài liệu sẽ khớp với kết quả của bạn. Đã có `5000001` dòng thì bỏ qua bước này.

**Dịch vụ cần bật:** không có. Buổi này chỉ dùng cụm lõi. Nếu buổi trước bạn đã bật Hive, HBase hay Kafka, tắt bằng `lab stop-extras` để giải phóng RAM.

> ⚠️ Codespace **tự dừng sau 30 phút không thao tác**. Dữ liệu HDFS không mất khi codespace dừng, nhưng bạn phải mở lại codespace và chờ cụm tự bật trước khi làm tiếp.

## PHẦN 1: MỞ CODESPACE VÀ KIỂM TRA CỤM

### Bước 1: Tìm hiểu cấu trúc repo bigdatalab

Codespace là một máy ảo Linux (người dùng `vscode`). Repo `bigdatalab` nằm tại `/workspaces/<tên-repo>`; Docker chạy bên trong codespace (Docker-in-Docker). Image `bigdata-lab:2026.10` đã được tải sẵn khi tạo codespace, nên **không phải** cài Docker, giải nén hay `docker compose build` như trên máy phòng lab.

<!-- shell: Terminal codespace -->
```bash
ls
ls .devcontainer src/kiem-tra
lab
```

**Kết quả mong đợi** (rút gọn; cách xếp cột phụ thuộc độ rộng terminal):

```text
Dockerfile		 docker-compose.extras.noports.yml  hive
HUONG-DAN-CODESPACES.md  docker-compose.extras.yml	    scripts
README.md		 docker-compose.noports.yml	    scripts-gv
TESTING.md		 docker-compose.yml		    scripts-host
conf			 downloads			    src
data			 gen_data.py
.devcontainer:
cau-hinh.env		devcontainer.json   khoi-dong.sh  lab.sh
devcontainer-lock.json	docker-san-sang.sh  khoi-tao.sh

src/kiem-tra:
WordCount.java	      kafka_streaming_test.py  pymongo_test.py
happybase_test.py     mapper.py		       reducer.py
kafka_python_test.py  mllib_test.py	       spark_hdfs_parquet.py
Các lệnh:
  lab start        bật cụm lõi (HDFS, YARN, Spark) — tự chạy khi mở codespace
  lab hive         bật Hive (Lab 3)          lab nosql   bật HBase + MongoDB (Lab 4)
  lab kafka        bật Kafka (Lab 6)         lab extras  bật tất cả dịch vụ bổ sung
  lab stop-extras  tắt dịch vụ bổ sung để giải phóng RAM
  lab stop         dừng mọi container (GIỮ dữ liệu)
  lab status       trạng thái container, HDFS, YARN, RAM
  lab ui           đường dẫn Web UI
  lab test         chạy smoke test toàn cụm
  lab data [N]     sinh lại dữ liệu mẫu N dòng (mặc định 200000)
  lab reset        xoá sạch dữ liệu cụm, làm lại từ đầu
...
```

Ý nghĩa các thành phần chính:

| Đường dẫn | Vai trò |
|---|---|
| `.devcontainer/` | Cấu hình codespace: `devcontainer.json` (máy 2 nhân/8 GB, cổng forward), `khoi-tao.sh` (chạy một lần khi tạo codespace), `khoi-dong.sh` (chạy mỗi lần mở codespace, bật cụm lõi), `lab.sh` (lệnh `lab`) |
| `docker-compose.yml` | Cụm lõi 8 container (dùng từ Buổi 1) |
| `docker-compose.extras.yml` | Hive, Kafka, HBase, MongoDB (dùng từ Buổi 3, bật bằng `lab hive`, `lab nosql`, `lab kafka`) |
| `conf/hadoop/*.xml` | Cấu hình Hadoop: `dfs.replication=2`, `dfs.blocksize=128 MB`, bộ nhớ YARN... |
| `scripts/entrypoint.sh` | Khởi động đúng vai trò (namenode, datanode, ...) cho từng container |
| `data/` | Dữ liệu dùng chung, mount thành `/data` trong container (không đưa lên GitHub, xem `.gitignore`) |
| `src/` | Mã nguồn bài lab, mount thành `/src` trong container — **bài làm của bạn đặt ở đây và được `git push`** |
| `Dockerfile`, `downloads/` | Công thức build image (giảng viên đã build sẵn, sinh viên không cần dùng) |
| `gen_data.py` | Sinh dữ liệu mẫu (lệnh `lab data` gọi script này) |

> 💡 Nhờ bind mount `./data → /data` và `./src → /src`, tệp đặt trong hai thư mục này (thấy trong khung **Explorer** bên trái VS Code) xuất hiện ngay bên trong container, **không cần `docker cp`**.

### Bước 2: Kiểm tra các container của cụm lõi

Cụm lõi gồm 8 container cùng một image, mỗi container chạy một vai trò (tham số `command` trong `docker-compose.yml`). Các container phụ thuộc nhau qua `depends_on` + `healthcheck`: DataNode, ResourceManager, Spark Master chỉ khởi động khi NameNode đã *healthy*; NodeManager chờ cả ResourceManager, Spark Master và hai DataNode. Khi mở codespace, `khoi-dong.sh` chạy `docker compose up -d --no-build --wait`; lệnh `lab start` cũng làm đúng việc này.

<!-- shell: Terminal codespace -->
```bash
docker compose ps --format 'table {{.Name}}\t{{.Service}}\t{{.Status}}'
```

**Kết quả mong đợi:**

```text
NAME              SERVICE           STATUS
datanode1         datanode1         Up 2 minutes (healthy)
datanode2         datanode2         Up 2 minutes (healthy)
historyserver     historyserver     Up 2 minutes (healthy)
namenode          namenode          Up 2 minutes (healthy)
nodemanager1      nodemanager1      Up About a minute (healthy)
nodemanager2      nodemanager2      Up About a minute (healthy)
resourcemanager   resourcemanager   Up 2 minutes (healthy)
spark-master      spark-master      Up 2 minutes (healthy)
```

> 💡 Lệnh `docker`, `docker compose` gõ trong terminal codespace điều khiển Docker **bên trong codespace**, nên các lệnh của README (`docker exec -it namenode bash`, `docker stop datanode2`, ...) dùng nguyên như trên máy phòng lab.

### Bước 3: Kiểm tra sức khoẻ HDFS, YARN, Spark từ dòng lệnh

Mở một shell bên trong container `namenode` (client HDFS có sẵn trong mọi container của image `bigdata-lab`). Gọi terminal này là **terminal A** và giữ nó mở cho đến hết buổi:

<!-- shell: Terminal codespace (terminal A) -->
```bash
docker exec -it namenode bash
```

Trong container, kiểm tra phiên bản và báo cáo HDFS:

<!-- shell: Bên trong container namenode -->
```bash
hadoop version | head -1
java -version 2>&1 | head -1
python3 --version
hdfs dfsadmin -safemode get
hdfs dfsadmin -report | grep -E 'Live datanodes|^Name:|^Hostname:'
hdfs dfs -ls /
```

**Kết quả mong đợi:**

```text
Hadoop 3.4.3
openjdk version "17.0.20.1" 2026-08-18
Python 3.12.3
Safe mode is OFF
Live datanodes (2):
Name: 172.19.0.4:9866 (datanode1.bigdata-lab_default)
Hostname: datanode1
Name: 172.19.0.7:9866 (datanode2.bigdata-lab_default)
Hostname: datanode2
Found 5 items
drwxrwxrwx   - root supergroup          0 2026-11-05 23:20 /data
drwxr-xr-x   - root supergroup          0 2026-11-05 23:20 /hbase
drwxrwxrwt   - root supergroup          0 2026-11-05 23:20 /spark-logs
drwxrwxrwt   - root supergroup          0 2026-11-05 23:20 /tmp
drwxr-xr-x   - root supergroup          0 2026-11-05 23:20 /user
```

Địa chỉ IP trên máy bạn có thể khác (và có thể đổi sau mỗi lần cụm khởi động lại). Các thư mục `/data`, `/user`, `/tmp`, `/spark-logs`, `/hbase` do entrypoint tạo sẵn. Nếu codespace đã dùng ở buổi khác, `hdfs dfs -ls /` có thể có thêm thư mục. Mở **terminal B** (nút **+** trong khung TERMINAL) để kiểm tra YARN và Spark:

<!-- shell: Terminal codespace (terminal B) -->
```bash
docker exec resourcemanager yarn node -list
docker exec spark-master curl -s localhost:8080/json/ | grep -o '"aliveworkers" : [0-9]*'
```

**Kết quả mong đợi:**

```text
WARNING: YARN_CONF_DIR has been replaced by HADOOP_CONF_DIR. Using value of YARN_CONF_DIR.
2026-11-05 23:22:20,054 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at resourcemanager/172.19.0.6:8032
Total Nodes:2
         Node-Id	     Node-State	Node-Http-Address	Number-of-Running-Containers
nodemanager1:37317	        RUNNING	nodemanager1:8042	                           0
nodemanager2:43997	        RUNNING	nodemanager2:8042	                           0
"aliveworkers" : 2
```

> 💡 Dòng `WARNING: YARN_CONF_DIR has been replaced by HADOOP_CONF_DIR` và dòng `INFO client.DefaultNoHARMFailoverProxyProvider ...` là bình thường, có thể bỏ qua.

### Bước 4: Mở các Web UI qua tab PORTS

Cổng của các Web UI đã được khai báo trong `devcontainer.json` nên codespace tự forward. Lấy đường dẫn:

<!-- shell: Terminal codespace (terminal B) -->
```bash
lab ui
```

**Kết quả mong đợi** (tên codespace của bạn khác `ten-codespace-abc123`):

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

Ctrl+click vào link (hoặc tab **PORTS** → rê chuột vào cột *Forwarded Address* → biểu tượng quả địa cầu *Open in Browser*) rồi ghi nhận:

1. Cổng 9870 → **Overview**: `Live Nodes 2`, `Dead Nodes 0`, *Configured Capacity*. Menu **Datanodes**: hai dòng `datanode1:9866`, `datanode2:9866` trạng thái *In Service*.
2. Cổng 8088 → **Nodes**: 2 node `RUNNING`, mỗi node 1792 MB bộ nhớ và 2 vCore.
3. Cổng 8080 → **Alive Workers: 2**, mỗi worker 2 core, 1024 MB (tổng 4 core, 2048 MB).
4. Cổng 18080 (Spark History) và 19888 (JobHistory) mở được, danh sách job đang trống.

Kiểm tra nhanh mã HTTP ngay trong terminal codespace (gọi `localhost`, không cần trình duyệt):

<!-- shell: Terminal codespace (terminal B) -->
```bash
for p in 9870 9864 9865 8088 8042 8043 8080 18080 19888; do
  printf "%-6s %s\n" $p $(curl -s -o /dev/null -w "%{http_code}" -L http://localhost:$p/)
done
```

**Kết quả mong đợi:**

```text
9870   200
9864   200
9865   200
8088   200
8042   200
8043   200
8080   200
18080  200
19888  200
```

> ⚠️ Liên kết bên trong Web UI dùng tên container (ví dụ `http://nodemanager1:8042/...`, `http://datanode2:9864/...`). Trình duyệt không phân giải được các tên này: dùng link tương ứng từ `lab ui` hoặc tab PORTS (nodemanager1 → cổng 8042, nodemanager2 → 8043, datanode1 → 9864, datanode2 → 9865) và giữ nguyên phần đường dẫn phía sau.

> 💡 Cổng forward mặc định ở chế độ *Private*: chỉ mở được khi trình duyệt đang đăng nhập đúng tài khoản GitHub sở hữu codespace. Nếu trình duyệt đòi đăng nhập GitHub, cứ đăng nhập rồi mở lại link.

## PHẦN 2: THAO TÁC CƠ BẢN VỚI HDFS

### Bước 5: Tạo thư mục và đưa tệp lên HDFS

`/data` trong container chính là thư mục `data/` của repo, nên có thể `put` trực tiếp. Các lệnh từ đây chạy trong **terminal A** (shell `docker exec -it namenode bash` đã mở ở Bước 3).

<!-- shell: Bên trong container namenode -->
```bash
ls -lh /data
hdfs dfs -mkdir -p /user/student/lab01
hdfs dfs -put /data/retail_transactions.csv /user/student/lab01/
hdfs dfs -ls /user/student/lab01
```

**Kết quả mong đợi:**

```text
total 524M
-rw-r--r-- 1 ubuntu ubuntu  23K Nov  5 23:21 products.csv
-rw-r--r-- 1 ubuntu ubuntu 387M Nov  5 23:22 retail_transactions.csv
-rw-r--r-- 1 ubuntu ubuntu  22M Nov  5 23:22 sensor_readings.csv
-rw-r--r-- 1 ubuntu ubuntu 116M Nov  5 23:22 web_access.log
Found 1 items
-rw-r--r--   2 root supergroup  405181403 2026-11-05 23:22 /user/student/lab01/retail_transactions.csv
```

Lệnh `put` tệp 405 MB mất khoảng 8,5 giây trên máy 2 nhân (ví dụ). Cột thứ hai (`2`) là hệ số nhân bản, lấy từ `dfs.replication=2` trong `conf/hadoop/hdfs-site.xml`. Chủ sở hữu là `root` vì lệnh chạy bằng root trong container (HDFS đã tắt kiểm tra quyền để thuận tiện học tập). Tên `ubuntu` của tệp trong `/data` chỉ là tên ứng với UID 1000 (người dùng `vscode` trong codespace). Giờ trong container theo múi giờ Việt Nam; `ls` trong terminal codespace hiển thị giờ UTC (chậm 7 tiếng).

> 💡 Trên Web UI cổng 9870 → **Utilities → Browse the file system**, mở `/user/student/lab01`, bấm vào tên tệp để xem danh sách block và DataNode chứa từng block.

### Bước 6: Xem dung lượng và nội dung tệp

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -ls -h /user/student/lab01
hdfs dfs -du -h /user/student/lab01
hdfs dfs -count -h /user/student/lab01
hdfs dfs -cat /user/student/lab01/retail_transactions.csv | head -n 5
hdfs dfs -tail /user/student/lab01/retail_transactions.csv | tail -n 3
```

**Kết quả mong đợi:**

```text
Found 1 items
-rw-r--r--   2 root supergroup    386.4 M 2026-11-05 23:22 /user/student/lab01/retail_transactions.csv
386.4 M  772.8 M  /user/student/lab01/retail_transactions.csv
           1            1            386.4 M /user/student/lab01
transaction_id,customer_id,product_id,category,quantity,unit_price,discount,store_id,region,ts
T0000000000,C0015081,P00343,me_va_be,3,13977000,0,S026,NgheAn,2025-07-14 06:46:58
T0000000001,C0001229,P00442,gia_dung,5,23031000,0.05,S037,Hanoi,2025-06-28 13:13:12
T0000000002,C0109496,P00011,gia_dung,2,2897000,0.3,S040,Hanoi,2025-04-23 10:28:38
T0000000003,C0094858,P00055,thoi_trang,4,10929000,0.1,S026,Hanoi,2025-02-04 05:25:48
cat: Unable to write to output stream.
T0004999997,C0003418,P00102,dien_tu,2,5336000,0.2,S005,Hanoi,2025-08-16 20:57:33
T0004999998,C0025816,P00137,thuc_pham,2,3770000,0.3,S001,HCM,2025-05-17 13:42:50
T0004999999,C0008469,P00467,thoi_trang,1,24288000,0.2,S022,HCM,2025-07-07 20:41:49
```

- `-du -h`: cột 1 là kích thước logic (386,4 MB), cột 2 là dung lượng thực chiếm trên cụm = kích thước × hệ số nhân bản (772,8 MB).
- `-count`: số thư mục, số tệp, tổng dung lượng.
- `-tail` in 1 KB cuối tệp (ở trên chỉ giữ 3 dòng cuối).

> 💡 Dòng `cat: Unable to write to output stream.` xuất hiện vì `head` đóng ống dẫn sau 5 dòng trong khi `hdfs dfs -cat` vẫn đang đọc — hoàn toàn bình thường.

### Bước 7: Lấy tệp về và kiểm tra toàn vẹn (md5sum)

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -get /user/student/lab01/retail_transactions.csv /tmp/retail_check.csv
md5sum /data/retail_transactions.csv /tmp/retail_check.csv
rm /tmp/retail_check.csv
```

**Kết quả mong đợi:**

```text
68c94673ab12df0ee80971c2b2a2c1ba  /data/retail_transactions.csv
68c94673ab12df0ee80971c2b2a2c1ba  /tmp/retail_check.csv
```

Hai mã MD5 giống nhau chứng tỏ dữ liệu không bị thay đổi khi đi qua HDFS (chia block, nhân bản rồi ghép lại). Với bộ dữ liệu sinh bằng seed mặc định, mã MD5 của bạn cũng phải đúng bằng giá trị trên.

### Bước 8: Tải một tệp từ HDFS về máy cá nhân

Trên máy phòng lab, tệp `get` từ HDFS nằm ngay trên ổ cứng. Trên Codespaces, tệp nằm trong máy ảo trên GitHub; muốn có bản trên máy của mình thì phải tải qua trình duyệt. Tệp 387 MB tải rất lâu, nên ta tạo một tệp mẫu 1 000 dòng trên HDFS, `get` nó vào `/data` (tức thư mục `data/` của repo) rồi tải về:

<!-- shell: Bên trong container namenode -->
```bash
head -n 1001 /data/retail_transactions.csv > /tmp/mau_1000_dong.csv
hdfs dfs -put -f /tmp/mau_1000_dong.csv /tmp/mau_1000_dong.csv
hdfs dfs -get -f /tmp/mau_1000_dong.csv /data/mau_1000_dong.csv
md5sum /tmp/mau_1000_dong.csv /data/mau_1000_dong.csv
hdfs dfs -rm -skipTrash /tmp/mau_1000_dong.csv
rm /tmp/mau_1000_dong.csv
```

**Kết quả mong đợi:**

```text
25017579f3eaa8ac2478540ed84ae6c3  /tmp/mau_1000_dong.csv
25017579f3eaa8ac2478540ed84ae6c3  /data/mau_1000_dong.csv
Deleted /tmp/mau_1000_dong.csv
```

Tải về máy: trong khung **Explorer** bên trái VS Code, mở thư mục `data`, **chuột phải** vào `mau_1000_dong.csv` → **Download...** → chọn nơi lưu trên máy cá nhân. Kiểm tra MD5 trên máy của bạn (chọn lệnh theo hệ điều hành, đứng ở thư mục chứa tệp vừa tải):

<!-- shell: Terminal máy cá nhân (PowerShell / Terminal macOS / Linux) -->
```bash
Get-FileHash .\mau_1000_dong.csv -Algorithm MD5   # Windows PowerShell
md5 mau_1000_dong.csv                              # macOS
md5sum mau_1000_dong.csv                           # Linux
```

Mã nhận được phải là `25017579f3eaa8ac2478540ed84ae6c3` (PowerShell in chữ hoa: `25017579F3EAA8AC2478540ED84AE6C3`). Xong thì xoá bản trong codespace:

<!-- shell: Terminal codespace (terminal B) -->
```bash
ls -l data/mau_1000_dong.csv
rm -f data/mau_1000_dong.csv
```

**Kết quả mong đợi:**

```text
-rw-r--r-- 1 root root 81274 Nov  5 16:23 data/mau_1000_dong.csv
```

> 💡 Tệp do container ghi vào `data/` thuộc `root`, nhưng vẫn tải về và xoá được từ terminal codespace (thư mục `data/` thuộc người dùng `vscode`). Thư mục `data/` nằm trong `.gitignore` nên tệp này không bị `git push`. Chiều ngược lại (máy cá nhân → codespace): kéo thả tệp vào khung Explorer, hoặc chuột phải vào thư mục → **Upload...**.

## PHẦN 3: HỆ SỐ NHÂN BẢN VÀ PHÂN BỐ BLOCK

### Bước 9: Xem phân bố block bằng hdfs fsck

HDFS chia tệp thành các block cố định (mặc định 128 MB = 134217728 byte) và đặt mỗi bản sao của một block trên một DataNode khác nhau.

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -stat "replication=%r blocksize=%o size=%b" /user/student/lab01/retail_transactions.csv
hdfs fsck /user/student/lab01/retail_transactions.csv -files -blocks -locations
```

**Kết quả mong đợi** (rút gọn):

```text
replication=2 blocksize=134217728 size=405181403
Connecting to namenode via http://namenode:9870/fsck?ugi=root&files=1&blocks=1&locations=1&path=%2Fuser%2Fstudent%2Flab01%2Fretail_transactions.csv
FSCK started by root (auth:SIMPLE) from /172.19.0.2 for path /user/student/lab01/retail_transactions.csv at Thu Nov 05 23:23:21 ICT 2026

/user/student/lab01/retail_transactions.csv 405181403 bytes, replicated: replication=2, 4 block(s):  OK
0. BP-1474839998-172.19.0.2-1793895595495:blk_1073741825_1001 len=134217728 Live_repl=2  [DatanodeInfoWithStorage[172.19.0.4:9866,DS-126f...,DISK], DatanodeInfoWithStorage[172.19.0.7:9866,DS-fd37...,DISK]]
1. BP-1474839998-172.19.0.2-1793895595495:blk_1073741826_1002 len=134217728 Live_repl=2  [...172.19.0.4:9866..., ...172.19.0.7:9866...]
2. BP-1474839998-172.19.0.2-1793895595495:blk_1073741827_1003 len=134217728 Live_repl=2  [...172.19.0.7:9866..., ...172.19.0.4:9866...]
3. BP-1474839998-172.19.0.2-1793895595495:blk_1073741828_1004 len=2528219 Live_repl=2  [...172.19.0.7:9866..., ...172.19.0.4:9866...]

Status: HEALTHY
 Number of data-nodes:	2
...
 Total blocks (validated):	4 (avg. block size 101295350 B)
 Minimally replicated blocks:	4 (100.0 %)
 Over-replicated blocks:	0 (0.0 %)
 Under-replicated blocks:	0 (0.0 %)
 Mis-replicated blocks:		0 (0.0 %)
 Default replication factor:	2
 Average block replication:	2.0
 Missing blocks:		0
...
The filesystem under path '/user/student/lab01/retail_transactions.csv' is HEALTHY
```

Đối chiếu: 405 181 403 / 134 217 728 ≈ 3,02 → 3 block đầy 128 MB và 1 block cuối chỉ 2 528 219 byte (block cuối không chiếm trọn 128 MB trên đĩa). Mỗi block có `Live_repl=2`, hai bản sao nằm trên hai địa chỉ IP khác nhau. Dùng kết quả `hdfs dfsadmin -report` ở Bước 3 để biết IP nào là `datanode1`, IP nào là `datanode2`.

### Bước 10: Tăng hệ số nhân bản lên 3 bằng setrep

Cụm chỉ có 2 DataNode, vậy yêu cầu 3 bản sao sẽ thế nào?

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -setrep 3 /user/student/lab01/retail_transactions.csv
hdfs dfs -ls /user/student/lab01/retail_transactions.csv
hdfs fsck /user/student/lab01/retail_transactions.csv -files -blocks -locations | grep -E "Target Replicas|Live_repl|Status|Under-replicated|Missing replicas"
hdfs dfsadmin -report | grep -i "under replicated"
```

**Kết quả mong đợi** (rút gọn):

```text
Replication 3 set: /user/student/lab01/retail_transactions.csv
-rw-r--r--   3 root supergroup  405181403 2026-11-05 23:22 /user/student/lab01/retail_transactions.csv
/user/student/lab01/retail_transactions.csv 405181403 bytes, replicated: replication=3, 4 block(s):  Under replicated BP-1474839998-172.19.0.2-1793895595495:blk_1073741825_1001. Target Replicas is 3 but found 2 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
...
0. BP-...:blk_1073741825_1001 len=134217728 Live_repl=2  [...]
...
Status: HEALTHY
 Under-replicated blocks:	4 (100.0 %)
 Missing replicas:		4 (33.333332 %)
	Under replicated blocks: 4
```

HDFS **chấp nhận** hệ số 3 như một mục tiêu (cột replication trong `ls` đổi thành 3) nhưng thực tế chỉ có 2 bản sao vì không có DataNode thứ ba để đặt. Trạng thái tổng vẫn `HEALTHY` vì dữ liệu chưa mất (mỗi block còn ít nhất 1 bản sao), chỉ bị đánh dấu *under-replicated*: thiếu 4 bản sao trên tổng 12 bản sao mong muốn = 33,3 %.

> ⚠️ **Không** dùng `hdfs dfs -setrep -w 3 ...` trên cụm 2 DataNode: tuỳ chọn `-w` chờ đến khi đủ 3 bản sao nên lệnh in `Waiting for ... .......` **mãi mãi** (đã thử: sau 30 giây vẫn chờ). Nếu lỡ chạy, bấm Ctrl+C (hệ số 3 vẫn đã được ghi nhận).

### Bước 11: Đưa hệ số nhân bản về 2

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -setrep -w 2 /user/student/lab01/retail_transactions.csv
hdfs dfsadmin -report | grep -i "under replicated"
```

**Kết quả mong đợi:**

```text
Replication 2 set: /user/student/lab01/retail_transactions.csv
Waiting for /user/student/lab01/retail_transactions.csv ... done
	Under replicated blocks: 0
```

## PHẦN 4: MÔ PHỎNG SỰ CỐ DATANODE

### Bước 12: Dừng datanode2 và theo dõi NameNode phát hiện node chết

DataNode gửi heartbeat cho NameNode mỗi 3 giây. Bộ môi trường đặt `dfs.namenode.heartbeat.recheck-interval=15000` nên NameNode đánh dấu một DataNode là *Dead* sau 2 × 15 s + 10 × 3 s = **60 giây** không nhận heartbeat (mặc định của Hadoop là 10 phút 30 giây).

Trong **terminal B** (terminal codespace, không phải shell namenode):

<!-- shell: Terminal codespace (terminal B) -->
```bash
docker stop datanode2
```

**Kết quả mong đợi:**

```text
datanode2
```

Khoảng 20 giây sau khi dừng, chạy báo cáo trong **terminal A**:

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes|Last contact"
```

**Kết quả mong đợi:** NameNode vẫn coi cả hai node là sống, nhưng thời điểm `Last contact` của một node đã dừng cập nhật:

```text
Live datanodes (2):
Last contact: Thu Nov 05 23:24:26 ICT 2026
Last contact: Thu Nov 05 23:24:05 ICT 2026
```

Đợi thêm khoảng 1 phút rồi chạy lại:

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes|^Name|^Hostname|Last contact"
```

**Kết quả mong đợi** (khoảng 66 giây sau khi dừng):

```text
Live datanodes (1):
Name: 172.19.0.4:9866 (datanode1.bigdata-lab_default)
Hostname: datanode1
Last contact: Thu Nov 05 23:25:11 ICT 2026
Dead datanodes (1):
Name: 172.19.0.7:9866 (172.19.0.7)
Hostname: datanode2
Last contact: Thu Nov 05 23:24:05 ICT 2026
```

Trên Web UI cổng 9870 (tải lại trang) → **Datanodes**, `datanode2` chuyển sang mục *Dead*; trang Overview hiển thị `Dead Nodes 1`. Lệnh `lab status` ở terminal B cũng báo `HDFS: Live datanodes (1):`.

> 💡 Trang của DataNode 2 (cổng 9865) không mở được trong lúc `datanode2` dừng (trong terminal codespace, `curl localhost:9865` trả mã `000`); tab PORTS vẫn liệt kê cổng này — đó là bình thường.

### Bước 13: Kiểm tra trạng thái block và khả năng đọc dữ liệu

<!-- shell: Bên trong container namenode -->
```bash
hdfs fsck /user/student/lab01/retail_transactions.csv -files -blocks -locations | grep -E "Live_repl|Status|Number of data-nodes|Under-replicated|Missing replicas|Average block replication"
hdfs dfsadmin -report | grep -i "under replicated"
hdfs dfs -cat /user/student/lab01/retail_transactions.csv | wc -l
```

**Kết quả mong đợi:**

```text
0. BP-1474839998-172.19.0.2-1793895595495:blk_1073741825_1001 len=134217728 Live_repl=1  [DatanodeInfoWithStorage[172.19.0.4:9866,...,DISK]]
1. BP-1474839998-172.19.0.2-1793895595495:blk_1073741826_1002 len=134217728 Live_repl=1  [DatanodeInfoWithStorage[172.19.0.4:9866,...,DISK]]
2. BP-1474839998-172.19.0.2-1793895595495:blk_1073741827_1003 len=134217728 Live_repl=1  [DatanodeInfoWithStorage[172.19.0.4:9866,...,DISK]]
3. BP-1474839998-172.19.0.2-1793895595495:blk_1073741828_1004 len=2528219 Live_repl=1  [DatanodeInfoWithStorage[172.19.0.4:9866,...,DISK]]
Status: HEALTHY
 Number of data-nodes:	1
 Under-replicated blocks:	4 (100.0 %)
 Average block replication:	1.0
 Missing replicas:		4 (50.0 %)
	Under replicated blocks: 4
5000001
```

Mỗi block chỉ còn 1 bản sao (trên `datanode1`) nhưng tệp **vẫn đọc được đầy đủ** (5 000 001 dòng = 5 triệu giao dịch + 1 dòng header). NameNode muốn tạo lại bản sao thứ hai nhưng không còn DataNode sống nào khác để đặt, nên các block giữ trạng thái *under-replicated*. Với cụm từ 3 DataNode trở lên, NameNode sẽ tự sao chép block sang node còn sống (Bài tập mở rộng 1: trên codespace, bản sao đủ lại khoảng 12 giây sau khi node bị đánh dấu Dead).

### Bước 14: Khởi động lại datanode2 và quan sát phục hồi

<!-- shell: Terminal codespace (terminal B) -->
```bash
docker start datanode2
```

Chờ khoảng 30 giây rồi kiểm tra trong terminal A:

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"
hdfs dfsadmin -report | grep -i "under replicated"
hdfs fsck /user/student/lab01/retail_transactions.csv -files -blocks -locations | grep -E "Live_repl|Status|Under-replicated|Average block replication"
```

**Kết quả mong đợi:**

```text
Live datanodes (2):
	Under replicated blocks: 0
0. BP-1474839998-172.19.0.2-1793895595495:blk_1073741825_1001 len=134217728 Live_repl=2  [...172.19.0.4:9866..., ...172.19.0.7:9866...]
1. BP-1474839998-172.19.0.2-1793895595495:blk_1073741826_1002 len=134217728 Live_repl=2  [...]
2. BP-1474839998-172.19.0.2-1793895595495:blk_1073741827_1003 len=134217728 Live_repl=2  [...]
3. BP-1474839998-172.19.0.2-1793895595495:blk_1073741828_1004 len=2528219 Live_repl=2  [...]
Status: HEALTHY
 Under-replicated blocks:	0 (0.0 %)
 Average block replication:	2.0
```

Khi khởi động lại, DataNode gửi *block report* (danh sách block còn trên đĩa của nó) cho NameNode, nên các bản sao cũ được công nhận lại ngay mà không phải sao chép lại dữ liệu.

## PHẦN 5: ẢNH HƯỞNG CỦA KÍCH THƯỚC BLOCK

### Bước 15: Tạo tệp thử nghiệm 1 GB

Ghép tệp giao dịch nhiều lần rồi cắt đúng 1 GiB (1073741824 byte). Tệp đặt trong `/tmp` của container `namenode` (không ghi vào `data/` của repo):

<!-- shell: Bên trong container namenode -->
```bash
cat /data/retail_transactions.csv /data/retail_transactions.csv /data/retail_transactions.csv \
  | head -c 1073741824 > /tmp/test_1gb.dat
ls -lh /tmp/test_1gb.dat
hdfs dfs -mkdir -p /user/student/lab01/bs64 /user/student/lab01/bs128
```

**Kết quả mong đợi:**

```text
-rw-rw-rw- 1 root root 1.0G Nov  5 23:26 /tmp/test_1gb.dat
```

> ⚠️ Phần 5 dùng thêm khoảng 5 GB đĩa codespace (tệp 1 GB trong container + 2 bản × 2 GB trên HDFS). Đo trong bản sao kiểm thử: `docker system df` báo tổng khoảng 11,2 GB lúc cao nhất, so với 5,8 GB khi chưa làm Phần 5. Nhớ dọn ở Bước 18 (quota lưu trữ tài khoản GitHub Free chỉ 15 GB/tháng).

### Bước 16: Đo thời gian put với block 64 MB và 128 MB

Tuỳ chọn `-D dfs.blocksize=...` chỉ áp dụng cho lệnh đó, không đổi cấu hình cụm. Chạy mỗi cấu hình **3 lần**, xoá tệp đích trước mỗi lần:

<!-- shell: Bên trong container namenode -->
```bash
for i in 1 2 3; do
  hdfs dfs -rm -f -skipTrash /user/student/lab01/bs64/test_1gb.dat > /dev/null
  echo "== 64 MB, lan $i"
  time hdfs dfs -D dfs.blocksize=67108864 -put /tmp/test_1gb.dat /user/student/lab01/bs64/test_1gb.dat
  hdfs dfs -rm -f -skipTrash /user/student/lab01/bs128/test_1gb.dat > /dev/null
  echo "== 128 MB, lan $i"
  time hdfs dfs -D dfs.blocksize=134217728 -put /tmp/test_1gb.dat /user/student/lab01/bs128/test_1gb.dat
done
```

**Kết quả mong đợi:** mỗi lần in ba dòng `real`, `user`, `sys`, ví dụ:

```text
== 64 MB, lan 1

real	0m7.214s
user	0m4.266s
sys	0m1.592s
== 128 MB, lan 1

real	0m5.481s
user	0m3.326s
sys	0m0.977s
...
```

Số đo `real` trên bản sao codespace 2 nhân / 8 GB (máy của bạn sẽ khác; cả vòng lặp mất khoảng 45 giây):

| Lần | 64 MB (giây) | 128 MB (giây) |
|---|---|---|
| 1 | 7,21 | 5,48 |
| 2 | 4,67 | 5,34 |
| 3 | 5,38 | 7,35 |
| Trung bình | 5,76 | 6,06 |

Trên máy phòng lab (28 nhân, SSD NVMe) cùng phép đo cho trung bình 3,76 s (64 MB) và 3,02 s (128 MB); lần đo ở trên lại cho 64 MB nhanh hơn một chút. Trong codespace, mọi DataNode dùng chung 2 nhân CPU và một ổ đĩa ảo nên số đo chậm hơn và dao động theo tải; lần chạy đầu thường chậm hơn (bộ nhớ đệm đĩa còn "lạnh"). Dao động giữa các lần đo lớn hơn chênh lệch giữa hai mức block, nên chỉ nên kết luận về **xu hướng** và **độ nhiễu** của phép đo, không kết luận về số tuyệt đối.

### Bước 17: Kiểm tra số block

<!-- shell: Bên trong container namenode -->
```bash
for d in bs64 bs128; do
  hdfs fsck /user/student/lab01/$d/test_1gb.dat -blocks 2>/dev/null | grep "Total blocks"
  hdfs dfs -stat "%n blocksize=%o" /user/student/lab01/$d/test_1gb.dat
done
hdfs dfs -du -h /user/student/lab01
```

**Kết quả mong đợi:**

```text
 Total blocks (validated):	16 (avg. block size 67108864 B)
test_1gb.dat blocksize=67108864
 Total blocks (validated):	8 (avg. block size 134217728 B)
test_1gb.dat blocksize=134217728
1 G      2 G      /user/student/lab01/bs128
1 G      2 G      /user/student/lab01/bs64
386.4 M  772.8 M  /user/student/lab01/retail_transactions.csv
```

1 GiB / 64 MiB = 16 block; 1 GiB / 128 MiB = 8 block. Mỗi block là một đối tượng metadata trong bộ nhớ NameNode và cần một pipeline ghi riêng; block nhỏ làm tăng số đối tượng NameNode phải quản lý và số lần thiết lập pipeline, nhưng cho phép nhiều map task song song hơn khi xử lý (Buổi 2).

## PHẦN 6: DỌN DẸP, DỪNG CỤM VÀ TÍNH BỀN VỮNG CỦA DỮ LIỆU

### Bước 18: Dọn dữ liệu thử nghiệm

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -rm -r -skipTrash /user/student/lab01/bs64 /user/student/lab01/bs128
rm -f /tmp/test_1gb.dat
hdfs dfsadmin -report | head -n 8
```

**Kết quả mong đợi:**

```text
Deleted /user/student/lab01/bs64
Deleted /user/student/lab01/bs128
Configured Capacity: 2884896522240 (2.62 TB)
Present Capacity: 1628139697052 (1.48 TB)
DFS Remaining: 1621912301568 (1.48 TB)
DFS Used: 6227395484 (5.80 GB)
DFS Used%: 0.38%
Replicated Blocks:
	Under replicated blocks: 0
	Blocks with corrupt replicas: 0
```

*Capacity* phụ thuộc ổ đĩa của máy chạy codespace nên sẽ khác (trên Codespaces thật xấp xỉ dung lượng đĩa 32 GB của codespace). *DFS Used* chưa giảm ngay vì DataNode xoá block bất đồng bộ. **Chờ khoảng 10 giây** rồi kiểm tra lại, trước khi chuyển sang Bước 19:

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfsadmin -report | grep -m1 "DFS Used:"
exit
```

**Kết quả mong đợi:**

```text
DFS Used: 816742974 (778.91 MB)
```

Còn khoảng 779 MB = tệp 386,4 MB × 2 bản sao. Lệnh `exit` thoát shell namenode, terminal A trở về terminal codespace.

> ⚠️ Nếu dừng cụm (`lab stop`, dừng codespace) **ngay** sau khi xoá, DataNode chưa kịp xoá block. Sau khi khởi động lại, các block "mồ côi" vẫn nằm trên đĩa (trong kiểm thử: mỗi DataNode giữ 2,4 GB thay vì 390 MB) cho tới lần block report định kỳ 6 giờ sau. Cách thu hồi ngay: `hdfs dfsadmin -triggerBlockReport datanode1:9867` và `hdfs dfsadmin -triggerBlockReport datanode2:9867` trong container namenode, chờ 15 giây.

### Bước 19: Dừng cụm và kiểm tra tính bền vững của volume

Trên máy phòng lab, bước này dùng `docker compose down` rồi `up`. Trong codespace dùng `lab stop` (dừng mọi container, **giữ** dữ liệu) và `lab start`:

<!-- shell: Terminal codespace -->
```bash
lab stop
docker volume ls | grep bigdata-lab
lab start
docker exec namenode hdfs dfs -ls /user/student/lab01
```

**Kết quả mong đợi** (`lab start` mất khoảng 1 phút):

```text
...
 Container datanode1  Stopped
 Container namenode  Stopping
 Container namenode  Stopped
local     bigdata-lab_datanode1
local     bigdata-lab_datanode2
local     bigdata-lab_namenode
...
 Container namenode  Healthy
 Container nodemanager2  Healthy
 Container nodemanager1  Healthy
 Container historyserver  Healthy
Found 1 items
-rw-r--r--   2 root supergroup  405181403 2026-11-05 23:22 /user/student/lab01/retail_transactions.csv
```

`lab stop` chỉ dừng container. Ba volume chứa metadata NameNode và block của DataNode vẫn còn, nên sau `lab start` dữ liệu HDFS vẫn còn. Lệnh `docker compose down` (xoá cả container và mạng) cũng **giữ** volume: thử `docker compose down` rồi `lab start`, kết quả `hdfs dfs -ls` vẫn như trên (đã kiểm chứng). Chỉ `lab reset` (tương đương `docker compose down -v`) mới xoá volume và mất toàn bộ dữ liệu HDFS.

> 💡 Sau khi cụm khởi động lại, địa chỉ IP của các DataNode có thể đổi và block có thể được đặt lại khác trước — điều này ảnh hưởng tới Bài tập mở rộng 1.

### Bước 20: Kiểm chứng dữ liệu còn sau khi dừng và mở lại codespace

Volume Docker nằm trên đĩa của codespace. Dừng codespace chỉ tắt máy ảo, không xoá đĩa. Thử như sau:

1. Dừng codespace: github.com/codespaces → `...` cạnh tên codespace → **Stop codespace** (hoặc trong VS Code: Command Palette Ctrl+Shift+P → *Codespaces: Stop Current Codespace*).
2. Chờ trạng thái chuyển thành *Stopped*, bấm lại vào tên codespace để mở.
3. Lệnh khởi động tự chạy `khoi-dong.sh`; chờ dòng `[khoi-dong] Cụm đã sẵn sàng...` (kiểm thử: 72 giây kể từ khi codespace chạy lại).

Kiểm tra:

<!-- shell: Terminal codespace -->
```bash
lab status
docker exec namenode hdfs dfs -ls /user/student/lab01
docker exec namenode bash -c 'hdfs dfs -cat /user/student/lab01/retail_transactions.csv | md5sum'
wc -l data/retail_transactions.csv
```

**Kết quả mong đợi:**

```text
NAME              STATUS
datanode1         Up About a minute (healthy)
...
spark-master      Up About a minute (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 2.1 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
Found 1 items
-rw-r--r--   2 root supergroup  405181403 2026-11-05 23:22 /user/student/lab01/retail_transactions.csv
68c94673ab12df0ee80971c2b2a2c1ba  -
5000001 data/retail_transactions.csv
```

Tệp HDFS, mã MD5 và dữ liệu trong `data/` đều còn nguyên. Codespace chỉ bị **xoá** (mất cả dữ liệu HDFS) khi bạn tự xoá hoặc khi không mở lại trong thời gian lưu giữ (mặc định 30 ngày) — vì vậy bài làm trong `src/` phải luôn được `git push`.

> 💡 Giữ lại tệp `/user/student/lab01/retail_transactions.csv` và dữ liệu 5 triệu dòng trong `data/` cho các buổi sau.

## Kết thúc buổi

1. Nếu đã làm Bài tập mở rộng 1, gỡ `datanode3` như hướng dẫn trong bài (lệnh `lab stop` **không** dừng container này). Buổi này không bật dịch vụ bổ sung; nếu có bật thì tắt bằng `lab stop-extras`.
2. Lưu bài làm lên GitHub (tệp trong `src/lab01` và tệp compose của bài mở rộng, nếu có):

<!-- shell: Terminal codespace -->
```bash
git status --short
git add src/lab01 docker-compose.datanode3.yml
git commit -m "Buoi 1: HDFS, datanode3, bench blocksize"
git push
```

**Kết quả mong đợi** (dòng `To ...` hiện địa chỉ repo của bạn):

```text
?? docker-compose.datanode3.yml
?? src/lab01/
[main 95029f0] Buoi 1: HDFS, datanode3, bench blocksize
 3 files changed, 64 insertions(+)
 create mode 100644 docker-compose.datanode3.yml
 create mode 100755 src/lab01/bench_blocksize.sh
 create mode 100644 src/lab01/ket_qua_blocksize.csv
...
   ecbe3d9..95029f0  main -> main
```

Nếu không làm bài mở rộng, chỉ `git add` những gì có trong `src/lab01` (bỏ `docker-compose.datanode3.yml` khỏi lệnh).

3. **Dừng codespace** để không tốn quota: github.com/codespaces → `...` → **Stop codespace**. Đóng tab trình duyệt **không** dừng codespace ngay; codespace chỉ tự dừng sau 30 phút không thao tác và vẫn bị tính core-hour trong thời gian đó.

## Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `lab status` có container chưa `healthy` hoặc `HDFS: chưa chạy` ngay sau khi mở codespace | Cụm đang khởi động (khoảng 1–1,5 phút), hoặc `khoi-dong.sh` hết thời gian chờ | Chờ 30 giây rồi `lab status` lại; vẫn lỗi thì `lab start` |
| `wc -l data/retail_transactions.csv` chỉ ra `200001` | Codespace tạo trước 06/11/2026 chỉ sinh 200 000 dòng | `lab data 5000000` (khoảng 30 giây) |
| `python3: command not found` trong terminal codespace | Python chỉ có bên trong container, không cài trên codespace | Chạy Python trong container (`docker exec -it namenode bash`); `lab data` đã tự dùng container |
| Link `https://...app.github.dev` báo lỗi hoặc không tải được | Container của cổng đó đang dừng (ví dụ 9865 khi `datanode2` dừng) hoặc cụm chưa bật | `lab status`; bật lại container (`docker start datanode2`, `lab start`) rồi tải lại trang |
| Cổng không có trong tab PORTS | Cổng không khai báo sẵn (ví dụ cổng của `datanode3`) | Tab PORTS → **Add Port** → gõ số cổng; hoặc dùng `curl localhost:<cổng>` trong terminal |
| Trình duyệt đòi đăng nhập GitHub khi mở cổng | Cổng forward ở chế độ Private | Đăng nhập đúng tài khoản sở hữu codespace |
| Trình duyệt không mở được liên kết `http://nodemanager1:8042/...` trong Web UI | Tên container không phân giải được bên ngoài cụm | Dùng link từ `lab ui` / tab PORTS (nodemanager2 → 8043, datanode2 → 9865), giữ nguyên phần đường dẫn |
| Codespace tự dừng giữa buổi, terminal báo mất kết nối | Không thao tác quá 30 phút | Mở lại codespace, chờ cụm tự bật; dữ liệu HDFS còn nguyên. Nếu đang ở Phần 4, chạy lại `docker start datanode2` |
| Container bị `OOMKilled`, lệnh chậm bất thường | Còn bật dịch vụ bổ sung của buổi trước (Hive, HBase, Kafka) trên máy 8 GB | `lab stop-extras`; xem mức dùng bằng `lab status` hoặc `docker stats --no-stream` |
| `DFS Used` vẫn lớn sau khi xoá tệp và khởi động lại cụm | Dừng cụm trước khi DataNode kịp xoá block | `hdfs dfsadmin -triggerBlockReport datanode1:9867` (và `datanode2:9867`), chờ 15 giây |
| Đĩa codespace gần đầy, báo `no space left on device` | Tệp 1 GB và các bản thử block size chưa dọn | Làm Bước 18; kiểm tra bằng `docker system df` và `df -h` |
| `lab status` không thấy `datanode3` dù nó đang chạy; `lab stop` không dừng nó | `datanode3` khai báo trong tệp compose riêng, `lab` không biết tệp này | Gỡ bằng lệnh `docker compose -f docker-compose.yml -f docker-compose.datanode3.yml rm -sf datanode3` |
| `SafeModeException` / `Name node is in safe mode` khi `put` | NameNode vừa khởi động, chưa nhận đủ block report | Chờ 30–60 giây; kiểm tra `hdfs dfsadmin -safemode get`; nếu cần `hdfs dfsadmin -safemode leave` |
| `hdfs dfs -setrep -w 3 ...` in `Waiting for ...` không dừng | Cụm chỉ có 2 DataNode, không thể đạt 3 bản sao | Ctrl+C; dùng `setrep` không có `-w`, hoặc đặt lại `setrep -w 2` |
| `cat: Unable to write to output stream.` | `head` đóng ống dẫn sớm | Bình thường, bỏ qua |
| `grep -o '"aliveworkers":[0-9]*'` không in gì | JSON của Spark Master có khoảng trắng quanh dấu `:` | Dùng `grep -o '"aliveworkers" : [0-9]*'` |
| DataNode không lên, log có `Incompatible clusterIDs` | Volume NameNode bị xoá nhưng còn volume DataNode cũ | `lab reset` (gõ `xoa` để xác nhận) rồi `lab start` — mất dữ liệu HDFS, `put` lại tệp |
| `git push` báo `Permission denied` hoặc `403` | Codespace mở từ repo của giảng viên chứ không phải repo của bạn | Tạo repo riêng bằng *Use this template* (hoặc nhận repo qua GitHub Classroom), tạo codespace từ repo đó |

## Câu hỏi kiểm tra

1. Vì sao khi cụm chỉ có 2 DataNode mà đặt hệ số nhân bản 3, HDFS không báo lỗi mà chỉ đánh dấu block là *under-replicated*, và `fsck` vẫn báo `Status: HEALTHY`?
   *Gợi ý: hệ số nhân bản là mục tiêu (target); NameNode ghi được bao nhiêu bản sao thì ghi, đánh dấu phần thiếu để bổ sung khi có node mới; HEALTHY nghĩa là không có block bị mất hay hỏng.*
2. Trong Bước 12, vì sao sau khoảng 20 giây `datanode2` vẫn nằm trong *Live datanodes*, còn sau khoảng 60 giây mới chuyển sang *Dead*? Thời gian này phụ thuộc những tham số cấu hình nào?
   *Gợi ý: `dfs.heartbeat.interval` (3 s) và `dfs.namenode.heartbeat.recheck-interval` (môi trường đặt 15 s); ngưỡng = 2 × recheck + 10 × heartbeat; mục đích là tránh kết luận sai khi mạng chập chờn.*
3. Tệp `retail_transactions.csv` dài 405 181 403 byte có bao nhiêu block với block size 128 MB, và block cuối dài bao nhiêu byte? Tệp 1 GiB với block 64 MB có bao nhiêu block?
   *Gợi ý: số block = làm tròn lên (kích thước / block size); block cuối = phần dư.*
4. `hdfs dfs -ls` và `hdfs dfs -du` khác nhau thế nào về ý nghĩa dung lượng hiển thị? Vì sao `du` báo 772,8 MB cho tệp 386,4 MB?
   *Gợi ý: kích thước logic so với dung lượng vật lý có tính hệ số nhân bản.*
5. Dựa vào Bước 13 và Bài tập mở rộng 1, một cụm sản xuất muốn vẫn có đủ 3 bản sao ngay sau khi mất một DataNode thì cần tối thiểu bao nhiêu DataNode? Vì sao?
   *Gợi ý: cần ít nhất hệ số nhân bản + 1 node để có nơi tái tạo bản sao.*
6. Ở Bước 19 và Bước 20, dữ liệu HDFS nằm ở đâu mà vẫn còn sau `lab stop`, `docker compose down` và sau khi dừng/mở lại codespace? Thao tác nào sẽ làm mất dữ liệu này?
   *Gợi ý: Docker volume nằm trên đĩa của codespace; dừng container hay dừng máy ảo không xoá đĩa; `lab reset` (`down -v`) xoá volume, xoá codespace xoá cả đĩa.*

## Bài tập mở rộng

1. **Thêm DataNode thứ ba và quan sát tái tạo bản sao thật.** Repo không có sẵn tệp này: tạo tệp `docker-compose.datanode3.yml` ở **thư mục gốc của repo** (cạnh `docker-compose.yml`) — trong Explorer: chuột phải vào vùng trống → **New File...**, dán nội dung dưới đây (hoặc giải nén `buoi1_codespaces_src.zip` do giảng viên đính kèm vào thư mục gốc repo). Node thứ ba giới hạn 512 MB RAM; trên máy 2 nhân / 8 GB, cụm lõi cộng `datanode3` dùng khoảng 2,3 GB (`lab status`), vẫn còn dư.

<!-- file: docker-compose.datanode3.yml -->
```yaml
# Bài tập mở rộng Buổi 1: thêm DataNode thứ ba (không mở cổng ra host)
services:
  datanode3:
    image: bigdata-lab:2026.10
    pull_policy: never
    restart: unless-stopped
    container_name: ${CT_PREFIX:-}datanode3
    hostname: datanode3
    command: ["datanode"]
    environment:
      TZ: Asia/Ho_Chi_Minh
      HADOOP_HEAPSIZE_MAX: 256m
    volumes: ["datanode3:/hadoop/dfs/data"]
    mem_limit: 512m
    depends_on:
      namenode: {condition: service_healthy}
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:9864/ >/dev/null"]
      interval: 10s
      timeout: 5s
      retries: 30
      start_period: 15s

volumes:
  datanode3:
```

   Khởi động thêm node, lặp lại Bước 10 với `setrep -w 3` (lần này lệnh kết thúc), đưa về `setrep -w 2`, rồi xem block đang nằm trên những node nào:

<!-- shell: Terminal codespace -->
```bash
docker compose -f docker-compose.yml -f docker-compose.datanode3.yml up -d --no-build --wait datanode3
docker exec namenode hdfs dfsadmin -report | grep -E "Live datanodes|^Hostname"
docker exec namenode hdfs dfs -setrep -w 3 /user/student/lab01/retail_transactions.csv
docker exec namenode hdfs dfs -setrep -w 2 /user/student/lab01/retail_transactions.csv
docker exec namenode hdfs dfsadmin -report | grep -E "^Hostname|Num of Blocks"
```

   **Kết quả mong đợi** (rút gọn; `setrep -w 3` kết thúc sau khoảng 11 giây):

```text
 Container namenode  Healthy
 Container datanode3  Healthy
Live datanodes (3):
Hostname: datanode3
Hostname: datanode2
Hostname: datanode1
Replication 3 set: /user/student/lab01/retail_transactions.csv
Waiting for /user/student/lab01/retail_transactions.csv .... done
Replication 2 set: /user/student/lab01/retail_transactions.csv
Waiting for /user/student/lab01/retail_transactions.csv ...
WARNING: the waiting time may be long for DECREASING the number of replications.
. done
Hostname: datanode3
Num of Blocks: 4
Hostname: datanode2
Num of Blocks: 0
Hostname: datanode1
Num of Blocks: 4
```

   Dừng **một DataNode đang giữ bản sao** (`Num of Blocks` > 0). Trong hai lần chạy thử, sau khi giảm về 2 bản sao thì `datanode2` không còn block nào nên ta dừng `datanode1`; trên máy bạn có thể là node khác. Theo dõi bằng `fsck` mỗi khoảng 10 giây (vòng lặp chạy gần 2 phút):

<!-- shell: Terminal codespace -->
```bash
docker stop datanode1
for i in $(seq 1 8); do
  sleep 10
  docker exec namenode bash -c 'hdfs dfsadmin -report | grep -E "Live datanodes|Dead datanodes"; hdfs fsck /user/student/lab01/retail_transactions.csv -files -blocks -locations 2>/dev/null | grep -E "Under-replicated|Average block replication"'
done
```

   **Kết quả mong đợi** (rút gọn; mỗi vòng khoảng 12,5 giây):

```text
datanode1
Live datanodes (3):
 Under-replicated blocks:	0 (0.0 %)
 Average block replication:	2.0
...
Live datanodes (3):
 Under-replicated blocks:	0 (0.0 %)
 Average block replication:	2.0
Live datanodes (2):
Dead datanodes (1):
 Under-replicated blocks:	4 (100.0 %)
 Average block replication:	1.0
Live datanodes (2):
Dead datanodes (1):
 Under-replicated blocks:	0 (0.0 %)
 Average block replication:	2.0
...
```

   Bốn vòng đầu (khoảng 50 giây) NameNode vẫn coi `datanode1` là sống. Ở vòng thứ 5 (khoảng 63 giây), node bị đánh dấu *Dead* và cả 4 block chỉ còn 1 bản sao. Đến vòng thứ 6 (khoảng 12 giây sau), NameNode đã tự sao chép các block sang `datanode2`: `Under-replicated blocks: 0`, `Average block replication: 2.0`. Xem vị trí mới của từng block bằng `docker exec namenode hdfs fsck /user/student/lab01/retail_transactions.csv -files -blocks -locations`.

   Ghi lại diễn biến bằng `hdfs fsck ... -locations` theo thời gian, so sánh với Bước 13 và nhận xét về mức độ chịu lỗi. Khi xong, bật lại node đã dừng rồi gỡ node thứ ba:

<!-- shell: Terminal codespace -->
```bash
docker start datanode1
docker compose -f docker-compose.yml -f docker-compose.datanode3.yml rm -sf datanode3
docker volume rm bigdata-lab_datanode3
```

**Kết quả mong đợi:**

```text
datanode1
Going to remove datanode3
 Container datanode3  Removing
 Container datanode3  Removed
bigdata-lab_datanode3
```

   Khoảng 1 phút sau khi gỡ, NameNode chuyển `datanode3` sang *Dead datanodes* và giữ nó ở đó cho tới lần khởi động lại `namenode` kế tiếp; `fsck /` vẫn `HEALTHY`, `Under-replicated blocks: 0`.

> ⚠️ Phải gỡ `datanode3` trước khi kết thúc buổi: `lab stop` và `lab status` chỉ biết các container trong `docker-compose.yml` và `docker-compose.extras.yml`. Đã kiểm chứng: sau `lab stop`, `datanode3` vẫn `Up` và tiếp tục chiếm RAM.

2. **Tự động hoá phép đo block size.** Tạo script đo 5 mức block (32, 64, 128, 256, 512 MB), mỗi mức 3 lần, ghi CSV và tính trung bình. Tạo thư mục `src/lab01` trong Explorer (hoặc `mkdir -p src/lab01` ở terminal codespace) rồi tạo tệp:

<!-- file: src/lab01/bench_blocksize.sh -->
```bash
#!/usr/bin/env bash
# Đo thời gian hdfs dfs -put với nhiều kích thước block, ghi kết quả ra CSV
# Chạy bên trong container namenode:  bash /src/lab01/bench_blocksize.sh /tmp/test_1gb.dat 3
FILE=${1:-/tmp/test_1gb.dat}
RUNS=${2:-3}
OUT=/src/lab01/ket_qua_blocksize.csv
DEST=/user/student/lab01/bench
hdfs dfs -mkdir -p $DEST
echo "block_mb,lan,giay,so_block" > $OUT
for MB in 32 64 128 256 512; do
  BS=$((MB * 1024 * 1024))
  for i in $(seq 1 $RUNS); do
    hdfs dfs -rm -f -skipTrash $DEST/test.dat > /dev/null 2>&1
    T0=$(date +%s.%N)
    hdfs dfs -D dfs.blocksize=$BS -put $FILE $DEST/test.dat
    T1=$(date +%s.%N)
    NB=$(hdfs fsck $DEST/test.dat -blocks 2>/dev/null | grep -oP 'Total blocks \(validated\):\s*\K[0-9]+')
    echo "$MB,$i,$(awk -v a=$T0 -v b=$T1 'BEGIN{printf "%.2f", b-a}'),$NB" | tee -a $OUT
  done
done
hdfs dfs -rm -r -f -skipTrash $DEST > /dev/null
echo "--- trung bình (giây) theo kích thước block ---"
awk -F, 'NR>1 {s[$1]+=$3; n[$1]++; nb[$1]=$4} END {for (k in s) printf "%s MB: %.2f s (%d block)\n", k, s[k]/n[k], nb[k]}' $OUT | sort -n
```

   Tạo lại `/tmp/test_1gb.dat` như Bước 15 rồi chạy trong container `namenode` (image không có `bc`, script dùng `awk` để tính; trên máy 2 nhân mất khoảng 2 phút 16 giây):

<!-- shell: Bên trong container namenode -->
```bash
bash /src/lab01/bench_blocksize.sh /tmp/test_1gb.dat 3
```

   **Kết quả mong đợi** (ví dụ trên bản sao codespace 2 nhân; số của bạn sẽ khác):

```text
32,1,5.11,32
32,2,8.66,32
32,3,9.25,32
64,1,8.28,16
...
512,3,6.98,2
--- trung bình (giây) theo kích thước block ---
32 MB: 7.67 s (32 block)
64 MB: 7.76 s (16 block)
128 MB: 4.62 s (8 block)
256 MB: 5.48 s (4 block)
512 MB: 7.12 s (2 block)
```

   Dao động giữa các lần đo (ví dụ 32 MB: 5,11 s đến 9,25 s) lớn hơn chênh lệch giữa các mức, đúng như nhận xét ở Bước 16. Tệp `src/lab01/ket_qua_blocksize.csv` hiện ngay trong Explorer. Dùng Python (pandas, matplotlib, có sẵn trong container — ví dụ `docker exec -it spark-master python3`) vẽ biểu đồ trung bình và độ lệch chuẩn từ `/src/lab01/ket_qua_blocksize.csv`, nhận xét xu hướng. Xoá `/tmp/test_1gb.dat` khi xong.

3. **Dữ liệu log theo ngày và mất toàn bộ DataNode.** Đưa `web_access.log` lên HDFS dưới dạng thư mục phân vùng theo ngày `/user/student/logs/dt=YYYY-MM-DD/` (gợi ý: tách tệp theo trường thời gian bằng `awk` trước khi `put`). Sau đó dừng **cả hai** DataNode (`docker stop datanode1 datanode2` ở terminal codespace) và thử `hdfs dfs -ls` và `hdfs dfs -cat` trên một tệp. Dự đoán trước khi làm: lệnh nào chạy được, lệnh nào không, vì sao? (gợi ý: metadata nằm ở NameNode, dữ liệu nằm ở DataNode). Khởi động lại hai DataNode và xác nhận dữ liệu không mất.

## Yêu cầu nộp bài và tiêu chí chấm

Nộp một tệp PDF hoặc Markdown đặt tên `MSSV_Buoi01_BigData.pdf` (hoặc `.md`) kèm **đường dẫn tới commit** trên repo GitHub của bạn (commit ở mục Kết thúc buổi), gồm:

- Ảnh chụp màn hình Web UI 9870 (Overview và Datanodes), 8088 (Nodes) và 8080 khi cụm chạy ổn định, mở qua địa chỉ `https://<tên-codespace>-<cổng>.app.github.dev`; ảnh 9870 khi `datanode2` ở trạng thái *Dead*.
- Log đầy đủ (copy từ terminal) của `lab status` và các lệnh từ Bước 3 đến Bước 17, Bước 19 và Bước 20.
- Mã MD5 của tệp mẫu tính trên máy cá nhân (Bước 8).
- Bảng số liệu đo thời gian `put` với hai kích thước block (3 lần mỗi mức, có trung bình) và nhận xét.
- Trả lời 6 câu hỏi kiểm tra.
- (Nếu làm) tệp `docker-compose.datanode3.yml`, script và CSV của bài tập mở rộng — đã `git push` trong `src/lab01`.

| Tiêu chí | Điểm | Mô tả |
|---|---|---|
| Mở codespace, kiểm tra cụm và Web UI (Phần 1) | 2,0 | `lab status` / `docker compose ps` đủ 8 container healthy; ảnh chụp các Web UI qua địa chỉ `app.github.dev`; `dfsadmin -report` có 2 Live datanodes |
| Thao tác HDFS cơ bản (Phần 2) | 1,5 | Đủ `mkdir, put, ls, du, count, cat, tail, get`; MD5 khớp (cả trên máy cá nhân); giải thích hai cột của `du` |
| `setrep` và phân tích `fsck` (Phần 3) | 1,5 | Đúng số block và độ dài block cuối; giải thích under-replicated khi hệ số 3 và vì sao không dùng `-w 3` |
| Mô phỏng sự cố DataNode (Phần 4) | 2,0 | Có log trước/sau khi dừng và sau khi khởi động lại; giải thích mốc 60 giây và vì sao không tái tạo được bản sao trên cụm 2 node |
| Thí nghiệm block size và tính bền vững (Phần 5–6) | 1,5 | Bảng 3 lần đo mỗi mức, số block đúng (16 và 8), nhận xét xu hướng và độ nhiễu của phép đo; log chứng minh dữ liệu còn sau `lab stop`/`lab start` và sau khi mở lại codespace |
| Câu hỏi kiểm tra và trình bày báo cáo | 1,5 | Trả lời đúng, có lập luận; báo cáo rõ ràng; bài làm đã `git push` |
| Bài tập mở rộng (điểm thưởng) | +1,0 | Hoàn thành ít nhất một bài, có log/biểu đồ minh chứng (tổng điểm không vượt quá 10) |
