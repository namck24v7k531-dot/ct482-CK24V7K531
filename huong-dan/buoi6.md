# Buổi 6 — Đường ống dữ liệu thời gian thực với Kafka và Spark Structured Streaming (GitHub Codespaces)

Bài thực hành kéo dài 4 tiết (tuần 12–13), phục vụ CLO4 và CLO5, chạy trên GitHub Codespaces. Máy khuyến nghị: 2 nhân / 8 GB RAM (mặc định) là đủ — RAM đỉnh đo được khi chạy toàn bộ bài là khoảng 5,4 GB; không cần máy 4 nhân. Dịch vụ cần bật: cụm lõi (tự bật khi mở codespace) và Kafka (`lab kafka`). Quota ước tính: khoảng 3,5 giờ codespace chạy × 2 nhân ≈ 7 core-hour (quota miễn phí của tài khoản cá nhân là 120 core-hour/tháng); dung lượng thêm cho buổi này khoảng 1,2 GB (image Kafka 0,7 GB, dữ liệu 5 triệu dòng 0,5 GB). Sinh viên dựng một đường ống dữ liệu thời gian thực hoàn chỉnh: producer Python phát giao dịch bán lẻ vào Kafka 4.3.1 (chế độ KRaft, không cần ZooKeeper), quan sát consumer group, rebalance và consumer lag, sau đó dùng Spark 4.1.3 Structured Streaming tổng hợp doanh thu theo cửa sổ 5 phút có watermark, ghi ra console, Parquet trên HDFS và bảng đích SQLite (upsert bằng `foreachBatch`), cuối cùng kiểm chứng khả năng khôi phục từ checkpoint. Dữ liệu dùng: `data/retail_transactions.csv` (producer đọc tuần tự tệp này và gán lại thời điểm hiện tại cho từng giao dịch).

## Mục tiêu

Sau khi hoàn thành bài thực hành, sinh viên có khả năng:

- Tạo và kiểm tra một Kafka topic nhiều partition, giải thích vai trò của partition và khóa (key) đối với khả năng mở rộng và thứ tự thông điệp (CLO4).
- Viết producer Python phát sự kiện giao dịch bán lẻ mô phỏng thời gian thực vào Kafka và kiểm tra số thông điệp theo từng partition bằng `kafka-get-offsets.sh` (CLO4).
- Sử dụng consumer console và consumer group nhiều thành viên (CLI và Python), quan sát rebalance, static membership, consumer lag và thao tác đặt lại offset (CLO4).
- Xây dựng ứng dụng Spark Structured Streaming đọc Kafka, tổng hợp theo cửa sổ 5 phút có watermark 2 phút, ghi kết quả ra console và Parquet trên HDFS (CLO5).
- Dùng `foreachBatch` để upsert kết quả theo khóa nghiệp vụ, quan sát ảnh hưởng của dữ liệu đến muộn trong và ngoài watermark (CLO5).
- Kiểm chứng khả năng chịu lỗi qua checkpoint: dừng job, khởi động lại, xác nhận dữ liệu không mất và không bị đếm trùng (CLO5).

## Chuẩn bị

**Mở codespace.** Vào https://github.com/codespaces, chọn codespace của repo bài thực hành (tạo từ template của giảng viên) và bấm **Open** (hoặc dấu `...` → *Open in browser*). Khi codespace khởi động, cụm lõi 8 container (HDFS, YARN, Spark) tự bật; chờ đến khi terminal báo `[khoi-dong] Cụm đã sẵn sàng` (khoảng 1–2 phút). Terminal mặc định đứng ở thư mục gốc của repo — mọi lệnh trong bài dùng đường dẫn tương đối tính từ đây. Thư mục `./data` là `/data`, thư mục `./src` là `/src` trong các container `namenode`, `spark-master`, `nodemanager1/2`. Mã nguồn bài này đặt trong `src/lab6/` (trong container: `/src/lab6/`).

Nếu giảng viên thông báo có cập nhật repo, lấy bản mới trước khi làm bài, sau đó kiểm tra cụm:

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
nodemanager1      Up 31 seconds (healthy)
nodemanager2      Up 31 seconds (healthy)
resourcemanager   Up About a minute (healthy)
spark-master      Up About a minute (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 2.2 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

**Dữ liệu 5 triệu dòng.** Codespace tạo trước 06/11/2026 chỉ có dữ liệu mẫu 200 000 dòng (codespace tạo sau ngày này đã có sẵn 5 triệu dòng). Kiểm tra số dòng; nếu kết quả là `200001` thì sinh bản đầy đủ (chỉ cần làm một lần cho cả học kỳ; nếu đã làm ở buổi trước thì bỏ qua):

<!-- shell: Terminal codespace -->
```bash
wc -l data/retail_transactions.csv
lab data 5000000
wc -l data/retail_transactions.csv
```

**Kết quả mong đợi** (lệnh `lab data` mất khoảng 30 giây, dữ liệu chiếm khoảng 0,5 GB đĩa):

```text
200001 data/retail_transactions.csv
  products.csv: 0.0 MB
  retail_transactions.csv: 405.2 MB
  web_access.log: 121.4 MB
  sensor_readings.csv: 22.7 MB
5000001 data/retail_transactions.csv
```

> 💡 Bài này chỉ đọc khoảng 210 000 dòng đầu, nên dùng bản 200 000 dòng vẫn chạy được tới Bước 12; riêng hai lệnh `--skip 200000`, `--skip 201000`, `--skip 202000` ở Bước 13–14 cần bản 5 triệu dòng (bản nhỏ sẽ không gửi được bản ghi nào).

**Bật Kafka.** Kafka 4.3.1 chạy ở chế độ KRaft: một container `kafka` đảm nhận cả vai trò broker và controller, không còn ZooKeeper như môi trường cũ.

<!-- shell: Terminal codespace -->
```bash
lab kafka
lab status
```

**Kết quả mong đợi:**

```text
 Container kafka  Creating
 Container kafka  Created
 Container kafka  Starting
 Container kafka  Started
 Container kafka  Waiting
 Container kafka  Healthy
NAME              STATUS
datanode1         Up About a minute (healthy)
datanode2         Up About a minute (healthy)
historyserver     Up About a minute (healthy)
kafka             Up 6 seconds (healthy)
namenode          Up About a minute (healthy)
nodemanager1      Up About a minute (healthy)
nodemanager2      Up About a minute (healthy)
resourcemanager   Up About a minute (healthy)
spark-master      Up About a minute (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 2.5 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

> 💡 Lần đầu tiên chạy `lab kafka` trên một codespace, Docker phải tải image `apache/kafka:4.3.1` (khoảng 690 MB) nên sẽ thấy thêm các dòng `Pulling`; những lần sau chỉ mất vài giây.

**Mã nguồn bài thực hành:** giảng viên cung cấp tệp `buoi6_codespaces_src.zip`; kéo thả tệp vào thư mục `src/` trong cây thư mục bên trái của VS Code rồi giải nén bằng `cd src && unzip buoi6_codespaces_src.zip && rm buoi6_codespaces_src.zip && cd ..` để có thư mục `src/lab6/`. Nếu không có tệp zip, tạo từng tệp theo nội dung trong các bước dưới đây (chuột phải vào thư mục `src` → *New Folder* `lab6` → *New File*).

**Mở nhiều terminal.** Nhiều bước cần 2–4 terminal chạy song song (terminal A, B, C, D). Trong VS Code của codespace, bấm nút **+** ở góc phải khung TERMINAL để mở terminal mới, hoặc nút **Split Terminal** (biểu tượng hai ô, phím tắt Ctrl+Shift+5) để chia đôi khung. Mỗi terminal mới đều đứng ở thư mục gốc repo. Nên đổi tên cho dễ nhận (chuột phải vào tên terminal → *Rename* → `A`, `B`, ...).

**Các Web UI dùng trong bài.** Gõ `lab ui` để lấy đường dẫn (dạng `https://<tên-codespace>-<cổng>.app.github.dev`) hoặc mở tab **PORTS** cạnh TERMINAL và bấm biểu tượng quả địa cầu ở cổng tương ứng:

<!-- shell: Terminal codespace -->
```bash
lab ui
```

| Giao diện | Cổng (tab PORTS) | Dùng để |
|---|---|---|
| Spark Master | 8080 | Xem ứng dụng streaming đang chạy, số core/bộ nhớ được cấp |
| Spark UI của driver (trong `spark-master`) | 4040 | Tab **Structured Streaming**: Input Rate, Process Rate, Batch Duration |
| HDFS NameNode | 9870 | Utilities → Browse the file system → `/user/student/retail_stream` |
| Spark History Server | 18080 | Xem lại job sau khi đã dừng |

> 💡 Kafka không có Web UI trong bộ môi trường này; mọi thao tác quản trị dùng các script `kafka-*.sh` có sẵn trên PATH của container `kafka`.

> ⚠️ Các liên kết nội bộ bên trong Web UI (ví dụ *Application Detail UI* trỏ tới `http://spark-master:4040`, hoặc liên kết tới worker `http://nodemanager1:...`) không mở được từ trình duyệt của bạn. Hãy dùng đường dẫn tương ứng do `lab ui` in ra. Bên trong codespace, `curl localhost:<cổng>` vẫn dùng được.

> ⚠️ Codespace tự dừng sau 30 phút không có thao tác (gõ phím, rê chuột hoặc có output trên terminal). Phần 3–4 có những đoạn phải chờ 8–10 phút; giữ tab trình duyệt mở và đừng bỏ đi quá lâu. Có thể tăng thời gian chờ ở https://github.com/settings/codespaces → *Default idle timeout* (áp dụng cho codespace tạo mới).

## PHẦN 1: KHỞI ĐỘNG CỤM VÀ TẠO TOPIC KAFKA

### Bước 1: Kiểm tra Kafka và quorum KRaft

Kafka đã được bật ở mục Chuẩn bị (`lab kafka`). Bên trong mạng Docker, các container khác (Spark, producer Python chạy trong `spark-master`) kết nối tới `kafka:9092`; chương trình chạy trực tiếp trong terminal codespace kết nối tới `localhost:9092` (cổng 9092 của codespace được map vào listener EXTERNAL của broker). Kiểm tra quorum KRaft (một controller duy nhất, id 1):

<!-- shell: Terminal codespace -->
```bash
docker exec -it kafka kafka-metadata-quorum.sh --bootstrap-server kafka:9092 describe --status
```

**Kết quả mong đợi** (ClusterId có thể khác trên máy bạn):

```text
ClusterId:              5L6g3nShT-eMCtK--X86sw
LeaderId:               1
LeaderEpoch:            1
HighWatermark:          52
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   0
CurrentVoters:          [{"id": 1, "endpoints": ["CONTROLLER://kafka:9093"]}]
CurrentObservers:       []
```

> 💡 Trong codespace không cần `cd moi-truong-bigdata-lab` hay gõ `docker compose -f ... -f ...` như ở phòng lab: lệnh `lab` đã gói sẵn các tệp compose. `lab status` thay cho `docker compose ps`, `lab kafka` thay cho `docker compose ... up -d kafka`.

### Bước 2: Tạo topic `retail-transactions` 3 partition

Broker được cấu hình `auto.create.topics.enable=false`, vì vậy phải tạo topic trước khi gửi dữ liệu. Topic có 3 partition để tối đa 3 consumer trong cùng một group đọc song song; replication factor là 1 vì cụm chỉ có một broker.

<!-- shell: Terminal codespace -->
```bash
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic retail-transactions --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 \
  --describe --topic retail-transactions
```

**Kết quả mong đợi:**

```text
Created topic retail-transactions.
Topic: retail-transactions	TopicId: 7fWG9NLBTwOSSvx8x5QpNA	PartitionCount: 3	ReplicationFactor: 1	Configs: min.insync.replicas=1
	Topic: retail-transactions	Partition: 0	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
	Topic: retail-transactions	Partition: 1	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
	Topic: retail-transactions	Partition: 2	Leader: 1	Replicas: 1	Isr: 1	Elr: 	LastKnownElr: 
```

Cả ba partition có `Leader: 1` (broker duy nhất). Hai cột `Elr`/`LastKnownElr` (Eligible Leader Replicas) là tính năng mới của Kafka 4, để trống khi replication factor bằng 1.

> 💡 Chạy lại lệnh `--create` lần thứ hai sẽ báo `TopicExistsException: Topic 'retail-transactions' already exists.` Đây không phải lỗi cụm; dùng `--describe` để kiểm tra cấu hình hoặc `--delete` rồi tạo lại nếu muốn làm lại từ đầu. Topic được giữ trong volume `kafka` qua các lần dừng/mở codespace.

## PHẦN 2: PRODUCER VÀ CONSUMER

### Bước 3: Viết producer Python

Producer đọc tuần tự `retail_transactions.csv`, thay cột `ts` bằng thời điểm hiện tại (UTC, định dạng ISO-8601) và gửi từng giao dịch dạng JSON vào topic với khóa `store_id`. Bộ phân vùng mặc định băm khóa (murmur2) để chọn partition, nên mọi giao dịch của cùng một cửa hàng luôn vào cùng một partition và giữ đúng thứ tự trong phạm vi cửa hàng đó. Tùy chọn `--late-ratio`/`--late-minutes` dùng ở Phần 4 để giả lập dữ liệu đến muộn; `--skip` bỏ qua N dòng đầu để mỗi lần chạy gửi các giao dịch khác nhau.

Tạo tệp `src/lab6/producer_retail.py` (trong VS Code của codespace):

<!-- file: src/lab6/producer_retail.py -->
```python
"""Producer mô phỏng luồng giao dịch bán lẻ thời gian thực (Buổi 6 - CT482).

Đọc tuần tự retail_transactions.csv, thay cột ts bằng thời điểm hiện tại (UTC, ISO-8601)
và gửi từng giao dịch dạng JSON vào Kafka, khóa (key) = store_id.
"""
import argparse
import csv
import json
import random
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--csv", default="/data/retail_transactions.csv")
parser.add_argument("--bootstrap", default="kafka:9092",
                    help="kafka:9092 khi chạy trong container; localhost:9092 khi chạy trên máy host")
parser.add_argument("--topic", default="retail-transactions")
parser.add_argument("--sleep", type=float, default=0.2, help="số giây nghỉ giữa hai bản ghi")
parser.add_argument("--limit", type=int, default=2000, help="số bản ghi sẽ gửi")
parser.add_argument("--skip", type=int, default=0, help="bỏ qua N dòng đầu của CSV")
parser.add_argument("--late-ratio", type=float, default=0.0, help="tỉ lệ bản ghi bị lùi thời gian")
parser.add_argument("--late-minutes", type=int, default=8, help="số phút lùi lại cho bản ghi đến muộn")
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)

futures = []
with open(args.csv, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    sent = 0
    for i, row in enumerate(reader):
        if i < args.skip:
            continue
        if sent >= args.limit:
            break
        now = datetime.now(timezone.utc)
        if random.random() < args.late_ratio:
            now = now - timedelta(minutes=args.late_minutes)
        row["ts"] = now.isoformat(timespec="milliseconds")
        row["quantity"] = int(row["quantity"])
        row["unit_price"] = float(row["unit_price"])
        row["discount"] = float(row["discount"])
        futures.append(producer.send(args.topic, key=row["store_id"], value=row))
        if sent % 50 == 0:
            print(f"da gui {sent} ban ghi, ts={row['ts']}", flush=True)
        sent += 1
        time.sleep(args.sleep)

producer.flush()
per_partition = Counter(fut.get(timeout=30).partition for fut in futures)
print("so ban ghi theo partition:", dict(sorted(per_partition.items())))
print(f"hoan tat phat su kien: {sent} ban ghi")
```

> 💡 Mỗi lời gọi `producer.send()` trả về một *future*; sau `flush()`, chương trình đọc `partition` từ kết quả của từng future để thống kê số bản ghi theo partition. Con số này phải khớp với offset đọc được ở Bước 4.

### Bước 4: Chạy producer và kiểm tra offset từng partition

Chạy producer trong container `spark-master` (đã có sẵn Python 3.12 và `kafka-python`; tệp CSV nằm ở `/data`). Với `--sleep 0.05`, 2000 bản ghi mất khoảng 100 giây (đo được 1 phút 41 giây).

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master python3 /src/lab6/producer_retail.py --sleep 0.05 --limit 2000
```

**Kết quả mong đợi** (thời điểm `ts` là giờ UTC lúc bạn chạy):

```text
da gui 0 ban ghi, ts=2026-11-05T16:22:21.390+00:00
da gui 50 ban ghi, ts=2026-11-05T16:22:24.009+00:00
da gui 100 ban ghi, ts=2026-11-05T16:22:26.525+00:00
...
so ban ghi theo partition: {0: 798, 1: 568, 2: 634}
hoan tat phat su kien: 2000 ban ghi
```

Kiểm chứng số thông điệp trong từng partition. Kafka 4 đã bỏ lớp `kafka.tools.GetOffsetShell` dùng trong tài liệu cũ, thay bằng script `kafka-get-offsets.sh`:

<!-- shell: Terminal codespace -->
```bash
docker exec -it kafka kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic retail-transactions
```

**Kết quả mong đợi:** ba dòng `topic:partition:offset-cuối`, khớp với thống kê của producer và có tổng bằng 2000.

```text
retail-transactions:0:798
retail-transactions:1:568
retail-transactions:2:634
```

> 💡 Ba partition không đều nhau vì phân vùng theo khóa: 60 cửa hàng được băm vào 3 partition, mỗi partition nhận một số cửa hàng khác nhau.

> ⚠️ Nếu gửi tới một topic chưa tồn tại (ví dụ gõ sai tên), producer không báo lỗi ngay mà treo 60 giây rồi dừng với `KafkaTimeoutError: Failed to update metadata after 60.0 secs.` vì broker không tự tạo topic.

**Phương án tùy chọn — chạy producer trực tiếp trong terminal codespace** (kết nối `localhost:9092`). Codespace (Ubuntu 24.04) không cài sẵn Python, và lệnh `pip install kafka-python` của tài liệu cũ không chạy được (`pip: command not found`; sau khi cài Python thì `pip install` toàn hệ thống bị chặn với lỗi `externally-managed-environment` theo PEP 668). Cách làm đúng là cài Python rồi tạo môi trường ảo `.venv` trong thư mục repo. Để không làm lệch số liệu của topic chính ở các bước sau, thử với một topic riêng `retail-test`:

<!-- shell: Terminal codespace -->
```bash
sudo apt-get update -qq && sudo apt-get install -y -qq python3-venv python3-pip
python3 -m venv .venv
. .venv/bin/activate
pip install -q kafka-python
docker exec -it kafka kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic retail-test --partitions 3 --replication-factor 1
python3 src/lab6/producer_retail.py --csv ./data/retail_transactions.csv \
  --bootstrap localhost:9092 --topic retail-test --sleep 0.05 --limit 20
docker exec -it kafka kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic retail-test
deactivate
```

**Kết quả mong đợi** (cài đặt mất khoảng 20 giây):

```text
Created topic retail-test.
da gui 0 ban ghi, ts=2026-11-05T16:24:39.022+00:00
so ban ghi theo partition: {0: 6, 1: 7, 2: 7}
hoan tat phat su kien: 20 ban ghi
retail-test:0:6
retail-test:1:7
retail-test:2:7
```

> ⚠️ Gói cài bằng `apt-get` mất khi codespace được tạo lại (Rebuild), còn `.venv` nằm trong repo nên được giữ lại khi dừng/mở codespace. **Không** đưa `.venv/` lên GitHub: luôn `git add src/lab6` (không dùng `git add .`). Nếu không muốn cài gì, có thể chạy cùng script bằng image có sẵn: `docker run --rm --network host --entrypoint python3 -v "$PWD":/w -w /w bigdata-lab:2026.10 src/lab6/producer_retail.py --csv ./data/retail_transactions.csv --bootstrap localhost:9092 --topic retail-test --sleep 0 --limit 10 --skip 20`.

### Bước 5: Consumer console đơn lẻ

Đọc lại 5 thông điệp đầu tiên của topic:

<!-- shell: Terminal codespace -->
```bash
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic retail-transactions --from-beginning --max-messages 5
```

**Kết quả mong đợi:**

```text
The consumer rebalance protocol (KIP-848) is production-ready! Set group.protocol=consumer to try it out. See https://kafka.apache.org/documentation/#consumer_rebalance_protocol
{"transaction_id": "T0000000001", "customer_id": "C0001229", "product_id": "P00442", "category": "gia_dung", "quantity": 5, "unit_price": 23031000.0, "discount": 0.05, "store_id": "S037", "region": "Hanoi", "ts": "2026-11-05T16:22:21.544+00:00"}
{"transaction_id": "T0000000008", "customer_id": "C0028821", "product_id": "P00324", "category": "gia_dung", "quantity": 3, "unit_price": 11734000.0, "discount": 0.1, "store_id": "S004", "region": "Hanoi", "ts": "2026-11-05T16:22:21.896+00:00"}
...
Processed a total of 5 messages
```

Dòng thông báo KIP-848 chỉ mang tính thông tin (giao thức rebalance mới của Kafka 4), không phải lỗi. Để thấy partition và khóa của từng thông điệp, dùng `--formatter-property` (Kafka 4 đổi tên từ `--property`, tên cũ vẫn chạy nhưng in cảnh báo deprecated):

<!-- shell: Terminal codespace -->
```bash
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic retail-transactions --from-beginning --max-messages 5 \
  --formatter-property print.partition=true --formatter-property print.key=true \
  --formatter-property print.value=false
```

**Kết quả mong đợi:**

```text
Partition:0	S037
Partition:0	S004
Partition:0	S059
Partition:0	S052
Partition:0	S017
Processed a total of 5 messages
```

> 💡 Thông điệp `T0000000000` không xuất hiện đầu tiên: consumer đọc lần lượt theo từng partition, và Kafka chỉ đảm bảo thứ tự **trong một partition**, không đảm bảo thứ tự toàn topic.

### Bước 6: Consumer group hai thành viên

Các consumer có cùng `--group` chia nhau các partition: mỗi partition chỉ được giao cho đúng một thành viên trong group. Mở **hai terminal** A và B (nút **+** hoặc **Split Terminal** của khung TERMINAL), mỗi terminal chạy một consumer của group `retail-group`. Thuộc tính `group.instance.id` bật *static membership*: thành viên được nhận diện bằng tên cố định `member-1`, `member-2` (Kafka 4 đổi tên tùy chọn `--consumer-property` thành `--command-property`).

<!-- shell: Terminal codespace (terminal A) -->
```bash
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic retail-transactions --group retail-group \
  --command-property group.instance.id=member-1
```

<!-- shell: Terminal codespace (terminal B) -->
```bash
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic retail-transactions --group retail-group \
  --command-property group.instance.id=member-2
```

Group mới tạo bắt đầu đọc từ cuối topic (mặc định `auto.offset.reset=latest`), nên hai terminal chưa in gì. Mở **terminal thứ ba** (C) và phát thêm 600 giao dịch (bỏ qua 2000 dòng đã gửi):

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it spark-master python3 /src/lab6/producer_retail.py --sleep 0.05 --limit 600 --skip 2000
```

**Kết quả mong đợi** ở terminal C:

```text
...
so ban ghi theo partition: {0: 246, 1: 159, 2: 195}
hoan tat phat su kien: 600 ban ghi
```

Kiểm tra phân bổ partition và độ trễ (lag) của group:

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group retail-group
docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group retail-group --members
```

**Kết quả mong đợi:**

```text
GROUP           TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                   HOST            CLIENT-ID
retail-group    retail-transactions 0          1044            1044            0               member-1-4cd2bcca-f77d-485d-a9f3-58e0934c89fe /172.19.0.10    console-consumer
retail-group    retail-transactions 1          727             727             0               member-1-4cd2bcca-f77d-485d-a9f3-58e0934c89fe /172.19.0.10    console-consumer
retail-group    retail-transactions 2          829             829             0               member-2-c7671139-5b35-4195-8443-ab7bd636cf51 /172.19.0.10    console-consumer

GROUP           CONSUMER-ID                                   GROUP-INSTANCE-ID HOST            CLIENT-ID        #PARTITIONS     
retail-group    member-1-4cd2bcca-f77d-485d-a9f3-58e0934c89fe member-1          /172.19.0.10    console-consumer 2               
retail-group    member-2-c7671139-5b35-4195-8443-ab7bd636cf51 member-2          /172.19.0.10    console-consumer 1               
```

3 partition được chia cho 2 thành viên (một thành viên giữ 2 partition), `LAG` bằng 0 vì cả hai đã đọc hết. Khi dừng hai consumer (bấm vào terminal A/B rồi Ctrl+C), dòng cuối mỗi terminal cho biết số thông điệp đã nhận; trong lần chạy kiểm thử: member-1 nhận 405 (= 246 + 159, partition 0 và 1), member-2 nhận 195 (partition 2), tổng đúng 600.

```text
Processed a total of 405 messages
```

> 💡 Mỗi thành viên nhận **toàn bộ** thông điệp của các partition nó giữ, không phải một nửa ngẫu nhiên. Số partition là giới hạn trên của mức song song trong một group.

### Bước 7: Quan sát rebalance khi thêm và bớt thành viên

Giữ `member-1`, `member-2` đang chạy (chạy lại lệnh ở Bước 6 trong terminal A, B nếu đã dừng). Mở **terminal D** và thêm thành viên thứ ba:

<!-- shell: Terminal codespace (terminal D) -->
```bash
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic retail-transactions --group retail-group \
  --command-property group.instance.id=member-3
```

Sau vài giây, kiểm tra lại thành viên của group:

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group retail-group --members
docker exec -it spark-master python3 /src/lab6/producer_retail.py --sleep 0.05 --limit 300 --skip 2600
docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group retail-group
```

**Kết quả mong đợi:** sau rebalance, mỗi thành viên giữ đúng 1 partition.

```text
GROUP           CONSUMER-ID                                   GROUP-INSTANCE-ID HOST            CLIENT-ID        #PARTITIONS     
retail-group    member-1-e78fad64-75d5-413c-b6b4-7fb3df786b75 member-1          /172.19.0.10    console-consumer 1               
retail-group    member-3-be84bda3-e66d-430d-924c-4cf8be7d6a50 member-3          /172.19.0.10    console-consumer 1               
retail-group    member-2-b7b7b1c7-c1ab-4449-a8bc-959ab6118f62 member-2          /172.19.0.10    console-consumer 1               
...
so ban ghi theo partition: {0: 118, 1: 91, 2: 91}
hoan tat phat su kien: 300 ban ghi

GROUP           TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                   HOST            CLIENT-ID
retail-group    retail-transactions 0          1162            1162            0               member-1-e78fad64-75d5-413c-b6b4-7fb3df786b75 /172.19.0.10    console-consumer
retail-group    retail-transactions 2          920             920             0               member-3-be84bda3-e66d-430d-924c-4cf8be7d6a50 /172.19.0.10    console-consumer
retail-group    retail-transactions 1          818             818             0               member-2-b7b7b1c7-c1ab-4449-a8bc-959ab6118f62 /172.19.0.10    console-consumer
```

Bây giờ dừng `member-3` (Ctrl+C ở terminal D), rồi chạy lệnh `--describe --members` ngay lập tức và chạy lại sau khoảng 50 giây:

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group retail-group --members
```

**Kết quả mong đợi:** ngay sau khi dừng (5 giây), `member-3` **vẫn còn** trong group; sau khoảng 45 giây mới xảy ra rebalance và `member-1` nhận lại 2 partition.

```text
(5 giây sau Ctrl+C)
retail-group    member-1-e78fad64-75d5-413c-b6b4-7fb3df786b75 member-1          /172.19.0.10    console-consumer 1               
retail-group    member-3-be84bda3-e66d-430d-924c-4cf8be7d6a50 member-3          /172.19.0.10    console-consumer 1               
retail-group    member-2-b7b7b1c7-c1ab-4449-a8bc-959ab6118f62 member-2          /172.19.0.10    console-consumer 1               

(52 giây sau Ctrl+C)
retail-group    member-1-e78fad64-75d5-413c-b6b4-7fb3df786b75 member-1          /172.19.0.10    console-consumer 2               
retail-group    member-2-b7b7b1c7-c1ab-4449-a8bc-959ab6118f62 member-2          /172.19.0.10    console-consumer 1               
```

Đây là hành vi của **static membership**: thành viên có `group.instance.id` không gửi yêu cầu rời group khi tắt, coordinator chờ hết `session.timeout.ms` (mặc định 45 giây) mới coi thành viên đã chết và chia lại partition. Cơ chế này tránh rebalance không cần thiết khi một consumer chỉ khởi động lại nhanh. Nếu bỏ `--command-property group.instance.id=...` (dynamic membership), consumer gửi LeaveGroup khi Ctrl+C và rebalance xảy ra ngay.

> 💡 Trong lúc rebalance, các thành viên tạm ngừng nhận dữ liệu vài giây. Dừng cả `member-1`, `member-2` (Ctrl+C ở terminal A, B) trước khi sang Bước 8 và **chờ khoảng 1 phút** để group trống hẳn. Có thể đóng terminal D (biểu tượng thùng rác) cho gọn.

### Bước 8: Consumer lag, consumer Python và đặt lại offset

*Consumer lag* = `LOG-END-OFFSET − CURRENT-OFFSET`: số thông điệp đã có trong partition nhưng group chưa xử lý. Lag tăng liên tục là dấu hiệu consumer xử lý chậm hơn tốc độ phát sinh dữ liệu.

Tạo consumer Python `src/lab6/consumer_retail.py`. Mỗi tiến trình là một thành viên của group, in partition được gán sau mỗi lần rebalance và đếm số bản ghi nhận được theo partition:

<!-- file: src/lab6/consumer_retail.py -->
```python
"""Consumer Python thuộc một consumer group (Buổi 6 - CT482).

Mỗi tiến trình là một thành viên của group; in ra partition được gán sau mỗi lần rebalance
và đếm số bản ghi nhận được theo partition.
"""
import argparse
import json
import socket
from collections import Counter

from kafka import KafkaConsumer, ConsumerRebalanceListener

parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap", default="kafka:9092")
parser.add_argument("--topic", default="retail-transactions")
parser.add_argument("--group", default="retail-py-group")
parser.add_argument("--name", default=socket.gethostname())
parser.add_argument("--idle", type=int, default=0,
                    help="tự dừng sau N giây không có bản ghi mới (0 = chạy mãi, Ctrl+C để dừng)")
args = parser.parse_args()


class InBaoRebalance(ConsumerRebalanceListener):
    def on_partitions_revoked(self, revoked):
        print(f"[{args.name}] bi thu hoi partition: {sorted(tp.partition for tp in revoked)}", flush=True)

    def on_partitions_assigned(self, assigned):
        print(f"[{args.name}] duoc gan partition: {sorted(tp.partition for tp in assigned)}", flush=True)


consumer = KafkaConsumer(
    bootstrap_servers=args.bootstrap,
    group_id=args.group,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    consumer_timeout_ms=args.idle * 1000 if args.idle > 0 else float("inf"),
)
consumer.subscribe([args.topic], listener=InBaoRebalance())

counts = Counter()
try:
    for msg in consumer:
        counts[msg.partition] += 1
        total = sum(counts.values())
        if total % 200 == 0:
            print(f"[{args.name}] da nhan {total} ban ghi, theo partition: {dict(sorted(counts.items()))}", flush=True)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print(f"[{args.name}] TONG: {sum(counts.values())} ban ghi, theo partition: {dict(sorted(counts.items()))}", flush=True)
```

Khi group `retail-group` không còn thành viên nào, phát thêm 500 giao dịch rồi xem lag:

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master python3 /src/lab6/producer_retail.py --sleep 0.01 --limit 500 --skip 3400
docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group retail-group
```

**Kết quả mong đợi:** tổng `LAG` bằng đúng 500 (213 + 137 + 150), cột `CONSUMER-ID` là `-` vì không có ai đang đọc.

```text
so ban ghi theo partition: {0: 213, 1: 137, 2: 150}
hoan tat phat su kien: 500 ban ghi

Consumer group 'retail-group' has no active members.

GROUP           TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
retail-group    retail-transactions 0          1162            1375            213             -               -               -
retail-group    retail-transactions 1          818             955             137             -               -               -
retail-group    retail-transactions 2          920             1070            150             -               -               -
```

> 💡 Offset của bạn có thể khác bảng trên (tùy số lần đã chạy producer ở các bước trước), nhưng tổng LAG phải bằng số bản ghi vừa phát.

Cho một consumer Python tham gia group để "trả nợ" lag; `--idle 15` để tự dừng sau 15 giây không có bản ghi mới:

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master python3 /src/lab6/consumer_retail.py --group retail-group --name py-1 --idle 15
docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group retail-group
```

**Kết quả mong đợi:** consumer đọc đúng 500 bản ghi còn tồn (bắt đầu từ offset đã commit, không đọc lại từ đầu), sau đó LAG về 0.

```text
[py-1] bi thu hoi partition: []
[py-1] duoc gan partition: [0, 1, 2]
[py-1] da nhan 200 ban ghi, theo partition: {0: 200}
[py-1] da nhan 400 ban ghi, theo partition: {0: 213, 1: 137, 2: 50}
[py-1] TONG: 500 ban ghi, theo partition: {0: 213, 1: 137, 2: 150}

Consumer group 'retail-group' has no active members.

GROUP           TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
retail-group    retail-transactions 0          1375            1375            0               -               -               -
retail-group    retail-transactions 1          955             955             0               -               -               -
retail-group    retail-transactions 2          1070            1070            0               -               -               -
```

Hai consumer Python chia tải: trong terminal A và B chạy đồng thời hai thành viên `py-1`, `py-2`, chờ khoảng 10 giây rồi ở terminal C phát 600 giao dịch:

<!-- shell: Terminal codespace (terminal A) -->
```bash
docker exec -it spark-master python3 /src/lab6/consumer_retail.py --group retail-group --name py-1 --idle 40
```

<!-- shell: Terminal codespace (terminal B) -->
```bash
docker exec -it spark-master python3 /src/lab6/consumer_retail.py --group retail-group --name py-2 --idle 40
```

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it spark-master python3 /src/lab6/producer_retail.py --sleep 0.02 --limit 600 --skip 3900
```

**Kết quả mong đợi:** consumer vào sau gia nhập làm consumer vào trước bị thu hồi cả 3 partition; sau rebalance một thành viên giữ 2 partition, thành viên kia giữ 1; tổng hai consumer nhận đúng 600 (404 + 196). Thành viên nào nhận 2 partition tùy thứ tự gia nhập — trong lần kiểm thử là `py-2`.

```text
so ban ghi theo partition: {0: 229, 1: 175, 2: 196}
hoan tat phat su kien: 600 ban ghi

[py-1] bi thu hoi partition: []
[py-1] duoc gan partition: [2]
[py-1] TONG: 196 ban ghi, theo partition: {2: 196}

[py-2] bi thu hoi partition: []
[py-2] duoc gan partition: [0, 1, 2]
[py-2] bi thu hoi partition: [0, 1, 2]
[py-2] duoc gan partition: [0, 1]
[py-2] da nhan 200 ban ghi, theo partition: {0: 118, 1: 82}
[py-2] da nhan 400 ban ghi, theo partition: {0: 227, 1: 173}
[py-2] TONG: 404 ban ghi, theo partition: {0: 229, 1: 175}
```

Đặt lại offset của group về đầu topic (group phải không có thành viên đang chạy). `--dry-run` chỉ in kế hoạch, `--execute` mới thực hiện:

<!-- shell: Terminal codespace -->
```bash
docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group retail-group \
  --topic retail-transactions --reset-offsets --to-earliest --execute
docker exec -it kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group retail-group
```

**Kết quả mong đợi:** `NEW-OFFSET` bằng 0 cho cả 3 partition; LAG bằng toàn bộ số thông điệp trong topic (lần kiểm thử: 1604 + 1130 + 1266 = 4000 thông điệp).

```text
GROUP           TOPIC               PARTITION  NEW-OFFSET
retail-group    retail-transactions 2          0
retail-group    retail-transactions 1          0
retail-group    retail-transactions 0          0

Consumer group 'retail-group' has no active members.

GROUP           TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
retail-group    retail-transactions 0          0               1604            1604            -               -               -
retail-group    retail-transactions 1          0               1130            1130            -               -               -
retail-group    retail-transactions 2          0               1266            1266            -               -               -
```

> ⚠️ Nếu chạy consumer Python khi các thành viên static (`member-1`, `member-2`) vừa tắt chưa quá 45 giây, consumer mới có thể không được gán partition nào và in `TONG: 0 ban ghi`. Chờ hết session timeout rồi chạy lại.

## PHẦN 3: STRUCTURED STREAMING VỚI CỬA SỔ THỜI GIAN VÀ WATERMARK

### Bước 9: Job streaming tổng hợp cửa sổ 5 phút ra console

Job đọc topic `retail-transactions`, tách JSON theo schema, chuyển `ts` thành `event_time` (thời điểm sự kiện, khác với thời điểm Spark nhận được), tính doanh thu `quantity * unit_price * (1 - discount)` rồi tổng hợp theo cửa sổ thời gian cố định (tumbling window) 5 phút và `category`. `withWatermark("event_time", "2 minutes")` cho Spark biết chỉ cần chờ dữ liệu đến muộn tối đa 2 phút so với thời điểm sự kiện lớn nhất đã thấy; trạng thái của cửa sổ cũ hơn ngưỡng đó sẽ được giải phóng.

Tạo tệp `src/lab6/streaming_console.py`:

<!-- file: src/lab6/streaming_console.py -->
```python
"""Structured Streaming: Kafka -> cửa sổ 5 phút + watermark 2 phút -> console (Buổi 6 - CT482)."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, sum as _sum, count as _count, window

spark = SparkSession.builder.appName("RetailStreamingConsole").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = ("transaction_id STRING, customer_id STRING, product_id STRING, "
          "category STRING, quantity INT, unit_price DOUBLE, discount DOUBLE, "
          "store_id STRING, region STRING, ts STRING")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "retail-transactions")
       .option("startingOffsets", "latest")
       .load())

parsed = (raw.selectExpr("CAST(value AS STRING) AS json_str")
          .select(from_json(col("json_str"), schema).alias("d"))
          .select("d.*")
          .withColumn("event_time", col("ts").cast("timestamp"))
          .withColumn("revenue", expr("quantity * unit_price * (1 - discount)")))

agg = (parsed
       .withWatermark("event_time", "2 minutes")
       .groupBy(window(col("event_time"), "5 minutes"), col("category"))
       .agg(_count("*").alias("n_txn"),
            _sum("revenue").alias("total_revenue"),
            _sum("quantity").alias("total_qty")))

query = (agg.writeStream
         .outputMode("update")
         .format("console")
         .option("truncate", "false")
         .option("numRows", 20)
         .trigger(processingTime="10 seconds")
         .start())

query.awaitTermination()
```

Những điểm khác với tài liệu cũ (Spark 3.4, Scala 2.12):

- Connector Kafka cho Spark 4.1.3/Scala 2.13 đã có sẵn trong `/opt/spark/jars`, **không cần** `--packages`. Nếu vẫn ghi `--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3` thì Spark tải lại từ Maven Central (cần Internet). Lệnh cũ `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1` tải thêm 11 jar (khoảng 56 MB) của Scala 2.12/Spark 3 — trộn jar của hai phiên bản Scala là cấu hình không được hỗ trợ, hãy bỏ hẳn `--packages`.
- `startingOffsets = latest`: job chỉ xử lý dữ liệu phát sau khi khởi động, tránh gộp hàng nghìn thông điệp cũ của Phần 2 vào kết quả.
- Mỗi Spark Worker chỉ có 2 core và 1 GB RAM. `--total-executor-cores 2` giới hạn job dùng 2 trong 4 core của cụm, để còn chỗ cho job thứ hai. `spark.sql.shuffle.partitions=3` thay cho mặc định 200, phù hợp với lượng dữ liệu nhỏ.

Terminal A — chạy job (giữ terminal này mở; job khởi động và in `Batch: 0` sau khoảng 25 giây trên máy 2 nhân):

<!-- shell: Terminal codespace (terminal A) -->
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
  --total-executor-cores 2 --conf spark.sql.shuffle.partitions=3 \
  /src/lab6/streaming_console.py
```

Khi terminal A in `Batch: 0` (bảng rỗng), sang terminal B phát dữ liệu liên tục khoảng 80 giây:

<!-- shell: Terminal codespace (terminal B) -->
```bash
docker exec -it spark-master python3 /src/lab6/producer_retail.py --sleep 0.1 --limit 800 --skip 4500
```

**Kết quả mong đợi** ở terminal A: cứ 10 giây một micro-batch; ở chế độ `update`, mỗi batch chỉ in những cửa sổ có giá trị thay đổi, và giá trị là **tổng lũy kế** của cửa sổ đó.

```text
WARN ResolveWriteToStream: Temporary checkpoint location created which is deleted normally when the query didn't fail: /tmp/temporary-eed6eb63-8db7-4134-9bdc-8de3741c2370. ...
WARN ResolveWriteToStream: spark.sql.adaptive.enabled is not supported in streaming DataFrames/Datasets and will be disabled.
WARN MicroBatchExecution: Disabling AQE since AQE is not supported in stateful workloads.
-------------------------------------------
Batch: 0
-------------------------------------------
+------+--------+-----+-------------+---------+
|window|category|n_txn|total_revenue|total_qty|
+------+--------+-----+-------------+---------+
+------+--------+-----+-------------+---------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------------+----------+-----+-------------+---------+
|window                                    |category  |n_txn|total_revenue|total_qty|
+------------------------------------------+----------+-----+-------------+---------+
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|thoi_trang|16   |7.151771E8   |55       |
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|the_thao  |15   |6.311049E8   |41       |
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|sach      |12   |3.403431E8   |31       |
...
-------------------------------------------
Batch: 9
-------------------------------------------
+------------------------------------------+----------+-----+-------------+---------+
|window                                    |category  |n_txn|total_revenue|total_qty|
+------------------------------------------+----------+-----+-------------+---------+
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|sach      |109  |3.7385363E9  |322      |
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|thoi_trang|107  |3.5497915E9  |326      |
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|the_thao  |113  |3.88208245E9 |338      |
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|dien_tu   |122  |4.48006865E9 |384      |
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|gia_dung  |123  |3.97481515E9 |341      |
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|me_va_be  |120  |4.418457E9   |371      |
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|thuc_pham |106  |3.34473935E9 |295      |
+------------------------------------------+----------+-----+-------------+---------+
```

Ba dòng `WARN` ở đầu là bình thường: sink console không có `checkpointLocation` nên Spark tạo checkpoint tạm; AQE (Adaptive Query Execution) không hỗ trợ truy vấn streaming có trạng thái nên bị tắt. Khi producer dừng, các batch tiếp theo in bảng rỗng. Tổng `n_txn` của 7 danh mục ở batch cuối bằng đúng 800.

> 💡 Producer ghi `ts` theo giờ UTC (`...T16:30:00+00:00`), còn cửa sổ được hiển thị theo múi giờ của các container Spark (`Asia/Ho_Chi_Minh`, UTC+7), vì vậy cửa sổ `23:30–23:35` chứa các sự kiện lúc 16:30–16:35 UTC. Terminal codespace cũng dùng giờ UTC (lệnh `date`). Phép `cast("timestamp")` hiểu được hậu tố múi giờ `+00:00`.

Trong lúc job chạy, mở Spark Master (cổng 8080, lấy đường dẫn bằng `lab ui`): ứng dụng `RetailStreamingConsole` ở trạng thái RUNNING với 2 core, 768 MB mỗi executor. Mở Spark UI (cổng 4040) → tab **Structured Streaming** → chọn query để xem biểu đồ Input Rate và Process Rate (khoảng 10 bản ghi/giây với `--sleep 0.1`). Nếu không mở được trình duyệt, kiểm tra nhanh từ terminal C:

<!-- shell: Terminal codespace (terminal C) -->
```bash
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:4040/StreamingQuery/
```

**Kết quả mong đợi** (mã HTTP 200: Spark UI của job đang phục vụ ở cổng 4040 của codespace):

```text
200
```

Dừng job bằng Ctrl+C ở terminal A trước khi sang bước sau (vài dòng `ERROR ... Py4JError` in ra khi dừng là bình thường).

> ⚠️ Spark 4 bật chế độ ANSI SQL mặc định: phép ép kiểu không hợp lệ (ví dụ `CAST('abc' AS INT)`) làm job dừng với `[CAST_INVALID_INPUT]` thay vì trả về NULL như Spark 3. Với dữ liệu streaming không kiểm soát được, nên dùng `try_cast`/`try_to_timestamp` cho các trường có thể bẩn.

### Bước 10: Ghi kết quả ra Parquet trên HDFS với checkpoint

Thay sink console bằng sink Parquet. Sink tệp chỉ hỗ trợ chế độ `append`; với phép tổng hợp có watermark, `append` chỉ ghi một cửa sổ **một lần duy nhất, khi cửa sổ đã đóng** (watermark vượt qua `window.end`). Với cửa sổ 5 phút và watermark 2 phút, cửa sổ `23:35–23:40` được ghi ra khi đã thấy sự kiện có thời điểm sau 23:42, tức khoảng 2–3 phút sau khi cửa sổ kết thúc. `checkpointLocation` lưu offset Kafka đã xử lý và trạng thái của các cửa sổ đang mở.

Tạo tệp `src/lab6/streaming_parquet.py`:

<!-- file: src/lab6/streaming_parquet.py -->
```python
"""Structured Streaming: Kafka -> cửa sổ 5 phút + watermark -> Parquet trên HDFS, có checkpoint (Buổi 6)."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, sum as _sum, count as _count, window

BASE = "hdfs://namenode:9000/user/student/retail_stream"

spark = SparkSession.builder.appName("RetailStreamingParquet").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = ("transaction_id STRING, customer_id STRING, product_id STRING, "
          "category STRING, quantity INT, unit_price DOUBLE, discount DOUBLE, "
          "store_id STRING, region STRING, ts STRING")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "retail-transactions")
       .option("startingOffsets", "latest")
       .load())

parsed = (raw.selectExpr("CAST(value AS STRING) AS json_str")
          .select(from_json(col("json_str"), schema).alias("d"))
          .select("d.*")
          .withColumn("event_time", col("ts").cast("timestamp"))
          .withColumn("revenue", expr("quantity * unit_price * (1 - discount)")))

agg = (parsed
       .withWatermark("event_time", "2 minutes")
       .groupBy(window(col("event_time"), "5 minutes"), col("category"))
       .agg(_count("*").alias("n_txn"),
            _sum("revenue").alias("total_revenue"),
            _sum("quantity").alias("total_qty")))

query = (agg.writeStream
         .outputMode("append")          # chỉ ghi cửa sổ đã "đóng" (watermark > window.end)
         .format("parquet")
         .option("path", f"{BASE}/parquet")
         .option("checkpointLocation", f"{BASE}/chk_parquet")
         .trigger(processingTime="15 seconds")
         .start())

query.awaitTermination()
```

Tạo thư mục HDFS (thường HDFS đã thoát safe mode sau khi khởi động; nếu gặp `Name node is in safe mode`, chạy thêm `docker exec -it namenode hdfs dfsadmin -safemode leave`):

<!-- shell: Terminal codespace -->
```bash
docker exec -it namenode hdfs dfs -mkdir -p /user/student/retail_stream
```

Terminal A — chạy job; terminal B — phát dữ liệu liên tục khoảng 10 phút (3000 bản ghi, 5 bản ghi/giây):

<!-- shell: Terminal codespace (terminal A) -->
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
  --total-executor-cores 2 --conf spark.sql.shuffle.partitions=3 \
  /src/lab6/streaming_parquet.py
```

<!-- shell: Terminal codespace (terminal B) -->
```bash
docker exec -it spark-master python3 /src/lab6/producer_retail.py --sleep 0.2 --limit 3000 --skip 6000
```

Sau khoảng 8–10 phút (đủ để ít nhất một cửa sổ 5 phút đóng hẳn), kiểm tra thư mục kết quả và checkpoint:

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it namenode hdfs dfs -ls /user/student/retail_stream/parquet | head -5
docker exec -it namenode hdfs dfs -ls /user/student/retail_stream/chk_parquet
docker exec -it namenode hdfs dfs -ls /user/student/retail_stream/chk_parquet/state/0
```

**Kết quả mong đợi** (kiểm thử: job chạy 23:33:45–23:45:16, producer chạy 23:34–23:44):

```text
Found 47 items
drwxr-xr-x   - root supergroup          0 2026-11-05 23:44 /user/student/retail_stream/parquet/_spark_metadata
-rw-r--r--   2 root supergroup        883 2026-11-05 23:34 /user/student/retail_stream/parquet/part-00000-08dec983-1bf7-47f2-81d1-f734fe1450d3-c000.snappy.parquet
-rw-r--r--   2 root supergroup        883 2026-11-05 23:39 /user/student/retail_stream/parquet/part-00000-21faa91a-6a9d-466f-bddd-0112ff297bfe-c000.snappy.parquet
-rw-r--r--   2 root supergroup        883 2026-11-05 23:34 /user/student/retail_stream/parquet/part-00000-239f79fa-b108-440b-ac03-69181155398a-c000.snappy.parquet
Found 5 items
drwxr-xr-x   - root supergroup          0 2026-11-05 23:44 /user/student/retail_stream/chk_parquet/commits
-rw-r--r--   2 root supergroup         45 2026-11-05 23:33 /user/student/retail_stream/chk_parquet/metadata
drwxr-xr-x   - root supergroup          0 2026-11-05 23:44 /user/student/retail_stream/chk_parquet/offsets
drwxr-xr-x   - root supergroup          0 2026-11-05 23:33 /user/student/retail_stream/chk_parquet/sources
drwxr-xr-x   - root supergroup          0 2026-11-05 23:33 /user/student/retail_stream/chk_parquet/state
Found 4 items
drwxr-xr-x   - root supergroup          0 2026-11-05 23:44 /user/student/retail_stream/chk_parquet/state/0/0
drwxr-xr-x   - root supergroup          0 2026-11-05 23:44 /user/student/retail_stream/chk_parquet/state/0/1
drwxr-xr-x   - root supergroup          0 2026-11-05 23:44 /user/student/retail_stream/chk_parquet/state/0/2
drwxr-xr-x   - root supergroup          0 2026-11-05 23:33 /user/student/retail_stream/chk_parquet/state/0/_metadata
```

- Checkpoint gồm `offsets/` (offset Kafka dự định xử lý ở mỗi batch), `commits/` (batch đã hoàn tất), `sources/`, `metadata` và `state/0/{0,1,2}` (trạng thái cửa sổ, chia theo 3 shuffle partition).
- `_spark_metadata/` là nhật ký của sink tệp: chỉ các tệp được ghi trong nhật ký này mới được coi là dữ liệu hợp lệ, nhờ đó sink Parquet đạt ngữ nghĩa *exactly-once*.
- Trong lần kiểm thử, 41 trong 46 tệp chỉ có 883 byte: đó là các tệp Parquet **rỗng** (chỉ có schema) do mỗi micro-batch không đóng cửa sổ nào vẫn ghi ra tệp. Chỉ 5 tệp khoảng 2 KB chứa dữ liệu. Đây là ví dụ điển hình của *vấn đề tệp nhỏ* (small files) trên HDFS khi ghi streaming với trigger ngắn. Có thể đếm nhanh bằng `docker exec -it namenode hdfs dfs -ls /user/student/retail_stream/parquet | awk 'NR>1{print $5}' | sort | uniq -c`.

Dừng job (Ctrl+C ở terminal A) sau khi đã kiểm tra.

### Bước 11: Đọc lại dữ liệu Parquet

Mở PySpark shell trong `spark-master` (terminal codespace hỗ trợ đầy đủ chế độ tương tác của `-it`):

<!-- shell: Terminal codespace -->
```bash
docker exec -it spark-master pyspark --master spark://spark-master:7077
```

<!-- shell: Python (pyspark) -->
```python
df = spark.read.parquet("hdfs://namenode:9000/user/student/retail_stream/parquet")
df.printSchema()
df.orderBy("window", "category").show(20, truncate=False)
df.groupBy("window").sum("n_txn").orderBy("window").show(truncate=False)
```

**Kết quả mong đợi:**

```text
root
 |-- window: struct (nullable = false)
 |    |-- start: timestamp (nullable = true)
 |    |-- end: timestamp (nullable = true)
 |-- category: string (nullable = true)
 |-- n_txn: long (nullable = false)
 |-- total_revenue: double (nullable = true)
 |-- total_qty: long (nullable = true)

+------------------------------------------+----------+-----+-------------+---------+
|window                                    |category  |n_txn|total_revenue|total_qty|
+------------------------------------------+----------+-----+-------------+---------+
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|dien_tu   |40   |1.07912365E9 |115      |
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|gia_dung  |41   |1.64496085E9 |132      |
...
|{2026-11-05 23:35:00, 2026-11-05 23:40:00}|dien_tu   |200  |6.49878115E9 |590      |
|{2026-11-05 23:35:00, 2026-11-05 23:40:00}|gia_dung  |211  |7.5782074E9  |623      |
...
+------------------------------------------+----------+-----+-------------+---------+

+------------------------------------------+----------+
|window                                    |sum(n_txn)|
+------------------------------------------+----------+
|{2026-11-05 23:30:00, 2026-11-05 23:35:00}|268       |
|{2026-11-05 23:35:00, 2026-11-05 23:40:00}|1496      |
+------------------------------------------+----------+
```

Cửa sổ `23:35–23:40` có 1496 giao dịch, xấp xỉ 5 phút × 5 bản ghi/giây. Cửa sổ `23:40–23:45` **không có** trong Parquet dù producer đã gửi dữ liệu cho nó: job dừng trước khi watermark vượt 23:45 (cần sự kiện sau 23:47), nên cửa sổ chưa đóng và chưa bao giờ được ghi ở chế độ `append`. Trạng thái dang dở của cửa sổ này vẫn nằm trong `chk_parquet/state/` (xem Bài tập mở rộng 3).

Thoát shell bằng `exit()`.

> ⚠️ PySpark shell không giới hạn số core nên chiếm cả 4 core của cụm (thấy `(0 + 4) / 4` trên thanh tiến trình). Phải `exit()` trước khi chạy job ở Bước 12, nếu không job mới sẽ báo `Initial job has not accepted any resources`. Muốn giữ shell song song với job streaming, mở shell bằng `pyspark --master spark://spark-master:7077 --total-executor-cores 2`.

## PHẦN 4: UPSERT, DỮ LIỆU ĐẾN MUỘN VÀ KHÔI PHỤC TỪ CHECKPOINT

### Bước 12: Dùng foreachBatch để upsert vào bảng đích

Sink Parquet ở chế độ `append` chỉ ghi mỗi cửa sổ một lần. Khi muốn bảng đích luôn phản ánh giá trị mới nhất của cửa sổ (kể cả khi được tính lại do dữ liệu đến muộn), dùng chế độ `update` cùng `foreachBatch`: mỗi micro-batch, Spark giao cho hàm của bạn một DataFrame chứa các cửa sổ vừa thay đổi; hàm thực hiện **upsert** (chèn nếu chưa có, ghi đè nếu đã có) theo khóa nghiệp vụ `(window_start, category)`. Môi trường không có Delta Lake hay CSDL quan hệ riêng, nên bảng đích là SQLite đặt trên driver (`/tmp/lab6/retail_agg.db` trong container `spark-master`). Vòng lặp cuối chương trình đọc `query.recentProgress` để in số bản ghi vào, số dòng bị loại vì quá watermark và giá trị watermark của từng batch.

Tạo tệp `src/lab6/streaming_upsert.py` gồm hai phần nối tiếp nhau. Phần 1 — đọc Kafka và tổng hợp (giống Bước 9):

<!-- file: src/lab6/streaming_upsert.py (phần 1/2) -->
```python
"""Structured Streaming + foreachBatch: upsert kết quả cửa sổ vào SQLite (Buổi 6 - CT482).

Bảng đích: /tmp/lab6/retail_agg.db (trong container spark-master, nơi chạy driver).
Khóa nghiệp vụ: (window_start, category).
"""
import os
import sqlite3
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, sum as _sum, count as _count, window

DB = "/tmp/lab6/retail_agg.db"
CHECKPOINT = "hdfs://namenode:9000/user/student/retail_stream/chk_upsert"

spark = SparkSession.builder.appName("RetailUpsert").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = ("transaction_id STRING, customer_id STRING, product_id STRING, "
          "category STRING, quantity INT, unit_price DOUBLE, discount DOUBLE, "
          "store_id STRING, region STRING, ts STRING")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "retail-transactions")
       .option("startingOffsets", "latest")   # chỉ có tác dụng ở lần chạy ĐẦU; lần sau đọc offset từ checkpoint
       .load())

parsed = (raw.selectExpr("CAST(value AS STRING) AS json_str")
          .select(from_json(col("json_str"), schema).alias("d"))
          .select("d.*")
          .withColumn("event_time", col("ts").cast("timestamp"))
          .withColumn("revenue", expr("quantity * unit_price * (1 - discount)")))

agg = (parsed
       .withWatermark("event_time", "2 minutes")
       .groupBy(window(col("event_time"), "5 minutes"), col("category"))
       .agg(_count("*").alias("n_txn"),
            _sum("revenue").alias("total_revenue"),
            _sum("quantity").alias("total_qty")))
```

Phần 2 — hàm upsert, khởi động query với checkpoint riêng và vòng lặp theo dõi tiến trình:

<!-- file: src/lab6/streaming_upsert.py (phần 2/2) -->
```python
def upsert_batch(batch_df, batch_id):
    rows = batch_df.collect()          # kết quả tổng hợp nhỏ (vài chục dòng) nên collect về driver được
    if not rows:
        return
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS agg_result (
            window_start TEXT, window_end TEXT, category TEXT,
            n_txn INTEGER, total_revenue REAL, total_qty INTEGER,
            last_batch INTEGER,
            PRIMARY KEY (window_start, category))
    """)
    for r in rows:
        cur.execute("""
            INSERT INTO agg_result VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(window_start, category) DO UPDATE SET
                window_end=excluded.window_end, n_txn=excluded.n_txn,
                total_revenue=excluded.total_revenue, total_qty=excluded.total_qty,
                last_batch=excluded.last_batch
        """, (str(r["window"]["start"]), str(r["window"]["end"]), r["category"],
              r["n_txn"], r["total_revenue"], r["total_qty"], batch_id))
    conn.commit()
    conn.close()
    print(f"batch {batch_id}: da upsert {len(rows)} dong", flush=True)


query = (agg.writeStream
         .outputMode("update")
         .foreachBatch(upsert_batch)
         .option("checkpointLocation", CHECKPOINT)
         .trigger(processingTime="10 seconds")
         .start())

# Theo dõi tiến trình: số bản ghi vào, số dòng bị loại vì quá watermark, giá trị watermark hiện tại
last_seen = -1
while query.isActive:
    time.sleep(5)
    for p in query.recentProgress:
        if p["batchId"] <= last_seen:
            continue
        last_seen = p["batchId"]
        if p["numInputRows"] == 0:
            continue
        dropped = sum(op.get("numRowsDroppedByWatermark", 0) for op in p.get("stateOperators", []))
        print(f"  [tien trinh] batch {p['batchId']}: input={p['numInputRows']}, "
              f"bi loai do watermark={dropped}, watermark={p['eventTime'].get('watermark')}", flush=True)
```

Tạo thêm script xem bảng đích `src/lab6/xem_ket_qua.py`:

<!-- file: src/lab6/xem_ket_qua.py -->
```python
"""Xem bảng đích SQLite của streaming_upsert.py (chạy trong container spark-master)."""
import sqlite3
import sys

conn = sqlite3.connect("/tmp/lab6/retail_agg.db")
cur = conn.cursor()
print("so dong (khoa window_start, category):", cur.execute("SELECT COUNT(*) FROM agg_result").fetchone()[0])
print("tong n_txn trong bang:", cur.execute("SELECT SUM(n_txn) FROM agg_result").fetchone()[0])
print("theo cua so:")
for row in cur.execute("""SELECT window_start, COUNT(*) AS n_category, SUM(n_txn) AS n_txn,
                                 ROUND(SUM(total_revenue)) AS revenue, MAX(last_batch) AS last_batch
                          FROM agg_result GROUP BY window_start ORDER BY window_start"""):
    print("  ", row)
if len(sys.argv) > 1:   # python3 xem_ket_qua.py chitiet  -> in thêm 10 dòng mới cập nhật nhất
    for row in cur.execute("SELECT * FROM agg_result ORDER BY last_batch DESC, window_start DESC LIMIT 10"):
        print("  ", row)
conn.close()
```

Ghi lại offset cuối của topic **trước khi** khởi động job (dùng để đối chiếu ở Bước 14):

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it kafka kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic retail-transactions
```

**Kết quả mong đợi** (lần kiểm thử; tổng = 7800):

```text
retail-transactions:0:3112
retail-transactions:1:2184
retail-transactions:2:2504
```

Terminal A — chạy job upsert; terminal B — phát dữ liệu "đúng giờ" liên tục (5 bản ghi/giây, giữ chạy suốt Bước 12–13):

<!-- shell: Terminal codespace (terminal A) -->
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
  --total-executor-cores 2 --conf spark.sql.shuffle.partitions=3 \
  /src/lab6/streaming_upsert.py
```

<!-- shell: Terminal codespace (terminal B) -->
```bash
docker exec -it spark-master python3 /src/lab6/producer_retail.py --sleep 0.2 --limit 100000 --skip 10000
```

**Kết quả mong đợi** ở terminal A:

```text
batch 1: da upsert 6 dong
  [tien trinh] batch 1: input=11, bi loai do watermark=0, watermark=1970-01-01T00:00:00.000Z
batch 2: da upsert 7 dong
  [tien trinh] batch 2: input=42, bi loai do watermark=0, watermark=2026-11-05T16:44:21.315Z
batch 3: da upsert 7 dong
  [tien trinh] batch 3: input=50, bi loai do watermark=0, watermark=2026-11-05T16:44:29.808Z
...
```

Mỗi batch 10 giây nhận khoảng 50 bản ghi (5 bản ghi/giây) và upsert 7 dòng (7 danh mục của cửa sổ hiện tại; batch đầu có thể ít hơn). Watermark (in theo UTC, hậu tố `Z`) luôn chậm hơn sự kiện mới nhất 2 phút. Sau khoảng 2 phút, xem bảng đích ở terminal C:

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it spark-master python3 /src/lab6/xem_ket_qua.py
```

**Kết quả mong đợi:**

```text
so dong (khoa window_start, category): 7
tong n_txn trong bang: 550
theo cua so:
   ('2026-11-05 23:45:00', 7, 550, 19481957200.0, 12)
```

Mỗi bộ trong phần "theo cua so" là `(window_start, số danh mục, tổng giao dịch, tổng doanh thu, batch cập nhật gần nhất)`. Số dòng của bảng bằng đúng số khóa `(window_start, category)` khác nhau, dù mỗi khóa đã được ghi lại ở nhiều batch.

### Bước 13: Dữ liệu đến muộn trong và ngoài watermark

Thí nghiệm 1 — dữ liệu muộn 1 phút (trong watermark 2 phút). Để kết quả dễ quan sát, chờ đến khi đồng hồ vừa bước sang một cửa sổ 5 phút mới được khoảng 15–25 giây (ví dụ 16:50:15 UTC, gõ `date` ở terminal C để xem giờ). Khi đó các sự kiện lùi 1 phút rơi vào **cửa sổ trước** (23:45–23:50 theo giờ hiển thị), là cửa sổ không còn nhận dữ liệu đúng giờ nhưng vẫn mở vì watermark (khoảng 23:48) chưa vượt 23:50. Giữ terminal B đang chạy, ở terminal C chạy:

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it spark-master python3 /src/lab6/xem_ket_qua.py
docker exec -it spark-master python3 /src/lab6/producer_retail.py --sleep 0.05 --limit 300 --skip 200000 \
  --late-ratio 1.0 --late-minutes 1
sleep 25
docker exec -it spark-master python3 /src/lab6/xem_ket_qua.py
```

**Kết quả mong đợi** (kiểm thử bắt đầu lúc 16:50:15 UTC):

```text
so dong (khoa window_start, category): 14
tong n_txn trong bang: 1148
theo cua so:
   ('2026-11-05 23:45:00', 7, 1098, 38535308850.0, 23)
   ('2026-11-05 23:50:00', 7, 50, 1603121000.0, 24)
da gui 0 ban ghi, ts=2026-11-05T16:49:16.118+00:00
...
hoan tat phat su kien: 300 ban ghi
so dong (khoa window_start, category): 14
tong n_txn trong bang: 1648
theo cua so:
   ('2026-11-05 23:45:00', 7, 1398, 48178854300.0, 27)
   ('2026-11-05 23:50:00', 7, 250, 8771362950.0, 28)
```

Cửa sổ 23:45 tăng từ 1098 lên **1398 = 1098 + 300**: toàn bộ 300 sự kiện muộn được chấp nhận và các dòng cũ được ghi đè (cột batch cập nhật đổi từ 23 sang 27), số dòng bảng vẫn là 14 — không phát sinh bản ghi trùng khóa. Cửa sổ 23:50 tăng do dữ liệu đúng giờ từ terminal B. Ở terminal A, các batch nhận dữ liệu muộn này in `da upsert 14 dong` (cả hai cửa sổ cùng thay đổi).

Thí nghiệm 2 — dữ liệu muộn 10 phút (vượt watermark):

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it spark-master python3 /src/lab6/producer_retail.py --sleep 0.05 --limit 300 --skip 201000 \
  --late-ratio 1.0 --late-minutes 10
sleep 25
docker exec -it spark-master python3 /src/lab6/xem_ket_qua.py
```

**Kết quả mong đợi:**

```text
da gui 0 ban ghi, ts=2026-11-05T16:40:56.878+00:00
...
so ban ghi theo partition: {0: 128, 1: 91, 2: 81}
hoan tat phat su kien: 300 ban ghi
so dong (khoa window_start, category): 14
tong n_txn trong bang: 1847
theo cua so:
   ('2026-11-05 23:45:00', 7, 1398, 48178854300.0, 27)
   ('2026-11-05 23:50:00', 7, 449, 15982741050.0, 32)
```

Các sự kiện mang thời điểm 16:40–16:41 UTC (cửa sổ 23:40) **không xuất hiện** trong bảng và cửa sổ 23:45 giữ nguyên 1398: Spark đã loại chúng vì thời điểm sự kiện nhỏ hơn watermark. Ở terminal A, các batch nhận dữ liệu muộn này báo `bi loai do watermark` khác 0:

```text
  [tien trinh] batch 28: input=50, bi loai do watermark=0, watermark=2026-11-05T16:48:39.965Z
  [tien trinh] batch 29: input=111, bi loai do watermark=7, watermark=2026-11-05T16:48:50.006Z
  [tien trinh] batch 30: input=247, bi loai do watermark=7, watermark=2026-11-05T16:48:59.827Z
  [tien trinh] batch 31: input=91, bi loai do watermark=7, watermark=2026-11-05T16:49:09.852Z
  [tien trinh] batch 32: input=50, bi loai do watermark=0, watermark=2026-11-05T16:49:19.900Z
```

> 💡 Chỉ số `numRowsDroppedByWatermark` đếm số **dòng đã tổng hợp cục bộ** bị loại tại toán tử trạng thái, không phải số bản ghi Kafka gốc. Spark gộp trước (partial aggregation) 300 sự kiện muộn thành tối đa 7 dòng `(cửa sổ 23:40, category)` trong mỗi batch, nên chỉ số là 7 ở ba batch 29–31.

### Bước 14: Dừng job, đọc checkpoint và khởi động lại

Mục tiêu: chứng minh rằng khi khởi động lại với cùng `checkpointLocation`, Spark tiếp tục từ offset đã xử lý — không đọc lại từ đầu, không bỏ sót dữ liệu phát sinh trong lúc dừng, và kết quả upsert không bị cộng trùng.

Dừng producer (Ctrl+C ở terminal B), sau đó dừng job (Ctrl+C ở terminal A, hoặc từ terminal khác: `docker exec -it spark-master pkill -f streaming_upsert.py`). Xem bảng đích và checkpoint:

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it spark-master python3 /src/lab6/xem_ket_qua.py
docker exec -it namenode bash -lc 'hdfs dfs -ls -C /user/student/retail_stream/chk_upsert/offsets | sort -V | tail -3'
docker exec -it namenode bash -lc 'f=$(hdfs dfs -ls -C /user/student/retail_stream/chk_upsert/offsets | sort -V | tail -1); echo $f; hdfs dfs -cat $f; echo'
docker exec -it namenode bash -lc 'hdfs dfs -ls -C /user/student/retail_stream/chk_upsert/commits | sort -V | tail -1'
docker exec -it kafka kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic retail-transactions
```

**Kết quả mong đợi:**

```text
so dong (khoa window_start, category): 14
tong n_txn trong bang: 1847
theo cua so:
   ('2026-11-05 23:45:00', 7, 1398, 48178854300.0, 27)
   ('2026-11-05 23:50:00', 7, 449, 15982741050.0, 32)
/user/student/retail_stream/chk_upsert/offsets/31
/user/student/retail_stream/chk_upsert/offsets/32
/user/student/retail_stream/chk_upsert/offsets/33
/user/student/retail_stream/chk_upsert/offsets/33
v1
{"batchWatermarkMs":1793897369918,"batchTimestampMs":1793897500002,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",...,"spark.sql.shuffle.partitions":"3",...}}
{"retail-transactions":{"0":3995,"1":2823,"2":3166}}
/user/student/retail_stream/chk_upsert/commits/32
retail-transactions:0:3995
retail-transactions:1:2823
retail-transactions:2:3166
```

- Tệp offset của một batch có ba dòng: phiên bản định dạng `v1`, siêu dữ liệu của batch (watermark, thời điểm batch, cấu hình không được thay đổi giữa các lần chạy — ví dụ `spark.sql.shuffle.partitions=3`) và **offset Kafka của từng partition** mà batch đó đọc tới.
- Dùng `sort -V` vì `hdfs dfs -ls` sắp tên tệp theo thứ tự chữ (`10` đứng trước `2`).
- Trong lần kiểm thử, job bị dừng giữa chừng batch 33: `offsets/33` đã được ghi (Spark đã *lên kế hoạch* đọc tới tổng offset 3995 + 2823 + 3166 = 9984) nhưng chưa có `commits/33` và bảng đích chỉ có `last_batch` tới 32. Batch cuối **đã hoàn tất** là 32, với offset (đọc bằng `hdfs dfs -cat .../offsets/32`) là `{"0":3980,"1":2812,"2":3155}`, tổng 9947. Nếu `offsets` và `commits` của bạn cùng số thì batch cuối đã hoàn tất.
- Đối chiếu: job bắt đầu ở tổng offset 7800 (Bước 12), đã hoàn tất tới 9947, tức 2147 sự kiện; trừ 300 sự kiện bị loại vì quá watermark còn **1847** — đúng bằng `tong n_txn` trong bảng. Kafka hiện có 9984 thông điệp, tức 37 thông điệp thuộc batch 33 chưa hoàn tất.

Trong lúc job dừng, phát thêm 500 giao dịch, rồi khởi động lại job **với cùng lệnh** (cùng checkpoint):

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it spark-master python3 /src/lab6/producer_retail.py --sleep 0.02 --limit 500 --skip 202000
docker exec -it kafka kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic retail-transactions
```

<!-- shell: Terminal codespace (terminal A) -->
```bash
docker exec -it spark-master spark-submit --master spark://spark-master:7077 \
  --total-executor-cores 2 --conf spark.sql.shuffle.partitions=3 \
  /src/lab6/streaming_upsert.py
```

Sau khoảng 45 giây, xem bảng đích ở terminal C:

<!-- shell: Terminal codespace (terminal C) -->
```bash
docker exec -it spark-master python3 /src/lab6/xem_ket_qua.py
```

**Kết quả mong đợi:**

```text
so ban ghi theo partition: {0: 218, 1: 143, 2: 139}
hoan tat phat su kien: 500 ban ghi
retail-transactions:0:4213
retail-transactions:1:2966
retail-transactions:2:3305

(terminal A)
batch 33: da upsert 7 dong
batch 34: da upsert 7 dong
  [tien trinh] batch 33: input=37, bi loai do watermark=0, watermark=2026-11-05T16:49:29.918Z
  [tien trinh] batch 34: input=500, bi loai do watermark=0, watermark=2026-11-05T16:49:37.331Z

so dong (khoa window_start, category): 14
tong n_txn trong bang: 2384
theo cua so:
   ('2026-11-05 23:45:00', 7, 1398, 48178854300.0, 27)
   ('2026-11-05 23:50:00', 7, 986, 33611150800.0, 34)
```

- Spark **chạy lại batch 33** với đúng phạm vi offset đã ghi trong `offsets/33` (input=37 = 9984 − 9947), vì batch này chưa có trong `commits/`; sau đó batch 34 đọc 500 sự kiện phát trong lúc dừng. Số batch tiếp tục, không bắt đầu lại từ 0.
- `tong n_txn` tăng từ 1847 lên 2384, tức đúng **537 = 37 + 500** sự kiện (offset 9947 → 10 484). Không có sự kiện nào bị đọc lại hay bỏ sót.
- Cửa sổ 23:45 giữ nguyên 1398 dù khởi động lại: trạng thái các cửa sổ được khôi phục từ `state/` trong checkpoint, nên upsert ghi đè đúng giá trị thay vì bắt đầu đếm lại.

> 💡 Spark **không** commit offset vào consumer group của Kafka; vị trí đọc nằm trong checkpoint. Vì vậy `kafka-consumer-groups.sh --list` chỉ thấy `retail-group` của Phần 2, không có group nào của Spark, và không dùng được lệnh `--describe` để xem lag của job streaming — hãy xem tab Structured Streaming hoặc so sánh offset trong checkpoint với `kafka-get-offsets.sh` như trên.

> ⚠️ Không đổi logic tổng hợp (độ dài cửa sổ, cột group by, schema kết quả) khi tái sử dụng checkpoint cũ: trạng thái đã lưu không còn khớp với truy vấn mới. Riêng `spark.sql.shuffle.partitions` được đọc lại từ checkpoint (dòng `conf` trong tệp offset), nên đổi giá trị trên dòng lệnh không có tác dụng. Muốn chạy lại từ đầu với logic mới, xóa checkpoint (Bước 15) hoặc dùng thư mục checkpoint khác.

Dừng job (Ctrl+C ở terminal A) khi đã ghi nhận kết quả.

### Bước 15: Dọn dẹp

Dừng mọi job streaming và producer (Ctrl+C). Xóa kết quả và checkpoint nếu muốn làm lại bài từ đầu, sau đó tắt Kafka để giải phóng RAM:

<!-- shell: Terminal codespace -->
```bash
docker exec -it namenode hdfs dfs -rm -r -skipTrash /user/student/retail_stream
docker exec -it spark-master rm -rf /tmp/lab6
lab stop-extras
```

**Kết quả mong đợi:**

```text
Deleted /user/student/retail_stream
 Container kafka  Stopping
 Container kafka  Stopped
```

> 💡 `lab stop-extras` giữ nguyên dữ liệu topic trong volume `kafka`; lần sau chỉ cần `lab kafka`. Lệnh `lab reset` sẽ xóa cả volume (mất toàn bộ topic và dữ liệu HDFS).

## Kết thúc buổi

**Tắt dịch vụ bổ sung** (nếu chưa làm ở Bước 15) và kiểm tra RAM trở về mức cụm lõi:

<!-- shell: Terminal codespace -->
```bash
lab stop-extras
lab status
```

**Kết quả mong đợi:**

```text
NAME              STATUS
datanode1         Up 33 minutes (healthy)
...
spark-master      Up 33 minutes (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 2.5 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

**Lưu bài làm lên GitHub** (chỉ thư mục `src/lab6`, **không** dùng `git add .` để tránh đưa `.venv/` và dữ liệu lên):

<!-- shell: Terminal codespace -->
```bash
git add src/lab6
git commit -m "Buoi 6: Kafka + Structured Streaming"
git push
```

**Dừng codespace** để không tốn quota: bấm vào tên codespace ở góc dưới bên trái VS Code → *Stop Current Codespace*, hoặc vào https://github.com/codespaces → dấu `...` → *Stop codespace*. Dữ liệu HDFS, topic Kafka và tệp trong repo được giữ lại cho buổi sau.

## Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `TopicExistsException: Topic 'retail-transactions' already exists.` | Chạy lại lệnh `--create` (topic được giữ qua các lần mở codespace) | Bỏ qua và dùng `--describe`; hoặc `kafka-topics.sh --delete --topic retail-transactions` rồi tạo lại |
| Producer treo 60 giây rồi báo `KafkaTimeoutError: Failed to update metadata after 60.0 secs.` | Topic chưa tồn tại hoặc sai tên (broker tắt tự tạo topic); hoặc sai địa chỉ bootstrap; hoặc chưa `lab kafka` | Tạo topic trước (Bước 2); trong container dùng `kafka:9092`, trong terminal codespace dùng `localhost:9092`; `lab status` phải thấy `kafka (healthy)` |
| `pip: command not found` hoặc `python3: command not found` trong terminal codespace | Image codespace (Ubuntu 24.04) không cài Python | Chạy producer trong `spark-master` như hướng dẫn; hoặc `sudo apt-get install -y python3-venv python3-pip` rồi dùng `.venv` (Bước 4) |
| `error: externally-managed-environment` khi `pip install kafka-python` | Ubuntu 24.04 chặn cài gói pip toàn hệ thống (PEP 668) | `python3 -m venv .venv && . .venv/bin/activate && pip install kafka-python` |
| `Error: Could not find or load main class kafka.tools.GetOffsetShell` | Lệnh của Kafka cũ, đã bị gỡ trong Kafka 4 | Dùng `kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic ...` |
| `Option --property is deprecated` / `Option --consumer-property is deprecated` | Kafka 4 đổi tên tùy chọn của console consumer | Dùng `--formatter-property` và `--command-property` (tên cũ vẫn chạy) |
| Dòng `The consumer rebalance protocol (KIP-848) is production-ready!...` | Thông báo thông tin của Kafka 4 | Không phải lỗi, bỏ qua |
| Tắt một consumer nhưng `--describe --members` vẫn thấy nó; consumer mới vào group nhận 0 partition | Static membership (`group.instance.id`): coordinator chờ `session.timeout.ms` = 45 giây | Chờ khoảng 1 phút; hoặc bỏ `group.instance.id` để dùng dynamic membership |
| Consumer group mới không in gì dù topic có dữ liệu | Group mới mặc định đọc từ cuối (`latest`) | Thêm `--from-beginning`, hoặc phát thêm dữ liệu sau khi consumer đã chạy |
| `spark-submit` dừng khoảng 15 giây ở `:: resolution report ::` và tải 11 jar | Dùng lệnh `--packages ..._2.12:3.4.1` của môi trường cũ (Scala 2.12, Spark 3) | Bỏ `--packages` (connector 2.13/4.1.3 đã có sẵn trong `/opt/spark/jars`); không trộn jar Scala 2.12 vào Spark 4 |
| Job thứ hai báo `Initial job has not accepted any resources` | Job trước hoặc PySpark shell đã chiếm hết 4 core của cụm, hoặc `--executor-memory` lớn hơn 1 GB | Thêm `--total-executor-cores 2` cho job streaming và cho `pyspark`; giữ `--executor-memory` ≤ 768m; dừng job/shell cũ |
| `[CAST_INVALID_INPUT]` làm job streaming dừng | Spark 4 bật ANSI SQL, dữ liệu có giá trị không ép kiểu được | Dùng `try_cast`, `try_to_timestamp`; hoặc tạm `--conf spark.sql.ansi.enabled=false` |
| Thư mục Parquet chỉ có tệp 883 byte, không có cửa sổ nào | Chế độ `append` chỉ ghi cửa sổ đã đóng; job chạy chưa đủ lâu hoặc producer đã dừng nên watermark không tăng | Chạy producer liên tục ít nhất 8–10 phút; các tệp nhỏ rỗng là bình thường |
| Khi dừng job bằng Ctrl+C/`pkill` thấy `ERROR ... FOREACH_BATCH_USER_FUNCTION_ERROR` hoặc `Py4JError` | Tiến trình Python của driver bị ngắt giữa một batch | Bình thường; batch dở dang chưa commit sẽ được chạy lại khi khởi động lại với cùng checkpoint (Bước 14) |
| `Name node is in safe mode` khi ghi HDFS | NameNode vừa khởi động (mới mở codespace) | Chờ 30–60 giây hoặc `docker exec -it namenode hdfs dfsadmin -safemode leave` |
| Không mở được http://localhost:8080 hay http://localhost:4040 trên trình duyệt | Trình duyệt chạy trên máy bạn, không phải trong codespace | Dùng đường dẫn `https://<codespace>-<cổng>.app.github.dev` do `lab ui` in ra hoặc tab PORTS → quả địa cầu |
| Tab PORTS không có cổng 4040, hoặc mở ra báo lỗi 502 | Chưa có job Spark nào đang chạy (driver chưa mở cổng 4040) | Chạy job ở Bước 9 trước rồi mở lại; xem ứng dụng đã dừng ở Spark History (18080) |
| Liên kết trong Web UI dạng `http://spark-master:4040` hoặc `http://nodemanager1:...` không mở được | Tên máy nội bộ trong mạng Docker | Dùng đường dẫn tương ứng từ `lab ui` |
| Không mở được Spark UI của job thứ hai (cổng 4041) | Codespace tạo trước 06/11/2026 chỉ chuyển tiếp cổng 4040 | Xem job thứ hai qua Spark Master (8080) hoặc chạy lần lượt từng job |
| Codespace tự dừng giữa lúc chờ cửa sổ đóng (Bước 10, 12–14) | Quá 30 phút không thao tác | Mở lại codespace, `lab kafka`, chạy lại job với cùng lệnh: checkpoint trên HDFS giúp job tiếp tục; nếu muốn làm lại sạch thì xóa `/user/student/retail_stream` |
| Job Spark chậm hẳn, terminal giật, container bị khởi động lại (`lab status` thấy `Restarting`) | Hết RAM: đã bật thêm Hive/HBase/MongoDB từ buổi trước, hoặc chạy nhiều job cùng lúc | `lab stop-extras` rồi `lab kafka`; chạy tối đa một job streaming và một producer; kiểm tra bằng `lab status` (dòng RAM các container nên dưới 5 GB) |

## Câu hỏi kiểm tra

1. Vì sao producer dùng `store_id` làm khóa? Điều gì xảy ra với thứ tự thông điệp của một cửa hàng nếu gửi không có khóa?
   *Gợi ý: cùng khóa → cùng partition → giữ thứ tự trong partition; không khóa thì các thông điệp được rải sang nhiều partition, thứ tự giữa chúng không còn được đảm bảo.*
2. Topic có 3 partition. Nếu group có 4 consumer thì consumer thứ tư làm gì? Muốn tăng mức song song lên 6 phải làm gì?
   *Gợi ý: consumer thứ tư rảnh (không có partition); phải tăng số partition (`kafka-topics.sh --alter --partitions 6`) — lưu ý ảnh hưởng tới ánh xạ khóa → partition.*
3. Ở Bước 7, vì sao `member-3` vẫn còn trong group khoảng 45 giây sau khi tắt? Ưu và nhược điểm của static membership?
4. Consumer lag là gì? Từ bảng `--describe` ở Bước 8, giải thích vì sao tổng LAG bằng đúng số bản ghi vừa phát.
5. Phân biệt *event time* và *processing time*. Trong bài, cột nào là event time và vì sao cửa sổ phải dựa trên event time?
6. Với cửa sổ 5 phút và watermark 2 phút, một cửa sổ (ví dụ `23:35–23:40` trong lần kiểm thử) được ghi ra Parquet ở chế độ `append` vào khi nào? Vì sao cửa sổ cuối cùng (ví dụ `23:40–23:45`) không có trong kết quả ở Bước 11?
7. So sánh ba chế độ `append`, `update`, `complete` khi có tổng hợp theo cửa sổ. Vì sao sink Parquet không dùng được `update`?
8. Ở Bước 13, vì sao dữ liệu muộn 1 phút được chấp nhận còn dữ liệu muộn 10 phút bị loại? Vì sao `numRowsDroppedByWatermark` là 7 chứ không phải 300?
9. Trong checkpoint có những thư mục nào và mỗi thư mục lưu gì? Dựa vào số liệu Bước 14 của chính bạn (offset trong `offsets/` và `commits/`, offset Kafka, tổng trong bảng), chứng minh job không mất và không đếm trùng dữ liệu sau khi khởi động lại.
10. `foreachBatch` + upsert theo khóa giúp bảng đích đạt kết quả đúng ngay cả khi một batch bị xử lý lại sau sự cố (như batch 33 trong lần kiểm thử). Giải thích vì sao thao tác upsert có tính *idempotent*.

## Bài tập mở rộng

1. Thêm `store_id` vào khóa tổng hợp và khóa chính SQLite (`window_start, category, store_id`). Đo số dòng của bảng đích sau 5 phút và so sánh với phiên bản chỉ dùng `category`.
2. Đổi sang cửa sổ trượt (sliding window) 10 phút, bước trượt 5 phút: `window(col("event_time"), "10 minutes", "5 minutes")`. Mỗi sự kiện được đếm vào bao nhiêu cửa sổ? Kiểm chứng bằng tổng `n_txn`.
3. Chạy lại `streaming_parquet.py` với cùng checkpoint của Bước 10 trong khi producer tiếp tục phát dữ liệu. Cửa sổ còn dang dở lúc dừng có được ghi ra không, và với giá trị nào? Giải thích dựa trên trạng thái trong `state/`.
4. Thử consumer với giao thức rebalance mới của Kafka 4: `--command-property group.protocol=consumer` (bỏ `group.instance.id`). So sánh thời gian và cách chia partition khi thêm/bớt thành viên với giao thức `classic`.
5. Đặt `maxOffsetsPerTrigger=100` cho nguồn Kafka, dừng job, phát 1000 bản ghi rồi khởi động lại. Quan sát số batch cần để xử lý hết phần tồn và giá trị `input` của từng batch.
6. Thay SQLite bằng MongoDB (bật bằng `lab nosql`, dùng `pymongo` với `update_one(..., upsert=True)` trong `foreachBatch`). Lưu ý RAM: tắt bớt dịch vụ không dùng bằng `lab stop-extras` rồi bật lại `lab kafka` và `lab nosql`.

## Yêu cầu nộp bài và tiêu chí chấm

Nộp một tệp nén `MSSV_buoi6.zip` gồm: thư mục `src/lab6/` (mọi script đã sửa; đồng thời đã `git push` lên repo codespace của bạn), tệp báo cáo `MSSV_buoi6.pdf` có ảnh chụp màn hình/đoạn log cho từng mục dưới đây và phần trả lời câu hỏi kiểm tra. Ảnh chụp Web UI lấy từ đường dẫn `lab ui` (thanh địa chỉ có `.app.github.dev`).

| Tiêu chí | Minh chứng cần có | Điểm |
|---|---|---|
| Tạo topic và producer | Kết quả `--describe` của topic; log producer và `kafka-get-offsets.sh` khớp số bản ghi theo partition | 1,0 |
| Consumer group và rebalance | Bảng `--describe --members` với 2 và 3 thành viên; giải thích độ trễ 45 giây của static membership | 1,5 |
| Consumer lag và đặt lại offset | Bảng LAG trước/sau khi consumer Python đọc; kết quả `--reset-offsets` | 1,0 |
| Streaming cửa sổ + watermark ra console | Ít nhất 2 batch của `streaming_console.py`; ảnh tab Structured Streaming (cổng 4040) | 1,5 |
| Sink Parquet + checkpoint | Danh sách thư mục `parquet/`, `chk_parquet/`; kết quả đọc lại Parquet và giải thích cửa sổ nào đã được ghi | 1,5 |
| foreachBatch upsert và dữ liệu muộn | Bảng SQLite trước/sau dữ liệu muộn 1 phút và 10 phút; log `bi loai do watermark` | 1,5 |
| Khôi phục từ checkpoint | Nội dung tệp offset cuối và `commits/` cuối, offset Kafka, số batch sau khi khởi động lại và phép đối chiếu không mất/không trùng | 1,0 |
| Câu hỏi kiểm tra và trình bày | Trả lời đúng, có dẫn chứng từ kết quả của chính mình | 1,0 |
| **Tổng** | | **10** |

Bài tập mở rộng (tùy chọn) được cộng tối đa 1 điểm, không vượt quá 10.
