# Buổi 2 — Lập trình MapReduce với Java và Hadoop Streaming (GitHub Codespaces)

Thời lượng 4 tiết (180 phút thực hành, 45 phút chữa bài/thảo luận), ứng với CLO2 (triển khai ứng dụng xử lý dữ liệu lớn trên nền tảng phân tán) và CLO4 (phân tích, đánh giá hiệu năng xử lý phân tán). Máy codespace khuyến nghị: **2 nhân / 8 GB** (mặc định) — đủ cho buổi này, RAM đỉnh đo được khoảng 4,7 GB (cụm Hadoop + Docker, chưa tính VS Code ~1–1,8 GB) khi job 8 map task chạy, không cần máy 4 nhân. Dịch vụ cần bật: chỉ **cụm lõi** 8 container (tự bật khi mở codespace; nếu chưa chạy thì gõ `lab start`), không cần `lab hive`, `lab nosql`, `lab kafka`. Dữ liệu: `retail_transactions.csv` 5 000 000 dòng (405 MB) và `products.csv` (500 sản phẩm), sinh bằng `lab data 5000000`. Quota ước tính: khoảng 3–3,5 giờ mở codespace × 2 nhân ≈ **6–7 core-hour** cho buổi (máy 4 nhân: gấp đôi). Toàn bộ lệnh và kết quả mẫu được chạy thật trên bản sao Dev Container giới hạn 2 nhân / 8 GB dùng đúng `.devcontainer` của repo, image `bigdata-lab:2026.10` (Hadoop 3.4.3, JDK 17, Python 3.12); thời gian chạy chỉ là ví dụ, phụ thuộc máy.

## Mục tiêu

Sau khi hoàn thành buổi thực hành, sinh viên có khả năng:

- Biên dịch, đóng gói và chạy chương trình MapReduce viết bằng Java (WordCount) trên YARN bằng JDK 17 có sẵn trong container, không cần Maven.
- Giải thích vai trò của **Combiner**, đọc các Counter về shuffle (`Map output materialized bytes`, `Reduce shuffle bytes`, `Combine input/output records`, `Spilled Records`) để đánh giá tác động của Combiner.
- Viết **Partitioner** tự định nghĩa và giải thích cách khoá được phân về các reducer.
- Viết cặp `mapper.py`/`reducer.py` bằng Python, kiểm thử cục bộ bằng pipeline Unix và chạy qua **Hadoop Streaming** để tính doanh thu theo danh mục.
- Cài đặt **reduce-side join** giữa bảng giao dịch và bảng sản phẩm bằng Hadoop Streaming với khoá phức hợp và `KeyFieldBasedPartitioner`.
- Theo dõi job trên ResourceManager và JobHistory Server, giải thích cách Hadoop xác định số map task và reduce task.

## Chuẩn bị

**1. Mở codespace.** Vào https://github.com/codespaces, bấm vào codespace của repo môn học (đã tạo ở Buổi 0) để mở lại; chưa có thì vào repo của bạn → nút **Code** → tab **Codespaces** → **Create codespace on main**. Giữ cấu hình mặc định 2 nhân / 8 GB. Terminal của VS Code trong trình duyệt (gọi là **Terminal codespace**) mặc định đứng ở thư mục gốc của repo; mọi đường dẫn trong bài là đường dẫn tương đối tính từ thư mục này (tên thư mục `/workspaces/<tên-repo>` của mỗi bạn khác nhau).

**2. Chờ cụm tự bật.** Mỗi lần mở codespace, cụm lõi tự khởi động (khoảng 1–2 phút). Khi terminal *postStartCommand* in dòng `[khoi-dong] Cụm đã sẵn sàng...` là dùng được.

**3. Lấy bản cập nhật của giảng viên (nếu có)** và kiểm tra cụm:

<!-- shell: Terminal codespace -->
```bash
git pull
lab status
```

**Kết quả mong đợi** (phần `lab status`; số RAM là ví dụ):

```text
NAME              STATUS
datanode1         Up About a minute (healthy)
datanode2         Up About a minute (healthy)
historyserver     Up About a minute (healthy)
namenode          Up About a minute (healthy)
nodemanager1      Up 51 seconds (healthy)
nodemanager2      Up 51 seconds (healthy)
resourcemanager   Up About a minute (healthy)
spark-master      Up About a minute (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 2.2 GB
RAM máy codespace: dùng ... / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

Nếu có container không `healthy` hoặc cụm chưa chạy, gõ `lab start` rồi `lab status` lại.

**4. Dữ liệu 5 triệu dòng.** Codespace tạo trước 06/11/2026 chỉ có dữ liệu mẫu 200 000 dòng (codespace tạo sau ngày này đã có sẵn 5 triệu dòng). Bài này (và các buổi sau) dùng 5 000 000 dòng; kiểm tra số dòng:

<!-- shell: Terminal codespace -->
```bash
wc -l data/retail_transactions.csv
```

Nếu kết quả là `200001 data/retail_transactions.csv` (chưa có bản đầy đủ), sinh lại — **chỉ cần làm một lần cho cả học kỳ**:

<!-- shell: Terminal codespace -->
```bash
time lab data 5000000
wc -l data/retail_transactions.csv
ls -lh data
```

**Kết quả mong đợi** (thời gian là ví dụ; bản sao kiểm thử mất khoảng 30 giây):

```text
Sinh du lieu vao ./data (5,000,000 giao dich)...
  giao dich: 500,000
...
  giao dich: 4,500,000
  products.csv: 0.0 MB
  retail_transactions.csv: 405.2 MB
  web_access.log: 121.4 MB
  sensor_readings.csv: 22.7 MB

real	0m28.975s
5000001 data/retail_transactions.csv
total 524M
-rw-r--r-- 1 vscode vscode  23K Nov  5 16:21 products.csv
-rw-r--r-- 1 vscode vscode 387M Nov  5 16:21 retail_transactions.csv
-rw-r--r-- 1 vscode vscode  22M Nov  5 16:21 sensor_readings.csv
-rw-r--r-- 1 vscode vscode 116M Nov  5 16:21 web_access.log
```

> 💡 Dữ liệu sinh với `seed` cố định nên mọi bạn có cùng một bộ dữ liệu, các số đếm và Counter trong bài sẽ khớp từng con số. Thư mục `data/` nằm trong `.gitignore` nên không bị đẩy lên GitHub; nó vẫn còn khi codespace dừng rồi mở lại (chỉ mất khi xoá codespace).

> ⚠️ Nếu vẫn dùng dữ liệu 200 000 dòng, mọi số đếm, số split và Counter sẽ khác kết quả mẫu.

**5. Dịch vụ của buổi.** Buổi này chỉ cần cụm lõi (đã tự bật). Nếu còn bật dịch vụ bổ sung từ buổi khác (Hive, HBase, MongoDB, Kafka), tắt để dành RAM cho YARN:

<!-- shell: Terminal codespace -->
```bash
lab stop-extras
```

**Mã nguồn:** mọi tệp của buổi này đặt trong thư mục `src/lab02/` của repo (trong container thấy ở `/src/lab02/`). Tạo tệp bằng trình soạn thảo VS Code của codespace: chuột phải thư mục `src` trong **Explorer** → **New Folder** / **New File**, hoặc gõ `code <đường dẫn tệp>` trong Terminal codespace để mở (tạo mới) tệp, dán nội dung rồi **Ctrl+S**. Cũng có thể giải nén `buoi2_src.zip` (giảng viên đính kèm) vào thư mục `src/` bằng cách kéo thả tệp zip vào Explorer rồi `unzip buoi2_src.zip -d src`.

**Hai loại terminal trong bài:**

- **Terminal codespace**: terminal của VS Code (user `vscode`, đứng ở gốc repo) — dùng cho `lab ...`, `git ...`, `code ...`, `docker exec ...`.
- **Bên trong container namenode**: shell mở bằng `docker exec -it namenode bash` từ Terminal codespace (dấu nhắc đổi thành `root@namenode:/#`). Mọi lệnh `hdfs`, `hadoop`, `javac`, `yarn` của buổi này chạy ở đây. Gõ `exit` để quay về Terminal codespace.

> 💡 Mở thêm một terminal (nút **+** trên khung TERMINAL) để vừa giữ shell trong `namenode`, vừa có Terminal codespace cho `lab`, `git`, `code`.

**Web UI dùng trong buổi này.** Trong codespace không dùng `http://localhost:<cổng>` trên trình duyệt của bạn được; lấy đường dẫn bằng `lab ui`, hoặc mở tab **PORTS** (cạnh TERMINAL) và bấm biểu tượng quả địa cầu ở cột *Forwarded Address* của cổng tương ứng.

<!-- shell: Terminal codespace -->
```bash
lab ui
```

**Kết quả mong đợi** (tên codespace của bạn khác):

```text
HDFS NameNode            https://<tên-codespace>-9870.app.github.dev
YARN ResourceManager     https://<tên-codespace>-8088.app.github.dev
Spark Master             https://<tên-codespace>-8080.app.github.dev
Spark UI (job đang chạy) https://<tên-codespace>-4040.app.github.dev
Spark History            https://<tên-codespace>-18080.app.github.dev
MapReduce JobHistory     https://<tên-codespace>-19888.app.github.dev
HiveServer2 UI           https://<tên-codespace>-10002.app.github.dev
HBase Master             https://<tên-codespace>-16010.app.github.dev
HBase RegionServer       https://<tên-codespace>-16030.app.github.dev
NodeManager 1            https://<tên-codespace>-8042.app.github.dev
NodeManager 2            https://<tên-codespace>-8043.app.github.dev
DataNode 1               https://<tên-codespace>-9864.app.github.dev
DataNode 2               https://<tên-codespace>-9865.app.github.dev
(Hoặc mở tab PORTS bên cạnh TERMINAL, bấm biểu tượng quả địa cầu.)
```

| Giao diện | Cổng (tab PORTS) | Dùng để |
|---|---|---|
| YARN ResourceManager | 8088 | Ứng dụng đang chạy/đã xong, tiến độ map/reduce |
| MapReduce JobHistory | 19888 | Counter, danh sách task, log của job đã xong |
| NodeManager 1 / 2 | 8042 · 8043 | Container đang chạy trên từng node (dòng *NodeManager 1/2* trong `lab ui`, hoặc tab PORTS) |
| HDFS NameNode | 9870 | Duyệt thư mục kết quả `/user/student/output` |

> 💡 Bên trong codespace, `curl -s localhost:8088` hay `curl -s localhost:19888` vẫn hoạt động (dùng để kiểm tra nhanh mà không cần trình duyệt). Liên kết nội bộ trong Web UI dạng `http://nodemanager1:8042/...`, `http://historyserver:19888/...` không mở được trên trình duyệt: thay phần `http://nodemanager1:8042` bằng địa chỉ của cổng 8042 (nodemanager2 → cổng 8043), `http://historyserver:19888` bằng địa chỉ cổng 19888, giữ nguyên phần đường dẫn phía sau.

**Thông số tài nguyên YARN của cụm** (từ `conf/hadoop/*.xml`): mỗi NodeManager có 1792 MB và 2 vCore; container map 512 MB, reduce 768 MB, ApplicationMaster 512 MB. Các tuỳ chọn `--add-opens` cần cho Java 17 đã cấu hình sẵn trong `mapred-site.xml`. Trên máy codespace 2 nhân, các vCore này dùng chung 2 nhân thật nên job chạy chậm hơn máy phòng lab khoảng 1,3–2,2 lần.

## PHẦN 1: CHUẨN BỊ DỮ LIỆU TRÊN HDFS

### Bước 1: Khởi động cụm và kiểm tra

Cụm đã tự bật khi mở codespace; `lab start` chỉ kiểm tra lại (bật những container còn thiếu) và chờ tất cả `healthy`.

<!-- shell: Terminal codespace -->
```bash
lab start
docker exec -it namenode hdfs dfsadmin -safemode get
docker exec -it resourcemanager yarn node -list
```

**Kết quả mong đợi** (rút gọn; cổng sau `nodemanagerN:` thay đổi mỗi lần khởi động):

```text
 Container nodemanager1  Healthy
 Container resourcemanager  Healthy
 Container historyserver  Healthy
Safe mode is OFF
Total Nodes:2
         Node-Id	     Node-State	Node-Http-Address	Number-of-Running-Containers
nodemanager2:38525	        RUNNING	nodemanager2:8042	                           0
nodemanager1:43503	        RUNNING	nodemanager1:8042	                           0
INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at resourcemanager/172.19.0.3:8032
```

### Bước 2: Nạp dữ liệu lên HDFS

Mở shell trong container `namenode`; các lệnh "Bên trong container namenode" của buổi này đều chạy trong shell này.

<!-- shell: Terminal codespace -->
```bash
docker exec -it namenode bash
```

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -mkdir -p /data/raw /user/student/output
hdfs dfs -put -f /data/retail_transactions.csv /data/raw/
hdfs dfs -mkdir -p /data/raw/products
hdfs dfs -put -f /data/products.csv /data/raw/products/
hdfs dfs -ls -h -R /data/raw
```

**Kết quả mong đợi** (lệnh `put` tệp 405 MB mất khoảng 10 giây):

```text
drwxr-xr-x   - root supergroup          0 2026-11-05 23:22 /data/raw/products
-rw-r--r--   2 root supergroup     22.1 K 2026-11-05 23:22 /data/raw/products/products.csv
-rw-r--r--   2 root supergroup    386.4 M 2026-11-05 23:22 /data/raw/retail_transactions.csv
```

> 💡 Không cần `docker cp`: `/data` trong container chính là thư mục `data/` của repo trong codespace. Lưu ý phân biệt `/data/...` **cục bộ** của container (lệnh `ls`, `cat`, `awk`) với `/data/raw/...` **trên HDFS** (lệnh `hdfs dfs`). Dữ liệu HDFS nằm trong volume Docker của codespace nên vẫn còn khi codespace dừng rồi mở lại; nếu ở Buổi 1 đã nạp rồi thì `put -f` chỉ ghi đè.

## PHẦN 2: WORDCOUNT BẰNG JAVA

### Bước 3: Tạo mã nguồn WordCount.java

Chương trình gồm Mapper (tách từ, phát cặp `(từ, 1)`), Reducer (cộng dồn), và hai tuỳ chọn bật/tắt qua tham số `-D` (nhờ `ToolRunner` + `GenericOptionsParser`) để không phải sửa mã khi thí nghiệm: `wordcount.combiner=true` bật Combiner, `wordcount.partitioner=true` dùng Partitioner tự viết.

Tạo thư mục và mở tệp mới trong trình soạn thảo:

<!-- shell: Terminal codespace -->
```bash
mkdir -p src/lab02/wordcount src/lab02/streaming
code src/lab02/wordcount/WordCount.java
```

Dán toàn bộ nội dung sau vào tab vừa mở rồi lưu (**Ctrl+S**):

<!-- file: src/lab02/wordcount/WordCount.java -->
```java
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordCount cho Buổi 2.
 *   -D wordcount.combiner=true     : bật Combiner (dùng lại IntSumReducer)
 *   -D wordcount.partitioner=true  : dùng FirstLetterPartitioner thay cho HashPartitioner
 *   -D mapreduce.job.reduces=N     : số reduce task
 */
public class WordCount extends Configured implements Tool {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, ONE);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    /** Từ bắt đầu bằng chữ in hoa (tên khu vực: HCM, Hanoi, ...) -> reducer 0,
     *  các từ còn lại (danh mục: dien_tu, sach, ...) -> reducer 1 (nếu có >= 2 reducer). */
    public static class FirstLetterPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            if (numPartitions < 2) {
                return 0;
            }
            char c = key.toString().isEmpty() ? 'a' : key.toString().charAt(0);
            return Character.isUpperCase(c) ? 0 : 1;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Cach dung: hadoop jar wordcount.jar WordCount [-D ...] <input> <output>");
            return 2;
        }
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        if (conf.getBoolean("wordcount.combiner", false)) {
            job.setCombinerClass(IntSumReducer.class);   // Combiner = Reducer (phép cộng có tính kết hợp)
        }
        if (conf.getBoolean("wordcount.partitioner", false)) {
            job.setPartitionerClass(FirstLetterPartitioner.class);
        }
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new WordCount(), args));
    }
}
```

Kiểm tra tệp đã lưu đủ:

<!-- shell: Terminal codespace -->
```bash
wc -l src/lab02/wordcount/WordCount.java
```

**Kết quả mong đợi:**

```text
96 src/lab02/wordcount/WordCount.java
```

> 💡 Cách khác không cần trình soạn thảo: trong Terminal codespace gõ `cat > src/lab02/wordcount/WordCount.java <<'EOF'`, dán nội dung tệp, rồi gõ một dòng `EOF` để kết thúc (đã thử, cho kết quả giống hệt). Nếu `wc -l` ra số khác 96, mở lại tệp kiểm tra xem có dán thiếu/thừa dòng không.

Điểm cần chú ý trong mã:

- `IntSumReducer` vừa là Reducer vừa dùng được làm Combiner vì phép cộng có tính giao hoán và kết hợp; kiểu dữ liệu vào/ra của nó giống hệt nhau (`Text, IntWritable`).
- `FirstLetterPartitioner.getPartition()` trả về số thứ tự reducer (0 … numPartitions−1) cho từng khoá: từ bắt đầu bằng chữ in hoa (tên khu vực `HCM`, `Hanoi`...) về reducer 0, các từ còn lại (danh mục `dien_tu`, `sach`...) về reducer 1.

### Bước 4: Biên dịch và đóng gói jar

JDK 17 (`javac`, `jar`) có sẵn trong mọi container của cụm (máy codespace không cần cài Java). `hadoop classpath` in ra classpath chứa các thư viện Hadoop cần để biên dịch.

<!-- shell: Bên trong container namenode -->
```bash
cd /src/lab02/wordcount
mkdir -p classes
javac -classpath "$(hadoop classpath)" -d classes WordCount.java
jar cf wordcount.jar -C classes .
ls -l
jar tf wordcount.jar
```

**Kết quả mong đợi:**

```text
total 12
-rw-r--r-- 1 ubuntu ubuntu 3932 Nov  5 23:22 WordCount.java
drwxrwxrwx 2 root   root    120 Nov  5 23:22 classes
-rw-rw-rw- 1 root   root   4207 Nov  5 23:22 wordcount.jar
META-INF/
META-INF/MANIFEST.MF
WordCount$FirstLetterPartitioner.class
WordCount$IntSumReducer.class
WordCount$TokenizerMapper.class
WordCount.class
```

> ⚠️ Phải đóng gói bằng `jar cf wordcount.jar -C classes .` để các tệp `.class` nằm ở **gốc** của jar. Nếu dùng `jar cf wordcount.jar classes/*.class`, các lớp nằm trong thư mục `classes/` của jar và task báo `ClassNotFoundException: Class WordCount$TokenizerMapper not found`.

> 💡 `classes/` và `wordcount.jar` do `root` trong container tạo nhưng có quyền ghi cho mọi người (`drwxrwxrwx`, `-rw-rw-rw-`), nên trong Terminal codespace vẫn xoá được bằng `rm -rf src/lab02/wordcount/classes src/lab02/wordcount/wordcount.jar` mà không cần `sudo` (đã thử). Hai mục này sinh lại được bằng Bước 4, không cần đưa lên GitHub (xem mục Kết thúc buổi).

### Bước 5: Tạo dữ liệu văn bản đầu vào

Dữ liệu gốc là CSV, nên tạo một tệp văn bản gồm hai "từ" mỗi dòng: cột `category` (thứ 4) và `region` (thứ 9).

<!-- shell: Bên trong container namenode -->
```bash
mkdir -p /tmp/lab02
cut -d, -f4,9 /data/retail_transactions.csv | tail -n +2 | tr , " " > /tmp/lab02/sample.txt
head -3 /tmp/lab02/sample.txt
wc -l -c /tmp/lab02/sample.txt
hdfs dfs -mkdir -p /user/student/text_input
hdfs dfs -put -f /tmp/lab02/sample.txt /user/student/text_input/
hdfs dfs -ls -h /user/student/text_input
```

**Kết quả mong đợi:**

```text
me_va_be NgheAn
gia_dung Hanoi
gia_dung Hanoi
 5000000 70960141 /tmp/lab02/sample.txt
Found 1 items
-rw-r--r--   2 root supergroup     67.7 M 2026-11-05 23:22 /user/student/text_input/sample.txt
```

### Bước 6: Chạy WordCount (chưa có Combiner) và đọc Counter

<!-- shell: Bên trong container namenode -->
```bash
cd /src/lab02/wordcount
hdfs dfs -rm -r -f /user/student/output/wc_no_combiner
time hadoop jar wordcount.jar WordCount /user/student/text_input /user/student/output/wc_no_combiner
```

**Kết quả mong đợi** (rút gọn; ID ứng dụng và thời gian trên máy bạn khác):

```text
INFO mapreduce.JobSubmitter: number of splits:1
INFO impl.YarnClientImpl: Submitted application application_1793895616834_0001
INFO mapreduce.Job: The url to track the job: http://resourcemanager:8088/proxy/application_1793895616834_0001/
INFO mapreduce.Job:  map 0% reduce 0%
INFO mapreduce.Job:  map 100% reduce 0%
INFO mapreduce.Job:  map 100% reduce 100%
INFO mapreduce.Job: Job job_1793895616834_0001 completed successfully
INFO mapreduce.Job: Counters: 54
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
	Map-Reduce Framework
		Map input records=5000000
		Map output records=10000000
		Map output bytes=110960141
		Map output materialized bytes=130960147
		Combine input records=0
		Combine output records=0
		Reduce input groups=14
		Reduce shuffle bytes=130960147
		Reduce input records=10000000
		Reduce output records=14
		Spilled Records=30000000
		CPU time spent (ms)=13800
...
real	0m29.322s
```

Xem kết quả:

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -ls /user/student/output/wc_no_combiner
hdfs dfs -cat /user/student/output/wc_no_combiner/part-r-00000
```

**Kết quả mong đợi:**

```text
Found 2 items
-rw-r--r--   2 root supergroup          0 2026-11-05 23:23 /user/student/output/wc_no_combiner/_SUCCESS
-rw-r--r--   2 root supergroup        209 2026-11-05 23:23 /user/student/output/wc_no_combiner/part-r-00000
CanTho	298941
DaNang	399549
HCM	2252467
HaiPhong	249216
Hanoi	1499922
LamDong	149731
NgheAn	150174
dien_tu	714649
gia_dung	714852
me_va_be	713265
sach	714852
the_thao	713600
thoi_trang	714576
thuc_pham	714206
```

Ghi lại vào báo cáo các giá trị `Map output records`, `Map output materialized bytes`, `Reduce shuffle bytes`, `Spilled Records`, `CPU time spent`. Trong Hadoop 3, dòng `Combine input records=0` vẫn xuất hiện khi không có Combiner (giá trị bằng 0). Tệp đầu vào 67,7 MB nhỏ hơn một block 128 MB nên chỉ có **1 split → 1 map task**. Theo dõi job trong lúc chạy ở ResourceManager (cổng 8088 → **Applications**), sau khi xong ở JobHistory (cổng 19888) — địa chỉ lấy bằng `lab ui`.

> 💡 Job chạy khoảng 30 giây trên máy 2 nhân. Chạy job ở terminal này, rồi chuyển sang tab trình duyệt ResourceManager và bấm làm mới để thấy job ở trạng thái `RUNNING`.

## PHẦN 3: COMBINER VÀ PARTITIONER

### Bước 7: Bật Combiner và so sánh Counter

Combiner chạy ngay trên phía map (sau khi sắp xếp từng lần spill), gộp các cặp `(từ, 1)` trùng khoá thành `(từ, n)` trước khi gửi qua mạng.

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -rm -r -f /user/student/output/wc_with_combiner
time hadoop jar wordcount.jar WordCount -D wordcount.combiner=true \
  /user/student/text_input /user/student/output/wc_with_combiner 2>&1 \
  | grep -E "completed|Map input records|Map output records|Map output bytes|materialized|Combine|Reduce input|Reduce shuffle|Spilled|Reduce output|CPU time"
```

**Kết quả mong đợi:**

```text
INFO mapreduce.Job: Job job_1793895616834_0002 completed successfully
		Map input records=5000000
		Map output records=10000000
		Map output bytes=110960141
		Map output materialized bytes=199
		Combine input records=10000056
		Combine output records=70
		Reduce input groups=14
		Reduce shuffle bytes=199
		Reduce input records=14
		Reduce output records=14
		Spilled Records=84
		CPU time spent (ms)=6800
real	0m20.244s
```

Bảng so sánh từ hai lần chạy (số liệu thật của lần kiểm thử trên codespace 2 nhân):

| Counter | Không Combiner | Có Combiner |
|---|---|---|
| Map output records | 10 000 000 | 10 000 000 |
| Combine input records | 0 | 10 000 056 |
| Combine output records | 0 | 70 |
| Map output materialized bytes | 130 960 147 | 199 |
| Reduce shuffle bytes | 130 960 147 | 199 |
| Reduce input records | 10 000 000 | 14 |
| Spilled Records | 30 000 000 | 84 |
| CPU time spent (ms) | 13 800 | 6 800 |
| Thời gian `real` (ví dụ) | 29,3 s | 20,2 s |

Dữ liệu chuyển qua shuffle giảm từ khoảng 125 MB xuống 199 byte vì chỉ có 14 khoá phân biệt. `Combine input records` (10 000 056) lớn hơn `Map output records` một chút vì Combiner chạy cả ở các lần spill lẫn khi trộn (merge) các spill: một số bản ghi đã gộp được đưa qua Combiner lần nữa. Hadoop **không đảm bảo** Combiner chạy bao nhiêu lần (0, 1 hay nhiều lần) — vì vậy hàm Combiner phải cho kết quả đúng bất kể số lần chạy. Các Counter về số bản ghi/byte không phụ thuộc máy (giống hệt kết quả trên máy phòng lab); chỉ `CPU time spent` và thời gian thay đổi.

### Bước 8: Kiểm tra Combiner không làm sai kết quả

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -cat /user/student/output/wc_no_combiner/part-r-00000 | sort > /tmp/lab02/a.txt
hdfs dfs -cat /user/student/output/wc_with_combiner/part-r-00000 | sort > /tmp/lab02/b.txt
diff /tmp/lab02/a.txt /tmp/lab02/b.txt && echo GIONG_NHAU
```

**Kết quả mong đợi:**

```text
GIONG_NHAU
```

### Bước 9: Partitioner mặc định (HashPartitioner) với 2 reducer

Mặc định Hadoop dùng `HashPartitioner`: reducer = `(key.hashCode() & Integer.MAX_VALUE) % số_reducer`.

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -rm -r -f /user/student/output/wc_hash2
hadoop jar wordcount.jar WordCount -D wordcount.combiner=true -D mapreduce.job.reduces=2 \
  /user/student/text_input /user/student/output/wc_hash2 2>&1 | grep -E "completed|Launched reduce"
for f in 00000 00001; do echo "== part-r-$f"; hdfs dfs -cat /user/student/output/wc_hash2/part-r-$f; done
```

**Kết quả mong đợi:**

```text
INFO mapreduce.Job: Job job_1793895616834_0003 completed successfully
		Launched reduce tasks=2
== part-r-00000
CanTho	298941
DaNang	399549
Hanoi	1499922
NgheAn	150174
sach	714852
thoi_trang	714576
thuc_pham	714206
== part-r-00001
HCM	2252467
HaiPhong	249216
LamDong	149731
dien_tu	714649
gia_dung	714852
me_va_be	713265
the_thao	713600
```

Mỗi tệp `part-r-NNNNN` là đầu ra của một reducer, khoá trong từng tệp đã sắp xếp. Khu vực và danh mục bị trộn lẫn ở hai reducer theo giá trị băm.

### Bước 10: Partitioner tự viết (FirstLetterPartitioner)

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -rm -r -f /user/student/output/wc_part2
hadoop jar wordcount.jar WordCount -D wordcount.combiner=true -D wordcount.partitioner=true \
  -D mapreduce.job.reduces=2 /user/student/text_input /user/student/output/wc_part2 2>&1 \
  | grep -E "completed|Launched reduce"
for f in 00000 00001; do echo "== part-r-$f"; hdfs dfs -cat /user/student/output/wc_part2/part-r-$f; done
```

**Kết quả mong đợi:**

```text
INFO mapreduce.Job: Job job_1793895616834_0004 completed successfully
		Launched reduce tasks=2
== part-r-00000
CanTho	298941
DaNang	399549
HCM	2252467
HaiPhong	249216
Hanoi	1499922
LamDong	149731
NgheAn	150174
== part-r-00001
dien_tu	714649
gia_dung	714852
me_va_be	713265
sach	714852
the_thao	713600
thoi_trang	714576
thuc_pham	714206
```

Reducer 0 nhận toàn bộ khu vực, reducer 1 nhận toàn bộ danh mục. Partitioner quyết định **khoá nào đi về reducer nào**; tất cả giá trị của cùng một khoá luôn về cùng một reducer — đây là điều kiện để Reducer cộng đúng, và cũng là nền tảng của reduce-side join ở Phần 5.

## PHẦN 4: HADOOP STREAMING VỚI PYTHON

### Bước 11: Viết mapper_revenue.py và reducer_revenue.py

Hadoop Streaming chạy chương trình bất kỳ đọc stdin/ghi stdout làm mapper/reducer. Mỗi dòng ra có dạng `khoá[TAB]giá trị`; Hadoop sắp xếp theo khoá trước khi đưa vào reducer. Doanh thu một giao dịch = `quantity × unit_price × (1 − discount)`.

Mở hai tệp mới trong trình soạn thảo (hoặc **Explorer** → thư mục `src/lab02/streaming` → **New File**):

<!-- shell: Terminal codespace -->
```bash
code src/lab02/streaming/mapper_revenue.py src/lab02/streaming/reducer_revenue.py
```

<!-- file: src/lab02/streaming/mapper_revenue.py -->
```python
#!/usr/bin/env python3
"""Mapper: doanh thu = quantity * unit_price * (1 - discount), khoá = category."""
import sys

for line in sys.stdin:
    parts = line.rstrip("\n").split(",")
    if len(parts) != 10 or parts[0] == "transaction_id":
        continue                      # bỏ header và dòng hỏng
    try:
        category = parts[3]
        revenue = int(parts[4]) * float(parts[5]) * (1 - float(parts[6]))
    except ValueError:
        continue
    print(f"{category}\t{revenue:.2f}")
```

<!-- file: src/lab02/streaming/reducer_revenue.py -->
```python
#!/usr/bin/env python3
"""Reducer: cộng dồn doanh thu theo khoá. Đầu vào đã được Hadoop sắp xếp theo khoá."""
import sys

current_key, total = None, 0.0
for line in sys.stdin:
    try:
        key, value = line.rstrip("\n").split("\t", 1)
        value = float(value)
    except ValueError:
        continue
    if key != current_key:
        if current_key is not None:
            print(f"{current_key}\t{total:.2f}")
        current_key, total = key, 0.0
    total += value
if current_key is not None:
    print(f"{current_key}\t{total:.2f}")
```

Lưu cả hai tệp (**Ctrl+S** ở từng tab) rồi kiểm tra:

<!-- shell: Terminal codespace -->
```bash
ls -l src/lab02/streaming
```

**Kết quả mong đợi:**

```text
total 8
-rw-r--r-- 1 vscode vscode 500 Nov  5 16:24 mapper_revenue.py
-rw-r--r-- 1 vscode vscode 566 Nov  5 16:24 reducer_revenue.py
```

> 💡 Mọi node đều có `/usr/bin/python3` (3.12). Gọi mapper bằng `-mapper "python3 mapper_revenue.py"` nên không phụ thuộc dòng shebang hay quyền thực thi (`chmod +x`). Máy codespace (bên ngoài container) **không** có `python3`; luôn chạy thử script bên trong container `namenode` như Bước 12.

> 💡 VS Code có thể hỏi cài extension Python hoặc chọn interpreter khi mở tệp `.py` — bỏ qua được, bài này không chạy Python trên máy codespace.

### Bước 12: Kiểm thử cục bộ bằng pipeline Unix

Trước khi gửi job lên cụm, mô phỏng MapReduce trên 1000 dòng đầu: `mapper | sort | reducer`.

<!-- shell: Bên trong container namenode -->
```bash
cd /src/lab02/streaming
head -n 1000 /data/retail_transactions.csv | python3 mapper_revenue.py | sort -k1,1 | python3 reducer_revenue.py
```

**Kết quả mong đợi:**

```text
dien_tu	4634986650.00
gia_dung	4815339850.00
me_va_be	4861058400.00
sach	4152397050.00
the_thao	5001511350.00
thoi_trang	5106002400.00
thuc_pham	5099112800.00
```

### Bước 13: Chạy Streaming job với 4 reducer

Đường dẫn jar Streaming có sẵn trong biến `$STREAM_JAR` của image.

<!-- shell: Bên trong container namenode -->
```bash
echo $STREAM_JAR
hdfs dfs -rm -r -f /user/student/output/revenue_by_category
time hadoop jar $STREAM_JAR \
  -D mapreduce.job.reduces=4 \
  -files /src/lab02/streaming/mapper_revenue.py,/src/lab02/streaming/reducer_revenue.py \
  -mapper "python3 mapper_revenue.py" \
  -reducer "python3 reducer_revenue.py" \
  -input /data/raw/retail_transactions.csv \
  -output /user/student/output/revenue_by_category 2>&1 \
  | grep -E "number of splits|Running job|map 100|completed|Launched|Map input records|Map output records|Reduce input groups|Reduce output records"
```

**Kết quả mong đợi** (thời gian là ví dụ; trên máy 2 nhân khoảng 45 giây):

```text
/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar
INFO mapreduce.JobSubmitter: number of splits:3
INFO mapreduce.Job: Running job: job_1793895616834_0005
INFO mapreduce.Job:  map 100% reduce 0%
INFO mapreduce.Job:  map 100% reduce 25%
INFO mapreduce.Job:  map 100% reduce 75%
INFO mapreduce.Job:  map 100% reduce 100%
INFO mapreduce.Job: Job job_1793895616834_0005 completed successfully
		Launched map tasks=3
		Launched reduce tasks=4
		Map input records=5000001
		Map output records=5000000
		Reduce input groups=7
		Reduce output records=7
real	0m43.921s
```

`-files` gửi hai script lên bộ nhớ đệm phân tán (distributed cache) để mọi node có bản sao trong thư mục làm việc của task. `Map input records=5000001` gồm cả dòng header (mapper bỏ qua nên `Map output records=5000000`). Xem từng tệp kết quả:

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -ls /user/student/output/revenue_by_category
for f in $(hdfs dfs -ls /user/student/output/revenue_by_category/part-* | awk '{print $8}'); do
  echo "== $f"; hdfs dfs -cat $f
done
```

**Kết quả mong đợi:**

```text
Found 5 items
-rw-r--r--   2 root supergroup          0 2026-11-05 23:25 /user/student/output/revenue_by_category/_SUCCESS
-rw-r--r--   2 root supergroup         80 2026-11-05 23:25 /user/student/output/revenue_by_category/part-00000
-rw-r--r--   2 root supergroup         80 2026-11-05 23:25 /user/student/output/revenue_by_category/part-00001
-rw-r--r--   2 root supergroup          0 2026-11-05 23:25 /user/student/output/revenue_by_category/part-00002
-rw-r--r--   2 root supergroup         27 2026-11-05 23:25 /user/student/output/revenue_by_category/part-00003
== /user/student/output/revenue_by_category/part-00000
sach	24319316680500.00
thoi_trang	24298821245400.00
thuc_pham	24302788049600.00
== /user/student/output/revenue_by_category/part-00001
dien_tu	24361361256650.00
me_va_be	24230671690150.00
the_thao	24306974849550.00
== /user/student/output/revenue_by_category/part-00002
== /user/student/output/revenue_by_category/part-00003
gia_dung	24372296414100.00
```

Nhận xét: tệp 405 MB gồm 4 block nhưng chỉ có **3 split** (Hadoop cho phép split cuối dài tới 1,1 × block size nên phần dư 2,5 MB được gộp vào split thứ ba). Với 7 khoá và 4 reducer, phân bố theo băm không đều: `part-00002` rỗng, `part-00000` nhận 3 khoá.

### Bước 14: Đối chứng bằng awk

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -cat /user/student/output/revenue_by_category/part-* | sort -k1,1
awk -F',' 'NR>1 && $4=="dien_tu" {s += $5*$6*(1-$7)} END {printf "dien_tu (awk) %.2f\n", s}' /data/retail_transactions.csv
```

**Kết quả mong đợi:**

```text
dien_tu	24361361256650.00
gia_dung	24372296414100.00
me_va_be	24230671690150.00
sach	24319316680500.00
the_thao	24306974849550.00
thoi_trang	24298821245400.00
thuc_pham	24302788049600.00
dien_tu (awk) 24361361256650.00
```

Kết quả MapReduce và `awk` khớp tuyệt đối cho `dien_tu`. Tổng doanh thu của 7 danh mục là 170 192 230 185 950 (dùng để đối chiếu ở Phần 5).

### Bước 15: Dùng reducer làm combiner trong Streaming

Reducer của bài này cộng dồn, nên dùng được làm Combiner qua tham số `-combiner`.

<!-- shell: Bên trong container namenode -->
```bash
cd /src/lab02/streaming
hdfs dfs -rm -r -f /user/student/output/revenue_combiner
hadoop jar $STREAM_JAR \
  -D mapreduce.job.reduces=4 \
  -files mapper_revenue.py,reducer_revenue.py \
  -mapper "python3 mapper_revenue.py" \
  -combiner "python3 reducer_revenue.py" \
  -reducer "python3 reducer_revenue.py" \
  -input /data/raw/retail_transactions.csv \
  -output /user/student/output/revenue_combiner 2>&1 \
  | grep -E "completed|Map output records|Combine input|Combine output|Reduce shuffle bytes|Reduce input records"
```

**Kết quả mong đợi:**

```text
INFO mapreduce.Job: Job job_1793895616834_0006 completed successfully
		Map output records=5000000
		Combine input records=5000000
		Combine output records=21
		Reduce shuffle bytes=654
		Reduce input records=21
```

21 = 3 map task × 7 danh mục: mỗi map chỉ gửi đi 7 bản ghi đã cộng sẵn. Kết quả cuối (`hdfs dfs -cat /user/student/output/revenue_combiner/part-* | sort`) giống hệt Bước 14 (đã kiểm tra).

> ⚠️ Ở lệnh này `-files mapper_revenue.py,...` dùng **đường dẫn tương đối**, nên phải đứng trong `/src/lab02/streaming` (dòng `cd` đầu khối lệnh). Đứng sai thư mục, job dừng ngay với `java.io.FileNotFoundException: File mapper_revenue.py does not exist`.

## PHẦN 5: REDUCE-SIDE JOIN

### Bước 16: Thiết kế join giữa giao dịch và sản phẩm

Bài toán: với mỗi sản phẩm, ghép tên, thương hiệu (`brand`), danh mục từ `products.csv` với số giao dịch và tổng doanh thu từ `retail_transactions.csv`. Ý tưởng reduce-side join:

1. Mapper đọc **cả hai** tệp, nhận biết nguồn theo số cột (5 cột: sản phẩm; 10 cột: giao dịch) và phát khoá phức hợp `product_id[TAB]tag`, trong đó tag `0` = bản ghi sản phẩm, tag `1` = giao dịch.
2. Hadoop sắp xếp theo toàn bộ khoá `(product_id, tag)` nên trong mỗi nhóm `product_id`, bản ghi sản phẩm (tag 0) luôn đứng **trước** các giao dịch (tag 1).
3. Partitioner chỉ được băm theo **trường thứ nhất** (`product_id`) để bản ghi sản phẩm và các giao dịch của nó về **cùng một reducer** — dùng `KeyFieldBasedPartitioner` với `-k1,1`.
4. Reducer nhớ thông tin sản phẩm khi gặp tag 0, cộng dồn doanh thu khi gặp tag 1, và in kết quả khi chuyển sang `product_id` mới.

> 💡 `products.csv` có cột `category` riêng, được sinh độc lập với cột `category` của từng giao dịch; trong kết quả join ta dùng thông tin của bảng sản phẩm (dimension table).

### Bước 17: Viết mapper_join.py và reducer_join.py

<!-- shell: Terminal codespace -->
```bash
code src/lab02/streaming/mapper_join.py src/lab02/streaming/reducer_join.py
```

<!-- file: src/lab02/streaming/mapper_join.py -->
```python
#!/usr/bin/env python3
"""Mapper reduce-side join: products.csv (5 cột) và retail_transactions.csv (10 cột).
Khoá ra = product_id [TAB] tag ; tag "0" = bản ghi sản phẩm, "1" = giao dịch.
Nhờ sắp xếp theo (product_id, tag), reducer luôn thấy bản ghi sản phẩm TRƯỚC các giao dịch."""
import sys

for line in sys.stdin:
    p = line.rstrip("\n").split(",")
    if p[0] in ("product_id", "transaction_id"):
        continue                                   # header của hai tệp
    if len(p) == 5:                                # products.csv
        product_id, name, category, brand = p[0], p[1], p[2], p[3]
        print(f"{product_id}\t0\t{name}|{brand}|{category}")
    elif len(p) == 10:                             # retail_transactions.csv
        try:
            revenue = int(p[4]) * float(p[5]) * (1 - float(p[6]))
        except ValueError:
            continue
        print(f"{p[2]}\t1\t{revenue:.2f}")
```

<!-- file: src/lab02/streaming/reducer_join.py -->
```python
#!/usr/bin/env python3
"""Reducer reduce-side join: ghép thông tin sản phẩm với tổng doanh thu của sản phẩm đó.
Đầu ra: product_id, product_name, brand, category_sp, so_giao_dich, doanh_thu."""
import sys

def flush(pid, info, n, total):
    if pid is None or n == 0:
        return
    name, brand, cat = info if info else ("KHONG_RO", "KHONG_RO", "KHONG_RO")
    print(f"{pid}\t{name}\t{brand}\t{cat}\t{n}\t{total:.2f}")

cur, info, n, total = None, None, 0, 0.0
for line in sys.stdin:
    try:
        pid, tag, val = line.rstrip("\n").split("\t", 2)
    except ValueError:
        continue
    if pid != cur:
        flush(cur, info, n, total)
        cur, info, n, total = pid, None, 0, 0.0
    if tag == "0":
        info = tuple(val.split("|"))
    else:
        n += 1
        total += float(val)
flush(cur, info, n, total)
```

Lưu cả hai tệp (**Ctrl+S**). Trong Explorer, thư mục `src/lab02/streaming` lúc này có 4 tệp `.py`.

### Bước 18: Kiểm thử cục bộ

`sort -k1,1 -k2,2` mô phỏng việc Hadoop sắp xếp theo khoá hai trường.

<!-- shell: Bên trong container namenode -->
```bash
cd /src/lab02/streaming
head -n 3 /data/products.csv
cat /data/products.csv <(head -n 2000 /data/retail_transactions.csv) | python3 mapper_join.py | sort -k1,1 -k2,2 > /tmp/lab02/join_map.txt
head -n 5 /tmp/lab02/join_map.txt
python3 reducer_join.py < /tmp/lab02/join_map.txt > /tmp/lab02/join_local.txt
wc -l < /tmp/lab02/join_local.txt
head -n 3 /tmp/lab02/join_local.txt
```

**Kết quả mong đợi:**

```text
product_id,product_name,category,brand,base_price
P00001,San pham 1,the_thao,Brand08,645000
P00002,San pham 2,gia_dung,Brand16,5596000
P00001	0	San pham 1|Brand08|the_thao
P00001	1	16722850.00
P00001	1	21888000.00
P00001	1	23257900.00
P00001	1	32097600.00
493
P00001	San pham 1	Brand08	the_thao	4	93966350.00
P00002	San pham 2	Brand16	gia_dung	7	151643050.00
P00003	San pham 3	Brand07	the_thao	5	117137300.00
```

Với 1999 giao dịch đầu tiên, 493/500 sản phẩm có ít nhất một giao dịch (reducer không in sản phẩm không có giao dịch).

### Bước 19: Chạy join với KeyFieldBasedPartitioner

<!-- shell: Bên trong container namenode -->
```bash
cd /src/lab02/streaming
hdfs dfs -rm -r -f /user/student/output/join_product_revenue
time hadoop jar $STREAM_JAR \
  -D mapreduce.job.reduces=3 \
  -D stream.num.map.output.key.fields=2 \
  -D mapreduce.partition.keypartitioner.options=-k1,1 \
  -files mapper_join.py,reducer_join.py \
  -mapper "python3 mapper_join.py" \
  -reducer "python3 reducer_join.py" \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -input /data/raw/retail_transactions.csv \
  -input /data/raw/products/products.csv \
  -output /user/student/output/join_product_revenue 2>&1 \
  | grep -E "number of splits|completed|Launched|Map input records|Map output records|Reduce input groups|Reduce output records"
```

**Kết quả mong đợi** (thời gian là ví dụ):

```text
INFO mapreduce.JobSubmitter: number of splits:4
INFO mapreduce.Job: Job job_1793895616834_0007 completed successfully
		Launched map tasks=4
		Launched reduce tasks=3
		Map input records=5000502
		Map output records=5000500
		Reduce input groups=1000
		Reduce output records=500
real	0m38.204s
```

Ý nghĩa các tham số:

| Tham số | Ý nghĩa |
|---|---|
| `stream.num.map.output.key.fields=2` | Hai trường đầu (`product_id`, `tag`) là khoá; phần còn lại là giá trị. Hadoop sắp xếp theo cả hai trường |
| `-partitioner ...KeyFieldBasedPartitioner` + `keypartitioner.options=-k1,1` | Chỉ băm theo trường 1 khi chọn reducer |
| Hai lần `-input` | Mapper đọc cả hai tệp (4 split = 3 split giao dịch + 1 split sản phẩm) |

`Reduce input groups=1000` vì mỗi `product_id` có hai giá trị khoá đầy đủ (`P…\t0` và `P…\t1`); reducer Streaming đọc theo dòng nên vẫn xử lý đúng.

### Bước 20: Kiểm chứng kết quả join và thử bỏ partitioner

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -cat /user/student/output/join_product_revenue/part-* | wc -l
hdfs dfs -cat /user/student/output/join_product_revenue/part-* | grep -c KHONG_RO
hdfs dfs -cat /user/student/output/join_product_revenue/part-* | sort -t$'\t' -k6,6 -g -r | head -n 5
hdfs dfs -cat /user/student/output/join_product_revenue/part-* | awk -F'\t' '{n += $5; s += $6} END {printf "tong giao dich=%d, tong doanh thu=%.2f\n", n, s}'
```

**Kết quả mong đợi:**

```text
500
0
P00275	San pham 275	Brand37	dien_tu	10322	353073859850.00
P00156	San pham 156	Brand13	sach	10172	351397137450.00
P00357	San pham 357	Brand01	thuc_pham	10288	351380129550.00
P00305	San pham 305	Brand35	sach	10190	351247050300.00
P00274	San pham 274	Brand32	me_va_be	10144	351116252050.00
tong giao dich=5000000, tong doanh thu=170192230185950.00
```

Đủ 500 sản phẩm, không dòng nào thiếu thông tin (`KHONG_RO` = 0), tổng 5 000 000 giao dịch và tổng doanh thu đúng bằng tổng 7 danh mục ở Bước 14.

**Thí nghiệm lỗi cố ý:** chạy lại job nhưng **bỏ** `-partitioner` và `keypartitioner.options` (Hadoop dùng HashPartitioner trên toàn bộ khoá `product_id[TAB]tag`):

<!-- shell: Bên trong container namenode -->
```bash
cd /src/lab02/streaming
hdfs dfs -rm -r -f /user/student/output/join_sai
hadoop jar $STREAM_JAR \
  -D mapreduce.job.reduces=3 \
  -D stream.num.map.output.key.fields=2 \
  -files mapper_join.py,reducer_join.py \
  -mapper "python3 mapper_join.py" \
  -reducer "python3 reducer_join.py" \
  -input /data/raw/retail_transactions.csv \
  -input /data/raw/products/products.csv \
  -output /user/student/output/join_sai 2>&1 | grep -E "completed|Reduce output records"
hdfs dfs -cat /user/student/output/join_sai/part-* | grep -c KHONG_RO
hdfs dfs -cat /user/student/output/join_sai/part-* | grep KHONG_RO | head -n 3
```

**Kết quả mong đợi:**

```text
INFO mapreduce.Job: Job job_1793895616834_0008 completed successfully
		Reduce output records=500
500
P00003	KHONG_RO	KHONG_RO	KHONG_RO	10000	339750138050.00
P00006	KHONG_RO	KHONG_RO	KHONG_RO	10213	345096768850.00
P00009	KHONG_RO	KHONG_RO	KHONG_RO	9985	341418929150.00
cat: Unable to write to output stream.
```

Job vẫn "thành công" nhưng **cả 500** sản phẩm mất thông tin: khoá `P00003\t0` và `P00003\t1` có giá trị băm khác nhau nên bản ghi sản phẩm và giao dịch của nó rơi vào hai reducer khác nhau. Đây là lỗi logic điển hình của reduce-side join — Hadoop không báo lỗi, chỉ kiểm chứng dữ liệu mới phát hiện được. Dòng `cat: Unable to write to output stream.` là bình thường: `head -n 3` đóng ống dẫn sớm.

## PHẦN 6: THEO DÕI JOB VÀ SỐ LƯỢNG TASK

### Bước 21: Xem job trên YARN và JobHistory

Mở thêm một Terminal codespace (nút **+** trên khung TERMINAL) để không phải thoát shell `namenode`:

<!-- shell: Terminal codespace (terminal B) -->
```bash
docker exec -it resourcemanager yarn application -list -appStates FINISHED
docker exec -it namenode mapred job -list all
```

**Kết quả mong đợi** (rút gọn; số ứng dụng, ID và thứ tự dòng trên máy bạn sẽ khác):

```text
Total number of applications (application-types: [], states: [FINISHED] and tags: []):8
                Application-Id	    Application-Name	    Application-Type	      User	     Queue	             State	       Final-State	       Progress	                       Tracking-URL
application_1793895616834_0008	streamjob5698387041054622939.jar	           MAPREDUCE	      root	root.default	          FINISHED	         SUCCEEDED	           100%	http://historyserver:19888/jobhistory/job/job_1793895616834_0008
...
application_1793895616834_0001	          word count	           MAPREDUCE	      root	root.default	          FINISHED	         SUCCEEDED	           100%	http://historyserver:19888/jobhistory/job/job_1793895616834_0001
Total jobs:8
                  JobId	                                 JobName	     State	     StartTime	    UserName	       Queue	  Priority	 UsedContainers	 RsvdContainers	 UsedMem	 RsvdMem	 NeededMem	   AM info
 job_1793895616834_0008	        streamjob5698387041054622939.jar	 SUCCEEDED	 1793896060383	        root	root.default	   DEFAULT	            N/A	            N/A	     N/A	     N/A	       N/A	http://resourcemanager:8088/proxy/application_1793895616834_0008/
...
```

> ⚠️ ResourceManager **không lưu** danh sách ứng dụng khi codespace dừng rồi mở lại: sau khi mở lại, lệnh trên chỉ liệt kê các job chạy từ lúc mở lại, ID bắt đầu lại từ `..._0001` với tiền tố mới. Lịch sử job cũ vẫn còn trên JobHistory (cổng 19888) và log container vẫn còn trên HDFS (đã kiểm tra).

JobHistory Server còn có REST API (gọi từ bên trong cụm theo tên `historyserver`; từ Terminal codespace dùng `curl -s localhost:19888/...`):

<!-- shell: Bên trong container namenode -->
```bash
curl -s http://historyserver:19888/ws/v1/history/mapreduce/jobs | python3 -m json.tool \
  | grep -E '"(id|name|state|mapsTotal|reducesTotal)"' | head -n 20
```

**Kết quả mong đợi** (4 job đầu):

```text
                "id": "job_1793895616834_0001",
                "name": "word count",
                "state": "SUCCEEDED",
                "mapsTotal": 1,
                "reducesTotal": 1,
                "id": "job_1793895616834_0002",
                "name": "word count",
                "state": "SUCCEEDED",
                "mapsTotal": 1,
                "reducesTotal": 1,
                "id": "job_1793895616834_0003",
                "name": "word count",
                "state": "SUCCEEDED",
                "mapsTotal": 1,
                "reducesTotal": 2,
                "id": "job_1793895616834_0004",
                "name": "word count",
                "state": "SUCCEEDED",
                "mapsTotal": 1,
                "reducesTotal": 2,
```

Trên trình duyệt, mở JobHistory (địa chỉ cổng 19888 từ `lab ui`), bấm vào một job → **Counters** để xem đầy đủ Counter như ở Bước 6–7; **Map tasks/Reduce tasks** để xem task chạy trên node nào và mất bao lâu. Các liên kết dạng `http://nodemanager1:8042/...` (log của task) không mở được: thay `http://nodemanager1:8042` bằng địa chỉ cổng 8042, `http://nodemanager2:8042` bằng địa chỉ cổng **8043** trong tab PORTS. Chụp màn hình trang **Applications** (cổng 8088) và trang **Counters** của một job (cổng 19888) cho báo cáo.

> 💡 Nếu trình duyệt báo lỗi hoặc trang trắng khi mở cổng lần đầu, kiểm tra tab PORTS xem cổng đã có trong danh sách chưa; chưa có thì bấm **Add Port** và nhập số cổng (8088, 19888, 8042, 8043...). Cổng để chế độ *Private* (mặc định) — chỉ tài khoản GitHub của bạn mở được.

> 💡 Job Streaming có tên mặc định khó đọc (`streamjob...jar`). Đặt tên dễ nhận bằng `-D mapreduce.job.name=ten_job` (dùng ở Bước 22).

### Bước 22: Số map task phụ thuộc split — thí nghiệm

Số **map task = số input split**; số **reduce task = `mapreduce.job.reduces`** (mặc định 1). Hai API của Hadoop tính split khác nhau:

- WordCount (API mới `org.apache.hadoop.mapreduce`): kích thước split = `max(minsize, min(maxsize, blocksize))` → giảm `mapreduce.input.fileinputformat.split.maxsize` để có nhiều split hơn.
- Streaming (API cũ `org.apache.hadoop.mapred`): kích thước split = `max(minsize, min(tổng_kích_thước / mapreduce.job.maps, blocksize))` → tăng gợi ý `mapreduce.job.maps`. Tham số `split.maxsize` **không** có tác dụng với Streaming.

<!-- shell: Bên trong container namenode -->
```bash
cd /src/lab02/streaming
hdfs dfs -rm -r -f /user/student/output/revenue_8maps
hadoop jar $STREAM_JAR \
  -D mapreduce.job.name=revenue_8maps \
  -D mapreduce.job.maps=8 \
  -D mapreduce.job.reduces=2 \
  -files mapper_revenue.py,reducer_revenue.py \
  -mapper "python3 mapper_revenue.py" \
  -reducer "python3 reducer_revenue.py" \
  -input /data/raw/retail_transactions.csv \
  -output /user/student/output/revenue_8maps 2>&1 | grep -E "number of splits|completed|Launched map|Launched reduce"
cd /src/lab02/wordcount
hdfs dfs -rm -r -f /user/student/output/wc_split16
hadoop jar wordcount.jar WordCount \
  -D wordcount.combiner=true \
  -D mapreduce.input.fileinputformat.split.maxsize=16777216 \
  /user/student/text_input /user/student/output/wc_split16 2>&1 | grep -E "number of splits|completed|Launched map"
```

**Kết quả mong đợi** (job 8 map chạy khoảng 50 giây, job 5 map khoảng 30 giây trên máy 2 nhân):

```text
INFO mapreduce.JobSubmitter: number of splits:8
INFO mapreduce.Job: Job job_1793895616834_0009 completed successfully
		Launched map tasks=8
		Launched reduce tasks=2
INFO mapreduce.JobSubmitter: number of splits:5
INFO mapreduce.Job: Job job_1793895616834_0010 completed successfully
		Launched map tasks=5
```

- Streaming: 405 MB / 8 ≈ 50,6 MB mỗi split → 8 map task (so với 3 ở Bước 13).
- WordCount: 67,7 MB / 16 MB → 5 split (so với 1 ở Bước 6).

Mỗi NodeManager có 1792 MB, map container 512 MB, nên toàn cụm chạy đồng thời tối đa khoảng 6 container (trừ 512 MB cho ApplicationMaster); 8 map task sẽ chạy thành nhiều "đợt". Quan sát cột *Running Containers* ở ResourceManager (cổng 8088) khi job đang chạy. Đây cũng là lúc máy codespace dùng nhiều RAM nhất trong buổi (cụm Hadoop + Docker khoảng 4,7 GB trên 8 GB khi đo, chưa tính VS Code); có thể xem bằng `lab status` ở terminal B trong lúc job chạy.

> 💡 Nhiều vCore YARN hơn số nhân thật: 2 NodeManager khai báo tổng 4 vCore nhưng máy codespace chỉ có 2 nhân, nên tăng số map từ 3 lên 8 **không** làm job nhanh hơn — ghi nhận điều này khi trả lời Câu hỏi 3.

### Bước 23: Xem log container bằng yarn logs

Cụm bật *log aggregation*: khi job xong, log của mọi container được gom về HDFS (`/tmp/logs`). Lấy `Application-Id` của job `revenue_8maps` (Bước 22) rồi liệt kê các container của nó:

<!-- shell: Bên trong container namenode -->
```bash
APP=$(yarn application -list -appStates FINISHED 2>/dev/null | awk '/revenue_8maps/ {print $1; exit}')
echo $APP
yarn logs -applicationId $APP 2>/dev/null | grep -E "^Container:" | sort -u
```

**Kết quả mong đợi:**

```text
application_1793895616834_0009
Container: container_1793895616834_0009_01_000001 on nodemanager1_43503
Container: container_1793895616834_0009_01_000002 on nodemanager2_38525
Container: container_1793895616834_0009_01_000003 on nodemanager2_38525
Container: container_1793895616834_0009_01_000004 on nodemanager2_38525
Container: container_1793895616834_0009_01_000005 on nodemanager2_38525
Container: container_1793895616834_0009_01_000006 on nodemanager1_43503
Container: container_1793895616834_0009_01_000007 on nodemanager1_43503
Container: container_1793895616834_0009_01_000009 on nodemanager2_38525
Container: container_1793895616834_0009_01_000010 on nodemanager2_38525
Container: container_1793895616834_0009_01_000011 on nodemanager1_43503
Container: container_1793895616834_0009_01_000013 on nodemanager2_38525
```

11 container = 1 ApplicationMaster (`..._000001`) + 8 map + 2 reduce, phân bố trên cả hai NodeManager (số thứ tự bị nhảy, ví dụ không có `_000008`, `_000012`, là container được cấp rồi thu hồi). Xem nội dung log của một loại (ví dụ `stderr` của script Python) bằng `yarn logs -applicationId $APP -log_files stderr`. ID ứng dụng, cổng NodeManager và cách phân bố container trên máy bạn sẽ khác.

> 💡 Nếu codespace đã dừng và mở lại sau Bước 22, `echo $APP` in ra dòng trống (ResourceManager đã quên job cũ). Lấy ID từ trang JobHistory (cổng 19888): `job_XXXXXXXXXXXXX_NNNN` ứng với `application_XXXXXXXXXXXXX_NNNN`, rồi gán thủ công `APP=application_XXXXXXXXXXXXX_NNNN` — `yarn logs` vẫn đọc được log cũ. Hoặc chạy lại job Bước 22.

### Bước 24: Dọn dẹp

Giữ lại `join_product_revenue` để so sánh ở các buổi sau, xoá các thư mục kết quả còn lại:

<!-- shell: Bên trong container namenode -->
```bash
hdfs dfs -rm -r -skipTrash "/user/student/output/wc_*" "/user/student/output/revenue_*"
hdfs dfs -rm -r -f -skipTrash /user/student/output/join_sai
hdfs dfs -ls /user/student/output
rm -rf /tmp/lab02
exit
```

**Kết quả mong đợi:**

```text
Deleted /user/student/output/wc_hash2
Deleted /user/student/output/wc_no_combiner
Deleted /user/student/output/wc_part2
Deleted /user/student/output/wc_split16
Deleted /user/student/output/wc_with_combiner
Deleted /user/student/output/revenue_8maps
Deleted /user/student/output/revenue_by_category
Deleted /user/student/output/revenue_combiner
Deleted /user/student/output/join_sai
Found 1 items
drwxr-xr-x   - root supergroup          0 2026-11-05 23:27 /user/student/output/join_product_revenue
```

Dữ liệu `/data/raw/...` trên HDFS được giữ lại cho Buổi 3. Lệnh `exit` đưa bạn về Terminal codespace.

## Kết thúc buổi

**1. Tắt dịch vụ bổ sung (nếu đã bật).** Buổi này không dùng dịch vụ bổ sung; nếu lỡ bật thì tắt để lần mở sau khởi động nhanh hơn:

<!-- shell: Terminal codespace -->
```bash
lab stop-extras
```

**2. Lưu bài làm lên GitHub.** Mã nguồn chỉ an toàn khi đã `commit` và `push`: codespace bị xoá (tự xoá sau một thời gian dài không dùng, hoặc bạn xoá nhầm) thì các thay đổi chưa đẩy lên sẽ mất.

<!-- shell: Terminal codespace -->
```bash
git add src/lab02
git status --short
git commit -m "Buoi 2: MapReduce Java + Hadoop Streaming"
git push
```

**Kết quả mong đợi** (phần `git status --short` và `git commit`; mã commit khác):

```text
A  src/lab02/streaming/mapper_join.py
A  src/lab02/streaming/mapper_revenue.py
A  src/lab02/streaming/reducer_join.py
A  src/lab02/streaming/reducer_revenue.py
A  src/lab02/wordcount/WordCount.java
A  src/lab02/wordcount/classes/WordCount$FirstLetterPartitioner.class
A  src/lab02/wordcount/classes/WordCount$IntSumReducer.class
A  src/lab02/wordcount/classes/WordCount$TokenizerMapper.class
A  src/lab02/wordcount/classes/WordCount.class
A  src/lab02/wordcount/wordcount.jar
[main 8d22273] Buoi 2: MapReduce Java + Hadoop Streaming
 10 files changed, 173 insertions(+)
```

> 💡 `classes/` và `wordcount.jar` là tệp sinh ra từ Bước 4 (vài KB), đưa lên cũng không sao. Muốn chỉ nộp mã nguồn thì thay dòng đầu bằng `git add src/lab02/wordcount/WordCount.java src/lab02/streaming/*.py`.

**3. Dừng codespace** để không tốn quota: vào https://github.com/codespaces → nút **...** cạnh codespace → **Stop codespace** (hoặc Command Palette **Ctrl+Shift+P** → *Codespaces: Stop Current Codespace*). Không cần `lab stop` trước: dữ liệu HDFS, image và tệp trong repo được giữ nguyên khi dừng; lần mở sau cụm tự bật lại (khoảng 1–2 phút).

> ⚠️ Đóng tab trình duyệt **không** dừng codespace ngay; codespace chỉ tự dừng sau 30 phút không thao tác và vẫn tính quota trong 30 phút đó.

## Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `ClassNotFoundException: Class WordCount$TokenizerMapper not found` | Jar đóng gói sai cấu trúc (`jar cf x.jar classes/*.class`) | `jar cf wordcount.jar -C classes .`; kiểm tra bằng `jar tf wordcount.jar` |
| `FileAlreadyExistsException: Output directory ... already exists` | Thư mục output đã tồn tại từ lần chạy trước | `hdfs dfs -rm -r -f THU_MUC_OUTPUT` trước khi chạy |
| `java.io.FileNotFoundException: File mapper_join.py does not exist` (job dừng ngay, chưa submit) | `-files` dùng tên tệp tương đối nhưng shell không đứng trong `/src/lab02/streaming` (ví dụ vừa mở lại shell `namenode`) | `cd /src/lab02/streaming` rồi chạy lại, hoặc dùng đường dẫn tuyệt đối như Bước 13 |
| `PipeMapRed.waitOutputThreads(): subprocess failed with code 2` | Quên `-files` (script không có trên node chạy task) hoặc sai tên tệp | Thêm `-files mapper.py,reducer.py`, tên trong `-mapper "python3 mapper.py"` phải trùng tên tệp |
| `subprocess failed with code 1` | Script Python ném ngoại lệ (dòng dữ liệu bất thường, header...) | Kiểm thử cục bộ như Bước 12/18; xem chi tiết bằng `yarn logs -applicationId APP_ID -log_files stderr` |
| Script chạy cục bộ được nhưng lỗi trên cụm, hoặc lỗi `\r` | Tệp soạn trên Windows rồi tải lên codespace có xuống dòng CRLF (thanh trạng thái VS Code hiện `CRLF`) | Bấm `CRLF` ở thanh trạng thái → chọn `LF` → lưu; hoặc `sed -i 's/\r$//' tep.py` |
| `wc -l` ra 96 nhưng `javac` báo lỗi cú pháp, hoặc `wc -l` khác 96 | Dán thiếu/thừa dòng khi copy vào trình soạn thảo | So sánh với nội dung trong bài; dùng **Ctrl+A** rồi dán lại toàn bộ |
| `BrokenPipeError` khi chạy `reducer.py \| head` cục bộ | `head` đóng ống dẫn sớm | Ghi ra tệp rồi `head` tệp đó (như Bước 18) |
| Join cho ra `KHONG_RO` cho mọi sản phẩm | Thiếu `KeyFieldBasedPartitioner` / `keypartitioner.options=-k1,1` | Thêm đúng hai tham số như Bước 19 |
| `split.maxsize` không làm tăng số map của job Streaming | Streaming dùng API `mapred` cũ | Dùng `-D mapreduce.job.maps=N` |
| `InaccessibleObjectException ... does not "opens" java.lang` | Tự ghi đè `mapreduce.map.java.opts` mà bỏ các tuỳ chọn `--add-opens` của Java 17 | Không ghi đè, hoặc chép lại đầy đủ giá trị trong `conf/hadoop/mapred-site.xml` rồi thêm tuỳ chọn của bạn |
| Job đứng ở `ACCEPTED` | NodeManager chưa đăng ký hoặc YARN hết bộ nhớ | `yarn node -list` phải thấy 2 node; dừng job thừa bằng `yarn application -kill APP_ID` |
| Số đếm khác hẳn bài (ví dụ `Map input records=200000`, 1 split ở Bước 13) | Vẫn dùng dữ liệu mẫu 200 000 dòng của codespace tạo trước 06/11/2026 | `lab data 5000000` (Terminal codespace), rồi nạp lại HDFS như Bước 2 và Bước 5 |
| Codespace tự dừng (hoặc mất kết nối lâu) khi job đang chạy; mở lại thấy thư mục output chỉ có `_temporary`, không có `_SUCCESS`, `yarn application -list` không còn job | Codespace dừng sau 30 phút không thao tác; YARN không khôi phục job đang chạy | Chờ cụm tự bật (`lab status`), `hdfs dfs -rm -r -f THU_MUC_OUTPUT` rồi chạy lại job. Dữ liệu HDFS (`/data/raw`, kết quả đã xong) vẫn còn. Không để máy rảnh quá 30 phút giữa buổi |
| Job chậm bất thường, `lab status` báo RAM các container trên 6 GB, hoặc task bị kill | Còn bật dịch vụ bổ sung (Hive, HBase, MongoDB, Kafka) chiếm RAM của máy 8 GB | `lab stop-extras`, rồi chạy lại job |
| Trình duyệt không mở được `http://localhost:8088` | Trong Codespaces, `localhost` là máy của bạn, không phải codespace | Dùng địa chỉ từ `lab ui` hoặc tab PORTS → quả địa cầu |
| Link `http://nodemanager1:8042/...` hoặc `http://historyserver:19888/...` trong Web UI không mở được | Tên máy nội bộ của cụm, trình duyệt không phân giải được | Thay bằng địa chỉ forward của cổng 8042 (nodemanager2 → 8043) hoặc 19888, giữ phần đường dẫn sau |
| Tab PORTS không có cổng 8042/8043/19888, hoặc trang báo lỗi 502 | Cổng chưa được forward, hoặc container chưa `healthy` | `lab status`; trong tab PORTS bấm **Add Port** nhập số cổng; chờ vài giây rồi tải lại trang |
| `the input device is not a TTY` | Chạy `docker exec -it ...` từ chỗ không phải terminal tương tác (script, tác vụ VS Code) | Bỏ `-t` (dùng `docker exec -i ...`) hoặc chạy trong Terminal codespace |
| `git push` bị từ chối (`rejected ... fetch first`) | Repo trên GitHub có commit mới (ví dụ giảng viên cập nhật) | `git pull` rồi `git push` lại |

## Câu hỏi kiểm tra

1. Vì sao `IntSumReducer` dùng được làm Combiner, nhưng một Reducer tính **trung bình** thì không thể dùng trực tiếp làm Combiner? Đề xuất cách sửa để vẫn dùng được Combiner khi tính trung bình.
   *Gợi ý: phép cộng giao hoán/kết hợp; trung bình của các trung bình ≠ trung bình chung — Combiner nên phát (tổng, số lượng).*
2. Ở Bước 7, vì sao `Combine input records` (10 000 056) lớn hơn `Map output records` (10 000 000)? Điều này nói gì về số lần Combiner được gọi?
   *Gợi ý: Combiner chạy ở mỗi lần spill và khi merge các spill; Hadoop không đảm bảo số lần chạy.*
3. Với tệp 405 181 403 byte và block 128 MB, vì sao job ở Bước 13 có 3 map task chứ không phải 4? Nếu muốn 8 map task cho job Streaming thì làm thế nào, vì sao `split.maxsize` không tác dụng?
   *Gợi ý: hệ số SPLIT_SLOP 1,1; API `mapred` dùng goalSize = tổng / mapreduce.job.maps.*
4. Trong reduce-side join ở Phần 5, giải thích vai trò của từng yếu tố: tag `0/1`, `stream.num.map.output.key.fields=2`, `KeyFieldBasedPartitioner -k1,1`. Điều gì xảy ra nếu thiếu partitioner (Bước 20)?
   *Gợi ý: sắp xếp theo khoá đầy đủ để bản ghi sản phẩm đứng trước; phân vùng theo product_id để cùng reducer.*
5. Khi nào nên dùng map-side join (nạp bảng nhỏ vào bộ nhớ mỗi mapper) thay cho reduce-side join? Với `products.csv` (22 KB) và giao dịch (405 MB), cách nào hiệu quả hơn, vì sao?
   *Gợi ý: map-side join tránh shuffle bảng lớn khi một bảng đủ nhỏ để nằm trong bộ nhớ; Buổi 3 sẽ thấy Hive tự chọn Map Join.*

## Bài tập mở rộng

1. **Doanh thu theo thương hiệu (chuỗi hai job).** Viết job Streaming thứ hai đọc đầu ra `join_product_revenue` (cột 3 = brand, cột 6 = doanh thu) và tính tổng doanh thu theo `brand`. So sánh kết quả với `hdfs dfs -cat /user/student/output/join_product_revenue/part-* | awk -F'\t' '{s[$3]+=$6} END {for (b in s) printf "%s\t%.2f\n", b, s[b]}' | sort -k2,2 -g -r | head`.
2. **Map-side join bằng Streaming.** Tạo `src/lab02/streaming/mapper_mapjoin.py`; dùng `-files /src/lab02/streaming/mapper_mapjoin.py,/data/products.csv` để gửi bảng sản phẩm tới mọi mapper; mapper nạp `products.csv` vào `dict` khi khởi động, ghép trực tiếp và phát `brand[TAB]doanh_thu`; reducer cộng dồn. So sánh `Reduce shuffle bytes`, thời gian chạy và kết quả với cách reduce-side join.
3. **Partitioner cho dữ liệu lệch.** Dữ liệu cố ý lệch: `HCM` chiếm 45 % giao dịch. Viết job Java (ví dụ `src/lab02/region/RegionRevenue.java`) tính doanh thu theo `region` với 4 reducer và một Partitioner dành riêng reducer 0 cho `HCM`, reducer 1 cho `Hanoi`, các khu vực còn lại băm vào reducer 2–3. So sánh thời gian từng reduce task trên JobHistory (cổng 19888, địa chỉ lấy bằng `lab ui`) với HashPartitioner mặc định.
4. **Top-N trong Reducer.** Viết job Streaming tìm 10 khách hàng (`customer_id`) có tổng doanh thu cao nhất với **một** reducer giữ heap kích thước 10 (`heapq`). Thảo luận vì sao cách này không mở rộng được khi số reducer > 1 và cách khắc phục (top-N cục bộ ở mỗi reducer rồi gộp).

## Yêu cầu nộp bài và tiêu chí chấm

Nộp một tệp PDF hoặc Markdown `MSSV_Buoi02_BigData.pdf` (hoặc `.md`) kèm mã nguồn, gồm:

- Mã nguồn `WordCount.java`, `mapper_revenue.py`, `reducer_revenue.py`, `mapper_join.py`, `reducer_join.py` (và mã bài mở rộng nếu làm) trong `src/lab02/`: đẩy lên repo GitHub của bạn (mục Kết thúc buổi) và ghi đường dẫn commit vào báo cáo, hoặc nén `src/lab02/` thành zip (chuột phải thư mục `lab02` trong Explorer → **Download...** để tải về máy) nếu giảng viên yêu cầu nộp tệp.
- Log Counter của Bước 6 và Bước 7, bảng so sánh có/không Combiner kèm nhận xét.
- Kết quả Bước 9–10 và giải thích sự khác nhau giữa hai Partitioner.
- Kết quả Streaming Bước 13–14 kèm đối chứng `awk`; kết quả join Bước 19–20 (cả thí nghiệm lỗi cố ý).
- Ảnh chụp Web UI ResourceManager (cổng 8088, danh sách ứng dụng) và JobHistory (cổng 19888, trang Counters của một job), mở qua địa chỉ `lab ui`.
- Trả lời 5 câu hỏi kiểm tra.

| Tiêu chí | Điểm | Mô tả |
|---|---|---|
| WordCount Java biên dịch, đóng gói, chạy thành công (Phần 2) | 1,5 | Jar đúng cấu trúc, kết quả 14 dòng, log Counter đầy đủ |
| Combiner: bảng so sánh Counter và giải thích (Bước 7–8) | 1,5 | Đủ các Counter về shuffle/spill; `diff` xác nhận kết quả không đổi; giải thích đúng |
| Partitioner (Bước 9–10) | 1,0 | Chạy được Partitioner tự viết, giải thích phân bố khoá |
| Hadoop Streaming doanh thu theo danh mục (Phần 4) | 2,0 | Kiểm thử cục bộ, job 4 reducer, đối chứng awk khớp, giải thích số split và tệp rỗng |
| Reduce-side join (Phần 5) | 2,0 | Kết quả 500 dòng, không thiếu thông tin, tổng khớp; giải thích lỗi khi bỏ partitioner |
| Theo dõi job và số task (Phần 6), câu hỏi kiểm tra, trình bày | 2,0 | Ảnh Web UI, thí nghiệm số split, trả lời đúng 5 câu hỏi |
| Bài tập mở rộng (điểm thưởng) | +1,0 | Ít nhất một bài, có mã và kết quả (tổng điểm không vượt quá 10) |
