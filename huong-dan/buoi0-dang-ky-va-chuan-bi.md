# Buổi 0 — Đăng ký GitHub, xác minh sinh viên và chuẩn bị môi trường thực hành trên GitHub Codespaces

Làm **một lần duy nhất**, trước Buổi 1 (ở nhà hoặc trong 45–60 phút đầu của Buổi 1). Cần: trình duyệt Chrome/Edge/Firefox bản mới, Internet ổn định, email sinh viên do trường cấp, thẻ sinh viên còn hạn (bản gốc để chụp trực tiếp), điện thoại cài ứng dụng xác thực (GitHub Mobile, Google Authenticator hoặc Microsoft Authenticator). Không cần cài Docker, Java, Hadoop hay Spark trên máy tính: toàn bộ cụm chạy trong codespace — một máy ảo Linux trên GitHub, mở bằng trình duyệt. Kết thúc buổi này mỗi sinh viên có một repo riêng và một codespace riêng, dùng lại cho cả 7 buổi thực hành, ở bất kỳ phòng máy nào hoặc ở nhà.

## Mục tiêu

Sau buổi này sinh viên:
- Có tài khoản GitHub với họ tên thật, email trường đã xác minh và xác thực hai lớp (2FA).
- Đã nộp (hoặc đã được duyệt) GitHub Education — Student Developer Pack để có quota Codespaces 180 core-hour/tháng thay vì 120.
- Có repo bài làm riêng tạo từ repo mẫu của học phần và một codespace chạy cụm Hadoop/Spark 8 container.
- Kiểm tra được cụm bằng `lab status`, `lab test`, mở được Web UI, sinh được bộ dữ liệu 5 triệu dòng dùng cho các buổi sau.
- Biết quy trình mỗi buổi (mở codespace → làm bài → commit/push → dừng codespace) và cách theo dõi quota.

## Chuẩn bị

| Thứ cần có | Ghi chú |
|---|---|
| Email sinh viên (dạng ...@student.ctu.edu.vn) | Đăng nhập được hộp thư để nhận mã xác minh |
| Thẻ sinh viên còn hạn hoặc giấy xác nhận sinh viên có ngày cấp | Họ tên trên giấy tờ phải khớp từng chữ với tên khai trên GitHub |
| Điện thoại có ứng dụng xác thực | Dùng cho 2FA |
| Link repo mẫu của học phần | Giảng viên cung cấp, ví dụ `https://github.com/tttquyen/bigdatalab` |

> 💡 Không cần chờ Student Developer Pack được duyệt mới làm tiếp. Tài khoản GitHub miễn phí đã có 120 core-hour/tháng — đủ cho khoảng 60 giờ chạy máy 2 nhân. Cứ nộp đơn rồi làm tiếp PHẦN 3.

## PHẦN 1: TẠO TÀI KHOẢN GITHUB

### Bước 1: Đăng ký tài khoản

1. Vào https://github.com/signup, nhập **email cá nhân hoặc email sinh viên**, mật khẩu, và **username**.
2. Chọn username dễ nhận ra, nên có MSSV, ví dụ `b2312345-nguyenvana`. Username xuất hiện trong tên codespace và đường dẫn repo, giảng viên dùng để đối chiếu danh sách lớp.
3. Nhập mã xác minh gửi về email, hoàn tất đăng ký, chọn gói **Free**.

> ⚠️ Mỗi sinh viên **một tài khoản cá nhân**. Không dùng chung tài khoản, không tạo tài khoản tổ chức (Organization): quota Codespaces miễn phí và Student Developer Pack chỉ áp dụng cho tài khoản cá nhân.

### Bước 2: Thêm và xác minh email trường

1. Vào **Settings → Emails** (https://github.com/settings/emails) → *Add email address* → nhập email sinh viên → **Add**.
2. Mở hộp thư sinh viên, bấm link xác minh. Dòng email đó phải chuyển sang trạng thái *Verified*.

### Bước 3: Khai họ tên thật — khớp từng chữ với thẻ sinh viên

Đây là lý do số một khiến đơn Student Developer Pack bị từ chối. Sửa **hai chỗ**:

| Chỗ sửa | Đường dẫn | Ví dụ (thẻ ghi NGUYEN VAN AN) |
|---|---|---|
| Tên hồ sơ | Settings → Public profile → *Name* | `Nguyen Van An` |
| Thông tin thanh toán | Settings → Billing and licensing → Payment information | *First name* = `Van An`, *Last name* = `Nguyen` |

- Viết **đúng như trên giấy tờ**: nếu thẻ có dấu (`Nguyễn Văn An`) thì ghi có dấu; nếu thẻ in không dấu thì ghi không dấu. Không dùng biệt danh, không viết tắt.
- Không cần nhập thẻ thanh toán — chỉ điền họ tên và địa chỉ.
- Sửa xong **đăng xuất rồi đăng nhập lại** GitHub.

### Bước 4: Bật xác thực hai lớp (2FA)

1. **Settings → Password and authentication → Enable two-factor authentication**.
2. Quét mã QR bằng ứng dụng xác thực, nhập mã 6 số.
3. **Tải về và cất kỹ tệp recovery codes** (in ra hoặc lưu vào nơi an toàn). Mất điện thoại mà không có recovery codes thì không vào lại được tài khoản.

> ⚠️ GitHub bắt buộc 2FA với tài khoản đẩy mã lên repo; tài khoản mới sẽ bị yêu cầu bật trong vài tuần đầu. Bật ngay để không bị khoá giữa học kỳ.

## PHẦN 2: XÁC MINH GITHUB EDUCATION (STUDENT DEVELOPER PACK)

### Bước 5: Nộp đơn

1. Vào https://github.com/settings/education/benefits → **Start an application** (hoặc từ https://education.github.com/pack → *Get student benefits*).
2. Chọn **Student**. Chọn email sinh viên đã xác minh ở Bước 2.
3. Trường học: chọn đúng **Can Tho University** (tên trường, không chọn tên khoa).
4. Khi trang yêu cầu, **cho phép trình duyệt truy cập vị trí** (nên nộp khi đang ở trường) và **tắt VPN**.
5. Bằng chứng: chụp **trực tiếp bằng camera** thẻ sinh viên còn hạn (hoặc giấy xác nhận sinh viên có ngày cấp). Ảnh rõ, đủ 4 góc, không lóa, thấy rõ họ tên, tên trường, thời hạn.
6. **Submit**. Kết quả thường có trong vòng vài ngày, gửi về email.

Khi được duyệt: tài khoản được nâng lên GitHub Pro, quota Codespaces tăng lên **180 core-hour và 20 GB lưu trữ mỗi tháng**.

### Bước 6: Nếu bị từ chối

Đọc kỹ email từ chối, sửa đúng lỗi rồi nộp lại (được nộp lại ngay):

| Nội dung từ chối | Cách sửa |
|---|---|
| Họ (last name) / tên (first name) trên giấy tờ không khớp thông tin thanh toán | Làm lại Bước 3: Last name = Họ, First name = Tên đệm + Tên, đúng dấu như trên thẻ; đăng xuất/đăng nhập lại |
| Hồ sơ chưa có họ tên đầy đủ | Điền *Name* ở Public profile giống giấy tờ |
| Giấy tờ không thể hiện đang là sinh viên / không có ngày | Dùng giấy xác nhận sinh viên do phòng Công tác sinh viên cấp, có ngày cấp, có chữ ký, dấu; tốt nhất có bản tiếng Anh |
| Ảnh mờ, bị cắt, không đọc được | Chụp lại dưới ánh sáng tốt, đủ khung |
| Không xác định được trường | Chọn đúng *Can Tho University*, dùng email trường |

> 💡 Trong lúc chờ duyệt vẫn dùng Codespaces bình thường bằng quota 120 core-hour của tài khoản miễn phí.

## PHẦN 3: TẠO REPO BÀI LÀM VÀ CODESPACE

### Bước 7: Tạo repo riêng từ repo mẫu

1. Mở link repo mẫu giảng viên cung cấp (ví dụ https://github.com/tttquyen/bigdatalab).
2. Bấm **Use this template → Create a new repository**.
3. *Owner*: tài khoản của mình. *Repository name*: `ct482-<MSSV>`, ví dụ `ct482-b2312345`. Chọn **Private**. Bấm **Create repository**.
4. Trong repo vừa tạo: **Settings → Collaborators → Add people** → nhập username giảng viên (vd. `tttquyen`) → giảng viên được xem bài làm để chấm.

> ⚠️ Không dùng **Fork**: bản fork của repo công khai luôn công khai, bài làm sẽ lộ cho cả lớp. Không tạo codespace trực tiếp trên repo mẫu của giảng viên — bài làm sẽ không đẩy lên được.

### Bước 8: Tạo codespace

1. Trong repo `ct482-<MSSV>` của mình: nút xanh **Code → tab Codespaces → dấu `...` → New with options**.
2. Chọn:
   - *Branch*: `main`
   - *Region*: **Southeast Asia**
   - *Machine type*: **2-core** (8 GB RAM, 32 GB đĩa) — mặc định, đủ cho Buổi 1–6
3. Bấm **Create codespace**. Trình duyệt mở giao diện VS Code; góc dưới có dòng *Setting up your codespace* / *Building codespace*.
4. Bấm **View creation log** (hoặc mở Command Palette `Ctrl+Shift+P` → *Codespaces: View Creation Log*) để theo dõi. Lần tạo đầu mất khoảng **3–5 phút** (tải image `bigdata-lab` dựng sẵn, sinh dữ liệu mẫu, bật cụm). Nếu log ghi `Build image từ Dockerfile` thì mất thêm khoảng 10 phút — báo giảng viên kiểm tra image trên GHCR.

**Kết quả mong đợi** (cuối log tạo codespace):
```text
[khoi-tao 16:20:19] Xong. Gõ 'lab' để xem các lệnh.
...
 Container nodemanager1  Healthy
 Container historyserver  Healthy
[khoi-dong] Cụm đã sẵn sàng. Gõ 'lab ui' để lấy đường dẫn Web UI.
```

> 💡 Tên codespace do GitHub tự đặt (vd. `fuzzy-space-giggle-x7g9q4...`). Danh sách codespace của mình luôn ở https://github.com/codespaces.

## PHẦN 4: KIỂM TRA MÔI TRƯỜNG

### Bước 9: Xem trạng thái cụm

Mở **Terminal** (menu ☰ → Terminal → New Terminal, hoặc phím tắt Ctrl + phím dấu huyền). Dấu nhắc có dạng `@<username> ➜ /workspaces/ct482-<MSSV> (main) $` — đây là **Terminal codespace**, nơi gõ mọi lệnh `lab ...`, `docker ...`, `git ...` của các buổi.

<!-- shell: Terminal codespace -->
```bash
lab
lab status
```

**Kết quả mong đợi** (máy 2 nhân / 8 GB; số RAM, đĩa và thời gian `Up ...` thay đổi theo máy):
```text
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
NAME              STATUS
datanode1         Up 14 minutes (healthy)
datanode2         Up 14 minutes (healthy)
historyserver     Up 14 minutes (healthy)
namenode          Up 14 minutes (healthy)
nodemanager1      Up 14 minutes (healthy)
nodemanager2      Up 14 minutes (healthy)
resourcemanager   Up 14 minutes (healthy)
spark-master      Up 14 minutes (healthy)

HDFS: Live datanodes (2):
YARN: Total Nodes:2
RAM các container: 2.1 GB
RAM máy codespace: dùng 2.6 / 8.0 GB
Đĩa Docker (image + volume + container): ... GB
Dữ liệu mẫu: 5000000 giao dịch
```

Đủ 8 container `(healthy)`, HDFS có 2 DataNode, YARN có 2 NodeManager, dữ liệu mẫu 5 000 000 giao dịch là đạt. Dòng *RAM máy codespace* tính cả VS Code — còn dưới khoảng 1,5 GB trống thì nên tắt bớt dịch vụ (`lab stop-extras`). Nếu có container `(health: starting)` thì chờ 30–60 giây rồi gõ lại.

### Bước 10: Chạy kiểm thử toàn cụm

<!-- shell: Terminal codespace -->
```bash
lab test
```

**Kết quả mong đợi** (khoảng 2,5–3,5 phút trên máy 2 nhân):
```text
PASS  HDFS 2 live datanodes
PASS  YARN 2 nodemanagers
PASS  Spark 2 alive workers
PASS  HDFS put
PASS  MR examples pi
PASS  MR wordcount
PASS  Hadoop Streaming python
PASS  SparkPi standalone
PASS  SparkPi YARN
PASS  PySpark HDFS parquet
PASS  Spark History lists apps
== 11 PASS, 0 FAIL
```

Chụp màn hình kết quả này để nộp (mục Yêu cầu nộp bài).

### Bước 11: Mở các Web UI

<!-- shell: Terminal codespace -->
```bash
lab ui
```

**Kết quả mong đợi** (tên codespace của mỗi người khác nhau):
```text
HDFS NameNode            https://ten-codespace-x7g9q4-9870.app.github.dev
YARN ResourceManager     https://ten-codespace-x7g9q4-8088.app.github.dev
Spark Master             https://ten-codespace-x7g9q4-8080.app.github.dev
Spark UI (job đang chạy) https://ten-codespace-x7g9q4-4040.app.github.dev
Spark History            https://ten-codespace-x7g9q4-18080.app.github.dev
MapReduce JobHistory     https://ten-codespace-x7g9q4-19888.app.github.dev
HiveServer2 UI           https://ten-codespace-x7g9q4-10002.app.github.dev
HBase Master             https://ten-codespace-x7g9q4-16010.app.github.dev
HBase RegionServer       https://ten-codespace-x7g9q4-16030.app.github.dev
NodeManager 1            https://ten-codespace-x7g9q4-8042.app.github.dev
NodeManager 2            https://ten-codespace-x7g9q4-8043.app.github.dev
DataNode 1               https://ten-codespace-x7g9q4-9864.app.github.dev
DataNode 2               https://ten-codespace-x7g9q4-9865.app.github.dev
(Hoặc mở tab PORTS bên cạnh TERMINAL, bấm biểu tượng quả địa cầu.)
```

- `Ctrl` + bấm vào link để mở. Lần đầu GitHub hỏi xác nhận — bấm **Continue**. Các cổng này chỉ chủ codespace mở được (chế độ *Private*).
- Cách khác: tab **PORTS** cạnh TERMINAL → di chuột vào dòng *HDFS NameNode (9870)* → biểu tượng quả địa cầu.
- Mở **HDFS NameNode**: mục *Live Nodes* = 2. Mở **YARN ResourceManager**: *Active Nodes* = 2.
- Link bên trong Web UI trỏ tới tên container (vd. `http://nodemanager1:8042/...`) sẽ không mở được trong trình duyệt — dùng link tương ứng từ `lab ui` thay thế.

> 💡 Bên trong Terminal codespace vẫn gọi được `curl localhost:9870` như trên máy thật — các bài thực hành dùng cách này để kiểm tra nhanh.

### Bước 12: Kiểm tra bộ dữ liệu 5 triệu dòng dùng cho các buổi

Các bài thực hành dùng 5 triệu giao dịch. Codespace tạo từ 06/11/2026 đã sinh sẵn bộ này khi tạo (dòng `Dữ liệu mẫu: 5000000 giao dịch` trong `lab status`). Nếu `lab status` báo ít hơn (codespace tạo trước đó, hoặc đã xoá thư mục `data/`), sinh lại **một lần** cho cả học kỳ:

<!-- shell: Terminal codespace -->
```bash
lab data 5000000
wc -l data/retail_transactions.csv
ls -lh data
```

**Kết quả mong đợi** (khoảng 30 giây):
```text
  giao dich: 4,500,000
  products.csv: 0.0 MB
  retail_transactions.csv: 405.2 MB
  web_access.log: 121.4 MB
  sensor_readings.csv: 22.7 MB
5000001 data/retail_transactions.csv
total 524M
-rw-r--r-- 1 vscode vscode  23K ... products.csv
-rw-r--r-- 1 vscode vscode 387M ... retail_transactions.csv
-rw-r--r-- 1 vscode vscode  22M ... sensor_readings.csv
-rw-r--r-- 1 vscode vscode 116M ... web_access.log
```

> 💡 Thư mục `data/` nằm trong `.gitignore` — dữ liệu không bị đẩy lên GitHub (quá lớn), nhưng vẫn còn trong codespace giữa các buổi. Mất codespace thì sinh lại bằng đúng lệnh trên (cùng hạt giống ngẫu nhiên nên ra cùng dữ liệu).

## PHẦN 5: LƯU BÀI LÀM BẰNG GIT

### Bước 13: Commit và push lần đầu

Trong codespace, Git đã đăng nhập sẵn bằng tài khoản GitHub của mình — không cần nhập mật khẩu hay token. Bài làm của mọi buổi đặt trong thư mục `src/` (vd. `src/lab01/`, `src/lab02/` ...).

<!-- shell: Terminal codespace -->
```bash
mkdir -p src/lab00
echo "Họ tên: <họ tên> — MSSV: <MSSV>" > src/lab00/thong-tin.txt
git add src/lab00
git commit -m "Buoi 0: tao moi truong"
git push
git log --oneline -3
```

**Kết quả mong đợi:**
```text
[main 1a2b3c4] Buoi 0: tao moi truong
 1 file changed, 1 insertion(+)
 create mode 100644 src/lab00/thong-tin.txt
...
To https://github.com/<username>/ct482-<MSSV>
   ...  main -> main
```

Mở repo trên github.com: thấy thư mục `src/lab00` là đã lưu thành công. Có thể dùng giao diện thay cho lệnh: biểu tượng **Source Control** (thanh bên trái) → nhập thông điệp → **Commit** → **Sync Changes**.

> ⚠️ Cái gì chưa `git push` thì chỉ nằm trong codespace. Codespace bị xoá (không mở quá 30 ngày, hoặc tự xoá nhầm) là mất. Cuối **mỗi** buổi đều phải push.

## PHẦN 6: QUY TRÌNH MỖI BUỔI VÀ QUẢN LÝ QUOTA

### Bước 14: Dừng codespace khi không dùng

Codespace **đang chạy** là đang trừ quota, kể cả khi chỉ để tab mở không làm gì. Dừng bằng một trong các cách:
- https://github.com/codespaces → dấu `...` cạnh tên codespace → **Stop codespace**.
- Trong codespace: `Ctrl+Shift+P` → *Codespaces: Stop Current Codespace*.

Codespace tự dừng sau **30 phút không thao tác**. Đóng tab trình duyệt **không** dừng ngay codespace — phải chờ hết 30 phút.

Mỗi buổi sau đó:
1. Vào https://github.com/codespaces → bấm tên codespace → chờ 1–2 phút, cụm lõi tự bật lại (dữ liệu HDFS, Hive, Kafka... vẫn còn).
2. `lab status` → làm bài theo hướng dẫn của buổi; bật dịch vụ bổ sung khi bài yêu cầu (`lab hive`, `lab nosql`, `lab kafka`).
3. Cuối buổi: `lab stop-extras` (nếu có bật), `git add src && git commit -m "Buoi N" && git push`, **Stop codespace**.

### Bước 15: Chỉnh cài đặt Codespaces và theo dõi quota

1. https://github.com/settings/codespaces:
   - *Default idle timeout*: giữ **30 phút** (tăng lên chỉ khi chạy job dài, nhớ hạ lại).
   - *Default retention period*: để tối đa **30 ngày**.
   - *Region*: Southeast Asia.
2. Theo dõi đã dùng bao nhiêu: **Settings → Billing and licensing → Usage** (lọc *Codespaces*): *Compute* (core-hour) và *Storage* (GB-tháng).

| Tài khoản | Core-hour/tháng | Giờ chạy máy 2 nhân | Giờ chạy máy 4 nhân | Lưu trữ |
|---|---|---|---|---|
| GitHub Free | 120 | 60 giờ | 30 giờ | 15 GB-tháng |
| Đã xác minh Student Developer Pack | 180 | 90 giờ | 45 giờ | 20 GB-tháng |

Một buổi thực hành 3 giờ trên máy 2 nhân tốn 6 core-hour. 7 buổi + làm thêm ở nhà vẫn còn dư nhiều nếu nhớ dừng codespace.

> ⚠️ Chỉ giữ **một** codespace cho học phần. Mỗi codespace chiếm khoảng 7–14 GB lưu trữ (image Hadoop/Spark và các dịch vụ bổ sung) — hai codespace là vượt quota lưu trữ 15 GB của tài khoản miễn phí. Xoá codespace thừa tại github.com/codespaces → `...` → *Delete*.

> 💡 Không khai báo phương thức thanh toán (thẻ) thì không phát sinh chi phí: hết quota, GitHub chỉ chặn tạo/mở codespace đến chu kỳ tháng sau. Khi đó vẫn lấy được bài làm đã push trên github.com.

### Bước 16 (tuỳ chọn): Mở codespace bằng VS Code trên máy

Nếu máy có VS Code: cài tiện ích **GitHub Codespaces**, `Ctrl+Shift+P` → *Codespaces: Connect to Codespace*. Cụm vẫn chạy trên GitHub, máy chỉ hiển thị — giao diện mượt hơn trình duyệt, cổng Web UI tự chuyển về `http://localhost:<cổng>`.

## Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Đơn Student Developer Pack bị từ chối vì tên | Tên hồ sơ / billing khác giấy tờ | Bước 3 và Bước 6; đăng xuất, đăng nhập lại rồi nộp lại |
| Không thấy nút *Use this template* | Repo mẫu chưa bật chế độ template | Báo giảng viên |
| Log tạo codespace ghi `Không tải được ghcr.io/...` rồi `Build image từ Dockerfile` | Image dựng sẵn chưa công khai | Vẫn chạy được, chờ thêm ~10 phút; báo giảng viên |
| `lab: command not found` | Bước khởi tạo chưa xong hoặc lỗi | Xem creation log; chạy `bash .devcontainer/khoi-tao.sh` |
| `lab status` có container `(health: starting)` hoặc `Exited` | Cụm đang khởi động / thiếu RAM | Chờ 1 phút; vẫn lỗi thì `lab start` |
| Mở link `lab ui` báo 404/502 | Dịch vụ chưa bật (vd. Hive) hoặc chưa sẵn sàng | Bật dịch vụ của buổi, chờ `healthy` rồi tải lại trang |
| `git push` bị từ chối (`Permission denied`) | Đang mở codespace trên repo mẫu của giảng viên | Tạo repo riêng (Bước 7) và codespace mới từ repo đó |
| Không mở được codespace, báo hết quota | Dùng hết core-hour hoặc vượt lưu trữ | Xoá codespace thừa; chờ tháng sau; báo giảng viên để dùng phương án dự phòng |
| Codespace “biến mất” | Không mở quá 30 ngày nên bị xoá tự động | Tạo codespace mới từ repo của mình (bài đã push vẫn còn), `lab data 5000000` |

## Câu hỏi kiểm tra

1. Codespace của bạn đang chạy ở đâu? Máy tính phòng thực hành đóng vai trò gì?
2. Vì sao đóng tab trình duyệt chưa phải là dừng codespace? Làm thế nào để dừng ngay?
3. Một buổi 3 giờ trên máy 4 nhân tốn bao nhiêu core-hour? Với quota 180 core-hour/tháng thì được bao nhiêu buổi như vậy?
4. Thư mục `data/` có được đẩy lên GitHub không? Nếu mất codespace thì lấy lại dữ liệu bằng cách nào?
5. Lệnh nào bật Hive, HBase + MongoDB, Kafka? Vì sao không nên bật tất cả cùng lúc trên máy 2 nhân / 8 GB?

## Yêu cầu nộp bài và tiêu chí chấm

Nộp theo hình thức giảng viên quy định (form/LMS):

| Nội dung nộp | Điểm |
|---|---|
| Username GitHub + link repo `ct482-<MSSV>` (Private, đã thêm giảng viên làm collaborator) | 3 |
| Ảnh chụp `lab status` đủ 8 container healthy | 2 |
| Ảnh chụp `lab test` `== 11 PASS, 0 FAIL` | 3 |
| Commit `Buoi 0` có tệp `src/lab00/thong-tin.txt` trên GitHub | 2 |
| Cộng: ảnh email xác nhận Student Developer Pack được duyệt | +1 |
