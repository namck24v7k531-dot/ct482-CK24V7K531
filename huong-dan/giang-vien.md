# Hướng dẫn giảng viên — Tổ chức thực hành CT482 trên GitHub Codespaces (dành cho GV)

Trang dành cho giảng viên: chuẩn bị repo và image một lần mỗi học kỳ, xác minh tài khoản giáo viên, tổ chức Buổi 0, thu bài, theo dõi quota, ghi chú dạy từng buổi trên Codespaces, xử lý sự cố và kết quả kiểm thử. Môi trường: repo `tttquyen/bigdatalab` (cụm `bigdata-lab:2026.10` — Hadoop 3.4.1, Spark 4.0.2, Hive 4.2.1, HBase, Kafka 4.3.1, MongoDB 8.0) chạy Docker-in-Docker trong codespace. Sinh viên làm theo các trang Buổi 0 → Buổi 7 (Codespaces).

## Mục tiêu

- Sinh viên không phải cài lại môi trường khi đổi phòng máy: chỉ cần trình duyệt; dữ liệu HDFS/Hive/Kafka và bài làm giữ nguyên giữa các buổi.
- Giảng viên chuẩn bị một lần (repo mẫu + image dựng sẵn), phát một link cho cả lớp.
- Kiểm soát được quota: mỗi sinh viên dùng quota Codespaces cá nhân (120 core-hour; 180 nếu xác minh sinh viên).

## Chuẩn bị

### Tình trạng hiện tại (kiểm tra ngày 05/11/2026)

| Hạng mục | Trạng thái | Việc cần làm |
|---|---|---|
| Repo `tttquyen/bigdatalab` | Public, nhánh `main` | — |
| Bật *Template repository* | Đã bật (06/11/2026) | — |
| Image `ghcr.io/tttquyen/bigdata-lab:2026.10` | Đã build bằng GitHub Actions; package **Public** (06/11/2026, đã thử tải không đăng nhập: được) | — |
| `LAB_IMAGE` trong `.devcontainer/cau-hinh.env` | `ghcr.io/tttquyen/bigdata-lab:2026.10` | — |
| Codespace của GV trên Codespaces thật | 8/8 healthy, `lab test` 11/11 PASS (25/10/2026) | — |
| Mã học phần trong repo | Image và cấu hình (Dockerfile, `conf/`, `scripts/`, docker-compose, cluster ID HDFS `CID-ct428-...`) còn ghi `CT428`; mã đúng là **CT482** | Giữ nguyên trong image/cấu hình (đổi sẽ phải build lại image); tài liệu (README, TESTING, hướng dẫn) và tên codespace đã đổi sang CT482 (06/11/2026) |
| Xác minh giáo viên GitHub Education | Lần nộp 25/10/2026 bị từ chối do lệch tên | Xem mục Xác minh tài khoản giáo viên |

> ⚠️ Khi package image còn Private, codespace của **sinh viên** không tải được image (codespace của GV tải được vì cùng chủ sở hữu). Khi đó `khoi-tao.sh` tự chuyển sang build từ Dockerfile: lần tạo đầu mất thêm khoảng 10 phút và tải ~1,5 GB từ máy chủ Apache cho **mỗi** sinh viên. Phải chuyển package sang Public trước Buổi 0.

### Kiểm tra sau khi chuyển Public

<!-- shell: Terminal máy bất kỳ có Docker (không đăng nhập ghcr.io) -->
```bash
docker logout ghcr.io
docker pull ghcr.io/tttquyen/bigdata-lab:2026.10
```

**Kết quả mong đợi:**
```text
2026.10: Pulling from tttquyen/bigdata-lab
...
Status: Downloaded newer image for ghcr.io/tttquyen/bigdata-lab:2026.10
```

Nếu báo `denied` / `unauthorized` thì package vẫn chưa Public.

## PHẦN 1: CHUẨN BỊ MỘT LẦN MỖI HỌC KỲ

### Bước 1: Repo mẫu và image dựng sẵn

1. Repo đã có đủ: `Dockerfile`, `docker-compose*.yml`, `conf/`, `gen_data.py`, `.devcontainer/` (cấu hình codespace, lệnh `lab`), `.github/workflows/build-lab-image.yml` (build image lên GHCR), thư mục `huong-dan/` (toàn bộ bài Buổi 0–7 bản Codespaces dạng Markdown).
2. Khi đổi phiên bản phần mềm (sửa `Dockerfile`/`conf/`/`scripts/`): tab **Actions → Build image bigdata-lab → Run workflow**; đổi tag (vd. `2027.02`) ở workflow, `docker-compose*.yml` và `LAB_IMAGE`. Không sửa image giữa học kỳ — codespace đã tạo vẫn giữ image cũ.
3. Bật **Template repository** và chuyển package image sang **Public** (bảng trên).
4. Tự tạo thử một codespace **bằng tài khoản phụ** (đóng vai sinh viên, không phải chủ repo) từ template: kiểm chứng tải image từ GHCR (log ghi `Tải image dựng sẵn`, không có `Build image từ Dockerfile`), `lab test` 11/11.

### Bước 2: Các tham số trong `.devcontainer/cau-hinh.env`

| Biến | Giá trị hiện tại | Ý nghĩa / khi nào đổi |
|---|---|---|
| `LAB_IMAGE` | `ghcr.io/tttquyen/bigdata-lab:2026.10` | Image dựng sẵn; để trống thì codespace tự build (~10 phút) |
| `AUTO_START` | `core` | Tự bật cụm lõi 8 container mỗi lần mở codespace; `none` để sinh viên tự `lab start` |
| `DATA_ROWS` | `5000000` (từ 06/11/2026; trước đó `200000`) | Số giao dịch sinh lúc tạo codespace (~30 giây, 524 MB). Codespace tạo trước ngày này phải chạy `lab data 5000000` một lần |
| `PULL_EXTRAS` | `0` | `1` = tải sẵn image Hive/Kafka/MongoDB/PostgreSQL lúc tạo codespace (+~7 GB đĩa); để `0` cho tài khoản Free 15 GB |

`hostRequirements` trong `devcontainer.json` là 2 nhân / 8 GB / 32 GB — máy nhỏ nhất được phép; sinh viên vẫn chọn được 4 nhân khi tạo codespace.

### Bước 3: GitHub Classroom không còn dùng được

GitHub Classroom đã ngừng hoạt động từ 28/08/2026. Hạn mức Codespaces theo lớp (Codespaces Education benefit) cũng chấm dứt cùng thời điểm. Giáo viên và sinh viên đã xác minh vẫn có quota Codespaces cá nhân 180 core-hour và 20 GB mỗi tháng. Vì vậy học phần dùng mô hình **repo mẫu (template) + repo riêng của từng sinh viên + quota cá nhân**:

| | Cách làm |
|---|---|
| Phát môi trường | Gửi link repo mẫu; sinh viên *Use this template* → repo Private `ct482-<MSSV>` |
| Chi phí | Mỗi sinh viên dùng quota cá nhân; không phát sinh chi phí cho GV/khoa nếu sinh viên không khai báo thẻ thanh toán |
| Thu bài | Sinh viên thêm GV làm collaborator; GV clone hàng loạt bằng `gh` (PHẦN 3) |
| Cập nhật hướng dẫn giữa kỳ | Sửa trên Notion; tệp trong repo mẫu không tự đồng bộ sang repo sinh viên đã tạo (xem PHẦN 3, Bước 9) |

> 💡 Nếu khoa cần quản lý tập trung (chấm tự động, xem tiến độ), các nền tảng đối tác GitHub khuyến nghị thay Classroom (Codio, Classroom 50) có thể dùng lại nguyên repo mẫu này. Không bắt buộc cho học phần.

### Bước 3b: Thay đổi repo ngày 06/11/2026 (đã kiểm thử trên bản sao)

| Tệp | Thay đổi | Lý do |
|---|---|---|
| `.devcontainer/lab.sh` | `lab status` in thêm RAM cả máy codespace, dung lượng Docker, số dòng dữ liệu mẫu (nhắc `lab data 5000000` khi thiếu) | Sinh viên thấy sắp hết RAM/đĩa |
| `.devcontainer/lab.sh` | `lab ui` thêm HBase RegionServer, NodeManager 1/2, DataNode 1/2; trả mã 0 khi chạy ngoài Codespaces | Buổi 2, 4 cần các cổng này |
| `.devcontainer/lab.sh` | `lab stop-extras` dọn dữ liệu tạm Tez (~1,7 GB) trước khi dừng | Buổi 3 làm đĩa vượt 15 GB |
| `.devcontainer/lab.sh` | `lab hive/nosql/kafka` báo trước khi phải tải image lần đầu | Sinh viên tưởng treo |
| `.devcontainer/cau-hinh.env` | `DATA_ROWS=5000000` | Mọi bài dùng 5 triệu dòng |
| `.devcontainer/devcontainer.json` | Forward cổng 4041; tên hiển thị CT482 | Spark UI của ứng dụng thứ hai |
| `.gitignore` | `__pycache__/`, `*.pyc`, `.venv/` | Buổi 4, 5, 6 đưa nhầm tệp sinh ra lên repo |
| `huong-dan/` | Bài Buổi 0–7 bản Codespaces + trang GV dạng Markdown | Sinh viên đọc ngay trong codespace |

Codespace đã tạo trước ngày này vẫn dùng `lab.sh` cũ cho tới khi sinh viên lấy tệp mới (PHẦN 3, Bước 9) — các bài hướng dẫn đúng với cả hai phiên bản, chỉ khác vài dòng in thêm.

## PHẦN 2: XÁC MINH TÀI KHOẢN GIÁO VIÊN

Không bắt buộc để dạy (codespace của GV chạy bằng quota 120 core-hour miễn phí), nhưng nên có để được 180 core-hour và hướng dẫn sinh viên từ kinh nghiệm thực tế.

### Bước 4: Khớp tên tuyệt đối với giấy tờ

- *Settings → Public profile → Name* và *Settings → Billing and licensing → Payment information* (Last name = Họ, First name = Tên đệm + Tên) phải trùng **từng ký tự** với giấy tờ, không kèm học hàm/học vị (TS., ThS.).
- Cách an toàn: giấy tờ song ngữ có tên không dấu (`Tran Thi To Quyen`) và GitHub ghi đúng không dấu như vậy.
- Đăng xuất, đăng nhập lại trước khi nộp.

### Bước 5: Giấy tờ thể hiện rõ là giảng viên

Giấy xác nhận công tác (phòng Tổ chức cán bộ), tốt nhất song ngữ Việt–Anh, có: họ tên khớp Bước 4; chức danh **Giảng viên / Lecturer**; tên trường đầy đủ **Trường Đại học Cần Thơ / Can Tho University**; ngày cấp gần đây; chữ ký, con dấu. Thẻ cán bộ không có ngày thường bị loại.

### Bước 6: Nộp đơn

Thêm và xác minh email `@ctu.edu.vn`; chọn loại **Teacher**; chọn trường **Can Tho University** (không chọn khoa); chụp giấy tờ bằng camera ngay trên trang; tắt VPN, nộp khi ở trường. Vẫn bị từ chối: mở yêu cầu tại support.github.com (mục GitHub Education), giải thích cấu trúc Họ / Đệm / Tên tiếng Việt.

## PHẦN 3: TỔ CHỨC LỚP

### Bước 7: Trước Buổi 0 (1 tuần)

1. Gửi sinh viên link trang **Buổi 0 — Đăng ký GitHub ...** và yêu cầu làm trước PHẦN 1–2 ở nhà (tạo tài khoản, khai tên, 2FA, nộp Student Developer Pack): duyệt mất vài ngày; xử lý trước tránh dồn vào giờ học.
2. Lập form thu: MSSV, họ tên, username GitHub, link repo.
3. Kiểm tra phòng máy: trình duyệt bản mới, không chặn `*.github.dev`, `*.app.github.dev`, `github.com`, `ghcr.io`, `*.githubusercontent.com`; WebSocket không bị proxy chặn. Mỗi sinh viên chỉ truyền giao diện (vài trăm kbps), image tải trong hạ tầng GitHub chứ không qua mạng phòng máy.

### Bước 8: Kịch bản Buổi 0 trên lớp (60 phút)

| Phút | Hoạt động | Ghi chú |
|---|---|---|
| 0–10 | Giới thiệu mô hình codespace, quota, quy trình mỗi buổi | Chiếu bảng quota ở Buổi 0, Bước 15 |
| 10–20 | Sinh viên tạo repo từ template, thêm GV làm collaborator | Kiểm tra repo là **Private**, không Fork |
| 20–30 | Tạo codespace (2-core, Southeast Asia), xem creation log | 3–5 phút nếu image Public |
| 30–45 | `lab status`, `lab test`, `lab ui` | Trong lúc `lab test` chạy (~3 phút) giải thích các Web UI |
| 45–55 | `lab data 5000000`, commit/push `src/lab00` | Đây là bước hay quên |
| 55–60 | Stop codespace, nộp form | Nhắc idle timeout 30 phút và hạn 30 ngày |

### Bước 9: Cập nhật tệp giữa học kỳ

Repo của sinh viên tạo từ template là bản **độc lập** (không có liên kết upstream), nên `git pull` không lấy được sửa đổi của GV. Khi cần phát tệp mới (sửa script, thêm dữ liệu):
- Ưu tiên: đưa nội dung vào hướng dẫn Notion (khối lệnh tạo tệp bằng heredoc), như các bài hiện tại.
- Hoặc sinh viên chạy một lần trong Terminal codespace:

<!-- shell: Terminal codespace (sinh viên) -->
```bash
git remote add mau https://github.com/tttquyen/bigdatalab.git
git fetch mau
git checkout mau/main -- .devcontainer/lab.sh
git commit -m "Cap nhat lab.sh tu repo mau" && git push
```

### Bước 10: Thu bài hàng loạt

Mỗi sinh viên đã thêm GV làm collaborator (GV nhận email mời, chấp nhận tại https://github.com/notifications hoặc chạy lệnh dưới). Trên máy GV có `gh` (đã `gh auth login`):

<!-- shell: Terminal máy giảng viên -->
```bash
# chấp nhận mọi lời mời collaborator đang chờ
for id in $(gh api user/repository_invitations --jq '.[].id'); do
  gh api -X PATCH user/repository_invitations/$id; done
# ds.csv: mssv,username,repo   (xuất từ form)
mkdir -p bai-nop && cd bai-nop
tail -n +2 ../ds.csv | while IFS=, read -r mssv user repo; do
  gh repo clone "$user/$repo" "$mssv" -- --quiet 2>/dev/null || (cd "$mssv" && git pull -q)
  echo "$mssv $(git -C "$mssv" log -1 --format='%ci %s' 2>/dev/null)"
done
```

**Kết quả mong đợi** (ví dụ):
```text
b2312345 2026-11-12 10:41:07 +0700 Buoi 1
b2312346 2026-11-12 10:44:52 +0700 Buoi 1
```

Thư mục bài từng buổi: `src/lab01` ... `src/lab07`. Thời điểm commit dùng để kiểm tra nộp đúng hạn.

### Bước 11: Theo dõi quota của lớp

GV không xem được quota của tài khoản cá nhân sinh viên. Dặn sinh viên tự xem **Settings → Billing and licensing → Usage** và chụp màn hình khi hết quota. Ước tính tiêu thụ ở mục *Ghi chú từng buổi*.

## Ghi chú từng buổi

Số liệu đo trên bản sao Dev Container cục bộ, dùng đúng `.devcontainer` của repo, giới hạn 2 nhân / 8 GB như máy Codespaces (5–6/11/2026). Đây là số **tạm tính** cho Codespaces thật: RAM chưa gồm VS Code server (khoảng 1,8 GB, đo trên Codespaces thật ngày 25/10); image dịch vụ bổ sung đã nạp sẵn nên chưa đo thời gian tải lần đầu.

| Buổi | Lệnh `lab` cần bật | Máy khuyến nghị | Thời gian máy chạy lệnh | RAM đỉnh các container | Đĩa / lưu ý | Quota ước tính |
|---|---|---|---|---|---|---|
| 0 | — | 2 nhân | ~5 phút (tạo codespace + `lab test`) | 2,1–2,6 GB | ~6–7 GB sau khi tạo | ~2 core-hour |
| 1 | — (cụm lõi tự bật) | 2 nhân | ~21 phút (gồm bài mở rộng 1–2) | 2,8 GB | Đỉnh `docker system df` 11,2 GB ở PHẦN 5 (tệp 1 GB, thử block size) → bắt buộc làm bước dọn | 6–7 core-hour |
| 2 | — | 2 nhân | ~8 phút | 4,7 GB | Nhẹ | 6–7 core-hour |
| 3 | `lab hive` | 2 nhân | ~9 phút | 5,2 GB | **Đỉnh ≈16 GB** (image Hive 3,9 GB + HDFS 2,7 GB + Tez 1,85 GB) — vượt 15 GB của tài khoản Free | 6–7 core-hour |
| 4 | `lab nosql` | 2 nhân | ~14 phút | ~6,2 GB (chạm trần cgroup khi spark-submit + MongoDB, không OOM) | +4,2 GB trong buổi (JSONL 1,25 GB xoá ở Bước 21) | 6–7 core-hour |
| 5 | — | 2 nhân (4 nhân nhanh hơn ~1,5 lần) | ~6 phút cho 5 spark-submit | 5,7 GB | +2,1 GB (HDFS) | ~8 core-hour cho 2 tuần |
| 6 | `lab kafka` | 2 nhân | ~33 phút (phần lớn chờ cửa sổ thời gian) | 5,4 GB | +1,2 GB | 6–7 core-hour |
| 7 | — | **4 nhân / 16 GB**; nếu 2 nhân bắt buộc mở PySpark một executor | 6,5 phút trên 4 nhân; CrossValidator 9–12 phút trên 2 nhân | 6,8 GB (4 nhân); 4,9 GB (2 nhân, một executor) | Nhẹ | 8 core-hour (4 nhân, ~2 giờ) |

Mọi số đếm, Counter, AUC... trùng với bài gốc chạy trên máy phòng lab (cùng `--seed 42`). Chỉ ID ứng dụng, thời gian và cổng khác. Riêng Buổi 7, CrossValidator chọn (20,10) hoặc (20,5) tuỳ lần chạy; chấp nhận cả hai.

### Buổi 1 — HDFS

- Bỏ toàn bộ phần cài Docker/WSL/build image. Đổi số bước thành 20 bước. Thêm Bước 8 (tải tệp từ codespace về máy, kiểm MD5) và Bước 20 (dừng/mở lại codespace, dữ liệu HDFS còn nguyên).
- Sau `setrep -w 2`, `datanode2` có thể giữ 0 block. Vì vậy bài hướng dẫn dừng node có *Num of Blocks* > 0 (khác bài gốc, vốn luôn dừng datanode2).
- Nếu dừng codespace ngay sau khi xoá tệp lớn, block mồ côi giữ lại 2,4 GB mỗi DataNode tới 6 giờ. Bài đã thêm bước chờ 10 giây và lệnh `hdfs dfsadmin -triggerBlockReport`.
- `lab stop` và `lab status` không thấy `datanode3` (khai báo trong tệp compose riêng). Bài hướng dẫn gỡ datanode3 trước khi kết thúc.
- Số đo put block 64 MB và 128 MB trên 2 nhân nhiễu, có lần ngược xu hướng bài gốc. Chấm theo nhận xét, không theo số tuyệt đối.

### Buổi 2 — MapReduce

- Job chậm hơn máy phòng lab 1,3–2,2 lần. Tổng thời gian máy chạy chỉ ~8 phút, buổi học dư thời gian.
- Bài gốc thiếu `cd /src/lab02/streaming` ở khối lệnh join sai (Bước 20). Bản Codespaces đã bổ sung.
- Codespace dừng giữa job: HDFS giữ nguyên, job mất, output chỉ còn `_temporary`. ResourceManager quên các ứng dụng cũ, nhưng JobHistory và `yarn logs` vẫn còn.
- `git add src/lab02` đưa cả `classes/` và `wordcount.jar` lên (vài KB, vô hại).

### Buổi 3 — Hive

- Lần đầu `lab hive` trên Codespaces thật tải apache/hive (3,9 GB) và postgres (0,6 GB), ước tính 3–6 phút. Nên yêu cầu sinh viên chạy `lab hive` một lần trước giờ học.
- Tez để lại ~1,7 GB dữ liệu tạm sau buổi. `lab stop-extras` (bản repo 06/11/2026) tự dọn; bài cũng có bước dọn thủ công.
- Không bật `lab nosql`/`lab kafka` cùng lúc với Hive trên máy 8 GB.
- Mở lại codespace thì chỉ cụm lõi tự bật: phải `lab hive` lại và đặt lại các lệnh SET của phiên beeline. Metadata và bảng còn nguyên.
- Tài khoản Free (15 GB lưu trữ) dễ vượt quota ở buổi này. Khuyến khích Student Developer Pack (20 GB) và DROP bảng thử nghiệm.

### Buổi 4 — HBase và MongoDB

- Lần đầu `lab nosql` tải mongo:8.0 (~1,3 GB).
- Khi chạy job, tổng RAM lên 6–7,5 GB / 8 GB. Dặn sinh viên chạy từng job một và không bật dịch vụ khác.
- Nếu codespace dừng khi HBase/MongoDB đang chạy, lần mở sau chúng tự bật lại (`restart: unless-stopped`). Nhắc `lab stop-extras` cuối buổi để Buổi 5 không thiếu RAM.
- `mongosh` in cảnh báo `GLIBC_TUNABLES=glibc.pthread.rseq=0`: bỏ qua, không đổi.
- Terminal codespace không có `python3`. Mọi script Python chạy trong container `spark-master` (như bài).

### Buổi 5 — Spark

- Job standalone trên 2 nhân chậm hơn máy phòng lab 3,8–4 lần (RFM 33 s → 127 s); job YARN chỉ chậm ~1,2 lần. Hiệu quả cache/Parquet vẫn thể hiện rõ.
- PySpark shell đang mở giữ cả 4 core: `spark-submit` ở terminal khác đứng chờ. Bài đã thêm cảnh báo `exit()` trước PHẦN 5.
- Bài chia hai tuần. Tuần 2 phải mở lại PySpark shell; dữ liệu HDFS còn, không cần put lại.
- Cổng 4040 chỉ trả lời khi có ứng dụng đang chạy. Job đã xong thì xem ở Spark History (18080).

### Buổi 6 — Kafka và Structured Streaming

- Phương án `pip install kafka-python` trên host của bài gốc không chạy (codespace không có python3; PEP 668). Bài dùng producer/consumer trong `spark-master` làm cách chính. Phương án tuỳ chọn: `.venv` + `pip install kafka-python`, kết nối `localhost:9092`, đã kiểm chứng; `.venv/` đã thêm vào `.gitignore`.
- Bước 6–9 cần 3–4 terminal song song. GV nên demo nút **+** / *Split Terminal* và đổi tên terminal A–D ngay đầu giờ.
- Cửa sổ thời gian hiển thị theo Asia/Ho_Chi_Minh, còn `date` trong terminal codespace là UTC.
- Bước 14: sinh viên có thể gặp `offsets = commits` hoặc `offsets = commits + 1` tuỳ thời điểm dừng. Chấm theo logic đối chiếu.

### Buổi 7 — MLlib

- Yêu cầu đổi codespace sang **4-core** trước buổi: github.com/codespaces → `...` → *Change machine type*; đổi lại 2-core sau buổi. Với lệnh PySpark mặc định, máy 2 nhân chạm trần 8 GB và phải dùng swap.
- Nếu ở lại 2 nhân: dùng lệnh PySpark một executor như Bước 2 của bài (`--executor-cores 2 --total-executor-cores 2`). Kết quả mô hình y hệt; CrossValidator mất 9–12 phút.
- Codespace tự dừng sau 30 phút không thao tác sẽ làm mất phiên PySpark. Nhắc sinh viên không bỏ máy trong lúc CrossValidator chạy.
- Chỉ số ổn định trên mọi cấu hình: AUC-ROC cơ sở 0.6088, AUC-PR 0.0842, F1 lớp churn 0 → 0.1503 sau khi đặt trọng số.

## Lỗi thường gặp

| Hiện tượng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Log tạo codespace của sinh viên: `Không tải được ghcr.io/... → chuyển sang build` | Package image chưa Public | Chuyển Public; codespace đã tự build vẫn dùng được, không cần tạo lại |
| Build image thất bại khi tạo codespace | Mirror Apache chậm/lỗi | Sinh viên `Ctrl+Shift+P` → *Codespaces: Rebuild Container*; hoặc sửa `APACHE_MIRROR` |
| Sinh viên không thấy *Use this template* | Repo chưa bật Template | PHẦN 1, Bước 1 |
| Bài làm của sinh viên lộ cho người khác | Sinh viên dùng Fork (luôn Public) | Tạo lại bằng template, chọn Private |
| `git push` bị từ chối trong codespace | Sinh viên tạo codespace trên repo mẫu của GV | Tạo repo riêng rồi codespace mới |
| Codespace báo hết quota giữa học kỳ | Để codespace chạy không dừng; nhiều codespace | Dặn Stop; xoá codespace thừa; phương án dự phòng |
| Job Spark lỗi ngẫu nhiên / container bị `Exited (137)` | Bật nhiều dịch vụ bổ sung cùng lúc trên máy 8 GB | `lab stop-extras`; bật đúng dịch vụ của buổi; hoặc máy 4 nhân |
| Codespace tự dừng giữa job dài | Idle timeout 30 phút | Tương tác định kỳ; hoặc tăng *Default idle timeout* tạm thời |
| Link trong Web UI (`http://nodemanager1:8042/...`) không mở | Tên container chỉ có nghĩa bên trong mạng Docker | Dùng link tương ứng từ `lab ui` |

## Phương án dự phòng

| Tình huống | Phương án |
|---|---|
| Sinh viên hết quota / không tạo được tài khoản | Máy cá nhân có Docker: VS Code + tiện ích *Dev Containers* → *Reopen in Container* (cùng cấu hình `.devcontainer/`); hoặc ghép cặp với bạn |
| Phòng máy mất Internet | Gói offline (trang *Buổi 1 (dự phòng) — Chạy offline khi mạng yếu*), `docker load`, `docker compose up -d` |
| Cả lớp không truy cập được GitHub | Bộ môi trường Docker trên máy phòng lab như các trang Buổi 1–7 gốc |

## Kết quả kiểm thử

| Kiểm thử | Môi trường | Kết quả |
|---|---|---|
| Tạo codespace, tải image từ GHCR, `lab test` | GitHub Codespaces thật, 2 nhân / 8 GB, tài khoản GV (25/10/2026) | 8/8 container healthy, 11/11 PASS, RAM cụm 1,9 GB, 13 cổng Web UI được forward |
| Bật toàn bộ dịch vụ bổ sung, `lab test` | Codespaces thật, 2 nhân / 8 GB | 16/17 lần đầu (PySpark parquet lỗi do thiếu RAM), chạy lại thì đạt; còn 2 GB trống |
| Buổi 0–7 chạy trọn từng bước với 5 triệu dòng | Bản sao Dev Container cục bộ (cùng `.devcontainer`, Docker-in-Docker, 2 nhân / 8 GB; Buổi 5 và 7 thêm 4 nhân / 16 GB), 05–06/11/2026 | Mọi bước đạt, không OOM; kết quả trùng bài gốc |
| Repo sau khi sửa (06/11/2026): tạo codespace với `DATA_ROWS=5000000`, `lab status`, `lab ui`, `lab hive` → `lab stop-extras` (dọn Tez), tải Kafka lần đầu, `.gitignore`, `lab test` | Bản sao 2 nhân / 8 GB | Tạo + khởi động 97 s, 5 000 000 giao dịch; Tez dọn còn 0 tệp; 11/11 PASS |

**Chưa kiểm chứng trên Codespaces thật:**
- Tạo codespace bằng tài khoản sinh viên. Việc này cần package image ở chế độ Public.
- Thời gian tải lần đầu các image Hive, MongoDB, Kafka.
- Mở Web UI qua `*.app.github.dev` trong trình duyệt. Trên bản sao mới chỉ kiểm bằng `curl localhost`.
- `git push` và Stop codespace trên github.com.
- Hành vi tự dừng sau 30 phút giữa job.
- Lượng RAM do VS Code chiếm thêm, và máy Codespaces có swap hay không.

Nhật ký chi tiết từng buổi nằm trong gói Markdown đính kèm (`buoiN_codespaces_testlog.md`).

## Tài liệu liên quan

- Repo môi trường: https://github.com/tttquyen/bigdatalab (hướng dẫn Markdown trong thư mục `huong-dan/`, ghi chú triển khai trong `HUONG-DAN-CODESPACES.md`).
- Trang sinh viên: Buổi 0 và Buổi 1–7 (GitHub Codespaces) cùng trang cha.
- Gợi ý trả lời câu hỏi kiểm tra: trang *Gợi ý trả lời — Câu hỏi kiểm tra và bài tập mở rộng (Buổi 1–7, dành cho GV)* — nội dung kiến thức không đổi giữa hai môi trường.
