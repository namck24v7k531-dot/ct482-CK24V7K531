# Thực hành CT482 trên GitHub Codespaces — không cài lại khi đổi phòng máy

> Hướng dẫn đầy đủ từng buổi (Buổi 0: đăng ký và tạo codespace, Buổi 1–7 bản Codespaces, trang giảng viên) nằm trong thư mục [`huong-dan/`](huong-dan/) và trên Notion của học phần.

## 1. Ý tưởng

Môi trường không nằm trên ổ cứng máy phòng thực hành (bị đóng băng/ghost lại), mà nằm trong **một codespace** — máy ảo Linux trên GitHub, mở bằng trình duyệt. Mỗi sinh viên có một codespace riêng. Trong đó cụm Hadoop/Spark chạy bằng đúng `docker-compose.yml` của bộ môi trường `bigdata-lab:2026.10` (Docker-in-Docker).

| | Cách cũ (Docker trên máy phòng lab) | Codespaces |
|---|---|---|
| Đổi phòng / đổi máy | Cài Docker, build/nạp image lại | Mở lại trình duyệt, cụm tự bật |
| Dữ liệu HDFS, Hive, Kafka | Mất khi máy được khôi phục | Giữ trong codespace giữa các buổi |
| Bài làm (`src/`) | Chép USB | `git commit` + `git push` |
| Yêu cầu máy phòng lab | ≥ 8 GB RAM, 15 GB đĩa, quyền admin | Chỉ cần trình duyệt + Internet |
| Làm ở nhà | Cài lại từ đầu | Cùng một codespace |

Cấu hình nằm trong thư mục `.devcontainer/`. Cùng cấu hình này cũng chạy được trên máy cá nhân bằng VS Code + tiện ích Dev Containers (mục 7).

## 2. Giảng viên chuẩn bị (một lần mỗi học kỳ)

1. **Tạo repo** trên GitHub (khuyến nghị trong một Organization của học phần), đẩy toàn bộ thư mục này lên. Vào *Settings → General* → tích **Template repository**.
2. **Build image một lần, đưa lên GitHub Container Registry** (để mỗi codespace chỉ tải ~2 phút thay vì tự build ~10 phút):
   - Cách A: tab **Actions** → *Build image bigdata-lab* → **Run workflow** (tệp `.github/workflows/build-lab-image.yml`).
   - Cách B: trên máy đã có image: `bash scripts-gv/day-image-ghcr.sh <owner>`.
3. Vào **Packages → bigdata-lab → Package settings → Change visibility → Public** (package mới mặc định là Private, sinh viên sẽ không tải được).
4. Sửa `.devcontainer/cau-hinh.env`: `LAB_IMAGE=ghcr.io/<owner>/bigdata-lab:2026.10` (chữ thường), commit.
   Nếu bỏ qua bước 2–4, codespace vẫn chạy được nhưng tự build từ Dockerfile ở lần tạo đầu.
5. Phát cho sinh viên: link repo template (sinh viên *Use this template* → repo Private riêng, thêm GV làm collaborator). GitHub Classroom đã ngừng từ 28/08/2026 nên không dùng nữa.

## 3. Sinh viên — lần đầu (khoảng 5–15 phút, chỉ một lần)

1. Có tài khoản GitHub; nên xác minh **GitHub Student Developer Pack** bằng email trường để có quota lớn hơn (mục 5).
2. Mở repo (bản của mình: *Use this template*) → **Code → Codespaces → `...` → New with options** → Machine type **2-core** (mặc định; chọn 4-core nếu cần chạy nhanh hơn) → **Create codespace**.
3. Chờ log tạo xong (tải image, sinh dữ liệu mẫu, tự bật cụm lõi). Khi terminal hiện `Cụm đã sẵn sàng`, gõ:
   ```bash
   lab status     # 8 container healthy, Live datanodes (2), Total Nodes:2
   lab ui         # đường dẫn Web UI
   ```

## 4. Sinh viên — mỗi buổi thực hành (ở bất kỳ phòng nào, hoặc ở nhà)

1. Vào https://github.com/codespaces → bấm tên codespace của mình. Cụm lõi **tự khởi động** (30–90 giây).
2. Làm bài theo hướng dẫn từng buổi như cũ — các lệnh `docker exec -it namenode bash`, `docker exec -it spark-master spark-submit ...` giữ nguyên.
3. Bật dịch vụ cần cho bài: `lab hive` (Lab 3), `lab nosql` (Lab 4), `lab kafka` (Lab 6). Gõ `lab` để xem tất cả lệnh.
4. Web UI: tab **PORTS** cạnh TERMINAL → biểu tượng quả địa cầu; hoặc bấm link từ `lab ui`.
5. Cuối buổi: `git add src && git commit -m "Lab N" && git push`, rồi **dừng codespace** (github.com/codespaces → `...` → *Stop codespace*) để không tốn quota.

## 5. Quota và lựa chọn cấu hình máy

Codespaces tính theo **core-hour** (số nhân × số giờ chạy) và **dung lượng đĩa** (GB-tháng, tính cả khi codespace dừng).

| Tài khoản | Core-hour/tháng | Giờ chạy máy 4 nhân | Giờ chạy máy 2 nhân | Lưu trữ |
|---|---|---|---|---|
| GitHub Free | 120 | 30 giờ | 60 giờ | 15 GB |
| Sinh viên đã xác minh (Student Pack) | 180 | 45 giờ | 90 giờ | 20 GB |

Ví dụ buổi thực hành 3 giờ: máy 2 nhân tốn 6 core-hour, máy 4 nhân tốn 12 core-hour → sinh viên đã xác minh đủ khoảng 30 buổi/tháng (2 nhân) hoặc 15 buổi/tháng (4 nhân).
GitHub Classroom và hạn mức Codespaces theo lớp đã chấm dứt từ 28/08/2026; mỗi sinh viên dùng quota cá nhân (giáo viên và sinh viên đã xác minh: 180 core-hour, 20 GB mỗi tháng).

**Mặc định là máy 2 nhân / 8 GB** (`hostRequirements` trong `.devcontainer/devcontainer.json`): đủ cho cụm lõi cộng dịch vụ bổ sung của **một** bài (`lab hive`, `lab nosql` hoặc `lab kafka`; tắt bằng `lab stop-extras` khi xong). Không nên bật **tất cả** dịch vụ bổ sung cùng lúc trên máy này: đo trên Codespaces thật chỉ còn ~2 GB RAM trống, job Spark có thể lỗi ngẫu nhiên (mục 8). Máy 4 nhân / 16 GB chạy nhanh hơn khoảng 1,6 lần nhưng tốn gấp đôi quota — nên dùng cho bài nặng (MLlib, dữ liệu 5 triệu dòng).

**Dung lượng đĩa** (đo thực tế, ước tính cho quota lưu trữ): cụm lõi ≈ 6,4 GB (image `bigdata-lab` 4,7 GB + nền dev container 1,7 GB); thêm toàn bộ image dịch vụ bổ sung ≈ 13,7 GB. Tài khoản Free (15 GB) nên tắt và xoá image dịch vụ không còn dùng (`docker image rm apache/hive:4.2.1` sau Lab 3...).

## 6. Lưu ý

- Codespace **tự dừng sau 30 phút không thao tác** (mặc định). Job dài: giữ tab mở và tương tác định kỳ, hoặc tăng *Default idle timeout* trong github.com/settings/codespaces.
- Codespace **không mở lại quá lâu sẽ bị tự xoá** (mặc định 30 ngày) → luôn `git push` bài làm. Dữ liệu HDFS chỉ là dữ liệu học, sinh lại được bằng `lab data`.
- Hết quota thì không mở được codespace đến kỳ sau; file vẫn còn, có thể *Export changes to a branch*.
- Liên kết bên trong Web UI dùng tên container (`http://nodemanager1:8042/...`) — đổi bằng link tương ứng từ `lab ui`, như README mục 7.
- Cần Internet ổn định trong phòng máy (chỉ truyền giao diện, không tải image).

## 7. Phương án dự phòng (cùng cấu hình)

- **Máy cá nhân có Docker**: VS Code + tiện ích *Dev Containers* → *Reopen in Container*. Dữ liệu nằm trong volume Docker của máy đó.
- **Không có Internet**: dùng gói offline (`docker load`) như trước, chạy `docker compose up -d` trực tiếp.

## 8. Kết quả kiểm thử (25/10/2026)

Kiểm thử bằng Dev Containers CLI 0.89.0 trên Linux (Docker 29.8.1), giới hạn tài nguyên giống máy Codespaces; tạo codespace từ đầu, `LAB_IMAGE` để trống (tự build từ Dockerfile).

| Kiểm thử | 2 nhân / 8 GB | 4 nhân / 16 GB |
|---|---|---|
| Tạo codespace lần đầu (build image + sinh dữ liệu + bật cụm) | 330 giây | 282 giây |
| Smoke test cụm lõi (`lab test`) | 11/11 PASS, 3 phút 19 giây | 11/11 PASS, 2 phút 01 giây |
| Smoke test cụm lõi + toàn bộ extras (Hive, Kafka, HBase, MongoDB) | 17/17 PASS, 4 phút 52 giây, không OOM | 17/17 PASS |
| RAM các container (cụm lõi rảnh / có extras) | 2,4 GB / 4,0 GB | 2,4 GB / 4,1 GB |
| Dừng rồi mở lại codespace | — | Cụm tự bật lại sau 86 giây; dữ liệu HDFS còn nguyên |

**Kiểm chứng trên GitHub Codespaces thật (25/10/2026, repo `tttquyen/bigdatalab`, máy 2 nhân / 8 GB):** workflow GitHub Actions build và đẩy image lên `ghcr.io/tttquyen/bigdata-lab:2026.10`; codespace tải image này (không tự build); cụm lõi 8/8 container healthy, `lab test` 11/11 PASS, RAM các container 1,9 GB, 13 cổng Web UI được forward.

**Bật toàn bộ dịch vụ bổ sung trên Codespaces thật (2 nhân / 8 GB):** `lab test` lần đầu 16/17 PASS — bài *PySpark HDFS parquet* lỗi, chạy lại riêng thì đạt (`ROWS_IN_PARQUET 7`). Lúc rảnh: 14 container dùng ~4,0 GB, máy còn 2,0 GB trống trên 7,8 GB, không swap; không container nào bị OOM. Phần chênh so với mô phỏng (~1,8 GB) là VS Code và extension chạy trong codespace. → Lỗi nhất thời do thiếu RAM; khuyến nghị bật dịch vụ bổ sung theo từng bài, hoặc dùng máy 4 nhân / 16 GB nếu cần bật tất cả.

Chưa kiểm chứng trên Codespaces thật: dừng/mở lại codespace, tài khoản sinh viên khác tải image (cần package ở chế độ Public).

Hai lỗi phát hiện khi kiểm thử và đã xử lý trong script:
- Kernel Linux mới (máy kiểm thử dùng 7.0) không còn iptables legacy → Docker-in-Docker không khởi động. `docker-san-sang.sh` tự chuyển sang iptables-nft.
- Docker Compose bản mới (build qua bake) từ chối `network: host` trong `docker-compose.yml` khi không có TTY (`pass "--allow=network.host"`). `khoi-tao.sh` gọi thẳng `docker build --network host`.

## 9. Cập nhật 06/11/2026

- Đã chạy trọn Buổi 0–7 với 5 triệu dòng trên bản sao Dev Container (2 nhân / 8 GB): mọi bước đạt, không OOM. Buổi 7 (MLlib) khuyến nghị máy 4 nhân / 16 GB; trên 2 nhân phải mở PySpark một executor.
- Đĩa lúc cao nhất: Buổi 1 ≈ 13 GB, Buổi 3 ≈ 16 GB (image Hive + Tez) — sát hoặc vượt 15 GB của tài khoản Free. `lab stop-extras` nay dọn dữ liệu tạm Tez.
- `lab status` in thêm RAM cả máy, đĩa Docker, số dòng dữ liệu; `lab ui` thêm NodeManager/DataNode/RegionServer; `DATA_ROWS=5000000`; forward cổng 4041; `.gitignore` thêm `__pycache__/`, `*.pyc`, `.venv/`.
- Package `ghcr.io/tttquyen/bigdata-lab` đã chuyển **Public** và repo đã bật **Template repository** (06/11/2026).
