#!/usr/bin/env bash
# khoi-dong.sh — chạy MỖI LẦN codespace khởi động (postStartCommand): bật cụm lõi nếu AUTO_START=core
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; . .devcontainer/cau-hinh.env 2>/dev/null; set +a
. .devcontainer/docker-san-sang.sh
cho_docker || { echo "[khoi-dong] Docker không chạy (xem /tmp/dockerd.log)"; exit 0; }
if [ "${AUTO_START:-core}" != "core" ]; then echo "[khoi-dong] AUTO_START=${AUTO_START} — không tự bật cụm. Gõ: lab start"; exit 0; fi
if ! docker image inspect bigdata-lab:2026.10 >/dev/null 2>&1; then
  echo "[khoi-dong] Chưa có image (khoi-tao.sh chưa xong hoặc lỗi). Xem log tạo codespace."; exit 0
fi
echo "[khoi-dong] Bật cụm lõi Hadoop/Spark..."
docker compose up -d --no-build --wait --wait-timeout 300 \
  && echo "[khoi-dong] Cụm đã sẵn sàng. Gõ 'lab ui' để lấy đường dẫn Web UI." \
  || echo "[khoi-dong] Cụm chưa healthy hết — gõ 'lab status' để kiểm tra."
