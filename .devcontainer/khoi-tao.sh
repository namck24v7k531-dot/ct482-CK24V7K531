#!/usr/bin/env bash
# =============================================================================
#  khoi-tao.sh — chạy MỘT LẦN khi tạo codespace (postCreateCommand)
#   1. chờ Docker (Docker-in-Docker) sẵn sàng
#   2. lấy image bigdata-lab:2026.10: có sẵn → bỏ qua; LAB_IMAGE → docker pull; không thì build
#   3. (tuỳ chọn) tải sẵn image dịch vụ bổ sung
#   4. sinh dữ liệu mẫu vào ./data
#   5. cài lệnh `lab`
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
set -a; . .devcontainer/cau-hinh.env 2>/dev/null; set +a
IMG=bigdata-lab:2026.10
log() { echo "[khoi-tao $(date +%H:%M:%S)] $*"; }

log "Chờ Docker sẵn sàng..."
. .devcontainer/docker-san-sang.sh
cho_docker || { log "LỖI: Docker không chạy. Mở Command Palette → 'Codespaces: Rebuild Container'."; exit 1; }

sudo ln -sf "$ROOT/.devcontainer/lab.sh" /usr/local/bin/lab && chmod +x "$ROOT/.devcontainer/lab.sh"

if docker image inspect "$IMG" >/dev/null 2>&1; then
  log "Đã có image $IMG"
else
  got=0
  if [ -n "${LAB_IMAGE:-}" ]; then
    log "Tải image dựng sẵn: $LAB_IMAGE"
    if docker pull "$LAB_IMAGE"; then
      docker tag "$LAB_IMAGE" "$IMG"; docker tag "$LAB_IMAGE" bigdata-lab:latest; got=1
    else
      log "Không tải được $LAB_IMAGE (sai tên hoặc package chưa để Public) → chuyển sang build"
    fi
  fi
  if [ $got -eq 0 ]; then
    log "Build image từ Dockerfile (tải ~1,5 GB, khoảng 10 phút, chỉ làm một lần)..."
    # Gọi docker build trực tiếp: Compose bản mới (build qua bake) đòi --allow=network.host khi không có TTY.
    MIRROR_ARG=(); [ -n "${APACHE_MIRROR:-}" ] && MIRROR_ARG=(--build-arg "APACHE_MIRROR=$APACHE_MIRROR")
    docker build --network host "${MIRROR_ARG[@]}" -t "$IMG" -t bigdata-lab:latest . \
      || { log "LỖI build. Xem README.md mục 11."; exit 1; }
    docker builder prune -af >/dev/null 2>&1   # bỏ build cache (~5 GB) để tiết kiệm dung lượng codespace
  fi
fi

if [ "${PULL_EXTRAS:-0}" = "1" ]; then
  log "Tải sẵn image dịch vụ bổ sung..."
  docker compose -f docker-compose.yml -f docker-compose.extras.yml pull \
    hive-metastore-db hive-metastore hiveserver2 kafka mongodb || log "Cảnh báo: tải extras chưa xong, sẽ tải lại khi bật."
fi

if [ ! -s data/retail_transactions.csv ]; then
  log "Sinh dữ liệu mẫu (${DATA_ROWS:-200000} giao dịch)..."
  if command -v python3 >/dev/null; then
    python3 gen_data.py --rows "${DATA_ROWS:-200000}" --out ./data
  else
    docker run --rm --entrypoint python3 --user "$(id -u):$(id -g)" -v "$ROOT":/w -w /w "$IMG" \
      gen_data.py --rows "${DATA_ROWS:-200000}" --out ./data
  fi
fi
log "Xong. Gõ 'lab' để xem các lệnh."
