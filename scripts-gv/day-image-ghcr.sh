#!/usr/bin/env bash
# =============================================================================
#  day-image-ghcr.sh — (cách thay cho GitHub Actions) đẩy image bigdata-lab đã có trên máy giảng viên
#  lên GitHub Container Registry để codespace của sinh viên tải về thay vì tự build.
#  Cần: Personal Access Token (classic) có quyền write:packages.
#  Dùng:  bash scripts-gv/day-image-ghcr.sh <owner-github>        (owner = tài khoản hoặc tổ chức)
# =============================================================================
set -euo pipefail
OWNER=$(echo "${1:?Thiếu tên owner GitHub}" | tr '[:upper:]' '[:lower:]')
SRC=bigdata-lab:2026.10
docker image inspect "$SRC" >/dev/null 2>&1 || { echo "Chưa có $SRC trên máy: chạy 'docker compose build' trước"; exit 1; }
echo ">> Đăng nhập ghcr.io (Username = tài khoản GitHub, Password = token write:packages)"
docker login ghcr.io
for t in 2026.10 latest; do
  docker tag "$SRC" "ghcr.io/$OWNER/bigdata-lab:$t"
  docker push "ghcr.io/$OWNER/bigdata-lab:$t"
done
cat <<M
>> Xong. Việc còn lại:
   1. github.com → Packages → bigdata-lab → Package settings → Change visibility → Public
   2. Sửa .devcontainer/cau-hinh.env:  LAB_IMAGE=ghcr.io/$OWNER/bigdata-lab:2026.10   rồi commit
M
