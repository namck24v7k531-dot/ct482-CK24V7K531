#!/usr/bin/env bash
# docker-san-sang.sh — đợi Docker-in-Docker sẵn sàng (dùng chung cho khoi-tao.sh / khoi-dong.sh / lab).
# Kernel Linux mới (≥ 6.17/7.x, ví dụ khi chạy Dev Container trên máy cá nhân) không còn iptables legacy,
# trong khi feature docker-in-docker mặc định dùng legacy → dockerd không lên. Khi gặp trường hợp đó,
# script chuyển sang iptables-nft rồi khởi động lại dockerd. Trên GitHub Codespaces thường không cần.
cho_docker() {
  local i
  for i in $(seq 1 15); do docker info >/dev/null 2>&1 && return 0; sleep 2; done
  if ! sudo iptables-legacy -t nat -L >/dev/null 2>&1 && sudo iptables-nft -t nat -L >/dev/null 2>&1; then
    echo "[docker] Kernel không hỗ trợ iptables legacy → chuyển sang iptables-nft và khởi động lại dockerd"
    sudo update-alternatives --set iptables  /usr/sbin/iptables-nft  >/dev/null
    sudo update-alternatives --set ip6tables /usr/sbin/ip6tables-nft >/dev/null 2>&1 || true
    sudo pkill -x dockerd 2>/dev/null; sudo pkill -x containerd 2>/dev/null; sleep 2
    sudo rm -f /var/run/docker.pid
    sudo sh -c 'setsid nohup dockerd >/tmp/dockerd.log 2>&1 </dev/null &'
  fi
  for i in $(seq 1 60); do docker info >/dev/null 2>&1 && return 0; sleep 2; done
  return 1
}
