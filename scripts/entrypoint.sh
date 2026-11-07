#!/usr/bin/env bash
# =============================================================================
#  entrypoint.sh — khởi động 1 vai trò (role) của cụm bigdata-lab
#  Dùng: entrypoint.sh <namenode|datanode|resourcemanager|nodemanager|spark-master|
#                       historyserver|hbase|bash|lệnh bất kỳ>
# =============================================================================
set -uo pipefail
ROLE="${1:-bash}"
log() { echo "[bigdata-lab $(date '+%H:%M:%S')] $*"; }

wait_port() {   # wait_port host port [timeout_s]
  local h=$1 p=$2 t=${3:-300} i=0
  until (echo > /dev/tcp/$h/$p) >/dev/null 2>&1; do
    i=$((i+2)); [ $i -ge $t ] && { log "Hết thời gian chờ $h:$p"; return 1; }
    sleep 2
  done
  log "$h:$p đã sẵn sàng"
}

wait_hdfs() {   # chờ NameNode thoát safe mode
  wait_port namenode 9000 600 || return 1
  until hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
    log "Đang chờ HDFS thoát safe mode..."; sleep 5
  done
}

init_hdfs_dirs() {  # thư mục dùng chung trên HDFS (idempotent)
  hdfs dfs -mkdir -p /tmp /tmp/logs /user/root /user/student /user/hive/warehouse \
                     /user/spark/warehouse /spark-logs /hbase /data 2>/dev/null
  hdfs dfs -chmod 1777 /tmp /tmp/logs /spark-logs 2>/dev/null
  hdfs dfs -chmod 777 /user/hive /user/hive/warehouse /user/spark/warehouse /user/student /data 2>/dev/null
  log "Đã tạo các thư mục HDFS dùng chung"
}

run_pair() {  # chạy 2 tiến trình nền; thoát khi một trong hai dừng (Docker sẽ restart)
  "$1" & local p1=$!
  "$2" & local p2=$!
  trap 'kill -TERM $p1 $p2 2>/dev/null; wait' TERM INT
  wait -n $p1 $p2; local rc=$?
  log "Một tiến trình đã dừng (mã $rc) — dừng container"
  kill -TERM $p1 $p2 2>/dev/null; wait
  exit $rc
}

case "$ROLE" in
  namenode)
    if [ ! -d /hadoop/dfs/name/current ]; then
      log "Format NameNode lần đầu (clusterId=${CLUSTER_ID:-CID-ct428-bigdata-lab})"
      hdfs namenode -format -force -nonInteractive -clusterId "${CLUSTER_ID:-CID-ct428-bigdata-lab}"
    fi
    ( wait_hdfs && init_hdfs_dirs ) &
    exec hdfs namenode
    ;;
  datanode)
    wait_port namenode 9000 600
    exec hdfs datanode
    ;;
  resourcemanager)
    wait_port namenode 9000 600
    exec yarn resourcemanager
    ;;
  nodemanager)
    wait_port resourcemanager 8031 600
    start_nm()     { exec yarn nodemanager; }
    start_worker() {
      wait_port spark-master 7077 600
      exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --cores "${SPARK_WORKER_CORES:-2}" --memory "${SPARK_WORKER_MEMORY:-1g}" \
        --host "$(hostname)" --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}" spark://spark-master:7077
    }
    run_pair start_nm start_worker
    ;;
  spark-master)
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
      --host "$(hostname)" --port 7077 --webui-port 8080
    ;;
  historyserver)
    wait_hdfs; init_hdfs_dirs
    start_jhs()   { exec mapred historyserver; }
    start_shs()   { exec /opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer; }
    run_pair start_jhs start_shs
    ;;
  hbase)
    wait_hdfs; hdfs dfs -mkdir -p /hbase 2>/dev/null
    mkdir -p /hbase-data/tmp /hbase-data/zookeeper
    start_hmaster() { exec hbase master start; }
    start_thrift()  { wait_port localhost 16010 600; sleep 5; exec hbase thrift start -p 9090; }
    run_pair start_hmaster start_thrift
    ;;
  *)
    exec "$@"
    ;;
esac
