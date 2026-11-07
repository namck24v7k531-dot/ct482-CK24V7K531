# hbase-env.sh — cụm thực hành CT428
export JAVA_HOME=/usr/lib/jvm/java-17
export HBASE_MANAGES_ZK=true
export HBASE_HEAPSIZE=${HBASE_HEAPSIZE:-768m}
export HBASE_LOG_DIR=/tmp/hbase-logs
export HBASE_PID_DIR=/tmp
# Dùng jar Hadoop 3.4.3 đi kèm HBase, không trộn classpath của /opt/hadoop
export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true
