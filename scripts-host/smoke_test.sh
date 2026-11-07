#!/usr/bin/env bash
# Smoke test cho cụm CT482 đang chạy. Dùng: CT_PREFIX=t1- ./scripts-host/smoke_test.sh
# (CT_PREFIX rỗng = cụm của sinh viên với tên container gốc). Chạy từ thư mục moi-truong-bigdata-lab/.
set -u
P="${CT_PREFIX:-}"
ok=0; fail=0
pass() { echo "PASS  $1"; ok=$((ok+1)); }
bad()  { echo "FAIL  $1"; fail=$((fail+1)); }
chk()  { local name=$1; shift; if out=$("$@" 2>&1); then pass "$name"; else bad "$name"; echo "$out" | tail -5; fi; }
running() { [ "$(docker inspect -f '{{.State.Running}}' "${P}$1" 2>/dev/null)" = "true" ]; }
X() { local c=$1; shift; docker exec -i "${P}$c" bash -lc "$*"; }

[ -f data/retail_transactions.csv ] || { echo "Chưa có data/retail_transactions.csv — chạy gen_data.py trước"; exit 2; }

chk "HDFS 2 live datanodes"   X namenode 'hdfs dfsadmin -report | grep -q "Live datanodes (2)"'
chk "YARN 2 nodemanagers"     X resourcemanager 'yarn node -list 2>/dev/null | grep -q "Total Nodes:2"'
chk "Spark 2 alive workers"   X spark-master 'curl -s localhost:8080/json/ | grep -q "\"aliveworkers\" : 2\|\"aliveworkers\":2"'
chk "HDFS put"                X namenode 'hdfs dfs -mkdir -p /data/raw /user/student/smoke && hdfs dfs -put -f /data/retail_transactions.csv /data/raw/ && hdfs dfs -put -f /opt/hadoop/README.txt /user/student/smoke/'
chk "MR examples pi"          X namenode 'hadoop jar $HADOOP_EXAMPLES_JAR pi 2 10 2>&1 | grep -q "Estimated value of Pi"'
chk "MR wordcount"            X namenode 'hdfs dfs -rm -r -f -skipTrash /user/student/smoke_wc >/dev/null; hadoop jar $HADOOP_EXAMPLES_JAR wordcount /user/student/smoke /user/student/smoke_wc 2>&1 | grep -q "completed successfully"'
chk "Hadoop Streaming python" X namenode 'hdfs dfs -rm -r -f -skipTrash /user/student/smoke_st >/dev/null; hadoop jar $STREAM_JAR -files /src/kiem-tra/mapper.py,/src/kiem-tra/reducer.py -mapper "python3 mapper.py" -reducer "python3 reducer.py" -input /data/raw/retail_transactions.csv -output /user/student/smoke_st 2>&1 | grep -q "completed successfully" && [ $(hdfs dfs -cat /user/student/smoke_st/part-* | wc -l) -eq 7 ]'
chk "SparkPi standalone"      X spark-master 'spark-submit --master spark://spark-master:7077 --class org.apache.spark.examples.SparkPi $SPARK_EXAMPLES_JAR 10 2>&1 | grep -q "Pi is roughly"'
chk "SparkPi YARN"            X spark-master 'spark-submit --master yarn --class org.apache.spark.examples.SparkPi $SPARK_EXAMPLES_JAR 10 2>&1 | grep -q "Pi is roughly"'
chk "PySpark HDFS parquet"    X spark-master 'spark-submit --master spark://spark-master:7077 /src/kiem-tra/spark_hdfs_parquet.py hdfs://namenode:9000/data/raw/retail_transactions.csv 2>&1 | grep -q "ROWS_IN_PARQUET 7"'
chk "Spark History lists apps" X historyserver 'sleep 12; curl -s localhost:18080/api/v1/applications | grep -q kiem-tra-hdfs-parquet'

if running hiveserver2; then
  SQL="CREATE DATABASE IF NOT EXISTS smoke; USE smoke; DROP TABLE IF EXISTS p_orc; CREATE TABLE p_orc (id INT, v STRING) PARTITIONED BY (dt STRING) STORED AS ORC; INSERT INTO p_orc PARTITION (dt) VALUES (1,'a','d1'),(2,'b','d2'); DROP TABLE IF EXISTS p_pq; CREATE TABLE p_pq (id INT) PARTITIONED BY (dt STRING) STORED AS PARQUET; INSERT INTO p_pq PARTITION (dt='x') VALUES (7); SELECT COUNT(*) AS smoke_n FROM p_orc;"
  chk "Hive ORC+Parquet partitions" bash -c "docker exec -e HADOOP_CLIENT_OPTS=-Dorg.jline.terminal.provider=dumb ${P}hiveserver2 beeline -u jdbc:hive2://localhost:10000/default -n hive --silent=true -e \"$SQL\" 2>&1 | grep -qE '\| *2 *\|' && docker exec ${P}namenode hdfs dfs -test -d /user/hive/warehouse/smoke.db/p_orc/dt=d2"
fi
if running kafka; then
  chk "Kafka topic + python clients" bash -c "docker exec ${P}kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic kiem-tra --partitions 3 --replication-factor 1 >/dev/null && docker exec ${P}spark-master python3 /src/kiem-tra/kafka_python_test.py kafka:9092 kiem-tra | grep -q 'kafka-python consumed [1-9]'"
  chk "Spark Structured Streaming <- Kafka" X spark-master 'spark-submit --master spark://spark-master:7077 /src/kiem-tra/kafka_streaming_test.py kafka:9092 kiem-tra 2>&1 | grep -q "STREAM_ROWS_WRITTEN [1-9]"'
fi
if running hbase; then
  chk "HBase shell create/put/scan" bash -c "printf \"create 'smoke_t','cf'\nput 'smoke_t','r1','cf:a','1'\nscan 'smoke_t'\n\" | docker exec -i ${P}hbase hbase shell -n 2>&1 | grep -q 'value=1'"
  chk "happybase via Thrift"        X spark-master 'python3 /src/kiem-tra/happybase_test.py | grep -q "happybase rows"'
fi
if running mongodb; then
  chk "mongosh + pymongo"           bash -c "docker exec ${P}mongodb mongosh --quiet --eval 'db.runCommand({ping:1}).ok' | grep -q 1 && docker exec ${P}spark-master python3 /src/kiem-tra/pymongo_test.py | grep -q tong"
fi
echo "== $ok PASS, $fail FAIL"
[ $fail -eq 0 ]
