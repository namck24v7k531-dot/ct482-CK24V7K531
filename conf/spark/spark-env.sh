#!/usr/bin/env bash
# spark-env.sh — cụm thực hành CT428
export JAVA_HOME=/usr/lib/jvm/java-17
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON:-python3}
export SPARK_LOG_DIR=/tmp/spark-logs
export SPARK_PID_DIR=/tmp
# Bộ nhớ heap cho tiến trình daemon (Master/Worker/HistoryServer); đặt riêng trong docker-compose.yml
export SPARK_DAEMON_MEMORY=${SPARK_DAEMON_MEMORY:-256m}
