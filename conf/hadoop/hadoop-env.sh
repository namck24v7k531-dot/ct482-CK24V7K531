# hadoop-env.sh — cụm thực hành CT428 (Java 17)
export JAVA_HOME=/usr/lib/jvm/java-17
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
# Heap tối đa cho tiến trình Hadoop; mỗi service đặt riêng qua biến môi trường trong docker-compose.yml
export HADOOP_HEAPSIZE_MAX=${HADOOP_HEAPSIZE_MAX:-512m}
# Java 17 chặn truy cập reflection vào nội bộ JDK; Hadoop 3.4 cần mở các gói sau
export JDK17_OPENS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED"
case " ${HADOOP_OPTS:-} " in *"java.base/java.lang=ALL-UNNAMED"*) ;; *) export HADOOP_OPTS="${HADOOP_OPTS:-} $JDK17_OPENS -Djava.net.preferIPv4Stack=true" ;; esac
export HADOOP_LOG_DIR=/tmp/hadoop-logs
export HADOOP_PID_DIR=/tmp
