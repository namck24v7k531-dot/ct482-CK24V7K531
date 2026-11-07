# syntax=docker/dockerfile:1.7
# =============================================================================
#  bigdata-lab:2026.10 — image dùng chung cho cụm thực hành CT428 "Xử lý dữ liệu lớn"
#  Ubuntu 24.04 + OpenJDK 17 + Python 3.12 + Hadoop 3.4.3 + Spark 4.1.3 (Scala 2.13)
#  + HBase 2.6.7 (bản build cho Hadoop 3)
#
#  Build:  docker compose build            (hoặc)
#          docker build -t bigdata-lab:2026.10 -t bigdata-lab:latest .
#
#  Mẹo mạng chậm: tải trước các tệp .tgz/.tar.gz/.jar vào thư mục ./downloads/
#  (đúng tên tệp gốc) — Dockerfile sẽ dùng bản có sẵn thay vì tải lại.
#  Đổi máy chủ tải Apache: docker compose build --build-arg APACHE_MIRROR="<url1> <url2>"
#  (hoặc đặt APACHE_MIRROR trong tệp .env). Các mirror được thử lần lượt.
# =============================================================================
FROM ubuntu:24.04

ARG TARGETARCH
ARG HADOOP_VERSION=3.4.3
ARG SPARK_VERSION=4.1.3
ARG HBASE_VERSION=2.6.7
ARG SCALA_BINARY=2.13
ARG KAFKA_CLIENTS_VERSION=3.9.1
ARG COMMONS_POOL2_VERSION=2.12.1
ARG MAVEN_REPO=https://repo1.maven.org/maven2

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=Asia/Ho_Chi_Minh \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# ---- Hệ điều hành, JDK 17, Python 3.12 --------------------------------------
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openjdk-17-jdk-headless python3 python3-pip python3-dev build-essential \
      curl ca-certificates procps iproute2 iputils-ping netcat-openbsd dnsutils \
      less nano vim-tiny tini tzdata unzip zip jq \
 && ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17 \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && rm -rf /var/lib/apt/lists/*

# ---- Thư viện Python cho các bài lab ----------------------------------------
#  pyspark KHÔNG cài bằng pip: dùng bản đi kèm Spark qua PYTHONPATH (đúng phiên bản 4.1.3,
#  tiết kiệm ~450 MB so với cài trùng bằng pip).
RUN pip3 install --no-cache-dir --break-system-packages \
      numpy==2.5.3 pandas==2.3.3 pyarrow==21.0.0 \
      kafka-python==2.3.2 confluent-kafka==2.15.1 \
      pymongo==4.18.2 happybase==1.2.0 thriftpy2==0.7.1 "setuptools<81" \
 && apt-get purge -y build-essential python3-dev && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/* /root/.cache

# Danh sách mirror Apache (cách nhau bởi dấu cách), thử lần lượt từ trái sang phải.
#  downloads.apache.org / mirrors.aliyun.com chỉ giữ bản phát hành hiện hành;
#  archive.apache.org giữ mọi bản nhưng hay chặn/giới hạn IP tải nhiều (cả phòng máy chung 1 IP).
ARG APACHE_MIRROR="https://downloads.apache.org https://mirrors.aliyun.com/apache https://archive.apache.org/dist"

# Hàm tải: ưu tiên tệp đã có (và không hỏng) trong ./downloads (mount lúc build, không làm phình image);
#  đối số là URL đầy đủ (Maven) hoặc đường dẫn tương đối trong kho Apache (thử từng mirror).
ARG FETCH='fetch() { f=$(basename "$1"); if [ -s "/downloads/$f" ]; then case "$f" in *.gz|*.tgz) if ! gzip -t "/downloads/$f" 2>/dev/null; then echo ">> CANH BAO: downloads/$f bi hong, tai lai"; else echo ">> dung ban co san: $f"; cp "/downloads/$f" "/tmp/$f"; return 0; fi;; *) echo ">> dung ban co san: $f"; cp "/downloads/$f" "/tmp/$f"; return 0;; esac; fi; case "$1" in http://*|https://*) urls="$1";; *) urls=""; for m in ${APACHE_MIRROR:-https://downloads.apache.org https://archive.apache.org/dist}; do urls="$urls ${m%/}/$1"; done;; esac; for u in $urls; do echo ">> tai: $u"; if curl -fSL --connect-timeout 20 --retry 3 --retry-delay 5 -o "/tmp/$f" "$u"; then return 0; fi; echo ">> loi khi tai tu $u, thu mirror tiep theo"; done; echo ">> KHONG TAI DUOC $f. Xem README muc 4 (tai truoc vao downloads/)"; return 1; }'

# ---- Hadoop 3.4.3 -----------------------------------------------------------
RUN --mount=type=bind,source=downloads,target=/downloads \
    set -eux; eval "$FETCH"; \
    if [ "$TARGETARCH" = "arm64" ]; then HT=hadoop-${HADOOP_VERSION}-aarch64.tar.gz; else HT=hadoop-${HADOOP_VERSION}.tar.gz; fi; \
    fetch hadoop/common/hadoop-${HADOOP_VERSION}/${HT}; \
    tar -xzf /tmp/${HT} -C /opt; rm /tmp/${HT}; \
    mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop; \
    rm -rf /opt/hadoop/share/doc /opt/hadoop/share/hadoop/*/sources /opt/hadoop/share/hadoop/tools/sources

# ---- Spark 4.1.3 (Scala 2.13, bin-hadoop3) + connector Kafka dựng sẵn --------
RUN --mount=type=bind,source=downloads,target=/downloads \
    set -eux; eval "$FETCH"; \
    ST=spark-${SPARK_VERSION}-bin-hadoop3.tgz; \
    fetch spark/spark-${SPARK_VERSION}/${ST}; \
    tar -xzf /tmp/${ST} -C /opt; rm /tmp/${ST}; \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark; \
    rm -rf /opt/spark/R; \
    for j in \
      org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar \
      org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
      org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar ; do \
        fetch ${MAVEN_REPO}/$j; mv /tmp/$(basename $j) /opt/spark/jars/; \
    done

# ---- HBase 2.6.7 (bản hadoop3) ----------------------------------------------
RUN --mount=type=bind,source=downloads,target=/downloads \
    set -eux; eval "$FETCH"; \
    BT=hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz; \
    fetch hbase/${HBASE_VERSION}/${BT}; \
    tar -xzf /tmp/${BT} -C /opt; rm /tmp/${BT}; \
    mv /opt/hbase-${HBASE_VERSION}-hadoop3 /opt/hbase; \
    rm -rf /opt/hbase/docs

ENV JAVA_HOME=/usr/lib/jvm/java-17 \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    YARN_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_MAPRED_HOME=/opt/hadoop \
    SPARK_HOME=/opt/spark \
    SPARK_CONF_DIR=/opt/spark/conf \
    HBASE_HOME=/opt/hbase \
    HBASE_CONF_DIR=/opt/hbase/conf \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    HADOOP_STREAMING_JAR=/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar \
    STREAM_JAR=/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar \
    HADOOP_EXAMPLES_JAR=/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.3.jar \
    SPARK_EXAMPLES_JAR=/opt/spark/examples/jars/spark-examples_2.13-4.1.3.jar \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:/opt/spark/sbin:/opt/hbase/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# ---- Cấu hình + script khởi động --------------------------------------------
COPY conf/hadoop/ /opt/hadoop/etc/hadoop/
COPY conf/spark/  /opt/spark/conf/
COPY conf/hbase/  /opt/hbase/conf/
COPY scripts/     /opt/bigdata/
RUN set -eux; chmod +x /opt/bigdata/*.sh; \
    mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hbase-data /data /src /tmp/spark-derby; \
    # biến môi trường cho cả shell đăng nhập (docker exec ... bash -lc "...")
    env | grep -E '^(JAVA_HOME|HADOOP_|YARN_CONF_DIR|SPARK_|HBASE_|PYSPARK_|PYTHONPATH|STREAM_JAR|PATH)=' \
      | sed 's/^/export /' > /etc/profile.d/bigdata.sh; \
    java -version; hadoop version | head -1; python3 -c "import pyspark,pandas,pyarrow,numpy,kafka,confluent_kafka,pymongo,happybase; print('pyspark', pyspark.__version__)"

WORKDIR /root
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/bigdata/entrypoint.sh"]
CMD ["bash"]
