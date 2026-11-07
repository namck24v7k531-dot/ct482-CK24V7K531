"""Spark Structured Streaming đọc Kafka (connector dựng sẵn trong /opt/spark/jars)."""
import sys
from pyspark.sql import SparkSession, functions as F
bootstrap = sys.argv[1] if len(sys.argv) > 1 else "kafka:9092"
topic = sys.argv[2] if len(sys.argv) > 2 else "kiem-tra"
spark = SparkSession.builder.appName("kiem-tra-kafka-streaming").getOrCreate()
df = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic).option("startingOffsets", "earliest").load())
q = (df.selectExpr("CAST(value AS STRING) AS v")
       .writeStream.format("parquet")
       .option("path", "hdfs://namenode:9000/user/student/kiem-tra/stream_out")
       .option("checkpointLocation", "hdfs://namenode:9000/user/student/kiem-tra/stream_chk")
       .trigger(availableNow=True).start())
q.awaitTermination()
n = spark.read.parquet("hdfs://namenode:9000/user/student/kiem-tra/stream_out").count()
print("STREAM_ROWS_WRITTEN", n)
spark.stop()
