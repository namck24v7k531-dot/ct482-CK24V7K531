"""Kiểm tra PySpark đọc CSV từ HDFS, ghi/đọc Parquet trên HDFS."""
import sys
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("kiem-tra-hdfs-parquet").getOrCreate()
src = sys.argv[1] if len(sys.argv) > 1 else "hdfs://namenode:9000/data/raw/retail_transactions.csv"
df = spark.read.csv(src, header=True, inferSchema=True)
agg = (df.withColumn("revenue", F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")))
         .groupBy("region").agg(F.round(F.sum("revenue"), 0).alias("revenue"), F.count("*").alias("n"))
         .orderBy(F.desc("revenue")))
out = "hdfs://namenode:9000/user/student/kiem-tra/revenue_by_region.parquet"
agg.write.mode("overwrite").parquet(out)
back = spark.read.parquet(out)
back.show()
print("ROWS_IN_PARQUET", back.count(), "MASTER", spark.sparkContext.master,
      "APP", spark.sparkContext.applicationId)
spark.stop()
