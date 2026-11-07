from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
s = SparkSession.builder.appName("kiem-tra-mllib").getOrCreate()
df = s.read.csv("hdfs://namenode:9000/data/raw/retail_transactions.csv", header=True, inferSchema=True)
df = df.withColumn("label", (F.col("discount") > 0).cast("double"))
pipe = Pipeline(stages=[StringIndexer(inputCol="region", outputCol="region_idx"),
                        StringIndexer(inputCol="category", outputCol="cat_idx"),
                        VectorAssembler(inputCols=["region_idx", "cat_idx", "quantity", "unit_price"], outputCol="features"),
                        LogisticRegression(maxIter=10)])
tr, te = df.randomSplit([0.8, 0.2], seed=42)
m = pipe.fit(tr)
m.write().overwrite().save("hdfs://namenode:9000/models/kiem_tra_lr")
auc = BinaryClassificationEvaluator().evaluate(PipelineModel.load("hdfs://namenode:9000/models/kiem_tra_lr").transform(te))
print("MLLIB_AUC", round(auc, 3))
s.stop()
