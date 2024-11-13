import os
os.environ['SPARK_HOME'] =  "C:\Program Files\spark-3.5.3-bin-hadoop3"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'notebook'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'notebook'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark_session = SparkSession.builder.appName("NewTextApp").getOrCreate()

rdd_text = spark_session.sparkContext.textFile("./new_data/data.txt")
result_rdd_text = rdd_text.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

print(result_rdd_text.take(10))

df_text = spark_session.read.text("./new_data/data.txt")
result_df_text = df_text.selectExpr("explode(split(value, ' ')) as word") \
    .groupBy("word").count().orderBy(desc("count"))

print(result_df_text.take(10))

spark_session.stop()
