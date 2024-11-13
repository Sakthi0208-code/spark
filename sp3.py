import os
os.environ['SPARK_HOME'] = "C:\\new-spark-path"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'notebook'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'notebook'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession

spark_new = SparkSession.builder \
    .appName("NewSparkApplication") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

print(spark_new)

spark_new.stop()
