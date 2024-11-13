import os
os.environ['SPARK_HOME'] = "C:\Program Files\spark-3.5.3-bin-hadoop3"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'notebook'  # Changed option to 'notebook'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'notebook'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark import SparkContext
from pyspark.sql import SparkSession

sc_new = SparkContext(appName="NewSparkApp")
print(sc_new)
sc_new.stop()
spark_new = SparkSession.builder \
    .appName("NewSparkApp") \
    .getOrCreate()

sc_new = spark_new.sparkContext
print(sc_new)

sc_new.stop()