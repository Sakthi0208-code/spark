

import os
os.environ['SPARK_HOME'] = "C:\Program Files\spark-3.5.3-bin-hadoop3"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'notebook'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'notebook'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession

spark_session = SparkSession.builder \
    .appName("new_sample_app") \
    .getOrCreate()

data_records = [("kumar", 29), ("arun", 31), ("deepak", 47)]
df_new = spark_session.createDataFrame(data_records, ["EmployeeName", "EmployeeAge"])
df_new.show()