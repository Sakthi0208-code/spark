import os
os.environ['SPARK_HOME'] = "C:\Program Files\spark-3.5.3-bin-hadoop3"
os.environ['PYSPARK_DRIVER_PYTHON'] = 'notebook'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'notebook'
os.environ['PYSPARK_PYTHON'] = 'python'

from pyspark.sql import SparkSession

spark_new = SparkSession.builder.appName("NewRDDApp").getOrCreate()

numbers = [1, 2, 3, 4, 5]
rdd_numbers = spark_new.sparkContext.parallelize(numbers)
print(rdd_numbers.collect())

data_records = [("kumar", 25), ("arun", 30), ("deepak", 35)]
rdd_data = spark_new.sparkContext.parallelize(data_records)

print("All elements of rdd_data:", rdd_data.collect())

count_elements = rdd_data.count()
print("Total number of elements in rdd_data:", count_elements)

first_element_data = rdd_data.first()
print("The first element of rdd_data:", first_element_data)

rdd_data.foreach(lambda x: print(x))

mapped_rdd_data = rdd_data.map(lambda x: (x[0].upper(), x[1]))
print("rdd_data with uppercase names:", mapped_rdd_data.collect())

filtered_rdd_data = rdd_data.filter(lambda x: x[1] > 30)
print(filtered_rdd_data.collect())

reduced_rdd_data = rdd_data.reduceByKey(lambda x, y: x + y)
print(reduced_rdd_data.collect())

sorted_rdd_data = rdd_data.sortBy(lambda x: x[1], ascending=False)
print(sorted_rdd_data.collect())

spark_new.stop()
