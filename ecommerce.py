from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import sum, avg, count, desc, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Clothing Type Transaction Analysis") \
    .getOrCreate()

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transaction_date", StringType(), True)
])

data = [
    ("t1", "u1", "p1", "Shirt", 100.0, "2023-10-01"),
    ("t2", "u1", "p2", "T-Shirt", 150.0, "2023-10-03"),
    ("t3", "u2", "p3", "Pant", 80.0, "2023-10-10"),
    ("t4", "u1", "p4", "Shirt", 70.0, "2023-10-15"),
    ("t5", "u2", "p5", "Pant", 120.0, "2023-10-20"),
    ("t6", "u3", "p6", "T-Shirt", 80.0, "2023-10-22"),
    ("t7", "u3", "p7", "Shirt", 40.0, "2023-10-25"),
    ("t8", "u1", "p8", "T-Shirt", 200.0, "2023-10-30")
]

df = spark.createDataFrame(data, schema=schema)

user_spending_df = df.groupBy("user_id") \
    .agg(
    sum("amount").alias("total_spent"),
    avg("amount").alias("avg_transaction")
)

category_count_df = df.groupBy("user_id", "category") \
    .agg(count("category").alias("category_count"))

window_spec = Window.partitionBy("user_id").orderBy(desc("category_count"))

favorite_category_df = category_count_df \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter("rank = 1") \
    .select("user_id", "category")

final_df = user_spending_df \
    .join(favorite_category_df, on="user_id", how="left") \
    .withColumnRenamed("category", "favorite_category") \
    .select("user_id", "total_spent", "avg_transaction", "favorite_category")

final_df.show()