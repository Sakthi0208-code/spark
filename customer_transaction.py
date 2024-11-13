from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import date

spark = SparkSession.builder.appName("Customer Transactions Analysis").getOrCreate()

data = [
    (1, date(2024, 11, 1), 120.0),
    (1, date(2024, 11, 2), 250.0),
    (1, date(2024, 11, 3), 70.0),
    (1, date(2024, 11, 5), 180.0),
    (2, date(2024, 11, 4), 320.0),
    (2, date(2024, 11, 6), 130.0),
    (2, date(2024, 11, 7), 210.0)
]

schema = ["customer_id", "transaction_date", "amount"]
transactions = spark.createDataFrame(data, schema=schema)

cumulative_window = Window.partitionBy("customer_id").orderBy("transaction_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

transactions = transactions.withColumn(
    "cumulative_amount",
    F.sum("amount").over(cumulative_window)
)

transactions = transactions.withColumn(
    "transaction_date_days",
    F.unix_timestamp(F.col("transaction_date"), "yyyy-MM-dd") / (24 * 60 * 60)
)

rolling_window = Window.partitionBy("customer_id").orderBy("transaction_date_days") \
    .rangeBetween(-6, 0)

transactions = transactions.withColumn(
    "rolling_avg_amount",
    F.avg("amount").over(rolling_window)
)

transactions.show()

spark.stop()
