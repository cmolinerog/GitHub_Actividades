import os
import sys

# 1. Environment Configuration
path_python = r"C:\pyspark-project\venv311\Scripts\python.exe"

os.environ["PYSPARK_PYTHON"] = path_python
os.environ["PYSPARK_DRIVER_PYTHON"] = path_python
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["SPARK_HADOOP_NATIVE"] = "false"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#2. Initialize Spark Session
spark = SparkSession.builder \
    .appName("`hasee4_q11") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.io.nativeio.enabled", "false") \
    .getOrCreate()

# Suppress verbose log messages
spark.sparkContext.setLogLevel("ERROR")

# 3. Data ingestion, read csv
customers_path = r"C:\pyspark-project\phase_4\customers.csv"
orders_path = r"C:\pyspark-project\phase_4\orders.csv"

df_customers = spark.read.csv(customers_path, header=True, inferSchema=True)
df_orders = spark.read.csv(orders_path, header=True, inferSchema=True)

# 4. Join DataFrames and calculate total_spend by customer_id and city
customer_spend = df_orders.join(df_customers, on="customer_id", how="inner") \
    .groupBy("city", "customer_id", "name") \
    .agg(F.sum("amount").alias("total_spend"))     #F.sum, sum all the values from a single column

# 3. The window partitions data by city and sorts by total spend descending.
window_spec = Window.partitionBy("city").orderBy(F.col("total_spend").desc())

# Rank  customers by city
ranked_customers = customer_spend.withColumn("rank", F.dense_rank().over(window_spec))

# 4. Filter Top 5 customers  by city and delete rank column
top_5_per_city = ranked_customers.filter(F.col("rank") <= 5).drop("rank")

# 5: Write Result as Parquet Partitioned by City
output_path = r"C:\pyspark-project\phase_4\top_customers_by_city"

top_5_per_city.write \
    .mode("overwrite") \
    .partitionBy("city") \
    .parquet(output_path)
