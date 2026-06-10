from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, stddev, date_format

spark = SparkSession.builder.getOrCreate()

# Read data
df_history = spark.read.parquet("/opt/data/output/*.parquet")

# Daily report saved on reports folder
df_daily_report = df_history \
    .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
    .groupBy("date", "sensor_id", "location") \
    .agg(
        min("temperature").alias("min_temp"),
        max("temperature").alias("max_temp"),
        avg("temperature").alias("avg_temp"),
        stddev("temperature").alias("std_dev_temp")
    )

df_daily_report.write.mode("overwrite").parquet("/opt/data/reports/daily_reports")

###Command
### docker exec spark-worker /opt/spark/bin/spark-submit /opt/data/batch_sensor.py