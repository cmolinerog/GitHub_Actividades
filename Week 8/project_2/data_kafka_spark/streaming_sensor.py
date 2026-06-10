from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, min, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("SensorStreamingProcessor") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Cast type
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("air_quality", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True) 
])

# 3. Read data from kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensors_raw") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Convert Kafka JSON into structured columns
# Convert Kafka value (in binary) to a text column ("json_content")
#Use schema to interpretate the data
#Extract the columns from data 
#Convert timestamp to timestamp type

df_clean = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_content") \
    .select(from_json(col("json_content"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

#5. Save df_clean (raw_data) in output folder for batch processing
query_to_parquet = df_clean.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "/opt/data/output") \
    .option("checkpointLocation", "/opt/data/checkpoints/output") \
    .start()

# 6. Temperature aggregations for every 5 minutes
df_agg = df_clean \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("sensor_id"),
        col("location")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature")
    )


# 7. Save registers where temperature > 35°C
df_alerts = df_clean.filter(col("temperature") > 35.0)


# 8. Outputs

# Output 1: Alerts in console
query_alerts_console = df_alerts.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start()

# Output 2: Alertas in JSON file
query_alerts_file = df_alerts.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", "/opt/data/alerts") \
    .option("checkpointLocation", "/opt/data/checkpoints/alerts") \
    .start()

# Output 3: Aggregations to console
query_agg_console = df_agg.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start()

# Keep the streaming application running until any active stream terminates
spark.streams.awaitAnyTermination()


###Command to execute 
### docker exec -it spark-worker /opt/spark/bin/spark-submit --conf "spark.jars.ivy=/tmp/.ivy2" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/data/streaming_sensor.py