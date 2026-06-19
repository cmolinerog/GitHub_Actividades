from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, avg, stddev, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Inicializar Spark Session con soporte para Kafka y Snowflake
spark = SparkSession.builder \
    .appName("SensorAnomalySimpleWindow") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Define schema
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("timestamp", StringType(), True)  # El timestamp viene como String en el JSON
])

# 3. Read streaming data from Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensors_raw") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Convert to JSON and add watermark
df_clean = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_content") \
    .select(from_json(col("json_content"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .drop("ts") \
    .withWatermark("timestamp", "1 hour") # Limit watermark to one hour

# 5. Calculate 1-hour window aggregations
df_stats = df_clean \
    .groupBy(
        window(col("timestamp"), "1 hour", "5 minutes"), #creates 1 hour window with window.end and window.start
        col("sensor_id")
    ) \
    .agg(
        avg("temperature").alias("rolling_mean"),
        stddev("temperature").alias("rolling_stddev")
    ) \
    .select("window.start", "window.end", "sensor_id", "rolling_mean", "rolling_stddev")

# 6. Join window agg with real-time data
df_joined = df_clean.join(df_stats, "sensor_id")

#7. Filter anomalies (temperature > 3 standard deviations from the 1-hour rolling mean per sensor)
df_alerts = df_joined.filter(
    (col("rolling_stddev").isNotNull()) & 
    (col("temperature") > (col("rolling_mean") + (3 * col("rolling_stddev"))))
)

#8. Prepare kafka format
df_kafka_alerts = df_alerts.select(
    col("sensor_id").alias("key"),
    to_json(struct("*")).alias("value")
)

# 9. Outputs
#Output A: alterts to kafka
query_kafka = df_kafka_alerts.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "sensor_alerts") \
    .option("checkpointLocation", "/opt/data/checkpoints/alerts_kafka") \
    .outputMode("append") \
    .start()

#Output B: alerts to Snowflake
sf_options = {
    "sfURL": "account.snowflakecomputing.com",
    "sfUser": "user",
    "sfPassword": "password",
    "sfDatabase": "IOT_ANALYTICS",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH"
}

query_snowflake = df_alerts.writeStream \
    .format("snowflake") \
    .options(**sf_options) \
    .option("dbtable", "ANOMALY_ALERTS") \
    .option("checkpointLocation", "/opt/data/checkpoints/alerts_snowflake") \
    .outputMode("append") \
    .start()

# Mantener la aplicación corriendo
spark.streams.awaitAnyTermination()