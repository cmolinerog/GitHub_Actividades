import os
# Force Spark to use localhost to bypass underscore computer name bug
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, min, max, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Initialize Spark Session with verified Snowflake version
spark = SparkSession.builder \
    .appName("KafkaSparkToSnowflake") \
    .master("local[*]") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "net.snowflake:snowflake-jdbc:3.16.0,"
            "net.snowflake:spark-snowflake_2.12:3.1.7") \
    .config("spark.hadoop.io.nativeio.enabled", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 2. Define Input Schema
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 3. Read data from Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iot_data") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Clean data and apply 10-minute Watermark
df_clean = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_content") \
    .select(from_json(col("json_content"), schema).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp", "10 minutes")  # if a value arrives 10 minutes late is discarded

# 5. Calculate 5-minute Window Aggregations
df_agg = df_clean \
    .groupBy(
        window(col("timestamp"), "5 minutes"), # groups by when  the event happened, not when it arrived
        col("sensor_id")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_id"),
        col("avg_temperature"),
        col("min_temperature"),
        col("max_temperature"),
        current_timestamp().alias("processed_at") # when it was processed by spark
    )

# Snowflake connection configurations
sf_options = {
    "sfURL": "VFAYJVI-OE71590.snowflakecomputing.com",  
    "sfUser": "CRISSSMG",                               
    "sfPassword": "*****.",                   
    "sfDatabase": "IOT_ANALYTICS",
    "sfSchema": "RAW",
    "sfWarehouse": "COMPUTE_WH"                   
}

#Instead of modifying existing rows, Snowflake inserts a 
# new row with the corrected data. Use 'processed_at' to find the latest update.
def write_to_snowflake(batch_df, batch_id):
    batch_df.write \
        .format("snowflake") \
        .options(**sf_options) \
        .option("dbtable", "SENSOR_AGGREGATES_RAW") \
        .mode("append") \
        .save()
#7. If a valid late event arrives, Spark recalculates only 
# that specific window and emits the updated row in the current micro-batch.
query_snowflake = df_agg.writeStream \
    .foreachBatch(write_to_snowflake) \
    .outputMode("update") \
    .option("checkpointLocation", "/opt/data/checkpoints/late_data_sf") \
    .start()

# Keep the streaming application running
spark.streams.awaitAnyTermination()