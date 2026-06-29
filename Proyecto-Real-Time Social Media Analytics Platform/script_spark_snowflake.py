import os
import sys

# 1. Fix the Windows Hostname Underscore bug
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# 2. Hadoop Home setups for Windows
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
os.environ["SPARK_HADOOP_NATIVE"] = "false"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_date, when, date_format, size, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Initialize Spark Session 
spark = SparkSession.builder \
    .appName("KafkaSparkToSnowflake") \
    .master("local[*]") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "net.snowflake:snowflake-jdbc:3.15.0,"
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4") \
    .config("spark.hadoop.io.nativeio.enabled", "false") \
    .getOrCreate()

# Suppress verbose log messages
spark.sparkContext.setLogLevel("ERROR")

# Define schema 
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("actor_user_id", StringType(), True),
    StructField("target_user_id", StringType(), True),
    StructField("target_post_id", StringType(), True),
    StructField("target_author_id", StringType(), True),
    StructField("comment_text", StringType(), True),
    StructField("hashtags", ArrayType(StringType()), True),
    StructField("timestamp", StringType(), True)
])

# Read from Kafka
kafka_raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "social_media_events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON payloads
parsed_df = kafka_raw_df \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

## Data quality check
checked_df = parsed_df.withColumn(
    "is_invalid",
    when(
        col("event_id").isNull() | 
        col("event_type").isNull() | 
        col("timestamp").isNull(), 
        True
    ).otherwise(False)
).withColumn(
    "hashtags",
    when(size(col("hashtags")) == 0, None).otherwise(col("hashtags"))
)


# Separate clean records from invalid ones
clean_events_df = checked_df.filter(col("is_invalid") == False)

# Streaming transformations
transformed_df = clean_events_df \
    .withColumn("event_date", to_date(col("timestamp"))) \
    .withColumn("event_time", date_format(col("timestamp"), "HH:mm:ss")) \
    .select(
        "event_id",
        "event_type",
        "actor_user_id",
        "target_user_id",
        "target_post_id",
        "comment_text",
        "timestamp",      
        "event_date",    
        "event_time",     
        "hashtags"
    )

# Conexion to Snowflake
sf_options = {
    "sfURL": "*****",  
    "sfUser": "****",                       
    "sfPassword": "****.",                   
    "sfDatabase": "SOCIAL_MEDIA_DB",
    "sfSchema": "RAW",
    "sfWarehouse": "COMPUTE_WH"                   
}

def process_micro_batch(batch_df, batch_id):
    if batch_df.count() > 0:
        # 1. Output to console
        print(f"\n--- [BATCH IDENTIFIER: {batch_id}] ---")
        batch_df.show(truncate=False)
        
        # 2. Output to snowflake
        print(f"📦 Enviando {batch_df.count()} registros limpios a Snowflake...")
        batch_df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**sf_options) \
            .option("dbtable", "RAW_EVENTS") \
            .mode("append") \
            .save()

query_stream = transformed_df.writeStream \
    .trigger(processingTime='5 seconds') \
    .foreachBatch(process_micro_batch) \
    .option("checkpointLocation", "./checkpoint_snowflake") \
    .start()

query_stream.awaitTermination()