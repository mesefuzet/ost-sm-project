#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, min, max, count, window
from pyspark.sql.types import StructType, StringType, DoubleType
import os

# Get checkpoint location
checkpoint_location = os.path.join(os.getcwd(), "spark_checkpoints")
os.makedirs(checkpoint_location, exist_ok=True)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingFeatureExtraction") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .config("spark.sql.streaming.microBatchDurationMs", "5000") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# Kafka configuration
kafka_broker = "localhost:9092"
topic_name = "hai-dataset"

# Schema definition
train_schema = StructType() \
    .add("timestamp", StringType(), True) \
    .add("data_type", StringType(), True) \
    .add("P1_FCV01D", DoubleType(), True) \
    .add("P1_FCV01Z", DoubleType(), True) \
    .add("P1_FCV03D", DoubleType(), True) \
    .add("P1_FT01", DoubleType(), True) \
    .add("P1_FT01Z", DoubleType(), True) \
    .add("P1_FT02", DoubleType(), True) \
    .add("P1_FT02Z", DoubleType(), True) \
    .add("P1_FT03", DoubleType(), True) \
    .add("P1_FT03Z", DoubleType(), True) \
    .add("P1_LCV01D", DoubleType(), True) \
    .add("P1_LCV01Z", DoubleType(), True) \
    .add("P1_LIT01", DoubleType(), True) \
    .add("x1003_24_SUM_OUT", DoubleType(), True)

# Read Kafka stream
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON and parse the data
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), train_schema).alias("data"))

# Select relevant columns and filter train data
train_data = parsed_stream.select("data.*").filter(col("data_type") == "train")

# Perform feature extraction
# Calculate statistics (mean, stddev, min, max) over a sliding window of 1 minute
feature_stream = train_data \
    .groupBy(window(col("timestamp"), "1 minute")) \
    .agg(
        mean("P1_FCV01D").alias("mean_P1_FCV01D"),
        stddev("P1_FCV01D").alias("stddev_P1_FCV01D"),
        min("P1_FCV01D").alias("min_P1_FCV01D"),
        max("P1_FCV01D").alias("max_P1_FCV01D"),
        mean("P1_FT01").alias("mean_P1_FT01"),
        stddev("P1_FT01").alias("stddev_P1_FT01"),
        min("P1_FT01").alias("min_P1_FT01"),
        max("P1_FT01").alias("max_P1_FT01"),
        count("*").alias("record_count")
    )

# Output extracted features to console
feature_query = feature_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", os.path.join(checkpoint_location, "feature_extraction")) \
    .start()

feature_query.awaitTermination()

