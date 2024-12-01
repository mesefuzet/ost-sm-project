from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, count, isnan, when, lit, split, regexp_replace, expr
from pyspark.sql.types import StructType, StringType, DoubleType
import json
import os
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_json
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import min, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# ================================================
#Working Spark solution

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamProcessor") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .getOrCreate()

# Kafka Configuration
kafka_broker = "localhost:9092"
topic_name = "hai-dataset"

# Read from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

#I just put this here for debug, it logs into a json what Spark gets from Kafka, can be commented out later:
kafka_stream.selectExpr("CAST(value AS STRING)").writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "debug/raw_kafka_output") \
    .option("checkpointLocation", "debug/raw_kafka_checkpoint") \
    .start()

# Parse JSON keys and values
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data") \
    .withColumn("header", expr("regexp_extract(raw_data, '^(.*?)\"\\s*:', 1)")) \
    .withColumn("data", expr("regexp_extract(raw_data, ':(.*?)$', 1)")) \
    .withColumn("header", regexp_replace(col("header"), r'[{}"]', '')) \
    .withColumn("data", regexp_replace(col("data"), r'[{}"]', ''))

# Split headers into columns
parsed_stream = parsed_stream.withColumn("headers", split(col("header"), ";")) \
    .withColumn("values", split(col("data"), ";"))

# Select specific columns (adjust indices as per your schema)
selected_columns = parsed_stream.select(
    col("values").getItem(0).alias("timestamp"),
    col("values").getItem(1).alias("P1_FCV01D"),
    col("values").getItem(2).alias("P1_FCV01Z"),
    col("values").getItem(3).alias("P1_FCV03D"),
    col("values").getItem(-1).alias("data_type")
)

#I just put this here for debug, it logs into a json what is being parsed into the Spark schema, can be commented out later:
parsed_stream.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "debug/parsed_output") \
    .option("checkpointLocation", "debug/parsed_checkpoint") \
    .start()

# Output filtered data to console
query = selected_columns.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
# ===============================================
### Sujahat 2 STREAMING PROCESSES ####

print("3. PROCESS: THRESHOLD - BASED ANOMALY FLAGS")
# 1. Threshold-Based Anomaly Flags
train_stats = train_data.select(
    *[mean(col(c)).alias(f"{c}_mean") for c in schema.fieldNames() if c != "timestamp" and c != "data_type"],
    *[stddev(col(c)).alias(f"{c}_stddev") for c in schema.fieldNames() if c != "timestamp" and c != "data_type"]
).collect()

# Prepare thresholds for each column
thresholds = {f"{c}": {"mean": train_stats[0][f"{c}_mean"], "stddev": train_stats[0][f"{c}_stddev"]}
              for c in schema.fieldNames() if c != "timestamp" and c != "data_type"}

# Add anomaly flags dynamically for all columns -> we consider everything an anomaly which is 3 standard deviations "away" from the mean (into + or - direction)
for col_name, stats in thresholds.items():
    upper_threshold = stats["mean"] + 3 * stats["stddev"]
    lower_threshold = stats["mean"] - 3 * stats["stddev"]
    test_data = test_data.withColumn(
        f"{col_name}_anomaly_flag",
        when((col(col_name) > upper_threshold) | (col(col_name) < lower_threshold), 1).otherwise(0)
    )

# Write anomaly flags to console for debugging
anomaly_flags_query = test_data.select(
    ["timestamp"] + [f"{c}_anomaly_flag" for c in schema.fieldNames() if c != "timestamp" and c != "data_type"]
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("4. PROCESS: ANOMALY CLASSIFICATION")
# 2. Anomaly Classification
def classify_anomaly(value, mean, stddev):
    deviation = abs(value - mean)
    if deviation > 3 * stddev:
        return "Severe Anomaly"
    elif deviation > 2 * stddev:
        return "Moderate Anomaly"
    elif deviation > stddev:
        return "Minor Anomaly"
    else:
        return "Normal"

classify_udf = udf(lambda value, mean, stddev: classify_anomaly(value, mean, stddev), StringType())

for col_name, stats in thresholds.items():
    test_data = test_data.withColumn(
        f"{col_name}_anomaly_classification",
        classify_udf(col(col_name), col(lit(stats["mean"])), col(lit(stats["stddev"])))
    )

# Write anomaly classifications to console
classification_query = test_data.select(
    ["timestamp"] + [f"{c}_anomaly_classification" for c in schema.fieldNames() if c != "timestamp" and c != "data_type"]
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Wait for Termination
spark.streams.awaitTermination(60)  # Stops after 60 seconds -> for the debug

# Test comment1