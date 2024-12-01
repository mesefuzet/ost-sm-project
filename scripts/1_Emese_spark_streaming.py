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
from pyspark.sql.functions import regexp_replace



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

raw_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data")

# Parse JSON keys and values
# parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data") \
#     .withColumn("header", expr("regexp_extract(raw_data, '^(.*?)\"\\s*:', 1)")) \
#     .withColumn("data", expr("regexp_extract(raw_data, ':(.*?)$', 1)")) \
#     .withColumn("header", regexp_replace(col("header"), r'[{}"]', '')) \
#     .withColumn("data", regexp_replace(col("data"), r'[{}"]', ''))

parsed_stream = raw_stream \
    .withColumn("timestamp", expr(r"regexp_extract(raw_data, '\"timestamp\":\\s*\"(.*?)\"', 1)")) \
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)")) \
    .withColumn("P1_FCV01Z", expr(r"regexp_extract(raw_data, '\"P1_FCV01Z\":\\s*([0-9.]+)', 1)")) \
    .withColumn("P1_FCV03D", expr(r"regexp_extract(raw_data, '\"P1_FCV03D\":\\s*([0-9.]+)', 1)")) \
    .withColumn("data_type", expr(r"regexp_extract(raw_data, '\"data_type\":\\s*\"(.*?)\"', 1)"))

parsed_stream = parsed_stream \
    .withColumn("P1_FCV01D", col("P1_FCV01D").cast("double")) \
    .withColumn("P1_FCV01Z", col("P1_FCV01Z").cast("double")) \
    .withColumn("P1_FCV03D", col("P1_FCV03D").cast("double"))

# Split headers into columns
# parsed_stream = parsed_stream.withColumn("headers", split(col("header"), ";")) \
#     .withColumn("values", split(col("data"), ";"))

# Select specific columns (adjust indices as per your schema)
selected_columns = parsed_stream.select(
    col("timestamp"),
    col("P1_FCV01D"),
    col("P1_FCV01Z"),
    col("P1_FCV03D"),
    col("data_type")
)
#I just put this here for debug, it logs into a json what is being parsed into the Spark schema, can be commented out later:
# parsed_stream.writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "debug/parsed_output") \
#     .option("checkpointLocation", "debug/parsed_checkpoint") \
#     .start()

# Debugging: Save parsed output to a file
selected_columns.writeStream \
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
### EMESE 2 STREAMING PROCESSES ####
# Data Exploration Tasks
# 1. Detect Missing Values & Basic Statistics

# print("1. PROCESS: DETECTION OF MISSING VALUES & MAIN STATISTICS")
# missing_counts_train = train_stream.select(
#     [(count(when(isnan(c) | col(c).isNull(), c))).alias(c) for c in train_data.columns]
# )

# missing_counts_test = test_stream.select(
#     [(count(when(isnan(c) | col(c).isNull(), c))).alias(c) for c in test_data.columns]
# )

# #debug for missing values
# missing_counts_train_query = missing_counts_train.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# missing_counts_test_query = missing_counts_test.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()
 
# train_stats = train_stream.select(
#     mean(col("x1003_24_SUM_OUT")).alias("train_mean"),
#     stddev(col("x1003_24_SUM_OUT")).alias("train_stddev")
# )

# test_stats = test_stream.select(
#     mean(col("x1003_24_SUM_OUT")).alias("test_mean"),
#     stddev(col("x1003_24_SUM_OUT")).alias("test_stddev")
# )

# # Write statistics to the console for debugging
# train_stats.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# test_stats.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# # Write statistics to the console (debugging)
# train_stats_query = train_stats.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# test_stats_query = test_stats.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# print("2. PROCESS: DATA NORMALIZATION")
# 2. Normalize the Data (MinMax Scaling)


# assembler = VectorAssembler(inputCols=["x1003_24_SUM_OUT"], outputCol="features")
# train_features = assembler.transform(train_data)
# test_features = assembler.transform(test_data)

# # test: static min and max
# static_min = 0.0 
# static_max = 200.0 


# #min_val = train_data.agg(min("x1003_24_SUM_OUT")).collect()[0][0]
# #max_val = train_data.agg(max("x1003_24_SUM_OUT")).collect()[0][0]

# # Normalize using the min-max formula
# scaled_train_data = train_data.withColumn(
#     "scaled_features",
#     (col("x1003_24_SUM_OUT") - lit(static_min)) / (lit(static_max) - lit(static_min))
# )

# scaled_test_data = test_data.withColumn(
#     "scaled_features",
#     (col("x1003_24_SUM_OUT") - lit(static_min)) / (lit(static_max) - lit(static_min))
# )


# #Write the Results to the Console (For Debugging)
# scaled_train_query = scaled_train_data.select("timestamp", "scaled_features").writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# scaled_test_query = scaled_test_data.select("timestamp", "scaled_features").writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

#await terminations so the code won't run until infinity
# train_data_query.awaitTermination()
# test_data_query.awaitTermination()
# missing_counts_train_query.awaitTermination()
# missing_counts_test_query.awaitTermination()
# train_stats_query.awaitTermination()
# test_stats_query.awaitTermination()