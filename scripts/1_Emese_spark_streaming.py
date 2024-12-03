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
import time



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
    .option("failOnDataLoss", "false") \
    .load()

#I just put this here for debug, it logs into a json what Spark gets from Kafka, can be commented out later:
# kafka_stream.selectExpr("CAST(value AS STRING)").writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "debug/raw_kafka_output") \
#     .option("checkpointLocation", "debug/raw_kafka_checkpoint") \
#     .start()

raw_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data")


parsed_stream = raw_stream \
    .withColumn("timestamp", expr(r"regexp_extract(raw_data, '\"timestamp\":\\s*\"(.*?)\"', 1)")) \
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)")) \
    .withColumn("P1_FCV01Z", expr(r"regexp_extract(raw_data, '\"P1_FCV01Z\":\\s*([0-9.]+)', 1)")) \
    .withColumn("P1_FCV03D", expr(r"regexp_extract(raw_data, '\"P1_FCV03D\":\\s*([0-9.]+)', 1)")) \
    .withColumn("x1003_24_SUM_OUT", expr(r"regexp_extract(raw_data, '\"x1003_24_SUM_OUT\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("data_type", expr(r"regexp_extract(raw_data, '\"data_type\":\\s*\"(.*?)\"', 1)")) \
    .withColumn("attack_label", expr(r"regexp_extract(raw_data, '\"attack_label\":\\s*([0-9]+)', 1)").cast("int"))

parsed_stream = parsed_stream \
    .withColumn("P1_FCV01D", col("P1_FCV01D").cast("double")) \
    .withColumn("P1_FCV01Z", col("P1_FCV01Z").cast("double")) \
    .withColumn("P1_FCV03D", col("P1_FCV03D").cast("double")) \
    .withColumn("P1_FCV03D", col("x1003_24_SUM_OUT").cast("double"))

#Filter out rows that are completely blank or contain only zeros in numeric columns
parsed_stream = parsed_stream.filter(
    (col("timestamp").isNotNull()) &
    ((col("P1_FCV01D") != 0) | (col("P1_FCV01Z") != 0) | (col("P1_FCV03D") != 0) | (col("x1003_24_SUM_OUT") != 0))
)

#check the schema
#parsed_stream.printSchema()

#train-test separation
train_stream = parsed_stream.filter(col("data_type") == "train") \
    .select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT", "data_type")

test_stream = parsed_stream.filter(col("data_type") == "test") \
    .select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT", "data_type", "attack_label")

# Select specific columns (adjust indices as per your schema)
selected_columns = parsed_stream.select(
    col("timestamp"),
    col("P1_FCV01D"),
    col("P1_FCV01Z"),
    col("P1_FCV03D"),
    col("x1003_24_SUM_OUT"),
    col("data_type"),
    col("attack_label")
    
)


# Debugging: Save parsed output to a file ---> when something is bad, we can comment this out and have a look at the log files in debug/
# selected_columns.writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "debug/parsed_output") \
#     .option("checkpointLocation", "debug/parsed_checkpoint") \
#     .start()

# train_stream.writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "debug/train_output") \
#     .option("checkpointLocation", "debug/train_checkpoint") \
#     .start()

# test_stream.writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "debug/test_output") \
#     .option("checkpointLocation", "debug/test_checkpoint") \
#     .start()
print("---------TRAIN DATA----------------")
time.sleep(1)
train_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("---------TEST DATA----------------")
time.sleep(1)
test_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("Train row count:")
train_stream.select(count("*")).writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

print("Test row count:")
test_stream.select(count("*")).writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# ===============================================
### EMESE 2 STREAMING PROCESSES ####
# Data Exploration Tasks
# 1. Detect Missing Values & Basic Statistics

print("1. PROCESS: DETECTION OF REMAINING MISSING VALUES & MAIN STATISTICS")
time.sleep(1)
missing_counts_train = train_stream.select(
    [(count(when(isnan(c) | col(c).isNull(), c))).alias(c) for c in train_stream.columns]
)

missing_counts_test = test_stream.select(
    [(count(when(isnan(c) | col(c).isNull(), c))).alias(c) for c in test_stream.columns]
)

missing_counts_train.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

missing_counts_test.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()



# Process 2: Statistics Calculation
train_stats = train_stream.select(
    mean(col("x1003_24_SUM_OUT")).alias("train_mean"),
    stddev(col("x1003_24_SUM_OUT")).alias("train_stddev")
)

test_stats = test_stream.select(
    mean(col("x1003_24_SUM_OUT")).alias("test_mean"),
    stddev(col("x1003_24_SUM_OUT")).alias("test_stddev")
)

train_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

test_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# print("2. PROCESS: DATA NORMALIZATION")
# 2. Normalize the Data (MinMax Scaling)


spark.streams.awaitAnyTermination()