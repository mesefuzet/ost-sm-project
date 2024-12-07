from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, count, isnan, when, lit
from pyspark.sql.types import StructType, StringType, DoubleType
import json
import os
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_json
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import min, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, expr, to_timestamp, window, avg, count
from pyspark.sql import functions as F
import time

# ================================================
### Bia Data Aggregation (streaming Process) ####

# Set Legacy Time Parser Policy ( to process timestamps correctly)
spark = SparkSession.builder \
    .appName("KafkaStreamProcessor") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()


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

# Filter out rows that are completely blank or contain only zeros in numeric columns
parsed_stream = parsed_stream.filter(
    (col("timestamp").isNotNull()) &
    ((col("P1_FCV01D") != 0) | (col("P1_FCV01Z") != 0) | (col("P1_FCV03D") != 0) | (col("x1003_24_SUM_OUT") != 0))
)

# Train-test separation
train_stream = parsed_stream.filter(col("data_type") == "train") \
    .select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT", "data_type")

test_stream = parsed_stream.filter(col("data_type") == "test") \
    .select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT", "data_type", "attack_label")

# Aggregation over a 1-minute window
aggregated_stream = parsed_stream \
    .withColumn("timestamp", to_timestamp("timestamp", "yyyy.MM.dd HH:mm")) \
    .groupBy(window(col("timestamp"), "1 minute")) \
    .agg(
        F.avg("P1_FCV01D").alias("avg_P1_FCV01D"),
        F.avg("P1_FCV01Z").alias("avg_P1_FCV01Z"),
        F.avg("P1_FCV03D").alias("avg_P1_FCV03D"),
        F.avg("x1003_24_SUM_OUT").alias("avg_x1003_24_SUM_OUT"),
        F.count("*").alias("record_count")
    )

# Output aggregation to console
aggregated_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Check the schema
# parsed_stream.printSchema()

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

# I just put this here for debug, it logs into a json what is being parsed into the Spark schema, can be commented out later:
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

spark.streams.awaitAnyTermination()







