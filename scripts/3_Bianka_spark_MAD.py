from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lag, abs, lit, expr, avg, to_timestamp, window, percentile_approx
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
import time
from pyspark.sql.functions import udf
import numpy as np
from pyspark.sql.types import DoubleType
#--------------------------------------------------------------------------------------------------------------------------------------
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
from pyspark.sql.functions import collect_list, expr, abs, percentile_approx


# ================================================
### Bia Data Aggregation (streaming Process) ####
# Data aggregated by minute, one minute contains 60 records

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
    .withColumn("timestamp", to_timestamp("timestamp", "yyyy.MM.dd HH:mm")) \
    .withWatermark("timestamp", "5 minutes") \
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

# Define UDF to calculate the median of an array
def calculate_median(array):
    return float(np.median(array)) if array else None

# Define UDF to calculate MAD
def calculate_mad(df, column_name):
    # Step 1: Compute the median
    median_col = F.expr(f"percentile_approx({column_name}, 0.5)")

    # Step 2: Compute absolute deviations from the median
    abs_deviation = F.abs(F.col(column_name) - median_col)

    # Step 3: Compute the MAD (median of absolute deviations)
    mad = F.expr(f"percentile_approx({abs_deviation}, 0.5)")

    return mad

# Register UDFs
#median_udf = udf(calculate_median, DoubleType())
#mad_udf = udf(calculate_mad, DoubleType())


# Add MAD computation with UDFs
# Add watermark before aggregation
anomalies_stream = parsed_stream \
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window(col("timestamp"), "1 minute")) \
    .agg(
        F.avg("P1_FCV01D").alias("avg_P1_FCV01D"),
        F.expr("percentile_approx(P1_FCV01D, 0.5)").alias("median_P1_FCV01D"),
        F.expr("percentile_approx(abs(P1_FCV01D - percentile_approx(P1_FCV01D, 0.5)), 0.5)").alias("mad_P1_FCV01D")
    ) \
    .withColumn(
        "anomaly_flag",
        when(
            abs(col("avg_P1_FCV01D") - col("median_P1_FCV01D")) > 3 * col("mad_P1_FCV01D"),
            lit(1)
        ).otherwise(lit(0))
    )
#anomalies = aggregated_with_mad \
    #.withColumn("anomaly_P1_FCV01D", expr("CASE WHEN abs(avg_P1_FCV01D - median_P1_FCV01D) > 3 * mad_P1_FCV01D THEN 1 ELSE 0 END"))

# Output aggregation to console
anomalies_stream.writeStream \
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

# Write to Kafka
query = anomalies_stream.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "anomaly-detection-results") \
    .option("checkpointLocation", "checkpoint/anomalies") \
    .outputMode("append") \
    .start()

# I just put this here for debug, it logs into a json what is being parsed into the Spark schema, can be commented out later:
#anomalies_stream.writeStream \
    #.format("json") \
    #.option("path", "output/anomalies") \
    #.option("checkpointLocation", "checkpoint/anomalies") \
    #.start()

# Output filtered data to console
anomalies_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()


query.awaitTermination()

spark.streams.awaitAnyTermination()







