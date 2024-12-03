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
    .option("failOnDataLoss", "false") \
    .load()

#I just put this here for debug, it logs into a json what Spark gets from Kafka, can be commented out later:
kafka_stream.selectExpr("CAST(value AS STRING)").writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "debug/raw_kafka_output") \
    .option("checkpointLocation", "debug/raw_kafka_checkpoint") \
    .start()

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

#check the schema
parsed_stream.printSchema()

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


# Debugging: Save parsed output to a file
selected_columns.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "debug/parsed_output") \
    .option("checkpointLocation", "debug/parsed_checkpoint") \
    .start()

train_stream.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "debug/train_output") \
    .option("checkpointLocation", "debug/train_checkpoint") \
    .start()

test_stream.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "debug/test_output") \
    .option("checkpointLocation", "debug/test_checkpoint") \
    .start()
print("---------TRAIN DATA----------------")
train_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("---------TEST DATA----------------")
test_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

spark.streams.awaitAnyTermination()
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