from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, count, isnan, when, lit
from pyspark.sql.types import StructType, StringType, DoubleType
import json
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_json

## first draft code

#init session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

#config Kafka
kafka_broker = "localhost:9092"
topic_name = "hai-dataset"

#Schema definition
schema = StructType() \
    .add("timestamp", StringType(), True) \
    .add("data_type", StringType(), True)\
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

#read step
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON and Parse the Data
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

train_data = parsed_stream.filter(col("data_type") == "train")
test_data = parsed_stream.filter(col("data_type") == "test")

train_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

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