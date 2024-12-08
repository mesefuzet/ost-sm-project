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


# # Set up checkpoint location
# checkpoint_location = os.path.join(os.getcwd(), "spark_checkpoints")
# os.makedirs(checkpoint_location, exist_ok=True)

# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("KafkaSparkStreaming") \
#     .master("local[*]") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
#     .config("spark.sql.streaming.microBatchDurationMs", "5000") \
#     .config("spark.sql.shuffle.partitions", "4") \
#     .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
#     .getOrCreate()

# # Kafka Configuration
# kafka_broker = "localhost:9092"
# topic_name = "hai-dataset"

# # Define Schema
# train_schema = StructType() \
#     .add("timestamp", StringType(), True) \
#     .add("data_type", StringType(), True) \
#     .add("P1_FCV01D", DoubleType(), True) \
#     .add("P1_FCV01Z", DoubleType(), True) \
#     .add("P1_FCV03D", DoubleType(), True) \
#     .add("P1_FT01", DoubleType(), True) \
#     .add("P1_FT01Z", DoubleType(), True) \
#     .add("P1_FT02", DoubleType(), True) \
#     .add("P1_FT02Z", DoubleType(), True) \
#     .add("P1_FT03", DoubleType(), True) \
#     .add("P1_FT03Z", DoubleType(), True) \
#     .add("P1_LCV01D", DoubleType(), True) \
#     .add("P1_LCV01Z", DoubleType(), True) \
#     .add("P1_LIT01", DoubleType(), True) \
#     .add("x1003_24_SUM_OUT", DoubleType(), True) \
#     .add("attack_label", StringType(), True)  # Include attack_label for test data

# # Read from Kafka
# kafka_stream = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_broker) \
#     .option("subscribe", topic_name) \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Deserialize and Parse the Data
# parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_data") \
#     .select(from_json(col("json_data"), train_schema).alias("data"))

# # Filter Train and Test Data
# train_data = parsed_stream.filter(col("data.data_type") == "train").select("data.*")
# test_data = parsed_stream.filter(col("data.data_type") == "test").select("data.*")

# # Start Writing Train Data Stream
# train_data_query = train_data.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("checkpointLocation", os.path.join(checkpoint_location, "train_data")) \
#     .start()

# # Start Writing Test Data Stream
# test_data_query = test_data.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("checkpointLocation", os.path.join(checkpoint_location, "test_data")) \
#     .start()

# # Await Termination for Streaming Queries
# train_data_query.awaitTermination()
# test_data_query.awaitTermination()


# ================================================
#Working Spark solution
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, regexp_replace, expr

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






