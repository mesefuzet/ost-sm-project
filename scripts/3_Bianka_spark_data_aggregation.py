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
### Bia Data Aggregation (streaming Process) ####
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, window, avg, lit, regexp_replace, to_timestamp

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

# Parse semicolon-delimited data
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data") \
    .withColumn("fields", split(col("raw_data"), ";")) \
    .select(
        col("fields").getItem(0).alias("timestamp"),
        regexp_replace(col("fields").getItem(1), "\\.", ",").cast("double").alias("P1_FCV01D"),
        regexp_replace(col("fields").getItem(2), "\\.", ",").cast("double").alias("P1_FCV01Z"),
        regexp_replace(col("fields").getItem(3), "\\.", ",").cast("double").alias("P1_FCV03D"),
        col("fields").getItem(-1).alias("data_type")  # Assuming 'data_type' is the last field
    )

# Convert timestamp to proper format
parsed_stream = parsed_stream.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy.MM.dd HH:mm"))

# Perform Aggregation Using Sliding Window
aggregated_stream = parsed_stream \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("data_type")
    ).agg(
        avg("P1_FCV01D").alias("avg_P1_FCV01D"),
        avg("P1_FCV01Z").alias("avg_P1_FCV01Z"),
        avg("P1_FCV03D").alias("avg_P1_FCV03D")
    )

# Output Aggregated Data to Console
query = aggregated_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()





