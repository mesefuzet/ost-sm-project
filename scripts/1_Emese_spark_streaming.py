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


## first draft code
#get checkpoint location
# checkpoint_location = os.path.join(os.getcwd(), "spark_checkpoints")
# os.makedirs(checkpoint_location, exist_ok=True)


# #init session
# spark = SparkSession.builder \
#     .appName("KafkaSparkStreaming") \
#     .master("local[*]") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
#     .config("spark.sql.streaming.microBatchDurationMs", "5000") \
#     .config("spark.sql.shuffle.partitions", "4") \
#     .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
#     .getOrCreate()

# #config Kafka
# kafka_broker = "localhost:9092"
# topic_name = "hai-dataset"

# #Schema definition
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
#     .add("x1003_24_SUM_OUT", DoubleType(), True)

# test_schema = StructType(train_schema.fields + [
#     StructField("attack_label", StringType(), True)
# ])


# #read step
# kafka_stream = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_broker) \
#     .option("subscribe", topic_name) \
#     .option("startingOffsets", "earliest") \
#     .load()


# print("DEBUG: Printing raw Kafka messages...")
# kafka_stream.selectExpr("CAST(value AS STRING)").writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# kafka_stream.selectExpr("CAST(value AS STRING)").writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "debug/raw_kafka_output") \
#     .option("checkpointLocation", "debug/raw_kafka_checkpoint") \
#     .start()


# # Deserialize JSON and Parse the Data
# #parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_data") \
# #    .select(from_json(col("json_data"), train_schema).alias("train_data"), 
# #            from_json(col("json_data"), test_schema).alias("test_data"))

# train_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_data") \
#     .filter(col("json_data").contains('"data_type":"train"')) \
#     .select(from_json(col("json_data"), train_schema).alias("train_data")) \
#     .select("train_data.*")  # Expand the train_data column


# test_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_data") \
#     .filter(col("json_data").contains('"data_type":"test"')) \
#     .select(from_json(col("json_data"), test_schema).alias("test_data")) \
#     .select("test_data.*")

# print("DEBUG--------------------------")

# # Write Parsed Data to JSON for Inspection
# train_stream.writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "debug/parsed_output") \
#     .option("checkpointLocation", "debug/parsed_checkpoint") \
#     .start()

# test_stream.writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "debug/parsed_output") \
#     .option("checkpointLocation", "debug/parsed_checkpoint") \
#     .start()

# print("Schema of Train Data:")
# train_stream.printSchema()

# print("Schema of Test Data:")
# test_stream.printSchema()



# #train_data = deserialized_stream.filter(col("train_data.data_type") == "train").select("train_data.*")
# #test_data = deserialized_stream.filter(col("test_data.data_type") == "test").select("test_data.*")



# print("DEBUG: Verifying TRAIN data stream...")
# train_stream.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# test_stream.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# print("--------")



# #train_data.writeStream \
# #    .outputMode("append") \
# #    .format("console") \
# #    .start()

# # Start writing train data stream with checkpointing
# train_data_query = train_stream.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("checkpointLocation", os.path.join(checkpoint_location, "train_data")) \
#     .start()

# # Start writing test data stream with checkpointing
# test_data_query = test_stream.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("checkpointLocation", os.path.join(checkpoint_location, "test_data")) \
#     .start()


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