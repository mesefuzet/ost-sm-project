#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, corr
from pyspark.sql.types import StructType, StringType, DoubleType
import os

# Get checkpoint location
checkpoint_location = os.path.join(os.getcwd(), "spark_checkpoints")
os.makedirs(checkpoint_location, exist_ok=True)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingCorrelation") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .config("spark.sql.streaming.microBatchDurationMs", "5000") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# Kafka configuration
kafka_broker = "localhost:9092"
topic_name = "hai-dataset"

# Schema definition
train_schema = StructType() \
    .add("timestamp", StringType(), True) \
    .add("data_type", StringType(), True) \
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

# Read Kafka stream
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON and parse the data
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), train_schema).alias("data"))

# Select relevant columns and filter train data
train_data = parsed_stream.select("data.*").filter(col("data_type") == "train")

# Perform correlation calculations
# Pairwise correlation for selected columns
columns_to_correlate = ["P1_FCV01D", "P1_FCV01Z", "P1_FT01", "P1_FT01Z"]
correlations = []

for col1 in columns_to_correlate:
    for col2 in columns_to_correlate:
        if col1 != col2:  # Avoid self-correlation
            correlation = train_data.stat.corr(col1, col2)
            correlations.append((col1, col2, correlation))

# Convert correlation results to a DataFrame
correlation_df = spark.createDataFrame(correlations, ["Feature1", "Feature2", "Correlation"])

# Write correlations to the console
query = correlation_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

