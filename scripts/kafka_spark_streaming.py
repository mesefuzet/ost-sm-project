from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, count, isnan
from pyspark.sql.types import StructType, StringType, DoubleType
import json

## first draft code

#init session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

#config Kafka
kafka_broker = "localhost:9092"
topic_name = "hai-dataset"

#Schema definition
schema = StructType() \
    .add("timestamp", StringType(), True) \
    .add("sensor_id", StringType(), True) \
    .add("value", DoubleType(), True)

#read step
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON and Parse the Data
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_data") \
    .selectExpr("json_data") \
    .rdd.map(lambda row: json.loads(row.json_data)) \
    .toDF(schema)

train_data = parsed_stream.filter(col("data_type") == "train")
test_data = parsed_stream.filter(col("data_type") == "test")
# ===============================================
### EMESE 2 STREAMING PROCESSES ####
# Data Exploration Tasks
# 1. Detect Missing Values & Basic Statistics
missing_counts = parsed_stream.select([(count(when(isnan(c) | col(c).isNull(), c))).alias(c) for c in parsed_stream.columns])
 
stats = parsed_stream.select(mean(col("value")).alias("mean"),
                             stddev(col("value")).alias("stddev"))

# 3. Normalize the Data (MinMax Scaling)
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=["value"], outputCol="features")
feature_data = assembler.transform(parsed_stream)

scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")
scaler_model = scaler.fit(feature_data)
scaled_data = scaler_model.transform(feature_data)

# Write the Results to the Console (For Debugging)
query = scaled_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for Termination
query.awaitTermination()