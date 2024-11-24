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
    .add("x1003_24_SUM_OUT", DoubleType(), True) \
    .add("data_type", StringType(), True) #---> consider 

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
missing_counts = parsed_stream.select(
    [(count(when(isnan(c) | col(c).isNull(), c))).alias(c) for c in ["x1003_24_SUM_OUT"]]
)

#debug for missing values
missing_counts_query = missing_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()
 
train_stats = train_data.select(
    mean(col("x1003_24_SUM_OUT")).alias("train_mean"),
    stddev(col("x1003_24_SUM_OUT")).alias("train_stddev")
)
test_stats = test_data.select(
    mean(col("x1003_24_SUM_OUT")).alias("test_mean"),
    stddev(col("x1003_24_SUM_OUT")).alias("test_stddev")
)
# Write statistics to the console (debugging)
train_stats_query = train_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

test_stats_query = test_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()


# 3. Normalize the Data (MinMax Scaling)
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=["x1003_24_SUM_OUT"], outputCol="features")
train_features = assembler.transform(train_data)
test_features = assembler.transform(test_data)

# Apply MinMaxScaler
scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")

train_scaler_model = scaler.fit(train_features)
scaled_train_data = train_scaler_model.transform(train_features)

test_scaler_model = scaler.fit(test_features)
scaled_test_data = test_scaler_model.transform(test_features)

# Write the Results to the Console (For Debugging)
scaled_train_query = scaled_train_data.select("timestamp", "scaled_features").writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

scaled_test_query = scaled_test_data.select("timestamp", "scaled_features").writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()
# Wait for Termination
spark.streams.awaitAnyTermination()

# Test comment