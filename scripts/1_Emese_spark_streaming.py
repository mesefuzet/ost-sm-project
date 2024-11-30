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


## first draft code
#get checkpoint location
checkpoint_location = os.path.join(os.getcwd(), "spark_checkpoints")
os.makedirs(checkpoint_location, exist_ok=True)


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

test_schema = StructType(train_schema.fields + [
    StructField("attack_label", StringType(), True)
])


#read step
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON and Parse the Data
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), train_schema).alias("train_data"), 
            from_json(col("json_data"), test_schema).alias("test_data"))

train_data = parsed_stream.select("train_data.*").filter(col("data_type") == "train")
test_data = parsed_stream.select("test_data.*").filter(col("data_type") == "test")

#train_data.writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .start()

# Start writing train data stream with checkpointing
train_data_query = train_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", os.path.join(checkpoint_location, "train_data")) \
    .start()

# Start writing test data stream with checkpointing
test_data_query = test_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", os.path.join(checkpoint_location, "test_data")) \
    .start()


# ===============================================
### EMESE 2 STREAMING PROCESSES ####
# Data Exploration Tasks
# 1. Detect Missing Values & Basic Statistics

print("1. PROCESS: DETECTION OF MISSING VALUES & MAIN STATISTICS")
missing_counts_train = train_data.select(
    [(count(when(isnan(c) | col(c).isNull(), c))).alias(c) for c in train_data.columns]
)

missing_counts_test = test_data.select(
    [(count(when(isnan(c) | col(c).isNull(), c))).alias(c) for c in test_data.columns]
)

#debug for missing values
missing_counts_train_query = missing_counts_train.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

missing_counts_test_query = missing_counts_test.writeStream \
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

print("2. PROCESS: DATA NORMALIZATION")
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
train_data_query.awaitTermination()
test_data_query.awaitTermination()
missing_counts_train_query.awaitTermination()
missing_counts_test_query.awaitTermination()
train_stats_query.awaitTermination()
test_stats_query.awaitTermination()