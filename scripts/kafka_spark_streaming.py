from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, count, isnan, when, lit
from pyspark.sql.types import StructType, StringType, DoubleType
import json
from pyspark.sql.functions import udf

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


# Test comment

# ===============================================
### Sujahat 2 STREAMING PROCESSES ####

# 1. Threshold-Based Anomaly Flags
train_stats = train_data.select(
    *[mean(col(c)).alias(f"{c}_mean") for c in schema.fieldNames() if c != "timestamp" and c != "data_type"],
    *[stddev(col(c)).alias(f"{c}_stddev") for c in schema.fieldNames() if c != "timestamp" and c != "data_type"]
).collect()

# Prepare thresholds for each column
thresholds = {f"{c}": {"mean": train_stats[0][f"{c}_mean"], "stddev": train_stats[0][f"{c}_stddev"]}
              for c in schema.fieldNames() if c != "timestamp" and c != "data_type"}

# Add anomaly flags dynamically for all columns
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
spark.streams.awaitAnyTermination()

# Test comment1