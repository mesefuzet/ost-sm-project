from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lag, abs, lit, expr, avg, to_timestamp, window, percentile_approx
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
import time
from pyspark.sql.functions import udf
import numpy as np
from pyspark.sql.types import DoubleType
#--------------------------------------------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, expr
from pyspark.sql.window import Window
import numpy as np
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StreamingEWMA") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
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

raw_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data")

# Parse the stream
parsed_stream = raw_stream \
    .withColumn("timestamp", expr(r"regexp_extract(raw_data, '\"timestamp\":\\s*\"(.*?)\"', 1)")) \
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("data_type", expr(r"regexp_extract(raw_data, '\"data_type\":\\s*\"(.*?)\"', 1)")) \
    .filter(col("timestamp").isNotNull()) \
    .withColumn("timestamp", expr("to_timestamp(timestamp, 'yyyy.MM.dd HH:mm')"))

# Define Smoothing Factor
alpha = 0.2

# Function to calculate EWMA
def ewma_update(new_value, state):
    if state is None:
        # Initialize state
        return new_value
    else:
        # Apply EWMA formula
        return alpha * new_value + (1 - alpha) * state

# Define a state update function
def update_ewma(batch_df, batch_id):
    global ewma_state
    ewma_updates = []
    for row in batch_df.collect():
        # Extract value and current state
        value = row["P1_FCV01D"]
        prev_ewma = ewma_state.get("P1_FCV01D", None)
        
        # Compute new EWMA
        new_ewma = ewma_update(value, prev_ewma)
        ewma_updates.append((row["timestamp"], value, new_ewma))
        
        # Update state
        ewma_state["P1_FCV01D"] = new_ewma

    # Print the updates for debugging
    print(ewma_updates)

# Initialize state
ewma_state = {}

# Select specific columns (adjust indices as per your schema)
# selected_columns = parsed_stream.select(
#     col("timestamp"),
#     col("P1_FCV01D"),
#     col("attack_label")
# )

# Process the parsed stream using foreachBatch
query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()