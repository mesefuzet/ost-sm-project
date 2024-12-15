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
from pyspark.sql.types import DoubleType, TimestampType, StringType
import pandas as pd

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("StreamingEWMA") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.local.dir", "C:/spark-temp") \
    .getOrCreate()

# Set custom temporary directory
spark.conf.set("spark.local.dir", "C:/spark-temp")

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
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)").cast(DoubleType())) \
    .withColumn("P1_FCV01Z", expr(r"regexp_extract(raw_data, '\"P1_FCV01Z\":\\s*([0-9.]+)', 1)").cast(DoubleType())) \
    .withColumn("P1_FCV03D", expr(r"regexp_extract(raw_data, '\"P1_FCV03D\":\\s*([0-9.]+)', 1)").cast(DoubleType())) \
    .withColumn("x1003_24_SUM_OUT", expr(r"regexp_extract(raw_data, '\"x1003_24_SUM_OUT\":\\s*([0-9.]+)', 1)").cast(DoubleType())) \
    .withColumn("data_type", expr(r"regexp_extract(raw_data, '\"data_type\":\\s*\"(.*?)\"', 1)")) \
    .withColumn("attack_label", expr(r"regexp_extract(raw_data, '\"attack_label\":\\s*([0-9]+)', 1)").cast("int"))


# Filter rows with valid timestamps and any of the required numeric columns
parsed_stream = parsed_stream.filter(
    (col("timestamp").isNotNull()) &
    ((col("P1_FCV01D").isNotNull()) |
     (col("P1_FCV01Z").isNotNull()) |
     (col("P1_FCV03D").isNotNull()) |
     (col("x1003_24_SUM_OUT").isNotNull()))
)
parsed_stream = parsed_stream.filter(
    col("timestamp").isNotNull() & col("P1_FCV01D").isNotNull()
)

# EWMA Parameters
alpha = 0.2  # Smoothing factor
global_ewma_state = {"P1_FCV01D": None}  # Holds the running EWMA value across batches


def process_ewma_batch(df, epoch_id):
    global global_ewma_state

    print(f"[DEBUG] Processing EWMA batch, Epoch: {epoch_id}")

    try:
        # Convert Spark DataFrame to Pandas
        batch_df = df.toPandas()
        if batch_df.empty:
            print(f"[INFO] Epoch {epoch_id}: Empty batch, skipping.")
            return

        # Initialize EWMA column
        batch_df["EWMA"] = None

        # Calculate EWMA incrementally
        for i, row in batch_df.iterrows():
            current_value = row["P1_FCV01D"]
            if pd.isna(current_value):
                continue

            if global_ewma_state["P1_FCV01D"] is None:
                # Initialize the first EWMA value
                global_ewma_state["P1_FCV01D"] = current_value
            else:
                # Update EWMA using exponential smoothing
                global_ewma_state["P1_FCV01D"] = (
                    alpha * current_value + (1 - alpha) * global_ewma_state["P1_FCV01D"]
                )

            # Assign the calculated EWMA to the row
            batch_df.at[i, "EWMA"] = global_ewma_state["P1_FCV01D"]

        # Log results for debugging
        print(f"[DEBUG] EWMA results for epoch {epoch_id}:\n{batch_df[['timestamp', 'P1_FCV01D', 'EWMA']]}")

    except Exception as e:
        print(f"[ERROR] Epoch {epoch_id}: Error processing batch: {e}")


# Write stream to process EWMA in each batch
query = parsed_stream.writeStream \
    .foreachBatch(process_ewma_batch) \
    .option("checkpointLocation", "checkpoints/ewma_checkpoint") \
    .start()

query.awaitTermination()