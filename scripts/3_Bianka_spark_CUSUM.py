from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, abs
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamCUSUM") \
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

# Predefined mean and stddev for each column
static_stats = {
    "P1_FCV01D": {"mean": 50.0, "stddev": 10.0},
    "P1_FCV01Z": {"mean": 60.0, "stddev": 15.0},
    "P1_FCV03D": {"mean": 70.0, "stddev": 20.0},
    "x1003_24_SUM_OUT": {"mean": 80.0, "stddev": 25.0},
}

# CUSUM threshold
cusum_threshold = 3.0

# Apply CUSUM logic
def apply_cusum(df, column_name, stats):
    mean_val = stats["mean"]
    stddev_val = stats["stddev"]
    
    return df.withColumn(
        f"CUSUM_{column_name}",
        (col(column_name) - lit(mean_val)) / lit(stddev_val)
    ).withColumn(
        f"Anomaly_{column_name}",
        (F.abs(col(f"CUSUM_{column_name}")) > cusum_threshold).cast("int")
    )

# Apply CUSUM for each column
for column in static_stats.keys():
    parsed_stream = apply_cusum(parsed_stream, column, static_stats[column])

# Write CUSUM results to the console
query = parsed_stream.select(
    "timestamp", "P1_FCV01D", "CUSUM_P1_FCV01D", "Anomaly_P1_FCV01D",
    "P1_FCV01Z", "CUSUM_P1_FCV01Z", "Anomaly_P1_FCV01Z",
    "P1_FCV03D", "CUSUM_P1_FCV03D", "Anomaly_P1_FCV03D",
    "x1003_24_SUM_OUT", "CUSUM_x1003_24_SUM_OUT", "Anomaly_x1003_24_SUM_OUT",
    "data_type", "attack_label"
).writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Await termination
query.awaitTermination()
