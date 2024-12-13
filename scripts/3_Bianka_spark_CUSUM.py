from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_timestamp, avg, stddev, lit
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F

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
data_stream = raw_stream \
    .withColumn("timestamp", expr(r"regexp_extract(raw_data, '\"timestamp\":\\s*\"(.*?)\"', 1)")) \
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)").cast(DoubleType())) \
    .withColumn("P1_FCV01Z", expr(r"regexp_extract(raw_data, '\"P1_FCV01Z\":\\s*([0-9.]+)', 1)").cast(DoubleType())) \
    .withColumn("P1_FCV03D", expr(r"regexp_extract(raw_data, '\"P1_FCV03D\":\\s*([0-9.]+)', 1)").cast(DoubleType())) \
    .withColumn("x1003_24_SUM_OUT", expr(r"regexp_extract(raw_data, '\"x1003_24_SUM_OUT\":\\s*([0-9.]+)', 1)").cast(DoubleType()))

# Precomputed static mean and stddev for each column (replace with actual values)
static_stats = {
    "mean_P1_FCV01D": 50.0, "std_P1_FCV01D": 10.0,
    "mean_P1_FCV01Z": 60.0, "std_P1_FCV01Z": 15.0,
    "mean_P1_FCV03D": 70.0, "std_P1_FCV03D": 20.0,
    "mean_x1003_24_SUM_OUT": 80.0, "std_x1003_24_SUM_OUT": 25.0
}

# Add mean and stddev columns to the DataFrame
for key, value in static_stats.items():
    data_stream = data_stream.withColumn(key, lit(value))

# Define CUSUM thresholds
cusum_threshold = 3.0

# Apply CUSUM logic
def compute_cusum(df, column_name, threshold):
    """
    Compute CUSUM anomaly detection for a given column in the DataFrame.
    """
    mean_col = f"mean_{column_name}"
    std_col = f"std_{column_name}"
    
    df = df.withColumn(
        f"CUSUM_{column_name}",
        (col(column_name) - col(mean_col)) / col(std_col)
    ).withColumn(
        f"Anomaly_{column_name}",
        (F.abs(col(f"CUSUM_{column_name}")) > threshold).cast("int")
    )
    return df

columns_to_check = ["P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT"]
for column in columns_to_check:
    data_stream = compute_cusum(data_stream, column, cusum_threshold)

# Write anomalies to console
query = data_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
