from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lag, abs, lit, expr, avg, to_timestamp, window, percentile_approx
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
import time

#--------------------------------------------------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, expr, to_timestamp, window, percentile_approx
import pyspark.sql.functions as F
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamProcessorWithMAD") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Kafka Configuration
kafka_broker = "localhost:9092"
topic_name = "hai-dataset"

# Read and Parse Stream
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data") \
    .withColumn("timestamp", expr(r"regexp_extract(raw_data, '\"timestamp\":\\s*\"(.*?)\"', 1)")) \
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FCV01Z", expr(r"regexp_extract(raw_data, '\"P1_FCV01Z\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FCV03D", expr(r"regexp_extract(raw_data, '\"P1_FCV03D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("x1003_24_SUM_OUT", expr(r"regexp_extract(raw_data, '\"x1003_24_SUM_OUT\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("data_type", expr(r"regexp_extract(raw_data, '\"data_type\":\\s*\"(.*?)\"', 1)")) \
    .withColumn("attack_label", expr(r"regexp_extract(raw_data, '\"attack_label\":\\s*([0-9]+)', 1)").cast("int")) \
    .filter(col("timestamp").isNotNull()) \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy.MM.dd HH:mm"))

# Step 1: Compute Median
median_stream = parsed_stream.withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "1 minute")).agg(
        F.expr("percentile_approx(P1_FCV01D, 0.5)").alias("median_P1_FCV01D"),
        F.expr("percentile_approx(P1_FCV01Z, 0.5)").alias("median_P1_FCV01Z"),
        F.expr("percentile_approx(P1_FCV03D, 0.5)").alias("median_P1_FCV03D"),
        F.expr("percentile_approx(x1003_24_SUM_OUT, 0.5)").alias("median_x1003_24_SUM_OUT")
    )

median_query = median_stream.writeStream \
    .queryName("median_query") \
    .outputMode("update") \
    .format("memory") \
    .start()

time.sleep(30)  # Ensure `median_query` has output

medians = spark.sql("SELECT * FROM median_query")

# Step 2: Compute MAD
mad_stream = parsed_stream.join(
    medians,
    parsed_stream["timestamp"].between(
        medians["window.start"],
        medians["window.end"]
    ),
    "left"
).withColumn(
    "abs_dev_P1_FCV01D", abs(col("P1_FCV01D") - col("median_P1_FCV01D"))
).withColumn(
    "abs_dev_P1_FCV01Z", abs(col("P1_FCV01Z") - col("median_P1_FCV01Z"))
).withColumn(
    "abs_dev_P1_FCV03D", abs(col("P1_FCV03D") - col("median_P1_FCV03D"))
).withColumn(
    "abs_dev_x1003_24_SUM_OUT", abs(col("x1003_24_SUM_OUT") - col("median_x1003_24_SUM_OUT"))
)

mad_result = mad_stream.groupBy(window(col("timestamp"), "1 minute")).agg(
    F.expr("percentile_approx(abs_dev_P1_FCV01D, 0.5)").alias("MAD_P1_FCV01D"),
    F.expr("percentile_approx(abs_dev_P1_FCV01Z, 0.5)").alias("MAD_P1_FCV01Z"),
    F.expr("percentile_approx(abs_dev_P1_FCV03D, 0.5)").alias("MAD_P1_FCV03D"),
    F.expr("percentile_approx(abs_dev_x1003_24_SUM_OUT, 0.5)").alias("MAD_x1003_24_SUM_OUT")
)

query = mad_result.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
