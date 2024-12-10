import os
from pyspark.sql import SparkSession

# Environment Variables
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ['PATH'] += ';C:\\hadoop\\bin'
os.environ['PYSPARK_PYTHON'] = 'C:/Users/Asus/AppData/Local/Programs/Python/Python310/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/Asus/AppData/Local/Programs/Python/Python310/python.exe'

# Define Paths
local_dir = "C:/spark-temp"  # Use forward slashes
checkpoint_dir = "file:///C:/spark-temp/checkpoints"  # Correct URI format
event_log_dir = "file:///C:/spark-events"
jar_files = [
    "file:///C:/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
    "file:///C:/spark/jars/scala-library-2.13.12.jar"  # Ensure this exists
]

# Ensure Required Directories Exist
os.makedirs("C:/spark-temp", exist_ok=True)
os.makedirs("C:/spark-events", exist_ok=True)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamProcessor") \
    .master("local[*]") \
    .config("spark.local.dir", local_dir) \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", event_log_dir) \
    .config("spark.jars", ",".join(jar_files)) \
    .getOrCreate()

# # Verify Spark Configuration
# print(f"Spark Version: {spark.version}")
# print(f"Hadoop Version: {spark.conf.get('spark.hadoop.hadoopVersion', 'Not Set')}")





# import os

# # Environment Variables
# os.environ["HADOOP_HOME"] = "C:\\hadoop"  # Path to Hadoop installation
# os.environ['PATH'] += ';C:\\hadoop\\bin'
# os.environ['PYSPARK_PYTHON'] = 'C:/Users/Asus/AppData/Local/Programs/Python/Python310/python.exe'
# os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/Asus/AppData/Local/Programs/Python/Python310/python.exe'

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, expr, count

# # Persistent Checkpoint Directory
# checkpoint_dir = "C:/spark-temp/checkpoints"

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("KafkaStreamProcessor") \
#     .master("local[*]") \
#     .config("spark.local.dir", "/spark-temp") \
#     .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
#     .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
#     .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
#     .config("spark.hadoop.hadoopVersion", "3.3.6") \
#     .config("spark.hadoop.io.nativeio.enable", "false") \
#     .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
#     .config("spark.executor.memory", "2g") \
#     .config("spark.driver.memory", "2g") \
#     .config("spark.eventLog.enabled", "true") \
#     .config("spark.eventLog.dir", "/spark-events") \
#     .getOrCreate()

# Test environment setup
print("Testing environment setup...")
try:
    # Verify Spark version
    spark_version = spark.version
    print(f"Spark Version: {spark_version}")

    # Verify Hadoop version
    hadoop_version = spark.conf.get("spark.hadoop.hadoopVersion", "Not Set")
    print(f"Hadoop Version: {hadoop_version}")

    # Check temporary directory
    local_dir = spark.conf.get("spark.local.dir", "Not Set")
    print(f"Spark Local Directory: {local_dir}")

    # Verify checkpoint directory
    print(f"Checkpoint Directory: {checkpoint_dir}")

    # Test configuration output
    print("\nCurrent Spark Configurations:")
    for item in spark.sparkContext.getConf().getAll():
        print(f"{item[0]}: {item[1]}")

except Exception as e:
    print("Error while testing environment:", e)

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

# Parse Kafka stream
raw_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data")
parsed_stream = raw_stream \
    .withColumn("timestamp", expr(r"regexp_extract(raw_data, '\"timestamp\":\\s*\"(.*?)\"', 1)")) \
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FCV01Z", expr(r"regexp_extract(raw_data, '\"P1_FCV01Z\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FCV03D", expr(r"regexp_extract(raw_data, '\"P1_FCV03D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("x1003_24_SUM_OUT", expr(r"regexp_extract(raw_data, '\"x1003_24_SUM_OUT\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("data_type", expr(r"regexp_extract(raw_data, '\"data_type\":\\s*\"(.*?)\"', 1)")) \
    .withColumn("attack_label", expr(r"regexp_extract(raw_data, '\"attack_label\":\\s*([0-9]+)', 1)").cast("int"))

# Check the schema
parsed_stream.printSchema()

# Filter rows with valid data
parsed_stream = parsed_stream.filter(
    (col("timestamp").isNotNull()) &
    ((col("P1_FCV01D") != 0) | (col("P1_FCV01Z") != 0) | (col("P1_FCV03D") != 0) | (col("x1003_24_SUM_OUT") != 0))
)

# Train-test separation
train_stream = parsed_stream.filter(col("data_type") == "train") \
    .select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT", "data_type")

test_stream = parsed_stream.filter(col("data_type") == "test") \
    .select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT", "data_type", "attack_label")

# Debug: Output train and test streams to console
train_query = train_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

test_query = test_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

# Count rows in train and test streams
train_count_query = train_stream.groupBy().count().writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

test_count_query = test_stream.groupBy().count().writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Keep the application running until terminated
train_query.awaitTermination()
test_query.awaitTermination()
train_count_query.awaitTermination()
test_count_query.awaitTermination()
