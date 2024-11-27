from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, count, isnan, when, lit
from pyspark.sql.types import StructType, StringType, DoubleType
import json
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_json

## first draft code

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
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

train_data = parsed_stream.filter(col("data_type") == "train")
test_data = parsed_stream.filter(col("data_type") == "test")

train_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


# ===================================================
### Bia Time Based Filtering (streaming Process) ####

from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime, timedelta

# Expected Workflow:
# The script tracks the time of the last processed message using a variable (last_processed_time). 
# Each incoming message is checked against this timestamp, 
# and only those that meet the time interval condition (filter_interval) are forwarded to the new Kafka topic.

# Kafka configuration
input_topic = 'hai-dataset'
output_topic = 'filtered-data'
bootstrap_servers = 'localhost:9092'
filter_interval = 5  # Process only every N seconds

# Initialize Kafka consumer and producer
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='time-filtering-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

last_processed_time = datetime.min

print(f"Starting time-based filtering with a {filter_interval}-second interval.")

try:
    for message in consumer:
        record = message.value
        current_time = datetime.now()
        
        # Process message only if the interval has passed
        if (current_time - last_processed_time).total_seconds() >= filter_interval:
            # Forward the message to the new Kafka topic
            producer.send(output_topic, value=record)
            print(f"Filtered message: {record}")
            
            last_processed_time = current_time

except KeyboardInterrupt:
    print("Time-based filtering stopped by user.")
finally:
    consumer.close()
    producer.close()
    print("Kafka consumer and producer closed.")

    # Github name check2
