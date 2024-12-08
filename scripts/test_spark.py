from pyspark.sql import SparkSession

# how to run this: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 test_spark.py
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaTest") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Read from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hai-dataset") \
    .option("startingOffsets", "earliest") \
    .load()

# Write to Console
#query = kafka_stream.selectExpr("CAST(value AS STRING)").writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .start()

query = kafka_stream.select("x1003_24_SUM_OUT").writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", "10") \
    .start()

query.awaitTermination()