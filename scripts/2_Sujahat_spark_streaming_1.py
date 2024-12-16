from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, expr, lit, when
from pyspark.sql.types import StringType, DoubleType, IntegerType
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import os
 
if os.path.exists("anomaly_detections.db"):
    os.remove("anomaly_detections.db")
    print("[DEBUG] Database file deleted.")
else:
    print("[DEBUG] No existing database file to delete.")
 
# SQLAlchemy DatabaseManager class
Base = declarative_base()
 
class DatabaseManager:
    def __init__(self, db_url: str, table_name: str, schema: dict):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.table_name = table_name
 
        # Dynamically create table class
        columns = {}
        for col_name, col_type in schema.items():
            if col_type == String:
                columns[col_name] = Column(String)
            elif col_type == Float:
                columns[col_name] = Column(Float)
            elif col_type == Integer:
                columns[col_name] = Column(Integer)
            else:
                raise ValueError(f"Unsupported column type: {col_type}")
        columns["id"] = Column(Integer, primary_key=True, autoincrement=True)
        columns["__tablename__"] = table_name
        self.model = type(table_name, (Base,), columns)
 
        # Create table
        Base.metadata.create_all(self.engine)
 
    def bulk_insert(self, df: pd.DataFrame):
        """Insert multiple records into the database."""
        session = self.Session()
        try:
            records = [self.model(**row.to_dict()) for _, row in df.iterrows()]
            session.bulk_save_objects(records)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error inserting data: {e}")
        finally:
            session.close()
 
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamProcessor") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///C:/tmp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
 
# Kafka Configuration
kafka_broker = "localhost:9092"
topic_name = "hai-dataset"
 
# Database Configuration
DATABASE_URL = "sqlite:///anomaly_detections.db"
train_schema = {"timestamp": String, "P1_FCV01D": Float, "P1_FCV01Z": Float, "P1_FCV03D": Float, "x1003_24_SUM_OUT": Float, "data_type": String}
test_schema = {**train_schema, "attack_label": Integer}
 
# Database manager configuration
train_db = DatabaseManager(DATABASE_URL, "train", train_schema)
test_db = DatabaseManager(DATABASE_URL, "test", test_schema)
 
# Batch query to compute thresholds BEFORE streaming
batch_data = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()
 
batch_parsed = batch_data.selectExpr("CAST(value AS STRING) as raw_data") \
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .filter(col("P1_FCV01D").isNotNull())
 
# Compute thresholds in a batch mode
train_stats = batch_parsed.agg(
    mean("P1_FCV01D").alias("train_mean"),
    stddev("P1_FCV01D").alias("train_stddev")
).collect()[0]
 
mean_val = train_stats["train_mean"]
stddev_val = train_stats["train_stddev"]
threshold_multiplier = 1
upper_threshold = mean_val + threshold_multiplier * stddev_val
lower_threshold = mean_val - threshold_multiplier * stddev_val
 
print(f"[DEBUG] Mean value: {mean_val}, St.dev value: {stddev_val}")
print(f"[DEBUG] Upper Threshold: {upper_threshold}, Lower Threshold: {lower_threshold}")
 
# Streaming pipeline
kafka_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "latest") \
    .load()
 
raw_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data")
 
parsed_stream = raw_stream \
    .withColumn("timestamp", expr(r"regexp_extract(raw_data, '\"timestamp\":\\s*\"(.*?)\"', 1)").cast("string")) \
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FCV01Z", expr(r"regexp_extract(raw_data, '\"P1_FCV01Z\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FCV03D", expr(r"regexp_extract(raw_data, '\"P1_FCV03D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("x1003_24_SUM_OUT", expr(r"regexp_extract(raw_data, '\"x1003_24_SUM_OUT\":\\s*([0-9.]+)', 1)").cast("double")) \
    .filter(col("P1_FCV01D").isNotNull())
 
 
# Apply anomaly detection logic with precomputed thresholds
anomaly_flagged_stream = parsed_stream.withColumn(
    "anomaly_flag",
    when((col("P1_FCV01D") > lit(upper_threshold)) |
         (col("P1_FCV01D") < lit(lower_threshold)), lit(1)).otherwise(lit(0))
).select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT", "anomaly_flag")
 
 
# Save anomalies into the database
anomalies_schema = {
    "timestamp": String,
    "P1_FCV01D": Float,
    "P1_FCV01Z": Float,
    "P1_FCV03D": Float,
    "x1003_24_SUM_OUT": Float,
    "anomaly_flag": Integer
}
anomalies_db = DatabaseManager(DATABASE_URL, "anomalies", anomalies_schema)
 
def write_anomalies_to_database(df, epoch_id):
    try:
        pdf = df.toPandas()
        anomalies_db.bulk_insert(pdf)
        print(f"Anomalies for epoch {epoch_id} written to database.")
    except Exception as e:
        print(f"Error writing anomalies: {e}")
 
# Write anomalies to database
query = anomaly_flagged_stream.writeStream \
    .foreachBatch(lambda df, epoch_id: write_anomalies_to_database(df, epoch_id)) \
    .outputMode("append") \
    .start()
 
spark.streams.awaitAnyTermination()