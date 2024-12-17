from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StringType, FloatType
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker
import os
import pandas as pd
import numpy as np
from sklearn.neighbors import NearestNeighbors
 
# Remove old database file
if os.path.exists("lfr_anomaly_detections.db"):
    os.remove("lfr_anomaly_detections.db")
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
        columns = {col_name: Column(Float if col_type == FloatType else String) for col_name, col_type in schema.items()}
        columns["id"] = Column(Integer, primary_key=True, autoincrement=True)
        columns["__tablename__"] = table_name
        self.model = type(table_name, (Base,), columns)
 
        # Create table
        Base.metadata.create_all(self.engine)
 
    def bulk_insert(self, df):
        session = self.Session()
        try:
            records = [self.model(**row) for row in df]
            session.bulk_save_objects(records)
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"Error inserting data: {e}")
        finally:
            session.close()
 
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaLFRProcessor") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///C:/tmp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
 
# Kafka Configuration
kafka_broker = "localhost:9092"
topic_name = "hai-dataset"
 
# Database Configuration
DATABASE_URL = "sqlite:///lfr_anomaly_detections.db"
anomalies_schema = {
    "timestamp": StringType(),
    "P1_FCV01D": FloatType(),
    "P1_FCV01Z": FloatType(),
    "P1_FCV03D": FloatType(),
    "lfr_score": FloatType()
}
anomalies_db = DatabaseManager(DATABASE_URL, "lfr_anomalies", anomalies_schema)
 
# Read from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 500) \
    .load()
 
raw_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data")
 
parsed_stream = raw_stream \
    .withColumn("timestamp", expr(r"regexp_extract(raw_data, '\"timestamp\":\\s*\"(.*?)\"', 1)").cast("string")) \
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FCV01Z", expr(r"regexp_extract(raw_data, '\"P1_FCV01Z\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FCV03D", expr(r"regexp_extract(raw_data, '\"P1_FCV03D\":\\s*([0-9.]+)', 1)").cast("double"))
 
parsed_stream = parsed_stream.filter(
    col("timestamp").isNotNull() &
    ((col("P1_FCV01D") != 0) | (col("P1_FCV01Z") != 0) | (col("P1_FCV03D") != 0))
)
 
# Batch Processing
def process_batch_lfr(df, epoch_id, k=5):
    try:
        print("Processing batch...")
        pdf = df.toPandas().dropna()
 
        # Perform LFR computation
        print("Calculate LFR...")
        data = pdf[["P1_FCV01D", "P1_FCV01Z", "P1_FCV03D"]].values
        neighbors = NearestNeighbors(n_neighbors=k + 1).fit(data)
        distances, _ = neighbors.kneighbors(data)
        avg_distances = distances[:, 1:].mean(axis=1)  #we need to exclide the self distance
        pdf["lfr_score"] = avg_distances / np.max(avg_distances)  # Normalize LFR scores
        print("LFR calculation successfull")
 
        # Drop unecessary columns and keep only those in the database schema
        pdf = pdf[["timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "lfr_score"]]
 
        # Insert into the database
        anomalies_db.bulk_insert(pdf.to_dict(orient="records"))
        print(f"[DEBUG] LFR Anomalies for epoch {epoch_id} written to database.")
    except Exception as e:
        print(f"Error processing batch: {e}")
 
# Write LFR results to the database
query = parsed_stream.writeStream \
    .foreachBatch(lambda df, epoch_id: process_batch_lfr(df, epoch_id)) \
    .outputMode("append") \
    .start()
 
spark.streams.awaitAnyTermination()