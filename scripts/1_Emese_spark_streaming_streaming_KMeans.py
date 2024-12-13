from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import os
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
from river.cluster import KMeans
import numpy as np

if os.path.exists("streaming_kmeans_result.db"):
    os.remove("streaming_kmeans_result.db")
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
            print(f"[ERROR] Error inserting data: {e}")
        finally:
            session.close()

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamProcessor") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .getOrCreate()

# Kafka Configuration
kafka_broker = "localhost:9092"
topic_name = "hai-dataset"

# Database Configuration
DATABASE_URL = "sqlite:///streaming_kmeans_result.db"
train_schema = {
    "timestamp": String,
    "P1_FCV01D": Float,
    "P1_FCV01Z": Float,
    "P1_FCV03D": Float,
    "P1_FT01": Float,
    "x1003_24_SUM_OUT": Float,
    "data_type": String,
    "cluster": Integer
}
test_schema = {**train_schema, "attack_label": Integer}

train_db = DatabaseManager(DATABASE_URL, "train", train_schema)
test_db = DatabaseManager(DATABASE_URL, "test", test_schema)

# Read from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

raw_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data")

parsed_stream = raw_stream \
    .withColumn("timestamp", expr(r"regexp_extract(raw_data, '\"timestamp\":\\s*\"(.*?)\"', 1)").cast("string")) \
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FCV01Z", expr(r"regexp_extract(raw_data, '\"P1_FCV01Z\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FCV03D", expr(r"regexp_extract(raw_data, '\"P1_FCV03D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FT01", expr(r"regexp_extract(raw_data, '\"P1_FCV03D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("x1003_24_SUM_OUT", expr(r"regexp_extract(raw_data, '\"x1003_24_SUM_OUT\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("data_type", expr(r"regexp_extract(raw_data, '\"data_type\":\\s*\"(.*?)\"', 1)").cast("string")) \
    .withColumn("attack_label", expr(r"regexp_extract(raw_data, '\"attack_label\":\\s*([0-9]+)', 1)").cast("int"))

# Filter rows with valid data
parsed_stream = parsed_stream.filter(
    (col("timestamp").isNotNull()) &
    ((col("P1_FCV01D") != 0) | (col("P1_FCV01Z") != 0) | (col("P1_FCV03D") != 0) | (col("P1_FT01") != 0) | (col("x1003_24_SUM_OUT") != 0))
)

# Separate train and test streams
train_stream = parsed_stream.filter(col("data_type") == "train") \
    .select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "P1_FT01", "x1003_24_SUM_OUT", "data_type")

test_stream = parsed_stream.filter(col("data_type") == "test") \
    .select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "P1_FT01", "x1003_24_SUM_OUT", "data_type", "attack_label")

# Initialize River's KMeans model
kmeans_model = KMeans(n_clusters=3, halflife=0.5)

#☺train Kmeans
def process_train_batch(df, epoch_id):
    global kmeans_model  
    print(f"[DEBUG] Invoked process_train_batch for Epoch: {epoch_id}")
    try:
        batch_df = df.toPandas()

        if batch_df.empty:
            print(f"[INFO] Epoch {epoch_id}: Empty training batch, skipping.")
            return

        print(f"[DEBUG] Epoch {epoch_id}: Training batch size: {len(batch_df)}")
        print(batch_df.head())  

        #train
        features = batch_df[["P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "P1_FT01", "x1003_24_SUM_OUT"]].to_numpy()
        for row in features:
            kmeans_model.learn_one(dict(enumerate(row)))  # Update model without reassigning

        #predict
        batch_df["cluster"] = [kmeans_model.predict_one(dict(enumerate(row))) for row in features]

        #wrtie back to database
        train_db.bulk_insert(batch_df)
        print(f"[DEBUG] Epoch {epoch_id}: Train results written to database.")
    except Exception as e:
        print(f"[ERROR] Epoch {epoch_id}: Error processing train batch: {e}")

#function for testing
def process_test_batch(df, epoch_id):
    global kmeans_model  #--> we need global instance so code goes good
    print(f"[DEBUG] Invoked process_test_batch for Epoch: {epoch_id}")
    try:
        batch_df = df.toPandas()

        if batch_df.empty:
            print(f"[INFO] Epoch {epoch_id}: Empty testing batch, skipping.")
            return

        print(f"[DEBUG] Epoch {epoch_id}: Testing batch size: {len(batch_df)}")
        print(batch_df.head())  

        #prediction
        features = batch_df[["P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT"]].to_numpy()
        batch_df["cluster"] = [kmeans_model.predict_one(dict(enumerate(row))) for row in features]

        # Write test results to database
        test_db.bulk_insert(batch_df)
        print(f"[DEBUG] Epoch {epoch_id}: Test results written to database.")
    except Exception as e:
        print(f"[ERROR] Epoch {epoch_id}: Error processing test batch: {e}")

#application of functions
train_stream.writeStream \
    .foreachBatch(lambda df, epoch_id: process_train_batch(df, epoch_id)) \
    .option("checkpointLocation", "checkpoints/train_kmeans_checkpoint") \
    .start()

test_stream.writeStream \
    .foreachBatch(lambda df, epoch_id: process_test_batch(df, epoch_id)) \
    .option("checkpointLocation", "checkpoints/test_kmeans_checkpoint") \
    .start()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("[DEBUG] Terminating Spark Streams...")
finally:
    print("[DEBUG] Stopping SparkSession...")
    spark.stop()