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
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker
import os
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

# Anomaly detection threshold
anomaly_threshold = 5  # Set this based on your data characteristics

# Define DatabaseManager
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

# Database setup
db_path = "EWMA.db"
if os.path.exists(db_path):
    os.remove(db_path)
    print("[DEBUG] Database file deleted.")
else:
    print("[DEBUG] No existing database file to delete.")

database_manager = DatabaseManager(
    db_url=f"sqlite:///{db_path}",
    table_name="ewma_anomaly_records",
    schema={
        "timestamp": String,
        "P1_FCV01D": Float,
        "EWMA": Float,
        "Anomaly": Integer,
    }
)

# Write all EWMA results to the database
def write_to_database(batch_df, batch_id):
    # Select only columns defined in the schema
    selected_columns = ["timestamp", "P1_FCV01D", "EWMA", "Anomaly"]
    full_df = batch_df[selected_columns]

    # Convert to Pandas for insertion into the database
    if not full_df.empty:
        database_manager.bulk_insert(full_df)

# Update process_ewma_batch to write all rows to the database
def process_ewma_batch(df, epoch_id):
    global global_ewma_state

    print(f"[DEBUG] Processing EWMA batch, Epoch: {epoch_id}")

    try:
        # Convert Spark DataFrame to Pandas
        batch_df = df.toPandas()
        if batch_df.empty:
            print(f"[INFO] Epoch {epoch_id}: Empty batch, skipping.")
            return

        # Initialize EWMA and anomaly flag columns
        batch_df["EWMA"] = None
        batch_df["Anomaly"] = 0  # Default to no anomaly (0)

        # Calculate EWMA incrementally and detect anomalies
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
            ewma_value = global_ewma_state["P1_FCV01D"]
            batch_df.at[i, "EWMA"] = ewma_value

            # Detect anomaly if the absolute difference exceeds the threshold
            if abs(current_value - ewma_value) > anomaly_threshold:
                batch_df.at[i, "Anomaly"] = 1  # Flag as anomaly

        # Log results for debugging
        print(f"[DEBUG] EWMA and anomaly detection results for epoch {epoch_id}:\n{batch_df[['timestamp', 'P1_FCV01D', 'EWMA', 'Anomaly']]}")

        # Write all rows to the database
        write_to_database(batch_df, epoch_id)

    except Exception as e:
        print(f"[ERROR] Epoch {epoch_id}: Error processing batch: {e}")

# Write stream to process EWMA and write all rows to the database
query = parsed_stream.writeStream \
    .foreachBatch(process_ewma_batch) \
    .option("checkpointLocation", "checkpoints/ewma_checkpoint") \
    .start()

query.awaitTermination()