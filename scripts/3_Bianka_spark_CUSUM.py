from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, abs
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.orm import sessionmaker
import os
import pandas as pd

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
    "P1_FCV01D": {"mean": 33.39, "stddev": 25.84},
    "P1_FCV01Z": {"mean": 33.26, "stddev": 26.11},
    "P1_FCV03D": {"mean": 53.87, "stddev": 2.87},
    "x1003_24_SUM_OUT": {"mean": 30.72, "stddev": 0.98},
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
db_path = "CUSUM.db"
if os.path.exists(db_path):
    os.remove(db_path)
    print("[DEBUG] Database file deleted.")
else:
    print("[DEBUG] No existing database file to delete.")

database_manager = DatabaseManager(
    db_url=f"sqlite:///{db_path}",
    table_name="anomaly_records",
    schema={
        "timestamp": String,
        "P1_FCV01D": Float,
        "CUSUM_P1_FCV01D": Float,
        "Anomaly_P1_FCV01D": Integer,
        "P1_FCV01Z": Float,
        "CUSUM_P1_FCV01Z": Float,
        "Anomaly_P1_FCV01Z": Integer,
        "P1_FCV03D": Float,
        "CUSUM_P1_FCV03D": Float,
        "Anomaly_P1_FCV03D": Integer,
        "x1003_24_SUM_OUT": Float,
        "CUSUM_x1003_24_SUM_OUT": Float,
        "Anomaly_x1003_24_SUM_OUT": Integer,
        "data_type": String,
        "attack_label": Integer,
    }
)

# Write anomalies to the database
def write_to_database(batch_df, batch_id):
    anomaly_df = batch_df.filter(
        (col("Anomaly_P1_FCV01D") == 1) |
        (col("Anomaly_P1_FCV01Z") == 1) |
        (col("Anomaly_P1_FCV03D") == 1) |
        (col("Anomaly_x1003_24_SUM_OUT") == 1)
    )
    # Select only columns defined in the schema
    selected_columns = [
        "timestamp", "P1_FCV01D", "CUSUM_P1_FCV01D", "Anomaly_P1_FCV01D",
        "P1_FCV01Z", "CUSUM_P1_FCV01Z", "Anomaly_P1_FCV01Z",
        "P1_FCV03D", "CUSUM_P1_FCV03D", "Anomaly_P1_FCV03D",
        "x1003_24_SUM_OUT", "CUSUM_x1003_24_SUM_OUT", "Anomaly_x1003_24_SUM_OUT",
        "data_type", "attack_label"
    ]
    anomaly_df = anomaly_df.select(*selected_columns)
    
    # Convert to Pandas for insertion into the database
    anomaly_pd_df = anomaly_df.toPandas()
    if not anomaly_pd_df.empty:
        database_manager.bulk_insert(anomaly_pd_df)

query = parsed_stream.writeStream \
    .foreachBatch(write_to_database) \
    .outputMode("append") \
    .start()

query.awaitTermination()