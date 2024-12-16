from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, when, lag, expr
from pyspark.sql.types import DoubleType, StringType, IntegerType
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import pyspark.sql.functions as F

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
    .withColumn("data_type", expr(r"regexp_extract(raw_data, '\"data_type\":\\s*\"(.*?)\"', 1)")) \
    .withColumn("attack_label", expr(r"regexp_extract(raw_data, '\"attack_label\":\\s*([0-9]+)', 1)").cast(IntegerType()))

# Filter rows with valid timestamps and numeric columns
parsed_stream = parsed_stream.filter(
    col("timestamp").isNotNull() & col("P1_FCV01D").isNotNull()
)

relevant_stream = parsed_stream.select("timestamp", "P1_FCV01D", "data_type", "attack_label")

# EWMA Parameters
alpha = 0.2  # Smoothing factor

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
        """Insert multiple records into the database in chunks."""
        session = self.Session()
        chunk_size = 10000  # Adjust based on system capacity
        try:
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                records = [self.model(**row.to_dict()) for _, row in chunk.iterrows()]
                session.bulk_save_objects(records)
                session.commit()
                print(f"[DEBUG] Inserted chunk {i // chunk_size + 1}")
        except Exception as e:
            session.rollback()
            print(f"[ERROR] Error inserting data into database: {e}")
        finally:
            session.close()


db_manager = DatabaseManager("sqlite:///EWMA.db", "EWMA_Table", {
    "timestamp": String,
    "P1_FCV01D": Float,
    "EWMA": Float,
    "data_type": String,
    "attack_label": Integer  # Include attack_label column
})

# Updated EWMA calculation using Spark-native functions
def calculate_ewma(df, alpha):
    # Define a Spark Window specification
    window_spec = Window.orderBy("timestamp")

    # Add a lagged column for the previous value of P1_FCV01D
    df = df.withColumn("lagged_P1_FCV01D", lag("P1_FCV01D").over(window_spec))

    # Calculate the EWMA
    df = df.withColumn(
        "EWMA",
        when(col("lagged_P1_FCV01D").isNull(), col("P1_FCV01D"))  # First row: EWMA = current value
        .otherwise(alpha * col("P1_FCV01D") + (1 - alpha) * col("lagged_P1_FCV01D"))
    )
    return df


def process_ewma_batch(df, epoch_id):
    print(f"[DEBUG] Processing EWMA batch, Epoch: {epoch_id}")
    try:
        # Calculate EWMA directly in Spark
        updated_df = calculate_ewma(df, alpha)

        # Collect rows as an iterator
        records = updated_df.select("timestamp", "P1_FCV01D", "EWMA", "data_type", "attack_label").toLocalIterator()

        # Convert to list of dictionaries for database insertion
        data = [{"timestamp": row["timestamp"], "P1_FCV01D": row["P1_FCV01D"], "EWMA": row["EWMA"], "data_type": row["data_type"], "attack_label": row["attack_label"]} for row in records]

        # Insert into the database
        db_manager.bulk_insert(pd.DataFrame(data))
        print(f"[DEBUG] Epoch {epoch_id}: Batch results written to database.")
    except Exception as e:
        print(f"[ERROR] Epoch {epoch_id}: Error processing batch: {e}")


query = relevant_stream.writeStream \
    .foreachBatch(process_ewma_batch) \
    .option("checkpointLocation", "checkpoints/ewma_checkpoint") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("[DEBUG] Stopping Spark Streams...")
finally:
    spark.stop()
