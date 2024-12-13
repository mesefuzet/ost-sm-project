from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr
from pyspark.sql.types import StringType, FloatType, IntegerType
import os
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
from river.drift import ADWIN
import time

if os.path.exists("adwin_results.db"):
    os.remove("adwin_results.db")
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
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
    .getOrCreate()

# Kafka Configuration
kafka_broker = "localhost:9092"
topic_name = "hai-dataset"

# Database Configuration
DATABASE_URL = "sqlite:///adwin_results.db"
#train_schema = {"timestamp": String, "P1_FCV01D": Float, "P1_FCV01Z": Float, "P1_FCV03D": Float, "x1003_24_SUM_OUT": Float, "data_type": String}
train_schema = {
    "timestamp": String,
    "P1_FCV01D": Float,
    "P1_FCV01Z": Float,
    "P1_FCV03D": Float,
    "x1003_24_SUM_OUT": Float,
    "data_type": String,
    "drift_detected": Integer,  
}
test_schema = {**train_schema, "attack_label": Integer}

#Database manager configuration
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

#I just put this here for debug, it logs into a json what Spark gets from Kafka, can be commented out later:
# kafka_stream.selectExpr("CAST(value AS STRING)").writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "debug/raw_kafka_output") \
#     .option("checkpointLocation", "debug/raw_kafka_checkpoint") \
#     .start()

raw_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data")


parsed_stream = raw_stream \
    .withColumn("timestamp", expr(r"regexp_extract(raw_data, '\"timestamp\":\\s*\"(.*?)\"', 1)").cast("string")) \
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)")) \
    .withColumn("P1_FCV01Z", expr(r"regexp_extract(raw_data, '\"P1_FCV01Z\":\\s*([0-9.]+)', 1)")) \
    .withColumn("P1_FCV03D", expr(r"regexp_extract(raw_data, '\"P1_FCV03D\":\\s*([0-9.]+)', 1)")) \
    .withColumn("x1003_24_SUM_OUT", expr(r"regexp_extract(raw_data, '\"x1003_24_SUM_OUT\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("data_type", expr(r"regexp_extract(raw_data, '\"data_type\":\\s*\"(.*?)\"', 1)").cast("string")) \
    .withColumn("attack_label", expr(r"regexp_extract(raw_data, '\"attack_label\":\\s*([0-9]+)', 1)").cast("int"))

parsed_stream = parsed_stream \
    .withColumn("P1_FCV01D", col("P1_FCV01D").cast("double")) \
    .withColumn("P1_FCV01Z", col("P1_FCV01Z").cast("double")) \
    .withColumn("P1_FCV03D", col("P1_FCV03D").cast("double")) \
    .withColumn("x1003_24_SUM_OUT", col("x1003_24_SUM_OUT").cast("double"))

#Filter out rows that are completely blank or contain only zeros in numeric columns
parsed_stream = parsed_stream.filter(
    (col("timestamp").isNotNull()) &
    ((col("P1_FCV01D") != 0) | (col("P1_FCV01Z") != 0) | (col("P1_FCV03D") != 0) | (col("x1003_24_SUM_OUT") != 0))
)

#check the schema
#parsed_stream.printSchema()

#train-test separation
train_stream = parsed_stream.filter(col("data_type") == "train") \
    .select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT", "data_type")

train_stream.printSchema()

test_stream = parsed_stream.filter(col("data_type") == "test") \
    .select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT", "data_type", "attack_label")

# Select specific columns
# selected_columns = parsed_stream.select(
#     col("timestamp"),
#     col("P1_FCV01D"),
#     col("P1_FCV01Z"),
#     col("P1_FCV03D"),
#     col("x1003_24_SUM_OUT"),
#     col("data_type"),
#     col("attack_label")
    
# )


# Debugging: Save parsed output to a file ---> when something is bad, we can comment this out and have a look at the log files in debug/
# selected_columns.writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "debug/parsed_output") \
#     .option("checkpointLocation", "debug/parsed_checkpoint") \
#     .start()

# train_stream.writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "debug/train_output") \
#     .option("checkpointLocation", "debug/train_checkpoint") \
#     .start()

# test_stream.writeStream \
#     .outputMode("append") \
#     .format("json") \
#     .option("path", "debug/test_output") \
#     .option("checkpointLocation", "debug/test_checkpoint") \
#     .start()


# ===============================================
### EMESE 2 STREAMING PROCESSES ####
# 1. ADWIN method

print("1. PROCESS: ADWIN")
time.sleep(1)

adwin = ADWIN()
def process_adwin_batch(df, epoch_id, is_train):
    print(f"[DEBUG] Invoked process_adwin_batch for {'train' if is_train else 'test'} stream, Epoch: {epoch_id}")
    try:
        print(f"[DEBUG] Epoch {epoch_id}: Processing {'train' if is_train else 'test'} batch...")
        batch_df = df.toPandas()

        if batch_df.empty:
            print(f"[INFO] Epoch {epoch_id}: Empty batch, skipping.")
            return

        batch_df["drift_detected"] = 0
        for index, row in batch_df.iterrows():
            value = row["P1_FCV01D"]

            if pd.isna(value):
                print(f"[WARNING] Skipping invalid value at index {index}")
                continue
            adwin.update(value)
            batch_df.at[index, "drift_detected"] = int(adwin.drift_detected)

        db_manager = train_db if is_train else test_db
        print(f"[DEBUG] Batch to insert (epoch {epoch_id}):\n{batch_df}")
        db_manager.bulk_insert(batch_df)
        print(f"[DEBUG] Epoch {epoch_id}: Batch results written to database.")

    except Exception as e:
        print(f"[ERROR] Epoch {epoch_id}: Error processing batch: {e}")




print("DEBUG: INVOKE ADWIN FOR TRAIN")
train_stream.writeStream \
    .foreachBatch(lambda df, epoch_id: process_adwin_batch(df, epoch_id, is_train=True)) \
    .option("checkpointLocation", "checkpoints/train_checkpoint_new") \
    .start()

print( "DEBUG: INVOKE ADWIN FOR TEST")
test_stream.writeStream \
    .foreachBatch(lambda df, epoch_id: process_adwin_batch(df, epoch_id, is_train=False)) \
    .option("checkpointLocation", "checkpoints/test_checkpoint_new") \
    .start()

# def write_to_database(df, epoch_id, db_manager):
#     """Write micro-batch to database."""
#     try:
#         print(f"[DEBUG] Epoch {epoch_id}: Converting Spark DataFrame to Pandas DataFrame...")
#         df.printSchema()
#         df.show(5, truncate=False)
#         pdf = df.toPandas()  #conversion to pandas dataframe
#         print(f"[DEBUG] Data types in Pandas DataFrame:\n{pdf.dtypes}")
#         print(f"[DEBUG] Sample rows from Pandas DataFrame:\n{pdf.head()}")
#         if "timestamp" in pdf.columns:
#             pdf["timestamp"] = pdf["timestamp"].astype(str)
#         if "data_type" in pdf.columns:
#             pdf["data_type"] = pdf["data_type"].astype(str)

#         print(f"[DEBUG] Epoch {epoch_id}: Pandas DataFrame conversion complete. Number of rows: {len(pdf)}")
        
#         db_manager.bulk_insert(pdf)  #insert into db
#         print(f"[DEBUG] Epoch {epoch_id}: Bulk insert complete. {len(pdf)} rows written to the database.")
#     except Exception as e:
#         print(f"[ERROR] Epoch {epoch_id}: An error occurred while writing to the database: {e}")
#         raise e

# #write train into database
# print("[DEBUG] Starting writeStream for train stream...")
# train_stream.writeStream \
#     .foreachBatch(lambda df, epoch_id: write_to_database(df, epoch_id, train_db)) \
#     .start()

# #write test into database
# print("[DEBUG] Starting writeStream for test stream...")
# test_stream.writeStream \
#     .foreachBatch(lambda df, epoch_id: write_to_database(df, epoch_id, test_db)) \
#     .start()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("[DEBUG] Terminating Spark Streams...")
finally:
    print("[DEBUG] Stopping SparkSession...")
    spark.stop()