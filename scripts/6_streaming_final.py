from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, count, isnan, when, lit, split, regexp_replace, expr, to_timestamp, window
from pyspark.sql.types import StructType, StringType, DoubleType
import json
import os
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_json
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import min, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import regexp_replace
from pyspark.sql import functions as F
import time
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd

if os.path.exists("hai_train_test.db"):
    os.remove("hai_train_test.db")
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
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Kafka Configuration
kafka_broker = "localhost:9092"
topic_name = "hai-dataset"

# Database Configuration
DATABASE_URL = "sqlite:///hai_train_test.db"
train_schema = {"timestamp": String, "P1_FCV01D": Float, "P1_FCV01Z": Float, "P1_FCV03D": Float, "x1003_24_SUM_OUT": Float, "data_type": String}
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
    .withColumn("P1_FCV03D", col("x1003_24_SUM_OUT").cast("double"))

# ===== PROCESS I.: FILTER EVERY ROWS THAT CONTAIN ZEROS IN TIMESTAMP OR NUMERIC COLUMNS
parsed_stream = parsed_stream.filter(
    (col("timestamp").isNotNull()) &
    ((col("P1_FCV01D") != 0) | (col("P1_FCV01Z") != 0) | (col("P1_FCV03D") != 0) | (col("x1003_24_SUM_OUT") != 0))
)
time.sleep(1)
#check the schema
#parsed_stream.printSchema()

# ==== PROCESS II.: TRAIN - TEST SEPARATION
train_stream = parsed_stream.filter(col("data_type") == "train") \
    .select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT", "data_type")

test_stream = parsed_stream.filter(col("data_type") == "test") \
    .select("timestamp", "P1_FCV01D", "P1_FCV01Z", "P1_FCV03D", "x1003_24_SUM_OUT", "data_type", "attack_label")
time.sleep(1)


# ==== PROCESS III.: AGGREGATION PER 1 MINUTE TIME WINDOW
aggregated_train_stream = train_stream \
    .withColumn("timestamp", to_timestamp("timestamp", "yyyy.MM.dd HH:mm")) \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "1 minute")) \
    .agg(
        F.avg("P1_FCV01D").alias("avg_P1_FCV01D"),
        F.avg("P1_FCV01Z").alias("avg_P1_FCV01Z"),
        F.avg("P1_FCV03D").alias("avg_P1_FCV03D"),
        F.avg("x1003_24_SUM_OUT").alias("avg_x1003_24_SUM_OUT"),
        F.count("*").alias("record_count")
    ) \
    .withColumn("window", col("window.start").cast("string")) \
    .select("window", "avg_P1_FCV01D", "avg_P1_FCV01Z", "avg_P1_FCV03D", "avg_x1003_24_SUM_OUT", "record_count")

aggregated_test_stream = test_stream \
    .withColumn("timestamp", to_timestamp("timestamp", "yyyy.MM.dd HH:mm")) \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "1 minute")) \
    .agg(
        F.avg("P1_FCV01D").alias("avg_P1_FCV01D"),
        F.avg("P1_FCV01Z").alias("avg_P1_FCV01Z"),
        F.avg("P1_FCV03D").alias("avg_P1_FCV03D"),
        F.avg("x1003_24_SUM_OUT").alias("avg_x1003_24_SUM_OUT"),
        F.count("*").alias("record_count")
    ) \
    .withColumn("window", col("window.start").cast("string")) \
    .select("window", "avg_P1_FCV01D", "avg_P1_FCV01Z", "avg_P1_FCV03D", "avg_x1003_24_SUM_OUT", "record_count")
time.sleep(1)

print("AGGREGATION: TRAIN")
aggregated_train_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("AGGREGATION: TEST")
aggregated_test_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

aggregated_schema = {
    "window": String,
    "avg_P1_FCV01D": Float,
    "avg_P1_FCV01Z": Float,
    "avg_P1_FCV03D": Float,
    "avg_x1003_24_SUM_OUT": Float,
    "record_count": Integer
}

aggregated_train_db = DatabaseManager(DATABASE_URL, "train_aggregated", aggregated_schema)
aggregated_test_db = DatabaseManager(DATABASE_URL, "test_aggregated", aggregated_schema)

print("---------TRAIN DATA----------------")
time.sleep(1)
train_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("---------TEST DATA----------------")
time.sleep(1)
test_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("Train row count:")
train_stream.select(count("*")).writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

print("Test row count:")
test_stream.select(count("*")).writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()


# ==== PROCESS IV.: DETECTION OF ANY REMAINING MISSING VALUES
time.sleep(1)
missing_counts_train = train_stream.select(
    [(count(when(isnan(c) | col(c).isNull(), c))).alias(c) for c in train_stream.columns]
)

missing_counts_test = test_stream.select(
    [(count(when(isnan(c) | col(c).isNull(), c))).alias(c) for c in test_stream.columns]
)

# print("MISSING COUNTS")
# missing_counts_train.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# missing_counts_test.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()



# ==== PROCESS V.: STATISTICS CALCULATION - IN PROGRESS
train_stats = train_stream.select(
    mean(col("x1003_24_SUM_OUT")).alias("train_mean"),
    stddev(col("x1003_24_SUM_OUT")).alias("train_stddev")
)

test_stats = test_stream.select(
    mean(col("x1003_24_SUM_OUT")).alias("test_mean"),
    stddev(col("x1003_24_SUM_OUT")).alias("test_stddev")
)

# train_stats.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# test_stats.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

def write_to_database(df, epoch_id, db_manager):
    """Write micro-batch to database."""
    try:
        print(f"[DEBUG] Epoch {epoch_id}: Converting Spark DataFrame to Pandas DataFrame...")
        df.printSchema()
        df.show(5, truncate=False)
        pdf = df.toPandas()  #conversion to pandas dataframe
        print(f"[DEBUG] Data types in Pandas DataFrame:\n{pdf.dtypes}")
        print(f"[DEBUG] Sample rows from Pandas DataFrame:\n{pdf.head()}")
        if "timestamp" in pdf.columns:
            pdf["timestamp"] = pdf["timestamp"].astype(str)
        if "data_type" in pdf.columns:
            pdf["data_type"] = pdf["data_type"].astype(str)

        print(f"[DEBUG] Epoch {epoch_id}: Pandas DataFrame conversion complete. Number of rows: {len(pdf)}")
        
        db_manager.bulk_insert(pdf)  #insert into db
        print(f"[DEBUG] Epoch {epoch_id}: Bulk insert complete. {len(pdf)} rows written to the database.")
    except Exception as e:
        print(f"[ERROR] Epoch {epoch_id}: An error occurred while writing to the database: {e}")
        raise e

#write train into database
print("[DEBUG] Starting writeStream for train stream...")
train_stream.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_database(df, epoch_id, train_db)) \
    .start()

#write test into database
print("[DEBUG] Starting writeStream for test stream...")
test_stream.writeStream \
    .foreachBatch(lambda df, epoch_id: write_to_database(df, epoch_id, test_db)) \
    .start()

def write_aggregated_to_database(df, epoch_id, db_manager):
    """Write micro-batch to database."""
    try:
        print(f"[DEBUG] Epoch {epoch_id}: Writing aggregated data to database...")
        df.printSchema()
        df.show(5, truncate=False)
        pdf = df.toPandas()
        pdf["window"] = pdf["window"].astype(str)
        print(f"[DEBUG] Aggregated data sample:\n{pdf.head()}")
        db_manager.bulk_insert(pdf)
        print(f"[DEBUG] Epoch {epoch_id}: Aggregated data written successfully.")
    except Exception as e:
        print(f"[ERROR] Epoch {epoch_id}: Failed to write aggregated data to database: {e}")
        raise e


#write aggregated train stream into database
print("[DEBUG] Writing Aggregated Train Stream to Database...")
aggregated_train_stream.writeStream \
    .foreachBatch(lambda df, epoch_id: write_aggregated_to_database(df, epoch_id, aggregated_train_db)) \
    .start()

#write aggregated test stream into database
print("[DEBUG] Writing Aggregated Test Stream to Database...")
aggregated_test_stream.writeStream \
    .foreachBatch(lambda df, epoch_id: write_aggregated_to_database(df, epoch_id, aggregated_test_db)) \
    .start()

spark.streams.awaitAnyTermination()