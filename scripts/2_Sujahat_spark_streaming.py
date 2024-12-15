from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, expr, lit, when
from sqlalchemy import create_engine, Column, Integer, Float, String
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import os

# Remove existing database file
if os.path.exists("hai_train_test.db"):
    os.remove("hai_train_test.db")
    print("[DEBUG] Database file deleted.")
else:
    print("[DEBUG] No existing database file to delete.")

# SQLAlchemy Base
Base = declarative_base()

# DatabaseManager class
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
DATABASE_URL = "sqlite:///hai_train_test.db"
anomalies_schema = {
    "timestamp": String,
    "P1_FCV01D": Float,
    "P1_FCV01Z": Float,
    "P1_FCV03D": Float,
    "x1003_24_SUM_OUT": Float,
    "anomaly_flag": Integer,
    "anomaly_class": String
}
anomalies_db = DatabaseManager(DATABASE_URL, "anomalies", anomalies_schema)

# Precompute thresholds from a batch DataFrame
train_batch_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as raw_data") \
    .withColumn("x1003_24_SUM_OUT", expr(r"regexp_extract(raw_data, '\"x1003_24_SUM_OUT\":\\s*([0-9.]+)', 1)").cast("double"))

train_stats_row = train_batch_df.select(
    mean(col("x1003_24_SUM_OUT")).alias("train_mean"),
    stddev(col("x1003_24_SUM_OUT")).alias("train_stddev")
).collect()[0]

mean_val = train_stats_row["train_mean"]
stddev_val = train_stats_row["train_stddev"]
threshold_multiplier = 3
upper_threshold = mean_val + threshold_multiplier * stddev_val
lower_threshold = mean_val - threshold_multiplier * stddev_val

# Read Kafka Stream
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

raw_stream = kafka_stream.selectExpr("CAST(value AS STRING) as raw_data")

# Parse and filter the stream
parsed_stream = raw_stream \
    .withColumn("timestamp", expr(r"regexp_extract(raw_data, '\"timestamp\":\\s*\"(.*?)\"', 1)").cast("string")) \
    .withColumn("P1_FCV01D", expr(r"regexp_extract(raw_data, '\"P1_FCV01D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FCV01Z", expr(r"regexp_extract(raw_data, '\"P1_FCV01Z\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("P1_FCV03D", expr(r"regexp_extract(raw_data, '\"P1_FCV03D\":\\s*([0-9.]+)', 1)").cast("double")) \
    .withColumn("x1003_24_SUM_OUT", expr(r"regexp_extract(raw_data, '\"x1003_24_SUM_OUT\":\\s*([0-9.]+)', 1)").cast("double")) \
    .filter(
        (col("timestamp").isNotNull()) &
        ((col("P1_FCV01D") != 0) | (col("P1_FCV01Z") != 0) | (col("P1_FCV03D") != 0) | (col("x1003_24_SUM_OUT") != 0))
    )

# Add anomaly detection logic
anomaly_flagged_stream = parsed_stream.withColumn(
    "anomaly_flag",
    (col("x1003_24_SUM_OUT") > lit(upper_threshold)) | (col("x1003_24_SUM_OUT") < lit(lower_threshold))
).withColumn(
    "anomaly_class",
    when(col("x1003_24_SUM_OUT") > lit(upper_threshold), lit("High"))
    .when(col("x1003_24_SUM_OUT") < lit(lower_threshold), lit("Low"))
    .otherwise(lit("Normal"))
)

# Write anomalies to the database
def write_anomalies_to_database(df, epoch_id):
    """Write anomaly batch to database."""
    try:
        pdf = df.toPandas()
        pdf["timestamp"] = pdf["timestamp"].astype(str)
        anomalies_db.bulk_insert(pdf)
        print(f"Anomalies for epoch {epoch_id} written to database.")
    except Exception as e:
        print(f"Error writing anomalies: {e}")

query = anomaly_flagged_stream.writeStream \
    .foreachBatch(lambda df, epoch_id: write_anomalies_to_database(df, epoch_id)) \
    .outputMode("append") \
    .start()

# Await termination
query.awaitTermination()
