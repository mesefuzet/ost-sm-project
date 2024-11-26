# ================================================
### Bia Data Aggregation (streaming Process) ####
from kafka import KafkaConsumer, KafkaProducer
import json
from collections import deque
from datetime import datetime, timedelta

# Expected Workflow:
# Producer sends raw data to the hai-dataset topic.
# Data Aggregation consumes data from hai-dataset, aggregates it in a sliding window, and sends aggregated results to aggregated-data(new topic).

# Kafka configuration
input_topic = 'hai-dataset'
output_topic = 'aggregated-data'
bootstrap_servers = 'localhost:9092'
window_size = 60  # Sliding window size in seconds
slide_interval = 10  # Slide interval in seconds

# Initialize Kafka consumer and producer
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='data-aggregation-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sliding window
window = deque()

start_time = datetime.now()

print(f"Starting data aggregation with a {window_size}-second window and {slide_interval}-second slide interval.")

try:
    for message in consumer:
        record = message.value
        current_time = datetime.now()
        
        # Add new record to the window
        window.append((current_time, record))
        
        # Remove records outside the window size
        while window and (current_time - window[0][0]).total_seconds() > window_size:
            window.popleft()
        
        # Perform aggregation at every slide interval
        if (current_time - start_time).total_seconds() >= slide_interval:
            # Initialize aggregation variables
            aggregation = {"train": {"count": 0, "sum": 0}, "test": {"count": 0, "sum": 0}}

            # Iterate over messages in the sliding window
            for _, rec in window:
                data_type = rec.get('data_type', 'Unknown')
                value = rec.get('value', 0)  # Assuming 'value' is a numeric field in the record

                if data_type in aggregation:
                    aggregation[data_type]["count"] += 1
                    aggregation[data_type]["sum"] += value

            # Calculate averages
            average_data = {
                "train": aggregation["train"]["sum"] / aggregation["train"]["count"] if aggregation["train"]["count"] > 0 else 0,
                "test": aggregation["test"]["sum"] / aggregation["test"]["count"] if aggregation["test"]["count"] > 0 else 0
            }

            # Send average data to the output topic
            producer.send(output_topic, value={"timestamp": current_time.isoformat(), "average": average_data})
            print(f"Aggregated average data: {average_data}")
            
            # Reset slide interval timer
            start_time = current_time

except KeyboardInterrupt:
    print("Data aggregation stopped by user.")
finally:
    consumer.close()
    producer.close()
    print("Kafka consumer and producer closed.")