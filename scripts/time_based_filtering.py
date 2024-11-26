# ===================================================
### Bia Time Based Filtering (streaming Process) ####

from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime, timedelta

# Expected Workflow:
# The script tracks the time of the last processed message using a variable (last_processed_time). 
# Each incoming message is checked against this timestamp, 
# and only those that meet the time interval condition (filter_interval) are forwarded to the new Kafka topic.

# Kafka configuration
input_topic = 'hai-dataset'
output_topic = 'filtered-data'
bootstrap_servers = 'localhost:9092'
filter_interval = 5  # Process only every N seconds

# Initialize Kafka consumer and producer
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='time-filtering-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

last_processed_time = datetime.min

print(f"Starting time-based filtering with a {filter_interval}-second interval.")

try:
    for message in consumer:
        record = message.value
        current_time = datetime.now()
        
        # Process message only if the interval has passed
        if (current_time - last_processed_time).total_seconds() >= filter_interval:
            # Forward the message to the new Kafka topic
            producer.send(output_topic, value=record)
            print(f"Filtered message: {record}")
            
            last_processed_time = current_time

except KeyboardInterrupt:
    print("Time-based filtering stopped by user.")
finally:
    consumer.close()
    producer.close()
    print("Kafka consumer and producer closed.")

    # Github name check2
