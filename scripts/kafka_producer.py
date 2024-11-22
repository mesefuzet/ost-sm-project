#!/usr/bin/env python
# coding: utf-8

# In[1]:


#pip install kafka-python


# In[5]:


#IGNORE
#Time Consuming
from kafka import KafkaProducer
import pandas as pd
import json
import time

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
)

file_path = 'C:/Users/Seif Jaber/hai-train1.csv' 
hai_data = pd.read_csv(file_path)

# Define a function to stream data into Kafka
def stream_data_to_kafka(producer, topic, data, delay=1):
    try:
        for index, row in data.iterrows():
            record = row.to_dict()
            # Send data to Kafka
            producer.send(topic, value=record)
            print(f"Sent: {record}")
            time.sleep(delay)  # Simulate streaming delay
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.close()
        print("Kafka Producer closed.")

# Stream data to Kafka
topic_name = 'hai-dataset'  
stream_data_to_kafka(producer, topic_name, hai_data)


# In[2]:


#BATCHES OF DATA
#Fastest Version and Better Tracking of Progress
from kafka import KafkaProducer
import pandas as pd
import json
import time
import logging

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
)

# Set up logging to track progress
logging.basicConfig(
    filename='kafka_streaming.log', 
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)


file_path = 'C:/Users/Seif Jaber/hai-train1.csv'  
hai_data = pd.read_csv(file_path)


def stream_data_in_batches(producer, topic, data, batch_size=1000, delay=0.1):
    try:
        total_records = len(data)
        batch_count = 0 
        
        
        for i in range(0, total_records, batch_size):
            batch = data.iloc[i:i+batch_size].to_dict(orient='records') 

            # Send each record in the batch to Kafka
            for record in batch:
                producer.send(topic, value=record)

            batch_count += 1
            logging.info(f"Batch {batch_count} sent. Records processed: {min(i + batch_size, total_records)}/{total_records}")
            print(f"Batch {batch_count} sent. Records processed: {min(i + batch_size, total_records)}/{total_records}")

            time.sleep(delay)

        # Final summary
        logging.info(f"Streaming completed. Total records sent: {total_records}")
        print(f"Streaming completed. Total records sent: {total_records}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")
    finally:
        producer.close()
        logging.info("Kafka Producer closed.")
        print("Kafka Producer closed.")

# Stream data to Kafka
topic_name = 'hai-dataset'  # Replace with your Kafka topic name
stream_data_in_batches(producer, topic_name, hai_data, batch_size=1000, delay=0.1)


# In[ ]:




