#!/usr/bin/env python
# coding: utf-8

# In[1]:






# In[5]:

""""
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
"""

# In[2]:


#BATCHES OF DATA
#Fastest Version and Better Tracking of Progress
from kafka import KafkaProducer
import pandas as pd
import json
import time
import logging
import os
import time

#Emese comment:
#for debugging and making the solution easier to handle -> I added a 5 minute counter so everything will stop until 5 mins
#I made this because the producer needs to constantly produce the data into the topic so the consumer and the Spark can process it later
#if it's not running constantly, consumer and Spark will have no data to consume and process, but for development we also need to regulate it somehow so it's not running until infinity

#iteration_count = 0
#max_iterations = 10


train_duration = 300  # 5minutes for train
test_duration = 300   # 5minutes for test
train_start_time = time.time()

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

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
file_path = os.path.join(root_dir, 'data', 'hai-train1.csv')
#file_path = "data/hai-train1.csv" 
hai_data = pd.read_csv(file_path)
file_path_test = os.path.join(root_dir, 'data', 'hai-test1_with_label.csv')
hai_data_test = pd.read_csv(file_path_test)

def validate_record(record):
    required_keys = {'timestamp', 'data_type'}
    return all(key in record for key in required_keys)

def stream_data_in_batches(producer, topic, data, data_type, batch_size=1000, delay=0.2):
    try:
        total_records = len(data)
        batch_count = 0 
        records_sent = 0
        
        while records_sent < total_records:
            batch = data.iloc[records_sent:records_sent+batch_size].to_dict(orient='records') 

            # Send each record in the batch to Kafka
            for record in batch:
                record['data_type'] = data_type #----> since we're sending the train and test data into the same Kafka topic, we need to "label" them as train and test
                producer.send(topic, value=record)

            if validate_record(record):  # Only send valid records
                    producer.send(topic, value=record)
            else:
                logging.warning(f"Invalid record skipped: {record}")

            batch_count += 1
            records_sent += len(batch)
            logging.info(f"Batch {batch_count} sent. Records processed: {min(records_sent + batch_size, total_records)}/{total_records}")
            print(f"Batch {batch_count} sent. Records processed: {min(records_sent + batch_size, total_records)}/{total_records}")

            time.sleep(delay)
            # Stop streaming if all records have been sent
            if records_sent >= total_records:
                print(f"Finished streaming {data_type} data. Total records sent: {records_sent}")
                break

        # Final summary
        logging.info(f"Streaming completed. Total records sent: {total_records}")
        print(f"Streaming completed. Total records sent: {total_records}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")
    #finally:
     #   producer.close()
      #  logging.info("Kafka Producer closed.")
       # print("Kafka Producer closed.")

# Stream data to Kafka
topic_name = 'hai-dataset'

try:
    print("DEBUG: Starting TRAIN data streaming...")
    stream_data_in_batches(producer, topic_name, hai_data, data_type="train")  # Stream train data once
    print("DEBUG: Completed TRAIN data streaming.")

    
    time.sleep(5)

    print("DEBUG: Starting TEST data streaming...")
    stream_data_in_batches(producer, topic_name, hai_data_test, data_type="test")  # Stream test data once
    print("DEBUG: Completed TEST data streaming.")
except Exception as e:
    print(f"Error occurred: {e}")

finally:
    producer.close()
    print("DEBUG: Kafka Producer closed.")




