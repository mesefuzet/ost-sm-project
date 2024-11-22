#!/usr/bin/env python
# coding: utf-8

# In[1]:


#pip install kafka-python


# In[12]:


#IGNORE
from kafka import KafkaConsumer
import json

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'hai-dataset',  # Topic name
    bootstrap_servers='localhost:9092',  # Kafka broker
    auto_offset_reset='earliest',  # Start reading from the earliest message
    enable_auto_commit=True,
    group_id='hai-consumer-group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Decode message values
)

print("Listening to messages from topic: hai-dataset")


max_messages = 5
message_count = 0

try:
    for message in consumer:
        message_count += 1
        print(f"Message {message_count}: {message.value}")
        
        # Stop processing after reaching the max message count
        if message_count >= max_messages:
            print(f"Processed {max_messages} messages. Stopping consumer.")
            break
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    consumer.close()
    print("Kafka Consumer closed.")

print("Done")


# In[3]:


#Retreive Message
print("Test")
from kafka import KafkaConsumer
import json

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'hai-dataset',  # Topic name
    bootstrap_servers='localhost:9092',  
    auto_offset_reset='earliest', 
    enable_auto_commit=True,
    group_id='hai-consumer-group',  
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
)

print("Listening to messages from topic: hai-dataset")

message_count = 0  
max_messages = 1  # Limit the number of messages to process #Chose 1 to make it faster and quickly test if no issue exist

try:
    for message in consumer:
        message_count += 1
        if message_count % 100 == 0:
            print(f"Message {message_count}: {message.value}")
        
        # Break after processing the maximum number of messages
        if message_count >= max_messages:
            print(f"Processed {max_messages} messages. Stopping consumer.")
            break

except KeyboardInterrupt:
    print("Stopped by user")
finally:
    consumer.close()
    print("Kafka Consumer closed.")

print("Done")


# In[4]:


#Retrieve Message Contents
from kafka import KafkaConsumer
import json

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'hai-dataset',  # Topic name
    bootstrap_servers='localhost:9092',  
    auto_offset_reset='earliest',  
    enable_auto_commit=True,
    group_id='hai-consumer-group',  
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
)

print("Listening to messages from topic: hai-dataset")

message_count = 0 
max_messages = 5  # Limit the number of messages to process

try:
    for message in consumer:
        message_count += 1
        # Print the full content of each message
        print(f"Message {message_count}: {message.value}")
        
        # Break after processing the maximum number of messages
        if message_count >= max_messages:
            print(f"Processed {max_messages} messages. Stopping consumer.")
            break

except KeyboardInterrupt:
    print("Stopped by user")
finally:
    consumer.close()
    print("Kafka Consumer closed.")

print("Done")



