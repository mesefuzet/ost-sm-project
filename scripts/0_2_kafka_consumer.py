


#Retreive Message
#print("Test")
from kafka import KafkaConsumer
import json
import time

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'hai-dataset',  # Topic name
    bootstrap_servers='localhost:9092',  
    auto_offset_reset='earliest', 
    enable_auto_commit=False,
    group_id='hai-consumer-group',  
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  
)

print("Listening to messages from topic: hai-dataset")

#message_count = 0  
#max_messages = 5  # Limit the number of messages to process #Chose 5 for debugging, later we'll delete this constraint in the final solution

train_count = 0
test_count = 0
max_train_messages = 10  # Limit the number of train messages to process
max_test_messages = 10
try:
    for message in consumer:
        record = message.value
        if isinstance(record, dict):
            #print(f"DEBUG: Processing record: {record}")
            if "data_type" in record:
                data_type = record["data_type"]
                #print("kismacska")

            # Process train data
                if data_type == "train" and train_count < max_train_messages:
                    train_count += 1
                    #print(f"TRAIN Message {train_count}: {record}")
                #print only every 100th message
                #if train_count % 100 == 0:
                #    print(f"Message {train_count}: {record}")

            # Process test data
                elif data_type == "test" and test_count < max_test_messages:
                    test_count += 1
                    #print(f"TEST Message {test_count}: {record}")
                #print only every 100th message
                #if train_count % 100 == 0:
                #    print(f"Message {test_count}: {record}")
                time.sleep(1)
            # Break after processing the maximum number of messages
                if train_count >= max_train_messages and test_count >= max_test_messages:
                    print(f"Processed {max_train_messages} train messages and {max_test_messages} test messages. Stopping consumer.")
                    break
        else:
            print("ERROR: 'data_type' field is missing or record is not valid JSON!")
            continue

except KeyboardInterrupt:
    print("Stopped by user")
finally:
    consumer.close()
    print("Kafka Consumer closed.")

print("Done")



