from kafka import KafkaProducer

#Just for testing Kafka that everything is working
try:
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    print("Kafka producer initialized successfully!")
except Exception as e:
    print(f"Failed to initialize Kafka producer: {e}")