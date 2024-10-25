import sys
from kafka import KafkaConsumer
import json

if len(sys.argv) != 2:
    print("Usage: python consumer.py <topic-name>")
    sys.exit(1)

topic = sys.argv[1]

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic,  # Topic to subscribe to, passed as a command-line argument
    bootstrap_servers=['localhost:9092'],  # Kafka broker address
    auto_offset_reset='earliest',  # Start reading at the earliest available message
    enable_auto_commit=True,  # Automatically commit message offsets
    group_id='my-group',  # Consumer group id
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON messages
)

# Consume messages
for message in consumer:
    print(f"Received message: {message.value}")

