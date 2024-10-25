from kafka import KafkaProducer
import json

# Create a Kafka producer with explicit API version
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 7, 0)  # Set API version to 2.7.0 (replace with your Kafka version if different)
)

# Send a message
message = {"message": "Hello from outside the container!"}
producer.send('test-topic', value=message)
producer.flush()  # Ensure all messages are sent

print("Message sent!")

