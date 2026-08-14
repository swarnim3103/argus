from kafka import KafkaProducer  # pyright: ignore[reportMissingModuleSource]
import time

# Connect to the Kafka broker running on localhost:9092
producer = KafkaProducer(bootstrap_servers="localhost:9092")

for i in range(10):
    message = f"hello from message {i}"
    # send() takes a topic name and the message bytes
    producer.send("test-topic", value=message.encode("utf-8"))
    print(f"sent: {message}")
    time.sleep(1)

producer.flush()  # make sure everything is actually sent before exiting