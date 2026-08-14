from kafka import KafkaConsumer # pyright: ignore[reportMissingModuleSource]

# Connect and subscribe to the same topic the producer is writing to
consumer = KafkaConsumer(
    "test-topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",  # read from the beginning if we're a new consumer
)

print("Listening for messages... (Ctrl+C to stop)")
for msg in consumer:
    print(f"received: {msg.value.decode('utf-8')}")