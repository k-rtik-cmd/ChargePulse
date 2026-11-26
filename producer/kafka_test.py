from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

test_event = {
    "event_id": "test-001",
    "session_id": "session-001",
    "station_id": "station-001",
    "energy_delta_kwh": 2.5
}

future = producer.send("charge-events", test_event)
metadata = future.get(timeout=10)

print("Message sent successfully!")
print("Topic:", metadata.topic)
print("Partition:", metadata.partition)
print("Offset:", metadata.offset)

producer.flush()
producer.close()