from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

message = {
    "station_id": "IMGW_TEST",
    "measurement_timestamp": "2026-06-08T17:00:00Z",
    "temperature": 20.1,
    "humidity": 60,
    "pressure_hpa": 1016.0,
    "wind_speed": 2.0,
    "ingestion_timestamp": "2026-06-08T17:00:00Z"
}

producer.send(
    "synop",
    key="IMGW_TEST",
    value=message
)

producer.flush()

print("Wysłano wiadomość do Kafka")