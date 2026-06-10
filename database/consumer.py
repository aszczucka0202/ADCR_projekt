import json
from dateutil.parser import parse  # pip install python-dateutil
from confluent_kafka import Consumer, KafkaError
from sqlalchemy.exc import IntegrityError
from models import init_db, SessionLocal, HydroMeasurement, SynopMeasurement, AlertRecord


def create_kafka_consumer():
    config = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'storage-and-ml-group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(config)
    consumer.subscribe(['hydro', 'synop'])
    return consumer


def main():
    init_db()
    consumer = create_kafka_consumer()
    db = SessionLocal()

    print("Konsument Kafki uruchomiony. Oczekiwanie na wiadomości...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Błąd Kafki: {msg.error()}")
                    break

            # Dekodowanie wiadomości
            topic = msg.topic()
            payload = json.loads(msg.value().decode('utf-8'))

            try:
                if topic == 'hydro':
                    record = HydroMeasurement(
                        gauge_id=payload['gauge_id'],
                        measurement_timestamp=parse(payload['measurement_timestamp']),
                        water_level_cm=payload['water_level_cm'],
                        flow_m3s=payload['flow_m3s'],
                        is_warning=payload['is_warning'],
                        ingestion_timestamp=parse(payload['ingestion_timestamp'])
                    )
                elif topic == 'synop':
                    record = SynopMeasurement(
                        station_id=payload['station_id'],
                        measurement_timestamp=parse(payload['measurement_timestamp']),
                        temperature=payload['temperature'],
                        humidity=payload['humidity'],
                        pressure_hpa=payload['pressure_hpa'],
                        wind_speed=payload['wind_speed'],
                        ingestion_timestamp=parse(payload['ingestion_timestamp'])
                    )
                elif topic == 'alerts':
                    record = AlertRecord(
                        alert_id=payload['alert_id'],
                        alert_type=payload['alert_type'],
                        source=payload['source'],
                        station_id=payload['station_id'],
                        lat=payload['lat'],
                        lon=payload['lon'],
                        severity=payload['severity'],
                        msg=payload['msg'],
                        value=payload['value'],
                        event_timestamp=parse(payload['event_timestamp']),
                        expires=parse(payload['expires']),
                        ingestion_timestamp=parse(payload['ingestion_timestamp'])
                    )


                db.add(record)
                db.commit()
                print(
                    f"Zapisano pomyślnie rekord z topiku {topic} dla stacji {payload.get('gauge_id') or payload.get('station_id')}")

            except IntegrityError:
                # Obsługa duplikatu (UniqueConstraint zablokował insert)
                db.rollback()
                print(
                    f"Zignorowano duplikat dla stacji {payload.get('gauge_id') or payload.get('station_id')} z czasu {payload['measurement_timestamp']}")
            except Exception as e:
                db.rollback()
                print(f"Błąd przetwarzania wiadomości: {e}")

    except KeyboardInterrupt:
        print("Zamykanie konsumenta...")
    finally:
        consumer.close()
        db.close()


if __name__ == '__main__':
    main()