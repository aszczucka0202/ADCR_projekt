import os
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
import json
import time

def load_seen(filename):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        return set()


def save_seen(filename, key):
    with open(filename, "a", encoding="utf-8") as f:
        f.write(key + "\n")

def fetch_synop():

    url = "https://danepubliczne.imgw.pl/api/data/synop"

    response = requests.get(url)
    data = response.json()

    messages = []

    for record in data:

        try:
            if record["cisnienie"] is None:
                continue
            message = {
                "station_id": f"IMGW_{record['id_stacji']}",
                "measurement_timestamp":
                    f"{record['data_pomiaru']}T{record['godzina_pomiaru'].zfill(2)}:00:00Z",
                "temperature": float(record["temperatura"]),
                "humidity": int(float(record["wilgotnosc_wzgledna"])),
                "pressure_hpa": float(record["cisnienie"]),
                "wind_speed": float(record["predkosc_wiatru"]),
                "ingestion_timestamp":
                    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            }

            messages.append(message)

        except Exception as e:
            print()
            print("BĹÄDNY REKORD:")
            print(record)
            print("BĹ‚Ä…d:", e)
            print()
    return messages

def fetch_hydro():

    url = "https://danepubliczne.imgw.pl/api/data/hydro"

    response = requests.get(url)
    data = response.json()

    messages = []

    for record in data:

        try:
            if record["stan_wody_data_pomiaru"] is None:
                continue

            measurement_time = (
                record["stan_wody_data_pomiaru"]
                .replace(" ", "T")
                + "Z"
            )

            message = {
                "gauge_id": f"H_{record['id_stacji']}",
                "measurement_timestamp": measurement_time,
                "water_level_cm": int(record["stan_wody"]),
                "flow_m3s": (
                    float(record["przeplyw"])
                    if record["przeplyw"] is not None
                    else None
                ),
                "is_warning": False,
                "ingestion_timestamp":
                    datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            }

            messages.append(message)

        except Exception as e:
            print("BĹ‚Ä…d rekordu hydro:", e)

    return messages
def run_once():
    print("Uruchamiam pobieranie danych...")

    synop_data = fetch_synop()

    print("Liczba rekordĂłw synop:", len(synop_data))

    sent_synop = 0

    for message in synop_data:

        dedup_key = (
            message["station_id"]
            + "|"
            + message["measurement_timestamp"]
        )

        if dedup_key in seen_synop:
            continue

        producer.send(
            "synop",
            key=message["station_id"],
            value=message
        )

        save_seen("seen_synop.txt", dedup_key)
        seen_synop.add(dedup_key)

        sent_synop += 1

    producer.flush()

    print(f"WysĹ‚ano nowych rekordĂłw synop: {sent_synop}")

    hydro_data = fetch_hydro()

    print("Liczba rekordĂłw hydro:", len(hydro_data))

    sent_hydro = 0

    for message in hydro_data:

        dedup_key = (
            message["gauge_id"]
            + "|"
            + message["measurement_timestamp"]
        )

        if dedup_key in seen_hydro:
            continue

        producer.send(
            "hydro",
            key=message["gauge_id"],
            value=message
        )

        save_seen("seen_hydro.txt", dedup_key)
        seen_hydro.add(dedup_key)

        sent_hydro += 1

    producer.flush()

    print(f"WysĹ‚ano nowych rekordĂłw hydro: {sent_hydro}")
seen_synop = load_seen("seen_synop.txt")
seen_hydro = load_seen("seen_hydro.txt")

producer = KafkaProducer(
    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

while True:

    try:
        run_once()

    except Exception as e:
        print("BĹ‚Ä…d podczas dziaĹ‚ania producenta:", e)

    print("Czekam 10 minut do nastÄ™pnego pobrania...")
    time.sleep(600)
