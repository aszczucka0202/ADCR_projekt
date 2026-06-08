import requests
from datetime import datetime, timezone

url = "https://danepubliczne.imgw.pl/api/data/synop"

response = requests.get(url)

data = response.json()

record = data[0]

message = {
    "station_id": f"IMGW_{record['id_stacji']}",
    "measurement_timestamp": f"{record['data_pomiaru']}T{record['godzina_pomiaru'].zfill(2)}:00:00Z",
    "temperature": float(record["temperatura"]),
    "humidity": int(float(record["wilgotnosc_wzgledna"])),
    "pressure_hpa": float(record["cisnienie"]),
    "wind_speed": float(record["predkosc_wiatru"]),
    "ingestion_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
}

print(message)