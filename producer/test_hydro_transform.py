import requests
from datetime import datetime, timezone

url = "https://danepubliczne.imgw.pl/api/data/hydro"

response = requests.get(url)

data = response.json()

record = data[0]

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
    "ingestion_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
}

print(message)