"""
make_stations_hydro.py  —  generator slownika wspolrzednych dla Osoby 4
=======================================================================
Dane hydro z IMGW zawieraja wprost pola lon/lat dla kazdego wodowskazu,
wiec slownik budujemy automatycznie z API (jest ich kilkaset - nie ma sensu
wpisywac recznie). Wynik: stations_hydro.csv z kolumnami id,lat,lon,name.

id = "H_" + id_stacji  (zgodnie z CONTRACTS.md v1.0).
Nazwy zapisywane bez polskich znakow (transliteracja na ASCII).

Uruchomienie:
  pip install requests
  python make_stations_hydro.py
"""

import csv
import requests

URL_HYDRO = "https://danepubliczne.imgw.pl/api/data/hydro"
OUT = "stations_hydro.csv"

# zamiana polskich znakow na ASCII
_PL = str.maketrans({
    "a": "a", "c": "c", "e": "e", "l": "l", "n": "n",
    "o": "o", "s": "s", "z": "z",
    "\u0105": "a", "\u0107": "c", "\u0119": "e", "\u0142": "l", "\u0144": "n",
    "\u00f3": "o", "\u015b": "s", "\u017c": "z", "\u017a": "z",
    "\u0104": "A", "\u0106": "C", "\u0118": "E", "\u0141": "L", "\u0143": "N",
    "\u00d3": "O", "\u015a": "S", "\u017b": "Z", "\u0179": "Z",
})


def ascii_pl(s):
    return (s or "").translate(_PL)


def main():
    print("Pobieram wodowskazy z IMGW...")
    data = requests.get(URL_HYDRO, timeout=30).json()

    rows = []
    for rec in data:
        lat = rec.get("lat")
        lon = rec.get("lon")
        # pomijamy stacje bez wspolrzednych (nie da sie ich naniesc na mape)
        if not lat or not lon:
            continue
        nazwa = f'{rec.get("stacja", "")} ({rec.get("rzeka", "")})'
        rows.append({
            "id": "H_" + str(rec.get("id_stacji")),
            "lat": lat,
            "lon": lon,
            "name": ascii_pl(nazwa),
        })

    with open(OUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "lat", "lon", "name"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Zapisano {len(rows)} wodowskazow do {OUT}")


if __name__ == "__main__":
    main()
