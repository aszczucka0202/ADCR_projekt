# 📜 Kontrakty komunikatów (Kafka) — wersja zamrożona v1.0

> **To jest umowa interfejsu między modułami.** Zmiany tylko za zgodą Lidera
> (Osoba 1) i po ogłoszeniu zespołowi. Wszyscy producenci/konsumenci trzymają
> się dokładnie tych pól.

## Topiki i klucze

| Topik    | Producent | Konsumenci      | Klucz wiadomości (Kafka key) |
| -------- | --------- | --------------- | ---------------------------- |
| `synop`  | Osoba 2   | Osoba 3, Osoba 5 | `station_id`                 |
| `hydro`  | Osoba 2   | Osoba 4, Osoba 5 | `gauge_id`                   |
| `alerts` | Osoba 3, Osoba 4 | Osoba 6, Osoba 5 | `alert_id`              |

**Czas:** wszystkie znaczniki w UTC, format ISO-8601 `YYYY-MM-DDTHH:MM:SSZ`.
**Deduplikacja (Osoba 2):** klucz logiczny = `station_id`/`gauge_id` + `measurement_timestamp`.

---

## 1) POGODA — topik `synop`

```json
{
  "station_id": "IMGW_126300",
  "measurement_timestamp": "2026-03-20T10:00:00Z",
  "temperature": 14.5,
  "humidity": 65,
  "pressure_hpa": 1008.3,
  "wind_speed": 12.0,
  "ingestion_timestamp": "2026-03-20T10:05:00Z"
}
```

| Pole                    | Typ     | Uwaga                                                      |
| ----------------------- | ------- | ---------------------------------------------------------- |
| `station_id`            | string  | `IMGW_` + `id_stacji`                                      |
| `measurement_timestamp` | string  | **DODANE** — czas pomiaru IMGW (`data_pomiaru`+`godzina_pomiaru`). Event-time dla okien Sparka. |
| `temperature`           | float   | °C, z `temperatura`                                        |
| `humidity`              | int     | %, z `wilgotnosc_wzgledna`                                 |
| `pressure_hpa`          | float   | **DODANE** — hPa, z `cisnienie`. Wymagane przez Osobę 3.   |
| `wind_speed`            | float   | m/s, z `predkosc_wiatru`                                   |
| `ingestion_timestamp`   | string  | czas wpłynięcia do producenta                              |

> Pola IMGW bywają puste (`null`) — konsument musi to obsłużyć (pomijać/oznaczać).

---

## 2) WODA — topik `hydro`

```json
{
  "gauge_id": "H_442",
  "measurement_timestamp": "2026-03-20T10:00:00Z",
  "water_level_cm": 245,
  "flow_m3s": 12.5,
  "is_warning": false,
  "ingestion_timestamp": "2026-03-20T10:05:00Z"
}
```

| Pole                    | Typ            | Uwaga                                                    |
| ----------------------- | -------------- | -------------------------------------------------------- |
| `gauge_id`              | string         | `H_` + `id_stacji`                                       |
| `measurement_timestamp` | string         | **DODANE** — z `stan_wody_data_pomiaru`. Event-time.     |
| `water_level_cm`        | int            | z `stan_wody`. Główny sygnał dla Osoby 4.                |
| `flow_m3s`              | float \| null  | przepływ — **w API hydro często niedostępny → null**.    |
| `is_warning`            | bool           | wartość WYLICZANA (próg), nie pochodzi wprost z IMGW.    |
| `ingestion_timestamp`   | string         | czas wpłynięcia                                          |

---

## 3) ALERTY — topik `alerts`

```json
{
  "alert_id": "WAR_009",
  "alert_type": "storm",
  "source": "spark-weather",
  "station_id": "IMGW_126300",
  "lat": 51.10,
  "lon": 17.03,
  "severity": "Yellow",
  "msg": "Spadek ciśnienia 9 hPa/3h — ryzyko wichury",
  "value": 9.0,
  "event_timestamp": "2026-03-20T10:05:00Z",
  "expires": "2026-03-21T00:00:00Z",
  "ingestion_timestamp": "2026-03-20T10:05:00Z"
}
```

| Pole              | Typ     | Uwaga                                                          |
| ----------------- | ------- | -------------------------------------------------------------- |
| `alert_id`        | string  | unikalny ID alertu                                             |
| `alert_type`      | enum    | **DODANE** — `storm` \| `flood` (rozszerzalne)                 |
| `source`          | string  | **DODANE** — `spark-weather` \| `spark-hydro`                  |
| `station_id`      | string  | **DODANE** — id stacji/wodowskazu, którego dotyczy alert       |
| `lat`, `lon`      | float   | **DODANE** — do naniesienia na mapę (Osoba 6)                  |
| `severity`        | enum    | `Yellow` \| `Orange` \| `Red`                                  |
| `msg`             | string  | treść dla użytkownika                                          |
| `value`           | float   | **DODANE** — wartość, która wywołała alert (opcjonalna)        |
| `event_timestamp` | string  | **DODANE** — kiedy wykryto                                     |
| `expires`         | string  | do kiedy alert ważny                                           |
| `ingestion_timestamp` | string | spójne z resztą                                            |

> **Współrzędne stacji:** ustal jedno źródło prawdy — np. mały plik
> `stations.csv` (id → lat/lon) wstrzykiwany do jobów Sparka, żeby Osoby 3 i 4
> dokładały `lat`/`lon` jednolicie.
