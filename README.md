# JSON contract POGODA 
{
  "station_id": "IMGW_126300",
  "temperature": 14.5,
  "humidity": 65,
  "wind_speed": 12,
  "ingestion_timestamp": "2026-03-20T10:05:00Z"
}

# JSON contract WODA 
{
  "gauge_id": "H_442",
  "water_level_cm": 245,
  "is_warning": false,
  "flow_m3s": 12.5,
  "ingestion_timestamp": "2026-03-20T10:05:00Z"
}

# JSON contract ALERTY 
{
  "alert_id": "WAR_009",
  "severity": "Yellow",
  "msg": "Gęsta mgła w dolinach",
  "expires": "2026-03-21T00:00:00Z",
  "ingestion_timestamp": "2026-03-20T10:05:00Z"
}





# Projekt Grupowy - Analiza Strumieniowa Danych IMGW (RTA 2026)

Celem projektu jest zbudowanie kompletnego potoku przetwarzania danych w czasie rzeczywistym (Real-Time Data Pipeline) opartego o dane meteorologiczne i hydrologiczne z publicznego API IMGW ([https://danepubliczne.imgw.pl/pl/apiinfo](https://danepubliczne.imgw.pl/pl/apiinfo)). System ma za zadanie wykrywać anomalie pogodowe (wichury) oraz zagrożenia hydrologiczne (podtopienia), zapisywać dane historyczne, generować modele predykcyjne oraz wizualizować stan alertów na żywo.

---

## 🛠️ Architektura i Podział Zadań w Zespole

Projekt wykorzystuje architekturę mikroserwisów uruchamianych za pomocą technologii Docker. Poniżej znajduje się podział odpowiedzialności wraz z przypisanymi katalogami w repozytorium:

* **Osoba 1 (Lider / DevOps / Integracja) — [Twoje Imię/Nick]**
    * *Odpowiedzialność:* Zarządzanie repozytorium, koordynacja prac, przygotowanie środowiska `docker-compose.yml` (Kafka, Spark, Postgres, Dashboard) oraz dokumentacji uruchomieniowej.
    * *Pliki:* Główny katalog, `docker-compose.yml`, `README.md`.
* **Osoba 2 (Producent danych: IMGW → Kafka)**
    * *Odpowiedzialność:* Serwis w Pythonie odpytujący API IMGW co ~10 minut, deduplikacja odczytów (klucz: `id_stacji` + `data_pomiaru`), obsługa błędów, wysyłka na topiki Kafki.
    * *Katalog:* `/producer`
* **Osoba 3 (Przetwarzanie strumieniowe: Pogoda / Wichury)**
    * *Odpowiedzialność:* Job Spark Structured Streaming analizujący topik synoptyczny. Detekcja spadków ciśnienia w oknach czasowych, wykrywanie silnego wiatru. Wysyłka alertów na topik `alerts`.
    * *Katalog:* `/spark-weather`
* **Osoba 4 (Przetwarzanie strumieniowe: Hydrologia / Podtopienia)**
    * *Odpowiedzialność:* Job Spark Structured Streaming analizujący topik hydrologiczny. Trend poziomu wód w oknach czasowych, porównanie z progami ostrzegawczymi/alarmowymi. Wysyłka alertów na topik `alerts`.
    * *Katalog:* `/spark-hydro`
* **Osoba 5 (Warstwa danych i model predykcyjny)**
    * *Odpowiedzialność:* Zapis danych historycznych do bazy (Postgres/Parquet). Budowa modelu predykcyjnego (np. prognoza poziomu rzeki na kolejne godziny na podstawie szeregów czasowych).
    * *Katalog:* `/database`
* **Osoba 6 (BI, Wizualizacja i Alerty końcowe)**
    * *Odpowiedzialność:* Dashboard (Grafana/Streamlit) prezentujący mapę stacji, wykresy parametrów oraz panel aktywnych ostrzeżeń pobieranych z topiku `alerts`.
    * *Katalog:* `/dashboard`

---

## 📂 Struktura Repozytorium (Monorepo)

```text
ADCR_projekt/
├── dashboard/          # Kod źródłowy panelu wizualizacyjnego (Osoba 6)
├── database/           # Skrypty bazy danych i modele predykcyjne (Osoba 5)
├── producer/           # Kod źródłowy producenta danych w Pythonie (Osoba 2)
├── spark-hydro/        # Analiza strumieniowa - hydrologia (Osoba 4)
├── spark-weather/      # Analiza strumieniowa - pogoda (Osoba 3)
├── docker-compose.yml  # Główny plik orkiestracji usług (Osoba 1)
└── README.md           # Dokumentacja projektu
