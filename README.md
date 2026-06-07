# Projekt RTA 2026 - Analiza strumieniowa danych IMGW

Potok danych w czasie rzeczywistym: dane meteo i hydro z API IMGW -> Kafka ->
Spark (wichury, podtopienia) -> alerty + magazyn historyczny + dashboard.

## Dokumentacja
- CONTRACTS.md  - format wiadomosci Kafki (ZAMROZONY)
- ONBOARDING.md - jak podlaczyc swoj modul

## Architektura
IMGW API
  -> producer            (Osoba 2)
  -> Kafka: synop, hydro
       -> spark-weather  (Osoba 3) -> Kafka: alerts
       -> spark-hydro    (Osoba 4) -> Kafka: alerts
       -> Postgres       (Osoba 5: historia + model)
  -> dashboard           (Osoba 6: mapa, wykresy, panel alertow)

## Jak postawic od zera
Wymagania: Docker Desktop, Git.
1. git clone https://github.com/aszczucka0202/ADCR_projekt.git
2. cd ADCR_projekt
3. (opcjonalnie) skopiuj .env.example do .env i ustaw wlasne haslo Postgresa
4. docker compose up -d
5. docker compose ps           # broker powinien byc "healthy"
6. Otworz http://localhost:8080 - w Kafka UI beda topiki: synop, hydro, alerts

## Adresy
- Kafka z kontenera:  broker:9092   | z hosta: localhost:29092
- Postgres:           postgres:5432 | z hosta: localhost:5432
- Kafka UI:           http://localhost:8080
- Dashboard:          http://localhost:8501

## Zatrzymanie
docker compose stop        # pauza, zachowuje wszystko
docker compose down        # usuwa kontenery, ZACHOWUJE dane
docker compose down -v     # czysty reset (USUWA dane)

## Praca w zespole
Kazdy pracuje w swoim katalogu na galezi feat/<modul> i scala do main.
Plik docker-compose.yml prowadzi Lider - zglos mu gotowy modul do podlaczenia.
