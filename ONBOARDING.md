# Onboarding - jak podlaczyc swoj modul

## 1. Postaw infrastrukture
docker compose up -d
docker compose ps   # broker healthy + topiki: synop, hydro, alerts
# Stack dziala bez .env. Wlasne haslo Postgresa: skopiuj .env.example do .env.

## 2. Adresy polaczen
- Kafka z KONTENERA:  broker:9092
- Kafka z HOSTA:      localhost:29092
- Postgres z kontenera: postgres:5432  (z hosta: localhost:5432)
- Topiki: synop, hydro, alerts
- Format wiadomosci: CONTRACTS.md (ZAMROZONE - nie zmieniamy)

## 3. Twoj zakres
| Osoba | Wejscie       | Wyjscie      | Katalog       | Kluczowe env |
|-------|---------------|--------------|---------------|--------------|
| 2     | API IMGW      | synop, hydro | producer      | KAFKA_BOOTSTRAP, SYNOP_TOPIC, HYDRO_TOPIC |
| 3     | synop         | alerts       | spark-weather | KAFKA_BOOTSTRAP, SOURCE_TOPIC=synop, ALERTS_TOPIC=alerts |
| 4     | hydro         | alerts       | spark-hydro   | KAFKA_BOOTSTRAP, SOURCE_TOPIC=hydro, ALERTS_TOPIC=alerts |
| 5     | synop, hydro  | Postgres     | database      | POSTGRES_HOST=postgres, POSTGRES_USER/PASSWORD/DB |
| 6     | alerts        | dashboard    | dashboard     | KAFKA_BOOTSTRAP, ALERTS_TOPIC=alerts, POSTGRES_HOST=postgres |

## 4. Workflow Git (bez Pull Requestow)
# feat/<modul> to nazwa GALEZI, nie folder. Pracujesz tylko w SWOIM katalogu.
git checkout main
git pull
git checkout -b feat/<modul>        # np. feat/producer
# ... praca w swoim katalogu ...
git add .
git commit -m "feat: opis"
git push -u origin feat/<modul>
# gdy modul DZIALA u Ciebie, scal do main samodzielnie:
git checkout main
git pull
git merge feat/<modul>
git push origin main

## 5. Zasady (wazne!)
- ZAWSZE "git pull" przed "git push".
- NIE edytuj docker-compose.yml - to plik Lidera. Gdy modul dziala, zglos Liderowi.
- NIE zmieniaj CONTRACTS.md.
- Pracuj wylacznie w swoim katalogu.
- Spark (Osoby 3/4): tryb local[*], pakiet org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
