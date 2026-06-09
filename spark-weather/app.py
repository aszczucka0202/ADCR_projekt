"""
spark-weather/app.py  —  DETEKCJA WICHUR (Osoba 3)
===================================================
Zgodny z CONTRACTS.md v1.0 (ZAMROŻONY).

Wejście:  topik "synop"   (klucz: station_id)
Wyjście:  topik "alerts"  (klucz: alert_id, source = "spark-weather")

Logika: w przesuwnym oknie 3h liczymy spadek ciśnienia (pressure_hpa) między
początkiem a końcem okna dla każdej stacji. Reagujemy też na silny wiatr.

Współrzędne lat/lon NIE są w topiku synop — dokładamy je z pliku stations.csv
(jedno źródło prawdy, zgodnie z notką w kontrakcie). CSV: kolumny id,lat,lon.

Uruchomienie (wersję pakietu dopasuj do Sparka z docker-compose):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 app.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType)

# ---------------------------- KONFIGURACJA ----------------------------
# broker:9092 = z wnętrza sieci Dockera (tak działają kontenery Sparka);
# z hosta byłoby localhost:29092. Można nadpisać zmienną środowiskową.
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:9092")
STATIONS_CSV = os.getenv("STATIONS_CSV", "stations.csv")
TOPIC_IN = os.getenv("SOURCE_TOPIC", "synop")     # wg ONBOARDING.md
TOPIC_OUT = os.getenv("ALERTS_TOPIC", "alerts")   # wg ONBOARDING.md
CHECKPOINT = os.getenv("CHECKPOINT_WEATHER", "/tmp/checkpoints/weather")

# Progi spadku ciśnienia [hPa] / wiatru [m/s] -> łatwo obniżyć na demo
DROP_YELLOW, DROP_ORANGE, DROP_RED = 3.0, 6.0, 9.0
WIND_YELLOW, WIND_ORANGE, WIND_RED = 15.0, 20.0, 25.0

OKNO, PRZESUNIECIE, WATERMARK = "3 hours", "1 hour", "1 hour"
WAZNOSC_ALERTU = "INTERVAL 12 HOURS"   # po jakim czasie alert wygasa (expires)

# ----------------- SCHEMAT topiku "synop" wg kontraktu ----------------
SCHEMA_SYNOP = StructType([
    StructField("station_id", StringType()),
    StructField("measurement_timestamp", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", IntegerType()),
    StructField("pressure_hpa", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("ingestion_timestamp", StringType()),
])


def main():
    spark = (SparkSession.builder
             .appName("RTA-Wichury")
             .master(os.getenv("SPARK_MASTER", "local[*]"))   # wg ONBOARDING.md
             .config("spark.sql.session.timeZone", "UTC")   # kontrakt: wszystko w UTC
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Słownik współrzędnych stacji (statyczny) — id,lat,lon
    stacje = (spark.read.option("header", True).csv(STATIONS_CSV)
              .select(F.col("id"),
                      F.col("lat").cast("double").alias("lat"),
                      F.col("lon").cast("double").alias("lon")))

    # 1) Strumień z Kafki
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC_IN)
           .option("startingOffsets", "latest")
           .load())

    # 2) Parsowanie JSON
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA_SYNOP).alias("d"))
                 .select("d.*"))

    # 3) Event-time z measurement_timestamp (ISO-8601 UTC, np. 2026-03-20T10:00:00Z)
    with_time = (parsed
                 .withColumn("event_time", F.to_timestamp(F.col("measurement_timestamp")))
                 .filter(F.col("pressure_hpa").isNotNull() & F.col("event_time").isNotNull()))

    # 4) Okno + agregacja per stacja
    agg = (with_time
           .withWatermark("event_time", WATERMARK)
           .groupBy(F.window("event_time", OKNO, PRZESUNIECIE), F.col("station_id"))
           .agg(F.expr("min_by(pressure_hpa, event_time)").alias("p_start"),
                F.expr("max_by(pressure_hpa, event_time)").alias("p_koniec"),
                F.max("wind_speed").alias("wind_max")))

    # 5) Spadek (dodatni = ciśnienie spadło) + severity wg kontraktu
    scored = (agg
              .withColumn("drop", F.col("p_start") - F.col("p_koniec"))
              .withColumn("severity",
                  F.when((F.col("drop") >= DROP_RED) | (F.col("wind_max") >= WIND_RED), F.lit("Red"))
                   .when((F.col("drop") >= DROP_ORANGE) | (F.col("wind_max") >= WIND_ORANGE), F.lit("Orange"))
                   .when((F.col("drop") >= DROP_YELLOW) | (F.col("wind_max") >= WIND_YELLOW), F.lit("Yellow"))
                   .otherwise(F.lit(None))))
    alerty = scored.filter(F.col("severity").isNotNull())

    # 6) Dokładamy lat/lon (left join ze statycznym słownikiem)
    alerty = (alerty.join(F.broadcast(stacje),
                          alerty.station_id == stacje.id, "left")
                    .drop("id"))

    # 7) Budujemy payload DOKŁADNIE wg kontraktu (topik alerts)
    payload = alerty.select(
        F.col("station_id").alias("_sid"),
        F.col("severity"), F.col("drop"), F.col("wind_max"),
        F.col("lat"), F.col("lon"), F.col("window")
    ).select(
        F.concat_ws("_", F.lit("STORM"), F.col("_sid"),
                    F.date_format(F.col("window.start"), "yyyyMMddHH")).alias("alert_id"),
        F.lit("storm").alias("alert_type"),
        F.lit("spark-weather").alias("source"),
        F.col("_sid").alias("station_id"),
        F.col("lat"), F.col("lon"),
        F.col("severity"),
        F.when(F.col("drop") >= DROP_YELLOW,
               F.concat(F.lit("Spadek ciśnienia "), F.round("drop", 1), F.lit(" hPa/3h — ryzyko wichury")))
         .otherwise(F.concat(F.lit("Silny wiatr "), F.round("wind_max", 1), F.lit(" m/s — ryzyko wichury"))).alias("msg"),
        F.when(F.col("drop") >= DROP_YELLOW, F.round("drop", 1)).otherwise(F.round("wind_max", 1)).alias("value"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_timestamp"),
        F.date_format(F.current_timestamp() + F.expr(WAZNOSC_ALERTU), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("expires"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("ingestion_timestamp"),
    )

    # 8) Do Kafki: key = alert_id, value = JSON całego rekordu
    out = payload.select(
        F.col("alert_id").alias("key"),
        F.to_json(F.struct("*")).alias("value"))

    q_kafka = (out.writeStream.format("kafka")
               .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
               .option("topic", TOPIC_OUT)
               .option("checkpointLocation", CHECKPOINT)
               .outputMode("append").start())

    # Podgląd na demo
    q_console = (payload.select("alert_id", "severity", "value", "msg")
                 .writeStream.format("console").option("truncate", False)
                 .outputMode("append").start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
