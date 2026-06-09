"""
spark-hydro/app.py  —  DETEKCJA PODTOPIEŃ (Osoba 4)
====================================================
Zgodny z CONTRACTS.md v1.0 (ZAMROŻONY).

Wejście:  topik "hydro"   (klucz: gauge_id)
Wyjście:  topik "alerts"  (klucz: alert_id, source = "spark-hydro")

Logika: w przesuwnym oknie 6h liczymy przyrost poziomu wody (water_level_cm)
między początkiem a końcem okna dla każdego wodowskazu.

Uwaga: w alercie pole nazywa się "station_id" (wspólne dla obu źródeł), ale
wstawiamy tam wartość gauge_id — zgodnie z kontraktem ("id stacji/wodowskazu").
Współrzędne lat/lon dokładamy z tego samego stations.csv co Osoba 3.

Uruchomienie:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 app.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               DoubleType, IntegerType, BooleanType)

# ---------------------------- KONFIGURACJA ----------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "broker:9092")
STATIONS_CSV = os.getenv("STATIONS_CSV", "stations.csv")
TOPIC_IN = os.getenv("SOURCE_TOPIC", "hydro")     # wg ONBOARDING.md
TOPIC_OUT = os.getenv("ALERTS_TOPIC", "alerts")   # wg ONBOARDING.md
CHECKPOINT = os.getenv("CHECKPOINT_HYDRO", "/tmp/checkpoints/hydro")

# Progi przyrostu poziomu wody [cm] -> łatwo obniżyć na demo
RISE_YELLOW, RISE_ORANGE, RISE_RED = 30.0, 60.0, 100.0

OKNO, PRZESUNIECIE, WATERMARK = "6 hours", "1 hour", "2 hours"
WAZNOSC_ALERTU = "INTERVAL 24 HOURS"

# ----------------- SCHEMAT topiku "hydro" wg kontraktu ----------------
SCHEMA_HYDRO = StructType([
    StructField("gauge_id", StringType()),
    StructField("measurement_timestamp", StringType()),
    StructField("water_level_cm", IntegerType()),
    StructField("flow_m3s", DoubleType()),          # bywa null
    StructField("is_warning", BooleanType()),
    StructField("ingestion_timestamp", StringType()),
])


def main():
    spark = (SparkSession.builder
             .appName("RTA-Podtopienia")
             .master(os.getenv("SPARK_MASTER", "local[*]"))   # wg ONBOARDING.md
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    stacje = (spark.read.option("header", True).csv(STATIONS_CSV)
              .select(F.col("id"),
                      F.col("lat").cast("double").alias("lat"),
                      F.col("lon").cast("double").alias("lon")))

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC_IN)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA_HYDRO).alias("d"))
                 .select("d.*"))

    with_time = (parsed
                 .withColumn("event_time", F.to_timestamp(F.col("measurement_timestamp")))
                 .filter(F.col("water_level_cm").isNotNull() & F.col("event_time").isNotNull()))

    agg = (with_time
           .withWatermark("event_time", WATERMARK)
           .groupBy(F.window("event_time", OKNO, PRZESUNIECIE), F.col("gauge_id"))
           .agg(F.expr("min_by(water_level_cm, event_time)").alias("lvl_start"),
                F.expr("max_by(water_level_cm, event_time)").alias("lvl_koniec")))

    scored = (agg
              .withColumn("rise", F.col("lvl_koniec") - F.col("lvl_start"))
              .withColumn("severity",
                  F.when(F.col("rise") >= RISE_RED, F.lit("Red"))
                   .when(F.col("rise") >= RISE_ORANGE, F.lit("Orange"))
                   .when(F.col("rise") >= RISE_YELLOW, F.lit("Yellow"))
                   .otherwise(F.lit(None))))
    alerty = scored.filter(F.col("severity").isNotNull())

    alerty = (alerty.join(F.broadcast(stacje),
                          alerty.gauge_id == stacje.id, "left")
                    .drop("id"))

    payload = alerty.select(
        F.col("gauge_id").alias("_gid"),
        F.col("severity"), F.col("rise"), F.col("lvl_koniec"),
        F.col("lat"), F.col("lon"), F.col("window")
    ).select(
        F.concat_ws("_", F.lit("FLOOD"), F.col("_gid"),
                    F.date_format(F.col("window.start"), "yyyyMMddHH")).alias("alert_id"),
        F.lit("flood").alias("alert_type"),
        F.lit("spark-hydro").alias("source"),
        F.col("_gid").alias("station_id"),         # wg kontraktu pole nazywa się station_id
        F.col("lat"), F.col("lon"),
        F.col("severity"),
        F.concat(F.lit("Wzrost poziomu wody "), F.round("rise", 0),
                 F.lit(" cm/6h — ryzyko podtopienia (poziom "), F.col("lvl_koniec"), F.lit(" cm)")).alias("msg"),
        F.round("rise", 0).cast("double").alias("value"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_timestamp"),
        F.date_format(F.current_timestamp() + F.expr(WAZNOSC_ALERTU), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("expires"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("ingestion_timestamp"),
    )

    out = payload.select(
        F.col("alert_id").alias("key"),
        F.to_json(F.struct("*")).alias("value"))

    q_kafka = (out.writeStream.format("kafka")
               .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
               .option("topic", TOPIC_OUT)
               .option("checkpointLocation", CHECKPOINT)
               .outputMode("append").start())

    q_console = (payload.select("alert_id", "severity", "value", "msg")
                 .writeStream.format("console").option("truncate", False)
                 .outputMode("append").start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
