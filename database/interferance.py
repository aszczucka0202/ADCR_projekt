import time
import joblib
import pandas as pd
from datetime import timedelta, datetime
from sqlalchemy.orm import sessionmaker
from models import engine, HydroPrediction
import os

from train_models import prepare_data_and_train

SessionLocal = sessionmaker(bind=engine)


def run_inference():
    db = SessionLocal()
    cycle_counter = 0

    while True:
        print(f"\n=== CYKL PREDYKCYJNY #{cycle_counter} ===")

        # AUTOMATYZACJA: Co 6 cykli (ok. godzinę) lub gdy brakuje plików, uruchamiam retraining
        if cycle_counter % 6 == 0 or not os.path.exists('models_t2.pkl'):
            print("[Auto-ML] Sprawdzanie nowych stacji i aktualizacja wag modeli...")
            prepare_data_and_train()

        cycle_counter += 1

        try:
            models_t2 = joblib.load('models_t2.pkl')
            models_t3 = joblib.load('models_t3.pkl')
        except FileNotFoundError:
            print("Modele nadal niedostępne. Zasypianie na 5 minut...")
            time.sleep(300)
            continue

        # ZAAWANSOWANY SQL: Pobiera 3 najnowsze odczyty dla KAŻDEJ stacji jednym, wydajnym zapytaniem
        query = """
                SELECT gauge_id, measurement_timestamp, water_level_cm
                FROM (SELECT gauge_id, \
                             measurement_timestamp, \
                             water_level_cm, \
                             ROW_NUMBER() OVER(PARTITION BY gauge_id ORDER BY measurement_timestamp DESC) as rn \
                      FROM hydro_measurements) t
                WHERE rn <= 3; \
                """
        try:
            recent_data = pd.read_sql(query, engine)
        except Exception as e:
            print(f"Błąd bazy: {e}")
            time.sleep(60)
            continue

        stations_predicted = 0

        # Generowanie prognoz
        for gauge_id in models_t2.keys():
            df_station = recent_data[recent_data['gauge_id'] == gauge_id].sort_values('measurement_timestamp',
                                                                                      ascending=False)

            if len(df_station) < 3:
                continue

            lag_0 = df_station.iloc[0]['water_level_cm']
            lag_1 = df_station.iloc[1]['water_level_cm']
            lag_2 = df_station.iloc[2]['water_level_cm']
            current_time = pd.to_datetime(df_station.iloc[0]['measurement_timestamp'])

            features = pd.DataFrame([[lag_0, lag_1, lag_2]], columns=['lag_0', 'lag_1', 'lag_2'])

            pred_2h = max(0, float(models_t2[gauge_id].predict(features)[0]))
            pred_3h = max(0, float(models_t3[gauge_id].predict(features)[0]))

            time_2h = current_time + timedelta(hours=2)
            time_3h = current_time + timedelta(hours=3)

            p1 = HydroPrediction(gauge_id=gauge_id, prediction_timestamp=time_2h, predicted_water_level=pred_2h,
                                 horizon_hours=2)
            p2 = HydroPrediction(gauge_id=gauge_id, prediction_timestamp=time_3h, predicted_water_level=pred_3h,
                                 horizon_hours=3)

            try:
                db.merge(p1)
                db.merge(p2)
                db.commit()
                stations_predicted += 1
            except Exception as e:
                db.rollback()

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Zapisano nowe prognozy dla {stations_predicted} stacji.")
        print("Zasypianie na 5 minut w oczekiwaniu na strumień z IMGW...")
        time.sleep(300)


if __name__ == "__main__":
    run_inference()