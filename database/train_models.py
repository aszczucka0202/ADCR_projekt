import pandas as pd
import joblib
from sklearn.linear_model import LinearRegression
from models import engine


def prepare_data_and_train():
    print("-> [Trening] Rozpoczynam pobieranie historii...")
    query = "SELECT gauge_id, measurement_timestamp, water_level_cm FROM hydro_measurements ORDER BY measurement_timestamp;"
    df = pd.read_sql(query, engine)

    if len(df) < 15:
        print("-> [Trening] Za mało danych w bazie. Czekam na więcej strumienia z Kafki.")
        return False

    df['measurement_timestamp'] = pd.to_datetime(df['measurement_timestamp'])
    models_t2, models_t3 = {}, {}
    trained_stations = 0

    for gauge, group in df.groupby('gauge_id'):
        group = group.set_index('measurement_timestamp').sort_index()
        group['lag_0'] = group['water_level_cm']
        group['lag_1'] = group['water_level_cm'].shift(1)
        group['lag_2'] = group['water_level_cm'].shift(2)
        group['target_2h'] = group['water_level_cm'].shift(-2)
        group['target_3h'] = group['water_level_cm'].shift(-3)

        train_df = group.dropna()

        if len(train_df) < 5:
            continue

        X = train_df[['lag_0', 'lag_1', 'lag_2']]

        model_2h = LinearRegression()
        model_2h.fit(X, train_df['target_2h'])
        models_t2[gauge] = model_2h

        model_3h = LinearRegression()
        model_3h.fit(X, train_df['target_3h'])
        models_t3[gauge] = model_3h

        trained_stations += 1

    if trained_stations > 0:
        joblib.dump(models_t2, 'models_t2.pkl')
        joblib.dump(models_t3, 'models_t3.pkl')
        print(f"-> [Trening] SUKCES! Przeuczono modele dla {trained_stations} stacji.")
        return True
    else:
        print("-> [Trening] Żadna stacja nie ma jeszcze pełnych 5 odczytów.")
        return False


if __name__ == "__main__":
    prepare_data_and_train()