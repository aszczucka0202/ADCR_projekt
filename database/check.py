import pandas as pd
from models import engine

df_check2 = pd.read_sql("SELECT * FROM hydro_predictions", engine)
print(df_check2[['gauge_id', 'prediction_timestamp', 'predicted_water_level', 'horizon_hours']])
df_check2.to_csv("check2.csv", index=False)