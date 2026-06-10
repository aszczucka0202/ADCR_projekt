from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import ForeignKey

Base = declarative_base()


class HydroMeasurement(Base):
    __tablename__ = 'hydro_measurements'

    id = Column(Integer, primary_key=True, autoincrement=True)
    gauge_id = Column(String(50), nullable=False)
    measurement_timestamp = Column(DateTime, nullable=False)
    water_level_cm = Column(Integer, nullable=True)
    flow_m3s = Column(Float, nullable=True)
    is_warning = Column(Boolean, default=False)
    ingestion_timestamp = Column(DateTime, default=datetime.utcnow)

    # Zabezpieczenie przed duplikatami: unikalna para stacja + czas pomiaru
    __table_args__ = (
        UniqueConstraint('gauge_id', 'measurement_timestamp', name='_gauge_timestamp_uc'),
    )


class SynopMeasurement(Base):
    __tablename__ = 'synop_measurements'

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String(50), nullable=False)
    measurement_timestamp = Column(DateTime, nullable=False)
    temperature = Column(Float, nullable=True)
    humidity = Column(Integer, nullable=True)
    pressure_hpa = Column(Float, nullable=True)
    wind_speed = Column(Float, nullable=True)
    ingestion_timestamp = Column(DateTime, default=datetime.utcnow)

    # Zabezpieczenie przed duplikatami: unikalna para stacja + czas pomiaru
    __table_args__ = (
        UniqueConstraint('station_id', 'measurement_timestamp', name='_station_timestamp_uc'),
    )


class AlertRecord(Base):
    __tablename__ = 'alerts_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    alert_id = Column(String(100), unique=True, nullable=False)  # np. STORM_IMGW_12295_2026060918
    alert_type = Column(String(50))
    source = Column(String(50))
    station_id = Column(String(50))
    lat = Column(Float)
    lon = Column(Float)
    severity = Column(String(20))  # Red, Orange, Yellow
    msg = Column(String(255))
    value = Column(Float)
    event_timestamp = Column(DateTime)
    expires = Column(DateTime)
    ingestion_timestamp = Column(DateTime)

class HydroPrediction(Base):
    __tablename__ = 'hydro_predictions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    gauge_id = Column(String(50), nullable=False)
    prediction_timestamp = Column(DateTime, nullable=False)  # Czas, NA KTÓRY robiona jest prognoza
    predicted_water_level = Column(Float, nullable=False)  # Wyliczona wartość
    horizon_hours = Column(Integer, nullable=False)  # 2 lub 3 (horyzont)
    created_at = Column(DateTime, default=datetime.utcnow)  # Kiedy model wypluł tę prognozę

    # Unikalność: dla danej stacji i konkretnej godziny w przyszłości mamy tylko jedną prognozę o danym horyzoncie
    __table_args__ = (
        UniqueConstraint('gauge_id', 'prediction_timestamp', 'horizon_hours', name='_gauge_pred_timestamp_uc'),
    )


# Dane połączenia

import os

DB_USER = os.getenv("POSTGRES_USER", "rta")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "rta_pass")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost") # Docker nadpisze to na 'postgres'
DB_NAME = os.getenv("POSTGRES_DB", "rta_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"

engine = create_engine(DATABASE_URL, echo=True) # echo = True pokaże wygenerowane SQL w konsoli
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    # Tworzy tabele w bazie, jeśli jeszcze nie istnieją
    Base.metadata.create_all(bind=engine)