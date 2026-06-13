import os
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timezone
from kafka import KafkaConsumer, TopicPartition
from sqlalchemy import create_engine, text
from streamlit_autorefresh import st_autorefresh

# ── Konfiguracja ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
ALERTS_TOPIC    = os.getenv("ALERTS_TOPIC", "alerts")
POSTGRES_HOST   = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT   = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER   = os.getenv("POSTGRES_USER", "rta")
POSTGRES_PASS   = os.getenv("POSTGRES_PASSWORD", "rta_pass")
POSTGRES_DB     = os.getenv("POSTGRES_DB", "rta_db")

# Okno czasu do wyświetlania — dostosuj jeśli trzeba
SYNOP_WINDOW_H  = 24   # ostatnie N godzin dla pogody
HYDRO_WINDOW_H  = 24   # ostatnie N godzin dla hydro

SEVERITY_COLOR = {"Yellow": "#D97706", "Orange": "#EA580C", "Red": "#DC2626"}
SEVERITY_PL    = {"Yellow": "Żółty", "Orange": "Pomarańczowy", "Red": "Czerwony"}
ALERT_TYPE_PL  = {"storm": "Burza", "flood": "Powódź"}

# ── Helpers ───────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def pg_engine():
    url = (f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}"
           f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    return create_engine(url, pool_pre_ping=True)


def _safe_deserialize(b: bytes):
    try:
        return json.loads(b.decode("utf-8", errors="replace"))
    except Exception:
        return None


def poll_new_alerts() -> list[dict]:
    msgs = []
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=_safe_deserialize,
            enable_auto_commit=False,
            group_id=None,
            consumer_timeout_ms=3000,
            request_timeout_ms=10000,
            session_timeout_ms=10000,
        )
        partitions = [TopicPartition(ALERTS_TOPIC, i) for i in range(3)]
        consumer.assign(partitions)
        consumer.poll(timeout_ms=0)
        consumer.seek_to_beginning(*partitions)
        for msg in consumer:
            if isinstance(msg.value, dict):
                msgs.append(msg.value)
        consumer.close()
    except StopIteration:
        pass
    except Exception:
        pass
    return msgs


@st.cache_resource(show_spinner=False)
def load_stations() -> pd.DataFrame:
    base = os.path.dirname(__file__)
    frames = []
    for fname, stype in [("stations_weather.csv", "weather"), ("stations_hydro.csv", "hydro")]:
        path = os.path.join(base, fname)
        if os.path.exists(path):
            df = pd.read_csv(path)
            df["type"] = stype
            frames.append(df)
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame(columns=["id", "lat", "lon", "name", "type"])


@st.cache_resource(show_spinner=False)
def station_name_map() -> dict:
    df = load_stations()
    return dict(zip(df["id"].astype(str), df["name"].astype(str)))


def query_postgres(sql: str, params: dict | None = None) -> pd.DataFrame:
    try:
        engine = pg_engine()
        with engine.connect() as conn:
            return pd.read_sql_query(text(sql), conn, params=params)
    except Exception as e:
        st.warning(f"Błąd połączenia z bazą: {e}")
        return pd.DataFrame()


def fmt_station(sid, names: dict) -> str:
    name = names.get(str(sid), "")
    return f"{name} ({sid})" if name and name != str(sid) else str(sid)


@st.cache_data(ttl=60, show_spinner=False)
def get_latest_synop() -> pd.DataFrame:
    """Najnowszy odczyt per stacja (do mapy)."""
    return query_postgres("""
        SELECT DISTINCT ON (station_id)
            station_id, temperature, humidity, pressure_hpa, wind_speed, measurement_timestamp
        FROM synop_measurements
        WHERE measurement_timestamp <= NOW()
        ORDER BY station_id, measurement_timestamp DESC
    """)


@st.cache_data(ttl=60, show_spinner=False)
def get_latest_hydro() -> pd.DataFrame:
    """Najnowszy odczyt per stacja (do mapy)."""
    return query_postgres("""
        SELECT DISTINCT ON (gauge_id)
            gauge_id, water_level_cm, flow_m3s, is_warning, measurement_timestamp
        FROM hydro_measurements
        WHERE measurement_timestamp <= NOW()
        ORDER BY gauge_id, measurement_timestamp DESC
    """)


# ── Sekcja: alerty ────────────────────────────────────────────────────────────

def section_alerts():
    st.subheader("⚡ Alerty live (Kafka)")

    if "alerts" not in st.session_state:
        st.session_state.alerts = {}
    if "_first_load" not in st.session_state:
        st.session_state._first_load = True

    prev_ids = set(st.session_state.alerts.keys())
    for alert in poll_new_alerts():
        aid = alert.get("alert_id")
        if aid:
            st.session_state.alerts[aid] = alert

    # Toast tylko dla nowych alertów (nie przy pierwszym ładowaniu)
    if not st.session_state._first_load:
        for aid in set(st.session_state.alerts.keys()) - prev_ids:
            a   = st.session_state.alerts[aid]
            sev = a.get("severity", "?")
            typ = ALERT_TYPE_PL.get(a.get("alert_type", ""), a.get("alert_type", "?"))
            st.toast(f"🚨 Nowy alert [{sev}]: {typ} — {a.get('msg', '')}", icon="⚡")
    st.session_state._first_load = False

    alerts_list = sorted(
        st.session_state.alerts.values(),
        key=lambda a: a.get("event_timestamp", ""),
        reverse=True,
    )

    if not alerts_list:
        st.info("Brak alertów w topiku.")
        return

    st.caption(f"Łącznie alertów: {len(alerts_list)}")
    for a in alerts_list:
        sev     = a.get("severity", "?")
        color   = SEVERITY_COLOR.get(sev, "#888")
        atype   = ALERT_TYPE_PL.get(a.get("alert_type", ""), a.get("alert_type", "").upper())
        ts      = a.get("event_timestamp", "?")
        msg     = a.get("msg", "brak opisu")
        sid     = a.get("station_id", "?")
        val     = a.get("value")
        expires = a.get("expires", "")
        val_str = f" | wartość: {val}" if val is not None else ""
        exp_str = f" | wygasa: {expires}" if expires else ""
        st.markdown(
            f'<div style="border-left:6px solid {color};padding:10px 14px;margin-bottom:10px;'
            f'background:#FFFBEB;border-radius:6px;color:#1a1a1a;box-shadow:0 1px 3px rgba(0,0,0,0.08)">'
            f'<b style="color:{color};font-size:1.05em">[{SEVERITY_PL.get(sev,sev)}] {atype}</b>'
            f'&nbsp;<span style="font-size:0.82em;color:#666">{ts}</span><br>'
            f'<span style="font-size:0.98em">{msg}</span><br>'
            f'<span style="font-size:0.8em;color:#888">stacja: {sid}{val_str}{exp_str}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )


# ── Sekcja: mapa ──────────────────────────────────────────────────────────────

def section_map():
    st.subheader("🗺️ Mapa alertów i stacji")

    stations    = load_stations()
    names       = station_name_map()
    alerts_list = list(st.session_state.get("alerts", {}).values())
    df_synop    = get_latest_synop()
    df_hydro    = get_latest_hydro()

    synop_idx = {} if df_synop.empty else {str(r["station_id"]): r for _, r in df_synop.iterrows()}
    hydro_idx = {} if df_hydro.empty else {str(r["gauge_id"]): r for _, r in df_hydro.iterrows()}

    rows = []
    for _, row in stations.iterrows():
        sid   = str(row.get("id", ""))
        stype = row.get("type", "")
        name  = str(row.get("name", sid))

        if stype == "weather" and sid in synop_idx:
            m    = synop_idx[sid]
            t    = f"{m['temperature']:.1f}°C"    if pd.notna(m.get("temperature"))   else "—"
            h    = f"{m['humidity']:.0f}%"         if pd.notna(m.get("humidity"))      else "—"
            p    = f"{m['pressure_hpa']:.1f} hPa"  if pd.notna(m.get("pressure_hpa")) else "—"
            w    = f"{m['wind_speed']:.1f} m/s"    if pd.notna(m.get("wind_speed"))    else "—"
            opis = f"🌡 {t}  💧 {h}  🔵 {p}  💨 {w}"
            kat  = "Stacja pogodowa"
        elif stype == "hydro" and sid in hydro_idx:
            m    = hydro_idx[sid]
            wl   = f"{m['water_level_cm']:.0f} cm"  if pd.notna(m.get("water_level_cm")) else "—"
            fl   = f"{m['flow_m3s']:.2f} m³/s"      if pd.notna(m.get("flow_m3s"))       else "—"
            warn = "  ⚠️ OSTRZEŻENIE" if m.get("is_warning") else ""
            opis = f"💧 Poziom: {wl}  🌊 Przepływ: {fl}{warn}"
            kat  = "Stacja hydrologiczna"
        else:
            opis = "Stacja pogodowa" if stype == "weather" else "Stacja hydrologiczna"
            kat  = opis

        rows.append({"lat": float(row["lat"]), "lon": float(row["lon"]),
                     "nazwa": name, "tooltip": opis, "kategoria": kat, "rozmiar": 7})

    for a in alerts_list:
        lat, lon = a.get("lat"), a.get("lon")
        if lat is None or lon is None:
            continue
        sid    = a.get("station_id", "?")
        sev    = a.get("severity", "?")
        sev_pl = SEVERITY_PL.get(sev, sev)
        atype  = ALERT_TYPE_PL.get(a.get("alert_type", ""), a.get("alert_type", "?"))
        val    = a.get("value")
        v_str  = f"  📊 Wartość: {val}" if val is not None else ""
        rows.append({"lat": float(lat), "lon": float(lon),
                     "nazwa":     names.get(str(sid), sid),
                     "tooltip":   f"⚠️ {atype} [{sev_pl}]  {a.get('msg','')}{v_str}",
                     "kategoria": f"Alert {sev_pl}", "rozmiar": 18})

    if not rows:
        st.info("Brak danych do wyświetlenia na mapie.")
        return

    df_map    = pd.DataFrame(rows)
    color_map = {
        "Stacja pogodowa":      "#3B82F6",
        "Stacja hydrologiczna": "#10B981",
        "Alert Żółty":          "#F59E0B",
        "Alert Pomarańczowy":   "#F97316",
        "Alert Czerwony":       "#EF4444",
    }

    fig = px.scatter_map(
        df_map, lat="lat", lon="lon",
        hover_name="nazwa",
        custom_data=["tooltip", "kategoria"],
        color="kategoria", color_discrete_map=color_map,
        size="rozmiar", size_max=22,
        zoom=5.8, center={"lat": 52.0, "lon": 19.5}, height=560,
    )
    fig.update_traces(hovertemplate=(
        "<b>%{hovertext}</b><br>"
        "<i style='color:#555'>%{customdata[1]}</i><br>"
        "%{customdata[0]}<extra></extra>"
    ))
    fig.update_layout(
        map_style="open-street-map",
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        legend=dict(title="Typ punktu", bgcolor="rgba(255,255,255,0.92)",
                    bordercolor="#ddd", borderwidth=1, font=dict(size=12),
                    yanchor="bottom", y=0.02, xanchor="left", x=0.01),
    )
    st.plotly_chart(fig, use_container_width=True)


# ── Sekcja: synop ─────────────────────────────────────────────────────────────

def section_synop():
    st.subheader("📊 Pomiary pogodowe — ostatnie 24 h")

    df_ids = query_postgres(
        "SELECT DISTINCT station_id FROM synop_measurements "
        "WHERE measurement_timestamp <= NOW() ORDER BY station_id LIMIT 100"
    )
    if df_ids.empty:
        st.info("Brak danych w synop_measurements.")
        return

    names       = station_name_map()
    station_ids = df_ids["station_id"].tolist()
    selected    = st.selectbox("Stacja pogodowa", station_ids,
                               format_func=lambda x: fmt_station(x, names),
                               key="synop_station")

    # Deduplikacja po minucie — spark-weather produkuje wiele odczytów w tej samej sekundzie
    df = query_postgres(
        f"""
        SELECT
            DATE_TRUNC('minute', measurement_timestamp) AS ts_min,
            AVG(temperature)  AS temperature,
            AVG(humidity)     AS humidity,
            AVG(pressure_hpa) AS pressure_hpa,
            AVG(wind_speed)   AS wind_speed
        FROM synop_measurements
        WHERE station_id = :sid
          AND measurement_timestamp >= NOW() - INTERVAL '{SYNOP_WINDOW_H} hours'
          AND measurement_timestamp <= NOW()
        GROUP BY DATE_TRUNC('minute', measurement_timestamp)
        ORDER BY ts_min ASC
        """,
        {"sid": selected},
    )

    if df.empty:
        last = query_postgres(
            "SELECT MAX(measurement_timestamp) AS last_ts FROM synop_measurements "
            "WHERE station_id = :sid AND measurement_timestamp <= NOW()",
            {"sid": selected},
        )
        last_ts = last.iloc[0]["last_ts"] if not last.empty else None
        if last_ts:
            st.warning(f"Brak danych z ostatnich {SYNOP_WINDOW_H} h. Ostatni odczyt: **{last_ts}**")
        else:
            st.info("Brak danych dla wybranej stacji.")
        return

    df = df.rename(columns={"ts_min": "measurement_timestamp"})
    df["measurement_timestamp"] = pd.to_datetime(df["measurement_timestamp"], utc=True)
    st.caption(f"Odczytów w oknie {SYNOP_WINDOW_H} h: **{len(df)}**  "
               f"| ostatni: {df['measurement_timestamp'].max().strftime('%H:%M UTC')}")

    col1, col2 = st.columns(2)
    for col_name, title, col in [
        ("temperature",  "Temperatura (°C)",      col1),
        ("humidity",     "Wilgotność (%)",         col2),
        ("pressure_hpa", "Ciśnienie (hPa)",        col1),
        ("wind_speed",   "Prędkość wiatru (m/s)",  col2),
    ]:
        with col:
            fig = px.line(df, x="measurement_timestamp", y=col_name, title=title,
                          labels={"measurement_timestamp": "Czas", col_name: title},
                          markers=True)
            fig.update_layout(margin={"t": 40, "b": 10})
            st.plotly_chart(fig, use_container_width=True)


# ── Sekcja: hydro ─────────────────────────────────────────────────────────────

def section_hydro():
    st.subheader("🌊 Pomiary hydrologiczne — ostatnie 24 h")

    df_ids = query_postgres(
        "SELECT DISTINCT gauge_id FROM hydro_measurements "
        "WHERE measurement_timestamp <= NOW() ORDER BY gauge_id LIMIT 100"
    )
    if df_ids.empty:
        st.info("Brak danych w hydro_measurements.")
        return

    names     = station_name_map()
    gauge_ids = df_ids["gauge_id"].tolist()
    selected  = st.selectbox("Wodowskaz", gauge_ids,
                             format_func=lambda x: fmt_station(x, names),
                             key="hydro_gauge")

    df = query_postgres(
        f"""
        SELECT measurement_timestamp, water_level_cm, flow_m3s, is_warning
        FROM hydro_measurements
        WHERE gauge_id = :gid
          AND measurement_timestamp >= NOW() - INTERVAL '{HYDRO_WINDOW_H} hours'
          AND measurement_timestamp <= NOW()
        ORDER BY measurement_timestamp ASC
        LIMIT 1000
        """,
        {"gid": selected},
    )

    if df.empty:
        last = query_postgres(
            "SELECT MAX(measurement_timestamp) AS last_ts FROM hydro_measurements "
            "WHERE gauge_id = :gid AND measurement_timestamp <= NOW()",
            {"gid": selected},
        )
        last_ts = last.iloc[0]["last_ts"] if not last.empty else None
        if last_ts:
            st.warning(f"Brak danych z ostatnich {HYDRO_WINDOW_H} h. Ostatni odczyt: **{last_ts}**")
        else:
            st.info("Brak danych dla wybranego wodowskazu.")
        return

    df["measurement_timestamp"] = pd.to_datetime(df["measurement_timestamp"], utc=True)
    st.caption(f"Odczytów w oknie {HYDRO_WINDOW_H} h: **{len(df)}**  "
               f"| ostatni: {df['measurement_timestamp'].max().strftime('%H:%M UTC')}")

    fig = px.line(df, x="measurement_timestamp", y="water_level_cm",
                  title="Poziom wody (cm)",
                  labels={"measurement_timestamp": "Czas", "water_level_cm": "Poziom wody (cm)"},
                  markers=True)
    warn = df[df["is_warning"] == True]
    if not warn.empty:
        fig.add_scatter(x=warn["measurement_timestamp"], y=warn["water_level_cm"],
                        mode="markers", marker=dict(color="red", size=10), name="Ostrzeżenie")
    st.plotly_chart(fig, use_container_width=True)

    if "flow_m3s" in df.columns and df["flow_m3s"].notna().any():
        fig2 = px.line(df, x="measurement_timestamp", y="flow_m3s",
                       title="Przepływ (m³/s)",
                       labels={"measurement_timestamp": "Czas", "flow_m3s": "Przepływ (m³/s)"},
                       markers=True)
        st.plotly_chart(fig2, use_container_width=True)


# ── Sekcja: ML ────────────────────────────────────────────────────────────────

def section_ml():
    st.subheader("🤖 Model ML — pomiar vs predykcja")

    df_ids = query_postgres(
        f"SELECT DISTINCT gauge_id FROM hydro_predictions "
        f"WHERE prediction_timestamp >= NOW() - INTERVAL '{HYDRO_WINDOW_H} hours' "
        f"  AND prediction_timestamp <= NOW() + INTERVAL '4 hours' ORDER BY gauge_id LIMIT 100"
    )
    if df_ids.empty:
        st.info("Brak predykcji w hydro_predictions.")
        return

    names     = station_name_map()
    gauge_ids = df_ids["gauge_id"].tolist()
    selected  = st.selectbox("Wodowskaz (ML)", gauge_ids,
                             format_func=lambda x: fmt_station(x, names),
                             key="ml_gauge")

    # Pomiar rzeczywisty — ostatnie 24 h (tylko przeszłe dane)
    df_real = query_postgres(
        f"""
        SELECT measurement_timestamp, water_level_cm
        FROM hydro_measurements
        WHERE gauge_id = :gid
          AND measurement_timestamp >= NOW() - INTERVAL '{HYDRO_WINDOW_H} hours'
          AND measurement_timestamp <= NOW()
        ORDER BY measurement_timestamp ASC
        LIMIT 1000
        """,
        {"gid": selected},
    )

    # Predykcje — od 24h temu do +4h w przód (okno prognozy)
    df_pred = query_postgres(
        f"""
        SELECT prediction_timestamp, predicted_water_level, horizon_hours
        FROM hydro_predictions
        WHERE gauge_id = :gid
          AND prediction_timestamp >= NOW() - INTERVAL '{HYDRO_WINDOW_H} hours'
          AND prediction_timestamp <= NOW() + INTERVAL '4 hours'
        ORDER BY prediction_timestamp ASC
        LIMIT 1000
        """,
        {"gid": selected},
    )

    if df_pred.empty and df_real.empty:
        st.info("Brak danych dla wybranego wodowskazu.")
        return

    fig = go.Figure()
    if not df_real.empty:
        df_real["measurement_timestamp"] = pd.to_datetime(df_real["measurement_timestamp"], utc=True)
        df_real = df_real.sort_values("measurement_timestamp")
        fig.add_trace(go.Scatter(
            x=df_real["measurement_timestamp"], y=df_real["water_level_cm"],
            mode="lines+markers", name="Pomiar rzeczywisty",
            line=dict(color="#2563EB", width=2), marker=dict(size=5),
        ))

    if not df_pred.empty:
        df_pred["prediction_timestamp"] = pd.to_datetime(df_pred["prediction_timestamp"], utc=True)
        df_pred = df_pred.sort_values("prediction_timestamp")
        fig.add_trace(go.Scatter(
            x=df_pred["prediction_timestamp"], y=df_pred["predicted_water_level"],
            mode="lines+markers", name="Predykcja ML",
            line=dict(color="#EA580C", dash="dot", width=2), marker=dict(size=6),
        ))

    # Linia "teraz"
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    fig.add_shape(type="line", x0=now_str, x1=now_str, y0=0, y1=1,
                  xref="x", yref="paper", line=dict(dash="dash", color="#888"))
    fig.add_annotation(x=now_str, y=1, xref="x", yref="paper",
                       text="teraz", showarrow=False, yanchor="bottom", font=dict(color="#888"))

    station_label = names.get(str(selected), selected)
    fig.update_layout(
        title=f"Poziom wody — {station_label}",
        xaxis_title="Czas", yaxis_title="Poziom wody (cm)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        height=460,
    )
    st.plotly_chart(fig, use_container_width=True)

    if not df_pred.empty and "horizon_hours" in df_pred.columns:
        horizons = sorted(df_pred["horizon_hours"].dropna().unique().tolist())
        st.caption(f"Horyzonty predykcji: {horizons} h")


# ── Główna aplikacja ──────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="System Alertów Pogodowo-Hydrologicznych",
        page_icon="🌩️",
        layout="wide",
    )

    st.title("🌩️ System alertów pogodowo-hydrologicznych")
    st.caption(
        f"Kafka: `{KAFKA_BOOTSTRAP}` | Postgres: `{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}` | "
        f"Czas: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"
    )

    st_autorefresh(interval=10_000, key="autorefresh")

    tab1, tab2, tab3, tab4 = st.tabs(["⚡ Alerty", "🗺️ Mapa", "📊 Pogoda", "🌊 Hydro + ML"])

    with tab1:
        section_alerts()
    with tab2:
        if "alerts" not in st.session_state:
            st.session_state.alerts = {}
        section_map()
    with tab3:
        section_synop()
    with tab4:
        section_hydro()
        st.divider()
        section_ml()


if __name__ == "__main__":
    main()
