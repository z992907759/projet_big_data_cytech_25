import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import plotly.express as px


# -----------------------------
# 1. Page configuration
# -----------------------------
st.set_page_config(page_title="NYC Taxi Trip Dashboard", layout="wide")
st.title("NYC Taxi Trip Dashboard")

st.markdown(
    """
    **Problématique :**  
    *Comment la demande de taxis à NYC évolue-t-elle dans le temps et quels enseignements peut-on en tirer pour l’organisation du service ?*
    """
)


# -----------------------------
# 2. Database connection
# -----------------------------
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "bigdata")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")
SCHEMA  = os.getenv("DWH_SCHEMA", "dwh")

@st.cache_resource
def get_engine():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(url, pool_pre_ping=True)

engine = get_engine()


# -----------------------------
# 3. Schema introspection
# -----------------------------
@st.cache_data(ttl=300)
def get_columns(table: str):
    q = text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t
        ORDER BY ordinal_position
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params={"s": SCHEMA, "t": table})

def pick(col_list, candidates):
    for c in candidates:
        if c in col_list:
            return c
    return None

FACT = "fact_trip"
DIM_DT = "dim_datetime"
DIM_PAY = "dim_payment_type"

fact_cols = get_columns(FACT)["column_name"].tolist()
dt_cols_df = get_columns(DIM_DT)
dt_cols = dt_cols_df["column_name"].tolist()

DT_KEY = pick(dt_cols, ["datetime_key", "date_time_key", "dt_key"])
DT_TIME = pick(
    dt_cols_df[dt_cols_df["data_type"].str.contains("timestamp", case=False, na=False)]["column_name"].tolist(),
    dt_cols
)
DT_DATE = pick(dt_cols, ["date", "full_date"])

PU_DT_KEY = "pickup_datetime_key"
PAX_COL = "passenger_count"

TOTAL_COL = pick(fact_cols, ["total_amount"])
FARE_COL  = pick(fact_cols, ["fare_amount"])

pay_cols = get_columns(DIM_PAY)["column_name"].tolist()
PAY_KEY = pick(pay_cols, ["payment_type_key"])
PAY_LABEL = pick(pay_cols, ["payment_type_name", "payment_type", "label", "name"])

if not DT_KEY or not DT_TIME:
    st.error("Missing required datetime columns.")
    st.stop()


# -----------------------------
# 4. Sidebar filters
# -----------------------------
@st.cache_data(ttl=300)
def get_time_bounds():
    q = text(f"SELECT MIN({DT_TIME}), MAX({DT_TIME}) FROM {SCHEMA}.{DIM_DT}")
    with engine.connect() as conn:
        return conn.execute(q).fetchone()

min_t, max_t = get_time_bounds()
min_d = min_t.date()
max_d = max_t.date()

with st.sidebar:
    st.header("Filters")
    date_range = st.date_input("Date range", value=(min_d, max_d), min_value=min_d, max_value=max_d)
    show_payment = st.toggle("Show payment mix", value=True)

ds = pd.to_datetime(date_range[0])
de = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1)


# -----------------------------
# 5. KPI section
# -----------------------------
@st.cache_data(ttl=120)
def load_kpi(ds, de):
    extras = []
    if TOTAL_COL:
        extras.append(f"SUM(f.{TOTAL_COL}) AS total_revenue")
    if FARE_COL:
        extras.append(f"AVG(f.{FARE_COL}) AS avg_fare")

    q = text(f"""
        SELECT
            COUNT(*) AS trips,
            AVG(f.{PAX_COL}) AS avg_passengers
            {"," if extras else ""} {", ".join(extras)}
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE d.{DT_TIME} >= :ds AND d.{DT_TIME} < :de
    """)
    with engine.connect() as conn:
        return conn.execute(q, {"ds": ds, "de": de}).fetchone()

kpi = load_kpi(ds, de)

cols = st.columns(4)
cols[0].metric("Trips", f"{int(kpi[0]):,}")
cols[1].metric("Avg passengers", f"{kpi[1]:.2f}" if kpi[1] else "—")

idx = 2
if TOTAL_COL:
    cols[idx].metric("Total revenue", f"{kpi[idx]:,.0f}" if kpi[idx] else "—"); idx += 1
if FARE_COL:
    cols[idx].metric("Avg fare", f"{kpi[idx]:.2f}" if kpi[idx] else "—")

st.divider()


# -----------------------------
# 6. Demand evolution (time series)
# -----------------------------
@st.cache_data(ttl=120)
def load_trips_timeseries(ds, de):
    day_expr = f"d.{DT_DATE}" if DT_DATE else f"DATE(d.{DT_TIME})"
    q = text(f"""
        SELECT {day_expr} AS day, COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE d.{DT_TIME} >= :ds AND d.{DT_TIME} < :de
        GROUP BY 1 ORDER BY 1
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params={"ds": ds, "de": de})

ts_df = load_trips_timeseries(ds, de)


# -----------------------------
# 7. Operational load analysis
# -----------------------------
@st.cache_data(ttl=120)
def load_trips_by_hour(ds, de):
    q = text(f"""
        SELECT EXTRACT(HOUR FROM d.{DT_TIME})::int AS hour, COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE d.{DT_TIME} >= :ds AND d.{DT_TIME} < :de
        GROUP BY 1 ORDER BY 1
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params={"ds": ds, "de": de})

@st.cache_data(ttl=120)
def load_trips_by_dow(ds, de):
    q = text(f"""
        SELECT EXTRACT(DOW FROM d.{DT_TIME})::int AS dow, COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE d.{DT_TIME} >= :ds AND d.{DT_TIME} < :de
        GROUP BY 1 ORDER BY 1
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params={"ds": ds, "de": de})

hour_df = load_trips_by_hour(ds, de)
dow_df = load_trips_by_dow(ds, de)
dow_map = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
dow_df["day"] = dow_df["dow"].map(dow_map)


# -----------------------------
# 8. Visualization layout
# -----------------------------
row1_left, row1_right = st.columns([1.6, 1])

with row1_left:
    st.subheader("Trips trend (daily)")
    fig = px.line(ts_df, x="day", y="trips", markers=True)
    st.plotly_chart(fig, use_container_width=True)

with row1_right:
    if show_payment and PAY_KEY:
        q = text(f"""
            SELECT p.{PAY_LABEL if PAY_LABEL else PAY_KEY} AS payment_type, COUNT(*) AS trips
            FROM {SCHEMA}.{FACT} f
            JOIN {SCHEMA}.{DIM_DT} d ON f.{PU_DT_KEY} = d.{DT_KEY}
            JOIN {SCHEMA}.{DIM_PAY} p ON f.payment_type_key = p.{PAY_KEY}
            WHERE d.{DT_TIME} >= :ds AND d.{DT_TIME} < :de
            GROUP BY 1 ORDER BY trips DESC
        """)
        with engine.connect() as conn:
            pay_df = pd.read_sql(q, conn, params={"ds": ds, "de": de})
        fig = px.pie(pay_df, names="payment_type", values="trips")
        st.plotly_chart(fig, use_container_width=True)

row2_left, row2_right = st.columns(2)

with row2_left:
    st.subheader("Demand profile by hour")
    fig = px.bar(hour_df, x="hour", y="trips")
    st.plotly_chart(fig, use_container_width=True)

with row2_right:
    st.subheader("Weekly demand pattern")
    fig = px.bar(dow_df, x="day", y="trips")
    st.plotly_chart(fig, use_container_width=True)


# -----------------------------
# 9. Passenger structure & implications
# -----------------------------
@st.cache_data(ttl=120)
def load_passenger_mix(ds, de):
    q = text(f"""
        SELECT CASE
            WHEN f.{PAX_COL} >= 4 THEN '4+'
            ELSE f.{PAX_COL}::text
        END AS pax_group,
        COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE d.{DT_TIME} >= :ds AND d.{DT_TIME} < :de
        GROUP BY 1 ORDER BY trips DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params={"ds": ds, "de": de})

pax_df = load_passenger_mix(ds, de)

row3_left, row3_right = st.columns([1.2, 1])

with row3_left:
    st.subheader("Passenger load distribution")
    fig = px.pie(pax_df, names="pax_group", values="trips")
    st.plotly_chart(fig, use_container_width=True)

with row3_right:
    st.subheader("Implications for service organization")
    st.markdown("""
- Renforcer l’offre aux heures de pointe.  
- Adapter la capacité selon les jours.  
- Privilégier une flotte standard (majorité 1–2 passagers).
    """)
