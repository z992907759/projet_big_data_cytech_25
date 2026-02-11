import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import plotly.express as px

# -----------------------------
# App layout
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
# Database connection
# -----------------------------
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "taxidb")
PG_USER = os.getenv("PG_USER", "myuser")
PG_PASS = os.getenv("PG_PASS", "mypassword")
SCHEMA  = os.getenv("DWH_SCHEMA", "dwh")

@st.cache_resource
def get_engine():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(url, pool_pre_ping=True)

engine = get_engine()

# -----------------------------
# Schema introspection
# -----------------------------
@st.cache_data(ttl=60)
def get_columns(table: str) -> pd.DataFrame:
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

# -----------------------------
# Tables & columns
# -----------------------------
FACT = "fact_trip"
DIM_DT = "dim_datetime"
DIM_LOC = "dim_location"
DIM_PAY = "dim_payment_type"
DIM_RATE = "dim_rate_code"

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
DIST_COL = "trip_distance"

TOTAL_COL = pick(fact_cols, ["total_amount"])
FARE_COL  = pick(fact_cols, ["fare_amount"])
TIP_COL   = pick(fact_cols, ["tip_amount"])

loc_cols = get_columns(DIM_LOC)["column_name"].tolist()
LOC_KEY = pick(loc_cols, ["location_key"])
LOC_ID  = pick(loc_cols, ["location_id"])

pay_cols = get_columns(DIM_PAY)["column_name"].tolist()
PAY_KEY = pick(pay_cols, ["payment_type_key"])
PAY_LABEL = pick(pay_cols, ["payment_type_name", "payment_type", "label", "name"])

rate_cols = get_columns(DIM_RATE)["column_name"].tolist()
RATE_KEY = pick(rate_cols, ["rate_code_key"])
RATE_LABEL = pick(rate_cols, ["rate_code_name", "rate_code", "label", "name"])

# Hard stops if essential columns missing
missing = []
if not DT_KEY: missing.append(f"{DIM_DT}.datetime_key")
if not DT_TIME: missing.append(f"{DIM_DT}.timestamp/date column")
if not LOC_KEY: missing.append(f"{DIM_LOC}.location_key")
if not PAY_KEY: missing.append(f"{DIM_PAY}.payment_type_key")
if not RATE_KEY: missing.append(f"{DIM_RATE}.rate_code_key")
if missing:
    st.error("Missing required columns:\n- " + "\n- ".join(missing))
    st.stop()

# -----------------------------
# Filters
# -----------------------------
@st.cache_data(ttl=60)
def get_time_bounds():
    q = text(f"SELECT MIN({DT_TIME}), MAX({DT_TIME}) FROM {SCHEMA}.{DIM_DT}")
    with engine.connect() as conn:
        return conn.execute(q).fetchone()

min_t, max_t = get_time_bounds()
min_d = min_t.date()
max_d = max_t.date()

with st.sidebar:
    st.header("Filters")
    date_range = st.date_input(
        "Date range",
        value=(min_d, max_d),
        min_value=min_d,
        max_value=max_d
    )

ds = pd.to_datetime(date_range[0])
de = pd.to_datetime(date_range[1]) + pd.Timedelta(days=1)

where_sql = f"d.{DT_TIME} >= :ds AND d.{DT_TIME} < :de"
params = {"ds": ds, "de": de}

# -----------------------------
# KPI
# -----------------------------
@st.cache_data(ttl=60)
def load_kpi():
    extras = []
    if TOTAL_COL:
        extras.append(f"SUM(f.{TOTAL_COL}) AS total_revenue")
    if FARE_COL:
        extras.append(f"AVG(f.{FARE_COL}) AS avg_fare")
    if TIP_COL:
        extras.append(f"AVG(f.{TIP_COL}) AS avg_tip")

    q = text(f"""
        SELECT
            COUNT(*) AS trips,
            AVG(f.{PAX_COL}) AS avg_passengers,
            AVG(f.{DIST_COL}) AS avg_distance
            {"," if extras else ""} {", ".join(extras)}
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE {where_sql}
    """)
    with engine.connect() as conn:
        return conn.execute(q, params).fetchone()

kpi = load_kpi()
k_cols = st.columns(6)
k_cols[0].metric("Trips", f"{int(kpi[0]):,}")
k_cols[1].metric("Avg passengers", f"{kpi[1]:.2f}")
k_cols[2].metric("Avg distance", f"{kpi[2]:.2f}")

idx = 3
if TOTAL_COL:
    k_cols[idx].metric("Total revenue", f"{kpi[idx]:,.0f}"); idx += 1
if FARE_COL:
    k_cols[idx].metric("Avg fare", f"{kpi[idx]:.2f}"); idx += 1
if TIP_COL:
    k_cols[idx].metric("Avg tip", f"{kpi[idx]:.2f}")

st.divider()

# -----------------------------
# Core trend + payment mix
# -----------------------------
@st.cache_data(ttl=60)
def load_trips_timeseries():
    day_expr = f"d.{DT_DATE}" if DT_DATE else f"DATE(d.{DT_TIME})"
    q = text(f"""
        SELECT
            {day_expr} AS day,
            COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY 1
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=params)

@st.cache_data(ttl=60)
def load_payment_mix():
    label = PAY_LABEL if PAY_LABEL else PAY_KEY
    q = text(f"""
        SELECT
            p.{label} AS payment_type,
            COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        JOIN {SCHEMA}.{DIM_PAY} p
          ON f.payment_type_key = p.{PAY_KEY}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY trips DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=params)

ts_df = load_trips_timeseries()
pay_df = load_payment_mix()

c1, c2 = st.columns([1.4, 1])

with c1:
    st.subheader("Trips trend (daily)")
    fig = px.line(ts_df, x="day", y="trips", markers=True)
    fig.update_layout(xaxis_title="", yaxis_title="Trips")
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Payment mix (share of trips)")
    fig = px.pie(pay_df, names="payment_type", values="trips")
    fig.update_traces(textinfo="percent+value", textposition="inside")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# -----------------------------
# Operational load: hour + day-of-week
# -----------------------------
@st.cache_data(ttl=60)
def load_trips_by_hour():
    q = text(f"""
        SELECT
            EXTRACT(HOUR FROM d.{DT_TIME})::int AS hour,
            COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY 1
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=params)

@st.cache_data(ttl=60)
def load_trips_by_dow():
    q = text(f"""
        SELECT
            EXTRACT(DOW FROM d.{DT_TIME})::int AS dow,
            COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY 1
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=params)

hour_df = load_trips_by_hour()
dow_df = load_trips_by_dow()
dow_map = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
dow_df["day"] = dow_df["dow"].map(dow_map)

c3, c4 = st.columns(2)

with c3:
    st.subheader("Demand profile by hour")
    fig = px.bar(hour_df, x="hour", y="trips", text="trips")
    fig.update_traces(textposition="outside")
    fig.update_layout(xaxis_title="Hour of day", yaxis_title="Trips")
    st.plotly_chart(fig, use_container_width=True)

with c4:
    st.subheader("Weekly demand pattern")
    fig = px.bar(dow_df, x="day", y="trips", text="trips")
    fig.update_traces(textposition="outside")
    fig.update_layout(xaxis_title="", yaxis_title="Trips")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# -----------------------------
# Service profile: avg distance + passenger mix
# -----------------------------
@st.cache_data(ttl=60)
def load_avg_distance_ts():
    day_expr = f"d.{DT_DATE}" if DT_DATE else f"DATE(d.{DT_TIME})"
    q = text(f"""
        SELECT
            {day_expr} AS day,
            AVG(f.{DIST_COL}) AS avg_distance
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY 1
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=params)

@st.cache_data(ttl=60)
def load_passenger_mix():
    q = text(f"""
        SELECT
            CASE
                WHEN f.{PAX_COL} IS NULL THEN 'Unknown'
                WHEN f.{PAX_COL} >= 4 THEN '4+'
                ELSE f.{PAX_COL}::text
            END AS pax_group,
            COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY trips DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=params)

avg_dist_df = load_avg_distance_ts()
pax_df = load_passenger_mix()

c5, c6 = st.columns([1.4, 1])

with c5:
    st.subheader("Service profile: average trip distance over time")
    fig = px.line(avg_dist_df, x="day", y="avg_distance", markers=True)
    fig.update_layout(xaxis_title="", yaxis_title="Avg distance")
    st.plotly_chart(fig, use_container_width=True)

with c6:
    st.subheader("Passenger load distribution")
    fig = px.pie(pax_df, names="pax_group", values="trips")
    fig.update_traces(textinfo="percent+value", textposition="inside")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# -----------------------------
# Tips (decision-oriented) + rate code
# -----------------------------
@st.cache_data(ttl=60)
def load_top_tips_locations():
    if not TIP_COL:
        return pd.DataFrame(columns=["pickup_location", "total_tips"])

    label_expr = f"l.{LOC_ID}::text" if LOC_ID else f"l.{LOC_KEY}::text"

    q = text(f"""
        SELECT
            {label_expr} AS pickup_location,
            SUM(f.{TIP_COL}) AS total_tips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        JOIN {SCHEMA}.{DIM_LOC} l
          ON f.pu_location_key = l.{LOC_KEY}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY total_tips DESC
        LIMIT 5
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=params)

@st.cache_data(ttl=60)
def load_rate_stats():
    label = RATE_LABEL if RATE_LABEL else RATE_KEY
    q = text(f"""
        SELECT
            r.{label} AS rate_code,
            COUNT(*) AS trips
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        JOIN {SCHEMA}.{DIM_RATE} r
          ON f.rate_code_key = r.{RATE_KEY}
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY trips DESC
        LIMIT 10
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=params)

top_tips_df = load_top_tips_locations()
rate_df = load_rate_stats()

c7, c8 = st.columns(2)

with c7:
    if TIP_COL:
        st.subheader("Top 5 pickup location IDs by total tips")
        fig = px.bar(top_tips_df, x="pickup_location", y="total_tips", text="total_tips")
        fig.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig.update_layout(xaxis_title="Pickup location ID", yaxis_title="Total tips")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.subheader("Top tips chart")
        st.info("Tip amount column not available in fact_trip.")

with c8:
    st.subheader("Trips by rate code")
    fig = px.bar(rate_df, x="rate_code", y="trips", text="trips")
    fig.update_traces(textposition="outside")
    fig.update_layout(xaxis_title="", yaxis_title="Trips")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# -----------------------------
# Data preview
# -----------------------------
@st.cache_data(ttl=60)
def load_preview(n: int):
    extras = f", f.{TIP_COL} AS tip_amount" if TIP_COL else ""
    q = text(f"""
        SELECT
            f.trip_key,
            d.{DT_TIME} AS pickup_time,
            f.{PAX_COL} AS passenger_count,
            f.{DIST_COL} AS trip_distance
            {extras}
        FROM {SCHEMA}.{FACT} f
        JOIN {SCHEMA}.{DIM_DT} d
          ON f.{PU_DT_KEY} = d.{DT_KEY}
        WHERE {where_sql}
        ORDER BY d.{DT_TIME} DESC
        LIMIT :n
    """)
    p = dict(params)
    p["n"] = n
    with engine.connect() as conn:
        return pd.read_sql(q, conn, params=p)

st.subheader("Data preview")
st.dataframe(load_preview(200), use_container_width=True)
