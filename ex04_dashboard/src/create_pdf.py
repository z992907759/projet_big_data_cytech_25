import argparse
import os
import re
from pathlib import Path

import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import font_manager, rcParams
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import FancyBboxPatch, Polygon
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def pick(col_list, candidates):
    for c in candidates:
        if c in col_list:
            return c
    return None


def fmt_num(v, ndigits=2):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "-"
    if isinstance(v, (int, np.integer)):
        return f"{int(v):,}"
    return f"{v:,.{ndigits}f}"


def configure_fonts():
    candidates = [
        "PingFang SC", "Hiragino Sans GB", "Heiti SC", "Microsoft YaHei",
        "Noto Sans CJK SC", "Arial Unicode MS", "DejaVu Sans"
    ]
    available = {f.name for f in font_manager.fontManager.ttflist}
    chosen = next((n for n in candidates if n in available), "DejaVu Sans")
    rcParams["font.family"] = chosen
    rcParams["axes.unicode_minus"] = False
    rcParams["pdf.fonttype"] = 42
    rcParams["ps.fonttype"] = 42


def add_header(fig, title, subtitle, logo_path=None):
    bar_ax = fig.add_axes([0.0, 0.945, 1.0, 0.055])
    bar_ax.set_facecolor("#0f4c67")
    bar_ax.set_xticks([])
    bar_ax.set_yticks([])
    for s in bar_ax.spines.values():
        s.set_visible(False)

    text_x = 0.02
    if logo_path and Path(logo_path).exists():
        logo_ax = fig.add_axes([0.012, 0.948, 0.038, 0.046])
        logo_ax.set_xticks([])
        logo_ax.set_yticks([])
        for s in logo_ax.spines.values():
            s.set_visible(False)
        try:
            logo_ax.imshow(mpimg.imread(logo_path))
            text_x = 0.058
        except Exception:
            text_x = 0.02

    bar_ax.text(text_x, 0.63, title, fontsize=24, weight="bold", color="#d7e9f7", va="center", transform=bar_ax.transAxes)
    bar_ax.text(text_x, 0.18, subtitle, fontsize=9.2, color="#8eb7cf", va="center", transform=bar_ax.transAxes)


def parse_wkt_rings(wkt_text: str):
    if not isinstance(wkt_text, str) or not wkt_text:
        return []
    ring_texts = re.findall(r"\((-?\d+\.\d+\s+-?\d+\.\d+(?:\s*,\s*-?\d+\.\d+\s+-?\d+\.\d+)*)\)", wkt_text)
    rings = []
    for rt in ring_texts:
        pts = []
        for pair in rt.split(","):
            p = pair.strip().split()
            if len(p) >= 2:
                pts.append((float(p[0]), float(p[1])))
        if len(pts) >= 3:
            rings.append(np.array(pts, dtype=float))
    return rings


def draw_card(ax, title, value, color="#3b82f6"):
    ax.axis("off")
    ax.add_patch(FancyBboxPatch((0, 0), 1, 1, boxstyle="round,pad=0.012,rounding_size=0.02",
                                facecolor="#f7f9fc", edgecolor="#d4dde8", transform=ax.transAxes))
    ax.add_patch(FancyBboxPatch((0.03, 0.73), 0.10, 0.18, boxstyle="round,pad=0.01,rounding_size=0.07",
                                facecolor=color, edgecolor="white", linewidth=1.0, transform=ax.transAxes))
    ax.text(0.16, 0.79, title, fontsize=9.5, color="#334e68", va="center", transform=ax.transAxes)
    txt = str(value)
    fs = 16 if len(txt) <= 10 else 13
    ax.text(0.05, 0.29, txt, fontsize=fs, weight="bold", color="#102a43", transform=ax.transAxes)


def draw_dot_map(ax, zone_df):
    ax.set_facecolor("#d3eaf8")
    ax.set_title("Pickup Distribution", fontsize=10.5, weight="bold", pad=8)

    for rings in zone_df["rings"]:
        for ring in rings:
            ax.add_patch(Polygon(ring, closed=True, facecolor="#f0f2f5", edgecolor="#c7d2de", linewidth=0.25, zorder=1))

    d = zone_df.dropna(subset=["centroid_lon", "centroid_lat"]).copy()
    if d.empty:
        ax.text(0.5, 0.5, "No map data", ha="center", va="center", transform=ax.transAxes)
        return

    q90, q98 = d["trips"].quantile(0.90), d["trips"].quantile(0.98)
    base = d[d["trips"] <= q90]
    hot = d[(d["trips"] > q90) & (d["trips"] <= q98)]
    peak = d[d["trips"] > q98]

    ax.scatter(base["centroid_lon"], base["centroid_lat"], s=18, c="#2fa866", alpha=0.68,
               edgecolor="white", linewidth=0.25, zorder=3)
    ax.scatter(hot["centroid_lon"], hot["centroid_lat"], s=36, c="#f0c419", alpha=0.9,
               edgecolor="white", linewidth=0.4, zorder=4)
    ax.scatter(peak["centroid_lon"], peak["centroid_lat"], s=72, c="#6c5ce7", alpha=0.85,
               edgecolor="white", linewidth=0.6, zorder=5)

    active = d[d["trips"] > 0]
    ref = active if not active.empty else d
    x0, x1 = ref["centroid_lon"].quantile(0.01), ref["centroid_lon"].quantile(0.99)
    y0, y1 = ref["centroid_lat"].quantile(0.01), ref["centroid_lat"].quantile(0.99)
    mx, my = (x1 - x0) * 0.05, (y1 - y0) * 0.05
    ax.set_xlim(x0 - mx, x1 + mx)
    ax.set_ylim(y0 - my, y1 + my)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_aspect("auto")
    for s in ax.spines.values():
        s.set_visible(False)


def main():
    parser = argparse.ArgumentParser(description="Generate NYC Taxi dashboard PDF.")
    parser.add_argument("--output", default="ex04_dashboard/src/NYC_Taxi_Dashboard_dense.pdf")
    args = parser.parse_args()

    configure_fonts()

    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = int(os.getenv("PG_PORT", "5432"))
    pg_db = os.getenv("PG_DB", "taxidb")
    pg_user = os.getenv("PG_USER", "myuser")
    pg_pass = os.getenv("PG_PASS", "mypassword")
    schema = os.getenv("DWH_SCHEMA", "dwh")
    logo_path = Path(os.getenv("DASHBOARD_LOGO_PATH", "ex04_dashboard/assets/nyc_taxi_logo.png"))
    if not logo_path.is_absolute():
        logo_path = PROJECT_ROOT / logo_path

    engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}", pool_pre_ping=True)

    def read_sql(q, **params):
        with engine.connect() as conn:
            return pd.read_sql(text(q), conn, params=params)

    def get_columns(table):
        return read_sql(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema=:s AND table_name=:t
            ORDER BY ordinal_position
            """,
            s=schema, t=table,
        )

    fact_cols = get_columns("fact_trip")["column_name"].tolist()
    dt_cols_df = get_columns("dim_datetime")
    dt_cols = dt_cols_df["column_name"].tolist()
    loc_cols = get_columns("dim_location")["column_name"].tolist()

    dt_key = pick(dt_cols, ["datetime_key", "date_time_key", "dt_key"])
    dt_time = pick(dt_cols_df[dt_cols_df["data_type"].str.contains("timestamp", case=False, na=False)]["column_name"].tolist(), dt_cols)
    dt_date = pick(dt_cols, ["date", "full_date"])

    pu_dt_key = pick(fact_cols, ["pickup_datetime_key", "pu_datetime_key"])
    pu_loc_key = pick(fact_cols, ["pu_location_key", "pickup_location_key"])
    do_loc_key = pick(fact_cols, ["do_location_key", "dropoff_location_key"])

    pax_col = pick(fact_cols, ["passenger_count"])
    dist_col = pick(fact_cols, ["trip_distance"])
    fare_col = pick(fact_cols, ["fare_amount"])
    tip_col = pick(fact_cols, ["tip_amount"])
    total_col = pick(fact_cols, ["total_amount"])
    pay_key = pick(fact_cols, ["payment_type_key"])
    vendor_key = pick(fact_cols, ["vendor_key"])
    rate_key = pick(fact_cols, ["rate_code_key"])
    src_col = pick(fact_cols, ["source_file"])

    loc_dim_key = pick(loc_cols, ["location_key"])
    loc_id = pick(loc_cols, ["location_id"])

    bounds = read_sql(f"SELECT MIN({dt_time}) AS mn, MAX({dt_time}) AS mx FROM {schema}.dim_datetime")
    ds = pd.to_datetime(bounds.loc[0, "mn"])
    de = pd.to_datetime(bounds.loc[0, "mx"]) + pd.Timedelta(days=1)

    day_expr = f"d.{dt_date}" if dt_date else f"DATE(d.{dt_time})"

    kpi = read_sql(
        f"""
        SELECT COUNT(*) AS trips,
               AVG(f.{pax_col}) AS avg_pax,
               SUM(f.{total_col}) AS total_revenue,
               AVG(f.{fare_col}) AS avg_fare,
               AVG(f.{tip_col}) AS avg_tip,
               AVG(f.{dist_col}) AS avg_dist
        FROM {schema}.fact_trip f
        JOIN {schema}.dim_datetime d ON f.{pu_dt_key}=d.{dt_key}
        WHERE d.{dt_time} >= :ds AND d.{dt_time} < :de
        """,
        ds=ds, de=de,
    )

    daily = read_sql(
        f"""
        SELECT {day_expr} AS day,
               COUNT(*) AS trips,
               SUM(f.{total_col}) AS revenue,
               AVG(f.{fare_col}) AS avg_fare,
               AVG(f.{tip_col}) AS avg_tip
        FROM {schema}.fact_trip f
        JOIN {schema}.dim_datetime d ON f.{pu_dt_key}=d.{dt_key}
        WHERE d.{dt_time} >= :ds AND d.{dt_time} < :de
        GROUP BY 1 ORDER BY 1
        """,
        ds=ds, de=de,
    )
    daily["day"] = pd.to_datetime(daily["day"])
    med = float(daily["trips"].median()) if not daily.empty else 0
    low_cut = max(1000.0, med * 0.02)
    daily = daily[daily["trips"] >= low_cut].copy() if not daily.empty else daily

    hourly = read_sql(
        f"""
        SELECT EXTRACT(HOUR FROM d.{dt_time})::int AS hour,
               COUNT(*) AS trips,
               AVG(f.{fare_col}) AS avg_fare,
               AVG(f.{tip_col}) AS avg_tip
        FROM {schema}.fact_trip f
        JOIN {schema}.dim_datetime d ON f.{pu_dt_key}=d.{dt_key}
        WHERE d.{dt_time} >= :ds AND d.{dt_time} < :de
        GROUP BY 1 ORDER BY 1
        """,
        ds=ds, de=de,
    )

    dow_hour = read_sql(
        f"""
        SELECT EXTRACT(DOW FROM d.{dt_time})::int AS dow,
               EXTRACT(HOUR FROM d.{dt_time})::int AS hour,
               COUNT(*) AS trips
        FROM {schema}.fact_trip f
        JOIN {schema}.dim_datetime d ON f.{pu_dt_key}=d.{dt_key}
        WHERE d.{dt_time} >= :ds AND d.{dt_time} < :de
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        ds=ds, de=de,
    )

    pay = read_sql(
        f"""
        SELECT f.{pay_key}::text AS payment, COUNT(*) AS trips
        FROM {schema}.fact_trip f
        JOIN {schema}.dim_datetime d ON f.{pu_dt_key}=d.{dt_key}
        WHERE d.{dt_time} >= :ds AND d.{dt_time} < :de
        GROUP BY 1 ORDER BY trips DESC
        """,
        ds=ds, de=de,
    )

    rate = read_sql(
        f"""
        SELECT f.{rate_key}::text AS rate_code, COUNT(*) AS trips
        FROM {schema}.fact_trip f
        JOIN {schema}.dim_datetime d ON f.{pu_dt_key}=d.{dt_key}
        WHERE d.{dt_time} >= :ds AND d.{dt_time} < :de
        GROUP BY 1 ORDER BY trips DESC
        """,
        ds=ds, de=de,
    )
    rate = rate.head(5)

    pickup_map = read_sql(
        f"""
        SELECT l.{loc_id}::int AS location_id, COUNT(*) AS trips
        FROM {schema}.fact_trip f
        JOIN {schema}.dim_location l ON f.{pu_loc_key}=l.{loc_dim_key}
        JOIN {schema}.dim_datetime d ON f.{pu_dt_key}=d.{dt_key}
        WHERE d.{dt_time} >= :ds AND d.{dt_time} < :de
        GROUP BY 1
        """,
        ds=ds, de=de,
    )

    top_pickup = read_sql(
        f"""
        SELECT l.{loc_id}::int AS location_id, COUNT(*) AS trips
        FROM {schema}.fact_trip f
        JOIN {schema}.dim_location l ON f.{pu_loc_key}=l.{loc_dim_key}
        JOIN {schema}.dim_datetime d ON f.{pu_dt_key}=d.{dt_key}
        WHERE d.{dt_time} >= :ds AND d.{dt_time} < :de
        GROUP BY 1 ORDER BY trips DESC LIMIT 8
        """,
        ds=ds, de=de,
    )

    top_dropoff = read_sql(
        f"""
        SELECT l.{loc_id}::int AS location_id, COUNT(*) AS trips
        FROM {schema}.fact_trip f
        JOIN {schema}.dim_location l ON f.{do_loc_key}=l.{loc_dim_key}
        JOIN {schema}.dim_datetime d ON f.{pu_dt_key}=d.{dt_key}
        WHERE d.{dt_time} >= :ds AND d.{dt_time} < :de
        GROUP BY 1 ORDER BY trips DESC LIMIT 8
        """,
        ds=ds, de=de,
    )

    source = read_sql(
        f"""
        SELECT f.{src_col} AS source_file, COUNT(*) AS trips
        FROM {schema}.fact_trip f
        JOIN {schema}.dim_datetime d ON f.{pu_dt_key}=d.{dt_key}
        WHERE d.{dt_time} >= :ds AND d.{dt_time} < :de
        GROUP BY 1 ORDER BY trips DESC LIMIT 10
        """,
        ds=ds, de=de,
    )

    distance_bins = read_sql(
        f"""
        SELECT width_bucket(f.{dist_col}, 0, 30, 30) AS bucket, COUNT(*) AS n
        FROM {schema}.fact_trip f
        JOIN {schema}.dim_datetime d ON f.{pu_dt_key}=d.{dt_key}
        WHERE d.{dt_time} >= :ds AND d.{dt_time} < :de
          AND f.{dist_col} >= 0 AND f.{dist_col} <= 30
        GROUP BY 1 ORDER BY 1
        """,
        ds=ds, de=de,
    )

    zone_lookup_path = PROJECT_ROOT / "ex05_ml_prediction_service/streamlit_app/references/taxi_zone_lookup.csv"
    zone_df = pd.read_csv(zone_lookup_path)
    zone_df["Location ID"] = pd.to_numeric(zone_df["Location ID"], errors="coerce")

    rings, cx, cy = [], [], []
    for geom in zone_df["Shape Geometry"].fillna(""):
        rr = parse_wkt_rings(geom)
        rings.append(rr)
        if rr:
            pts = np.vstack(rr)
            cx.append(float(np.mean(pts[:, 0])))
            cy.append(float(np.mean(pts[:, 1])))
        else:
            cx.append(np.nan)
            cy.append(np.nan)

    zone_df["rings"] = rings
    zone_df["centroid_lon"] = cx
    zone_df["centroid_lat"] = cy

    map_df = zone_df.merge(pickup_map.rename(columns={"location_id": "Location ID"}), on="Location ID", how="left")
    map_df["trips"] = map_df["trips"].fillna(0)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with PdfPages(out_path) as pdf:
        fig = plt.figure(figsize=(16.54, 11.69))
        add_header(fig, "NYC Taxi Trip Dashboard", "Dense one-page summary", logo_path=logo_path)
        gs = fig.add_gridspec(32, 24)

        k_names = ["Trips", "Avg Pax", "Total Revenue", "Avg Fare", "Avg Tip", "Avg Distance"]
        k_vals = [
            fmt_num(kpi.loc[0, "trips"], 0),
            fmt_num(kpi.loc[0, "avg_pax"], 2),
            fmt_num(kpi.loc[0, "total_revenue"], 0),
            fmt_num(kpi.loc[0, "avg_fare"], 2),
            fmt_num(kpi.loc[0, "avg_tip"], 2),
            fmt_num(kpi.loc[0, "avg_dist"], 2),
        ]
        colors = ["#3b82f6", "#22c55e", "#ef4444", "#f97316", "#0ea5a4", "#8b5cf6"]
        for i in range(6):
            ax = fig.add_subplot(gs[0:3, i * 4 : (i + 1) * 4])
            draw_card(ax, k_names[i], k_vals[i], colors[i])

        ax_center = fig.add_subplot(gs[4:13, 0:12])
        if not daily.empty:
            ax_center.plot(daily["day"], daily["trips"], color="#2563eb", lw=1.7, label="Trips")
            ax_center.plot(daily["day"], daily["revenue"] / max(daily["revenue"].max(), 1) * daily["trips"].max(),
                           color="#ef4444", lw=1.5, label="Revenue (scaled)")
            ax_center.plot(daily["day"], daily["avg_fare"] * 10000, color="#8b5cf6", lw=1.5, label="Avg Fare (scaled)")
            ax_center.plot(daily["day"], daily["avg_tip"] * 16000, color="#10b981", lw=1.5, label="Avg Tip (scaled)")
        ax_center.set_title("Trend Evolution (multi-metric)", fontsize=10.5, weight="bold", pad=10)
        ax_center.grid(alpha=0.25)
        ax_center.legend(fontsize=7, ncol=2, loc="upper left")

        ax_map = fig.add_subplot(gs[3:21, 12:24])
        draw_dot_map(ax_map, map_df)

        ax_combo = fig.add_subplot(gs[14:22, 0:12])
        ax_combo.bar(hourly["hour"], hourly["trips"], color="#b94a48", alpha=0.85, label="Trips")
        ax2 = ax_combo.twinx()
        ax2.plot(hourly["hour"], hourly["avg_fare"], color="#4b5563", lw=2.0, label="Avg Fare")
        ax_combo.set_title("Hour Pattern", fontsize=10, weight="bold", pad=8)
        ax_combo.grid(axis="y", alpha=0.22)

        ax_tbl = fig.add_subplot(gs[22:31, 0:8])
        ax_tbl.axis("off")
        borough = map_df.groupby("Borough", as_index=False)["trips"].sum().sort_values("trips", ascending=False).head(6)
        tip_by_loc = read_sql(
            f"""
            SELECT l.{loc_id}::int AS location_id, AVG(f.{tip_col}) AS avg_tip
            FROM {schema}.fact_trip f
            JOIN {schema}.dim_location l ON f.{pu_loc_key}=l.{loc_dim_key}
            JOIN {schema}.dim_datetime d ON f.{pu_dt_key}=d.{dt_key}
            WHERE d.{dt_time} >= :ds AND d.{dt_time} < :de
            GROUP BY 1
            """,
            ds=ds, de=de,
        )
        tmap = zone_df[["Location ID", "Borough"]].merge(tip_by_loc, left_on="Location ID", right_on="location_id", how="left")
        tip_b = tmap.groupby("Borough", as_index=False)["avg_tip"].mean()
        borough = borough.merge(tip_b, on="Borough", how="left")
        disp = borough[["Borough", "avg_tip", "trips"]].copy()
        disp["avg_tip"] = disp["avg_tip"].map(lambda x: f"${x:.2f}" if pd.notna(x) else "-")
        disp["trips"] = disp["trips"].map(lambda x: fmt_num(x, 0))
        table = ax_tbl.table(cellText=disp.values.tolist(), colLabels=["Borough", "Avg Tip", "Riders"], loc="center")
        table.auto_set_font_size(False)
        table.set_fontsize(8.6)
        table.scale(1.12, 1.28)
        ax_tbl.set_title("Borough Mix", fontsize=10, weight="bold", pad=8)

        ax_donut = fig.add_subplot(gs[22:31, 8:12])
        pp = pay.head(6)
        ax_donut.pie(pp["trips"], labels=pp["payment"], wedgeprops=dict(width=0.35), startangle=90, textprops={"fontsize": 7})
        ax_donut.set_title("Trips by Payment", fontsize=10, weight="bold", pad=8)

        ax_week1 = fig.add_subplot(gs[22:31, 12:24])
        weekday1 = dow_hour.groupby("dow", as_index=False)["trips"].sum().sort_values("dow")
        weekday1["lbl"] = weekday1["dow"].map({0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"})
        ax_week1.bar(weekday1["lbl"], weekday1["trips"], color="#3fa46a")
        ax_week1.set_title("Trips by Weekday", fontsize=10, weight="bold", pad=8)
        ax_week1.grid(axis="y", alpha=0.25)

        fig.subplots_adjust(left=0.04, right=0.98, top=0.944, bottom=0.04, wspace=0.95, hspace=1.32)
        pdf.savefig(fig, dpi=220)
        plt.close(fig)

        fig2 = plt.figure(figsize=(16.54, 11.69))
        add_header(fig2, "NYC Taxi Trip Dashboard", "Supplementary diagnostics", logo_path=logo_path)
        gs2 = fig2.add_gridspec(30, 24)

        ax_r = fig2.add_subplot(gs2[1:10, 0:8])
        rr = rate.copy()
        if len(rr) > 6:
            other = rr.iloc[5:]["trips"].sum()
            rr = pd.concat([rr.head(5), pd.DataFrame([{"rate_code": "Other", "trips": other}])], ignore_index=True)
        rr = rr.sort_values("trips")
        ax_r.barh(rr["rate_code"].astype(str), rr["trips"], color="#d4a017")
        ax_r.set_title("Rate Code Mix", fontsize=10, weight="bold", pad=8)
        ax_r.grid(axis="x", alpha=0.25)

        ax_tp = fig2.add_subplot(gs2[1:10, 8:16])
        tpp = top_pickup.sort_values("trips")
        ax_tp.barh(tpp["location_id"].astype(str), tpp["trips"], color="#2b8cbe")
        ax_tp.set_title("Top Pickup Zones", fontsize=10, weight="bold", pad=8)
        ax_tp.grid(axis="x", alpha=0.25)

        ax_td = fig2.add_subplot(gs2[1:10, 16:24])
        tdd = top_dropoff.sort_values("trips")
        ax_td.barh(tdd["location_id"].astype(str), tdd["trips"], color="#f28e2b")
        ax_td.set_title("Top Dropoff Zones", fontsize=10, weight="bold", pad=8)
        ax_td.grid(axis="x", alpha=0.25)

        ax_src = fig2.add_subplot(gs2[11:20, 0:12])
        sl = source["source_file"].astype(str).map(lambda s: s[:28] + "..." if len(s) > 31 else s)
        ax_src.barh(sl, source["trips"], color="#e15759")
        ax_src.set_title("Source File Contribution", fontsize=10, weight="bold", pad=8)
        ax_src.grid(axis="x", alpha=0.25)

        ax_dist = fig2.add_subplot(gs2[11:20, 12:24])
        ax_dist.bar(distance_bins["bucket"].astype(int), distance_bins["n"].astype(float), color="#59a14f")
        ax_dist.set_title("Trip Distance Distribution", fontsize=10, weight="bold", pad=8)
        ax_dist.grid(axis="y", alpha=0.25)

        ax_pay2 = fig2.add_subplot(gs2[22:29, 0:12])
        p2 = pay.head(4)
        ax_pay2.bar(p2["payment"].astype(str), p2["trips"], color=["#c08b7d", "#2fa866", "#cfd8e3", "#8fa3bf"][:len(p2)])
        ax_pay2.set_title("Top Payment Types", fontsize=10, weight="bold", pad=8)
        ax_pay2.grid(axis="y", alpha=0.25)

        ax_ft2 = fig2.add_subplot(gs2[22:29, 12:24])
        ax_ft2.plot(hourly["hour"], hourly["avg_fare"], color="#7c3aed", lw=2.0, marker="o", ms=2.8, label="Avg Fare")
        ax_ft2.plot(hourly["hour"], hourly["avg_tip"], color="#0ea5a4", lw=2.0, marker="o", ms=2.8, label="Avg Tip")
        ax_ft2.set_title("Fare vs Tip by Hour", fontsize=10, weight="bold", pad=8)
        ax_ft2.grid(alpha=0.25)
        ax_ft2.legend(fontsize=7, loc="upper right")

        fig2.subplots_adjust(left=0.04, right=0.98, top=0.944, bottom=0.05, wspace=1.05, hspace=1.65)
        pdf.savefig(fig2, dpi=220)
        plt.close(fig2)

    print(f"PDF generated: {out_path}")


if __name__ == "__main__":
    main()
