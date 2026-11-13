"""
BusPulse dashboard: punctuality, delays, and speeds of Kolumbus buses on the Stavanger road network.

    streamlit run dashboard/app.py -- --gold <gold folder> --map dashboard/map

Reads the gold tables (parquet, from analysis/gold/build_gold.py) and the static Kolumbus road map
(from dashboard/build_map.py). Queries run straight from pre-aggregated gold cubes with DuckDB, and
maps, queries, and figures are cached globally across visitors so the dashboard stays instant and
works 100% offline.
"""
import argparse
import os
import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

HERE = Path(__file__).resolve().parent
ap = argparse.ArgumentParser()
_default_gold = HERE.parent / "tables" / "gold-layer"
if not _default_gold.exists():
    _default_gold = HERE.parent / "tables/parquet_2026-10-24/gold-layer-parquet"
ap.add_argument("--gold", default=os.environ.get("BUSPULSE_GOLD_DIR", str(_default_gold)))
ap.add_argument("--map", default=os.environ.get("BUSPULSE_MAP_DIR", str(HERE / "map")))
ARGS, _ = ap.parse_known_args(sys.argv[1:])
GOLD_DIR, MAP_DIR = Path(ARGS.gold), Path(ARGS.map)

st.set_page_config(page_title="BusPulse · Kolumbus", page_icon="🚌", layout="wide")
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
STAVANGER = dict(latitude=58.955, longitude=5.725, zoom=11.2)
CACHE_TTL = 43200  # 12 hours: computed once for the first visitor and served globally to everyone


# ------------------------------------------------------------------ global server-wide cache & data access
@st.cache_resource
def connect():
    """Single shared in-memory DuckDB connection with views over all gold parquet tables and the Kolumbus road map."""
    con = duckdb.connect()
    found = []
    for p in sorted(GOLD_DIR.glob("*.parquet")):
        src = f"{p.as_posix()}/*.parquet" if p.is_dir() else p.as_posix()
        con.execute(f"CREATE VIEW {p.stem} AS SELECT * FROM read_parquet('{src}')")
        found.append(p.stem)
    seg = MAP_DIR / "road_segments.parquet"
    if seg.exists():
        con.execute(f"CREATE VIEW map_seg AS SELECT from_stop_ref, to_stop_ref, length_m, n_trips FROM read_parquet('{seg.as_posix()}')")
        found.append("map_seg")
    return con, set(found)


def q(sql, params=()):
    return _q(sql, tuple(params))


@st.cache_data(show_spinner=False, max_entries=2048, ttl=CACHE_TTL)
def _q(sql, params):
    con, _ = connect()
    return con.cursor().execute(sql, list(params)).df()


@st.cache_resource
def road_geometry():
    """Kolumbus road segments with geometry — loaded once into server RAM and shared across all users."""
    p = MAP_DIR / "road_segments.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    df["path"] = df["path"].map(lambda a: [list(x) for x in a])
    return df


@st.cache_resource
def base_road_layer():
    """Pre-built static PyDeck PathLayer for the Kolumbus bus road network (100% offline, never rebuilt)."""
    geo = road_geometry()
    if geo is None:
        return None
    return pdk.Layer(
        "PathLayer",
        data=geo[["path"]],
        get_path="path",
        get_color=[150, 150, 150, 105],
        get_width=8,
        width_min_pixels=1,
        width_max_pixels=2,
        pickable=False,
    )


@st.cache_resource
def stop_names():
    p = MAP_DIR / "stops.parquet"
    return pd.read_parquet(p).set_index("stop_ref")["stop_name"] if p.exists() else pd.Series(dtype=str)


con, TABLES = connect()
if "fact_stop_visits" not in TABLES and "gold_delay_hour" not in TABLES:
    st.error(f"No gold tables found in `{GOLD_DIR}`. Run `analysis/gold/build_gold.py` first, or start with "
             "`streamlit run dashboard/app.py -- --gold <folder with the gold parquet files>`.")
    st.stop()

HAS = {t: t in TABLES for t in [
    "fact_stop_visits", "gold_delay_hour", "gold_segment_hour", "gold_trip_summary", "gold_headway",
    "map_seg", "dim_line", "gold_data_quality", "gold_meta", "gold_speed_hour", "gold_busy_roads",
    "gold_headway_hour", "gold_trip_metrics", "gold_thresholds", "dim_stop",
]}
RULES = (q("SELECT rule, value FROM gold_thresholds").set_index("rule")["value"].to_dict()
         if HAS["gold_thresholds"] else {})
BUNCH, GAP = RULES.get("bunch_ratio", 0.25), RULES.get("gap_ratio", 2.0)

DH_COLS = set(q("SELECT * FROM gold_delay_hour LIMIT 0").columns) if HAS["gold_delay_hour"] else set()
USE_DH = HAS["gold_delay_hour"] and ("d51" in DH_COLS)


def insight(text):
    """Plain-English takeaway banner explaining what the numbers show and what they mean for riders."""
    st.info(text, icon="💡")


def fmt_s(x):
    return "–" if x is None or pd.isna(x) else f"{x / 60:.1f} min" if abs(x) >= 120 else f"{x:.0f} s"


# ------------------------------------------------------------------ sidebar filters
if USE_DH:
    dates = q("""SELECT CAST(service_date AS VARCHAR) AS d, SUM(n_valid) AS n, ANY_VALUE(weekday) AS wd
                 FROM gold_delay_hour GROUP BY 1 HAVING SUM(n_valid) > 0 ORDER BY 1""")
else:
    dates = q("""SELECT CAST(service_date AS VARCHAR) AS d, COUNT(*) AS n, ISODOW(ANY_VALUE(service_date)) - 1 AS wd
                 FROM fact_stop_visits WHERE valid GROUP BY 1 ORDER BY 1""")

if dates.empty:
    st.error("The gold dataset has no valid stop visits.")
    st.stop()
dates["label"] = [f"{d} ({WEEKDAYS[int(w)][:3] if 0 <= w < 7 else '?'}): {int(n):,} visits"
                  for d, n, w in zip(dates.d, dates.n, dates.wd)]
full = dates[dates.n >= 0.2 * dates.n.max()]
dates["week"] = pd.to_datetime(dates.d).dt.isocalendar().week.astype(int)

st.sidebar.title("🚌 BusPulse")
st.sidebar.caption("Kolumbus buses · Stavanger region")
weeks_all = sorted(dates.week.unique())
sel_weeks = st.sidebar.multiselect("Weeks (ISO)", weeks_all, default=weeks_all,
                                   help="Filter the available dates below by calendar week")
date_opts = dates[dates.week.isin(sel_weeks)]
default_dates = [d for d in full.d if d in set(date_opts.d)]
sel_dates = st.sidebar.multiselect(
    "Service dates",
    date_opts.d.tolist(),
    default=default_dates,
    format_func=dict(zip(dates.d, dates.label)).get,
    help="Dates with only a few hundred visits are overnight tail runs from the previous evening and are unchecked by default.",
)
hours = st.sidebar.slider("Hours (local time)", 0, 23, (0, 23))

if USE_DH:
    routes_all = q("""SELECT line_ref, ANY_VALUE(route_short_name) AS route, SUM(n_valid) AS n
                      FROM gold_delay_hour GROUP BY 1 HAVING SUM(n_valid) > 0 ORDER BY n DESC""")
    dirs = q("SELECT DISTINCT direction_ref FROM gold_delay_hour ORDER BY 1").direction_ref.tolist()
else:
    routes_all = q("""SELECT f.line_ref, ANY_VALUE(f.route_short_name) AS route, COUNT(*) AS n
                      FROM fact_stop_visits f WHERE valid GROUP BY 1 ORDER BY n DESC""")
    dirs = q("SELECT DISTINCT direction_ref FROM fact_stop_visits ORDER BY 1").direction_ref.tolist()

if HAS["dim_line"]:
    routes_all = routes_all.merge(q("SELECT line_ref, route_type_name FROM dim_line"), on="line_ref", how="left")
    types = sorted(routes_all["route_type_name"].dropna().unique())
    sel_types = st.sidebar.multiselect("Service types", types, default=types)
    routes_all = routes_all[routes_all["route_type_name"].isin(sel_types)]

route_label = {r.line_ref: f"{r.route}" for r in routes_all.itertuples()}
sel_routes = st.sidebar.multiselect("Routes (empty = all)", routes_all.line_ref.tolist(), format_func=route_label.get)
sel_dir = st.sidebar.selectbox("Direction", ["all"] + dirs)

with st.sidebar.expander("Punctuality settings", expanded=False):
    early_s, late_s = st.slider(
        "What counts as 'on time' (seconds from schedule)",
        -300, 900, (-60, 300), step=30,
        help="Default standard: up to 1 minute early and under 5 minutes late counts as on time.",
    )
    min_n = st.slider("Minimum bus visits to show a stop or road", 1, 50, 10)
st.sidebar.caption(f"On time: {abs(early_s)//60 if early_s < 0 else 0} min early to {late_s//60} min late · min. {min_n} visits")

if not sel_dates:
    st.warning("Select at least one service date in the sidebar.")
    st.stop()
line_filter = sel_routes or routes_all.line_ref.tolist()
if not line_filter:
    st.warning("No routes match the selected service types.")
    st.stop()


def make_where(dates_t, hours_t, lines_t, dir_val, hour_col="local_hour", alias=""):
    a = f"{alias}." if alias else ""
    sql = [
        f"CAST({a}service_date AS VARCHAR) IN ({','.join('?' * len(dates_t))})",
        f"{a}{hour_col} BETWEEN ? AND ?",
        f"{a}line_ref IN ({','.join('?' * len(lines_t))})",
    ]
    params = [*dates_t, hours_t[0], hours_t[1], *lines_t]
    if dir_val != "all":
        sql.append(f"{a}direction_ref = ?")
        params.append(dir_val)
    return " AND ".join(sql), tuple(params)


def where(hour_col="local_hour", alias=""):
    return make_where(tuple(sel_dates), tuple(hours), tuple(line_filter), sel_dir, hour_col=hour_col, alias=alias)


def bucket_sum(lo_idx, hi_idx, alias=""):
    a = f"{alias}." if alias else ""
    cols = [f"{a}d{i}" for i in range(lo_idx, hi_idx)]
    return f"SUM({' + '.join(cols)})" if cols else "0"


def delay_exprs(early_val, late_val, alias=""):
    i_early, i_late = (int(early_val) + 300) // 30, (int(late_val) + 300) // 30
    a = f"{alias}." if alias else ""
    valid_s = f"SUM({a}n_valid)"
    early_s_expr = bucket_sum(0, i_early + 1, alias)
    ontime_s_expr = bucket_sum(i_early + 1, i_late + 1, alias)
    late_s_expr = bucket_sum(i_late + 1, 52, alias)
    otp_pct = f"{ontime_s_expr} * 100.0 / NULLIF({valid_s}, 0)"
    late_pct = f"{late_s_expr} * 100.0 / NULLIF({valid_s}, 0)"
    mean_d = f"SUM({a}sum_delay_s) / NULLIF({valid_s}, 0)"
    return early_s_expr, ontime_s_expr, late_s_expr, otp_pct, late_pct, mean_d


W, P = where()
ON_TIME = f"(delay_s >= {int(early_s)} AND delay_s < {int(late_s)})"
EARLY_SUM, ONTIME_SUM, LATE_SUM, OTP_PCT, LATE_PCT, MEAN_DELAY = delay_exprs(early_s, late_s)


# ------------------------------------------------------------------ cached charts & map builders
DELAY_SCALE = [(0, "#1a9850"), (0.2, "#91cf60"), (0.4, "#fee08b"), (0.7, "#fc8d59"), (1, "#d73027")]
OTP_SCALE = [(0, "#d73027"), (0.3, "#fc8d59"), (0.6, "#fee08b"), (0.8, "#91cf60"), (1, "#1a9850")]
SPEED_STOPS = [(10, (215, 48, 39)), (20, (252, 141, 89)), (30, (254, 224, 139)), (40, (145, 207, 96)), (55, (26, 152, 80))]
DELAY_STOPS = [(0, (26, 152, 80)), (60, (145, 207, 96)), (180, (254, 224, 139)), (300, (252, 141, 89)), (600, (215, 48, 39))]
GAIN_STOPS = [(-30, (69, 117, 180)), (0, (240, 240, 240)), (15, (254, 224, 139)), (30, (252, 141, 89)), (60, (215, 48, 39))]
OTP_STOPS = [(50, (215, 48, 39)), (65, (252, 141, 89)), (75, (254, 224, 139)), (85, (145, 207, 96)), (95, (26, 152, 80))]
TTI_STOPS = [(1.0, (26, 152, 80)), (1.2, (145, 207, 96)), (1.4, (254, 224, 139)), (1.7, (252, 141, 89)), (2.0, (215, 48, 39))]


def ramp(values, stops):
    xs = np.array([s[0] for s in stops], float)
    cols = np.array([s[1] for s in stops], float)
    v = np.clip(np.asarray(values, float), xs[0], xs[-1])
    return np.column_stack([np.interp(v, xs, cols[:, i]) for i in range(3)]).astype(int).tolist()


def road_heat_layer(seg, weight_col, radius=35):
    pts = seg[["path", weight_col]].dropna()
    pts = pts[pts[weight_col] > 0]
    if pts.empty:
        return None
    rows = [(x, y, w / len(path)) for path, w in zip(pts["path"], pts[weight_col]) for x, y in path]
    heat = pd.DataFrame(rows, columns=["lon", "lat", "w"])
    return pdk.Layer(
        "HeatmapLayer", data=heat, get_position=["lon", "lat"], get_weight="w",
        radius_pixels=radius, intensity=1.2, threshold=0.03, opacity=0.8, aggregation="SUM",
    )


def legend(title, stops, unit):
    grad = ", ".join(f"rgb{tuple(c)} {100 * i / (len(stops) - 1):.0f}%" for i, (_, c) in enumerate(stops))
    ticks = "".join(f"<span>{v:g}{unit}</span>" for v, _ in stops)
    st.markdown(f"""<div style="font-size:0.8rem;margin:-0.3rem 0 0.6rem 0"><b>{title}</b>
      <div style="height:10px;border-radius:3px;background:linear-gradient(90deg,{grad})"></div>
      <div style="display:flex;justify-content:space-between">{ticks}</div></div>""", unsafe_allow_html=True)


@st.cache_data(show_spinner=False, max_entries=512, ttl=CACHE_TTL)
def heatmap(df, x, y, z, title, zlabel, scale, zmin=None, zmax=None, fmt=".0f", height=None, y_order=None):
    pv = df.pivot_table(index=y, columns=x, values=z, aggfunc="first")
    if y_order is not None:
        pv = pv.reindex([r for r in y_order if r in pv.index])
    fig = px.imshow(
        pv, aspect="auto", color_continuous_scale=scale, zmin=zmin, zmax=zmax,
        labels=dict(
            color=zlabel,
            x={"local_hour": "Hour (local time)", "local_time_bin": "Time of day"}.get(x, x.replace("_", " ").capitalize()),
            y="",
        ),
        text_auto=fmt if pv.size <= 700 else False,
    )
    fig.update_layout(
        title=title, height=height or max(260, 22 * len(pv) + 120),
        margin=dict(l=10, r=10, t=50, b=10), coloraxis_colorbar=dict(title=zlabel),
    )
    fig.update_xaxes(type="category")
    fig.update_yaxes(type="category")
    return fig


@st.cache_resource(max_entries=128)
def build_road_map_deck(dates_t, hours_t, lines_t, dir_val, min_n_val, early_val, late_val, metric, points):
    """Builds and caches the complete offline Kolumbus road-network Deck across all users."""
    geo = road_geometry()
    base = base_road_layer()
    if geo is None or base is None:
        return None, None, pd.DataFrame(), [], "", [], ""

    layers = [base]
    seg_tab = None
    stops_, unit = [], ""
    w_sql, p_tup = make_where(dates_t, hours_t, lines_t, dir_val)

    if metric != "Off" and HAS["gold_segment_hour"]:
        ws_sql, ps_tup = make_where(dates_t, hours_t, lines_t, dir_val, alias="s")
        seg = q(f"""SELECT from_stop_ref, to_stop_ref, SUM(n) n, SUM(sum_actual_s) act, SUM(sum_aimed_s) aim,
                           COUNT(DISTINCT route_short_name) n_routes, STRING_AGG(DISTINCT route_short_name, ', ') routes
                    FROM gold_segment_hour s WHERE {ws_sql} GROUP BY 1, 2 HAVING SUM(n) >= ?""", (*ps_tup, min_n_val))
        seg = seg.merge(geo[["from_stop_ref", "to_stop_ref", "length_m", "path"]], on=["from_stop_ref", "to_stop_ref"])
        if not seg.empty:
            seg["speed"] = 3.6 * seg.length_m * seg.n / seg.act
            seg["gain"] = (seg.act - seg.aim) / seg.n
            names = stop_names()
            seg["name"] = (seg.from_stop_ref.map(names).fillna(seg.from_stop_ref)
                           + " → " + seg.to_stop_ref.map(names).fillna(seg.to_stop_ref))
            seg["tip"] = [
                f"<b>{nm}</b><br/>Routes: {r}<br/>{int(n):,} buses · {l:,.0f} m<br/>"
                f"Avg. speed: {s:.0f} km/h · {g:+.0f} s vs schedule per bus"
                for nm, r, n, l, s, g in zip(seg.name, seg.routes, seg.n, seg.length_m, seg.speed, seg.gain)
            ]
            if metric.startswith("Heatmap"):
                seg["lost_h"] = (seg.act - seg.aim).clip(lower=0) / 3600
                hl = road_heat_layer(seg, "lost_h" if "lose" in metric else "n")
                if hl is not None:
                    layers.append(hl)
                seg_tab = seg
            else:
                if metric.startswith("Speed"):
                    seg["color"], stops_, unit = ramp(seg.speed, SPEED_STOPS), SPEED_STOPS, " km/h"
                elif metric.startswith("Delay gained"):
                    seg["color"], stops_, unit = ramp(seg.gain, GAIN_STOPS), GAIN_STOPS, " s"
                else:
                    lo, hi = np.log10(min_n_val), np.log10(max(seg.n.max(), min_n_val * 10))
                    vstops = [(round(10 ** (lo + i * (hi - lo) / 4)), c) for i, (_, c) in enumerate(SPEED_STOPS[::-1])]
                    seg["color"], stops_, unit = ramp(seg.n, vstops), vstops, ""
                seg["width"] = 6 + 8 * np.log10(seg.n)
                layers.append(pdk.Layer(
                    "PathLayer", data=seg[["path", "color", "width", "tip"]], get_path="path",
                    get_color="color", get_width="width", width_min_pixels=1.5, width_max_pixels=10,
                    cap_rounded=True, joint_rounded=True, pickable=True, auto_highlight=True,
                ))
                seg_tab = seg

    if USE_DH and HAS["dim_stop"]:
        wh_sql, ph_tup = make_where(dates_t, hours_t, lines_t, dir_val, alias="h")
        _, _, _, otp_h, _, mean_h = delay_exprs(early_val, late_val, alias="h")
        i_late = (int(late_val) + 300) // 30
        stop_df = q(f"""SELECT h.stop_ref, ANY_VALUE(d.stop_name) stop_name, ANY_VALUE(d.stop_lat) lat, ANY_VALUE(d.stop_lon) lon,
                               SUM(h.n_valid) n, {mean_h} mean_delay, {otp_h} otp,
                               {bucket_sum(i_late + 1, 52, 'h')} n_late
                        FROM gold_delay_hour h LEFT JOIN dim_stop d USING (stop_ref)
                        WHERE {wh_sql} GROUP BY 1 HAVING SUM(h.n_valid) >= ?""", (*ph_tup, min_n_val))
    else:
        ot_sql = f"(delay_s >= {int(early_val)} AND delay_s < {int(late_val)})"
        stop_df = q(f"""SELECT stop_ref, ANY_VALUE(stop_name) stop_name, AVG(stop_lat) lat, AVG(stop_lon) lon, COUNT(*) n,
                               AVG(delay_s) mean_delay, AVG({ot_sql}::INT) * 100 otp,
                               SUM((delay_s >= {int(late_val)})::INT) n_late
                        FROM fact_stop_visits WHERE valid AND {w_sql} GROUP BY 1 HAVING COUNT(*) >= ?""", (*p_tup, min_n_val))

    s_stops, s_unit = [], ""
    if points != "Off" and not stop_df.empty:
        stop_df = stop_df.copy()
        stop_df["tip"] = [
            f"<b>{s}</b><br/>{int(n):,} visits<br/>Mean delay: {fmt_s(d)} · {o:.0f}% on time"
            for s, n, d, o in zip(stop_df.stop_name, stop_df.n, stop_df.mean_delay, stop_df.otp)
        ]
        if points.startswith("Lateness"):
            layers.append(pdk.Layer(
                "HeatmapLayer", data=stop_df[["lon", "lat", "n_late"]], get_position=["lon", "lat"],
                get_weight="n_late", radius_pixels=40, intensity=1, threshold=0.05, opacity=0.75,
            ))
        else:
            if points.startswith("Mean"):
                stop_df["color"], s_stops, s_unit = ramp(stop_df.mean_delay, DELAY_STOPS), DELAY_STOPS, " s"
            else:
                stop_df["color"], s_stops, s_unit = ramp(stop_df.otp, OTP_STOPS), OTP_STOPS, "%"
            stop_df["r"] = 15 + 12 * np.log10(stop_df.n)
            layers.append(pdk.Layer(
                "ScatterplotLayer", data=stop_df[["lon", "lat", "color", "r", "tip"]],
                get_position=["lon", "lat"], get_fill_color="color", get_radius="r",
                radius_min_pixels=2, radius_max_pixels=12, stroked=True,
                get_line_color=[40, 40, 40], line_width_min_pixels=0.5, pickable=True,
            ))

    deck = pdk.Deck(
        map_provider=None,
        layers=layers,
        initial_view_state=pdk.ViewState(**STAVANGER),
        tooltip={"html": "{tip}", "style": {"fontSize": "12px"}},
    )
    return deck, seg_tab, stop_df, stops_, unit, s_stops, s_unit


@st.cache_resource(max_entries=128)
def build_stop_map_deck(mp_df, colour_by, pick_key, zoom_to_pick):
    """Builds and caches the offline Kolumbus stop map Deck."""
    mp = mp_df.dropna(subset=["lat", "lon"]).copy()
    if colour_by == "Mean delay":
        mp["color"], s_stops, s_unit = ramp(mp.mean_delay * 60, DELAY_STOPS), DELAY_STOPS, " s"
    else:
        mp["color"], s_stops, s_unit = ramp(mp.otp, OTP_STOPS), OTP_STOPS, "%"
    mp["r"] = 15 + 12 * np.log10(mp.visits)
    mp["tip"] = [
        f"<b>{s}</b><br/>{int(n):,} visits · Routes: {r}<br/>Mean delay: {d:.1f} min · {o:.0f}% on time"
        for s, n, r, d, o in zip(mp.stop, mp.visits, mp.routes, mp.mean_delay, mp.otp)
    ]
    layers = []
    base = base_road_layer()
    if base is not None:
        layers.append(base)
    layers.append(pdk.Layer(
        "ScatterplotLayer", data=mp[["lon", "lat", "color", "r", "tip"]], get_position=["lon", "lat"],
        get_fill_color="color", get_radius="r", radius_min_pixels=3, radius_max_pixels=14,
        stroked=True, get_line_color=[40, 40, 40], line_width_min_pixels=0.5,
        pickable=True, auto_highlight=True,
    ))
    sel = mp[mp.k == pick_key]
    if not sel.empty:
        layers.append(pdk.Layer(
            "ScatterplotLayer", data=sel[["lon", "lat", "tip"]], get_position=["lon", "lat"],
            get_radius=60, radius_min_pixels=12, filled=False, stroked=True,
            get_line_color=[20, 20, 255], line_width_min_pixels=3, pickable=True,
        ))
    view = (pdk.ViewState(latitude=float(sel.lat.iloc[0]), longitude=float(sel.lon.iloc[0]), zoom=13)
            if (not sel.empty and zoom_to_pick) else pdk.ViewState(**STAVANGER))
    deck = pdk.Deck(
        map_provider=None,
        layers=layers,
        initial_view_state=view,
        tooltip={"html": "{tip}", "style": {"fontSize": "12px"}},
    )
    return deck, s_stops, s_unit


@st.cache_resource(max_entries=128)
def build_busy_roads_deck(dates_t, hours_t, lines_t, dir_val, min_n_val, colour, stop_pick):
    """Builds and caches the offline Kolumbus busy-roads Deck across all users."""
    ws_sql, ps_tup = make_where(dates_t, hours_t, lines_t, dir_val, alias="s")
    stop_sql, stop_par = (("", ()) if stop_pick.startswith("(")
                          else (" AND (s.from_stop_ref = ? OR s.to_stop_ref = ?)", (stop_pick, stop_pick)))
    seg = q(f"""SELECT s.from_stop_ref, s.to_stop_ref, SUM(n_runs) n, SUM(sum_distance_m) dist, SUM(sum_run_s) run_s,
                       SUM(sum_sched_s) sched_s, SUM(sum_excess_s) excess_s,
                       STRING_AGG(DISTINCT route_short_name, ', ') routes
                FROM gold_speed_hour s WHERE {ws_sql}{stop_sql}
                GROUP BY 1, 2 HAVING SUM(n_runs) >= ?""", (*ps_tup, *stop_par, min_n_val))
    if seg.empty:
        return None, seg, [], ""

    br = q("""SELECT from_stop_ref, to_stop_ref, from_stop_name, to_stop_name, free_flow_kmh,
                     from_lat, from_lon, to_lat, to_lon FROM gold_busy_roads""")
    seg = seg.merge(br, on=["from_stop_ref", "to_stop_ref"], how="left")
    seg["kmh"] = seg.dist / seg.run_s * 3.6
    seg["tti"] = seg.free_flow_kmh / seg.kmh
    seg["lost_h"] = seg.excess_s / 3600
    seg["vs_tt"] = (seg.run_s - seg.sched_s) / seg.n

    geo = road_geometry()
    if geo is not None:
        seg = seg.merge(geo[["from_stop_ref", "to_stop_ref", "path"]], on=["from_stop_ref", "to_stop_ref"], how="left")
    else:
        seg["path"] = None
    straight = [[[a, b], [c_, d]] for a, b, c_, d in zip(seg.from_lon, seg.from_lat, seg.to_lon, seg.to_lat)]
    seg["path"] = [p_ if isinstance(p_, list) else s_ for p_, s_ in zip(seg["path"], straight)]
    seg = seg[seg.from_lat.notna() & seg.to_lat.notna()].copy()

    if colour.startswith("Slowdown"):
        seg["color"], stops_, unit = ramp(seg.tti.fillna(1), TTI_STOPS), TTI_STOPS, "×"
    elif colour.startswith("Speed"):
        seg["color"], stops_, unit = ramp(seg.kmh, SPEED_STOPS), SPEED_STOPS, " km/h"
    else:
        hi = max(float(seg.lost_h.quantile(0.98)), 1.0)
        stops_ = [(round(hi * f, 1), col) for f, (_, col) in zip([0, .25, .5, .75, 1], TTI_STOPS)]
        seg["color"], unit = ramp(seg.lost_h, stops_), " h"

    seg["width"] = 6 + 8 * np.log10(seg.n)
    seg["tip"] = [
        f"<b>{a} → {b}</b><br/>Routes: {r}<br/>{int(n):,} runs · {k:.1f} km/h (free-flow {ff:.1f})<br/>"
        f"Slowdown: {t:.2f}× · {l:.1f} bus-hours lost · {v:+.0f} s vs schedule per bus"
        for a, b, r, n, k, ff, t, l, v in zip(
            seg.from_stop_name, seg.to_stop_name, seg.routes, seg.n,
            seg.kmh, seg.free_flow_kmh, seg.tti, seg.lost_h, seg.vs_tt,
        )
    ]
    layers = []
    base = base_road_layer()
    if base is not None:
        layers.append(base)
    if colour.startswith("Heatmap"):
        hl = road_heat_layer(seg, "lost_h")
        if hl is not None:
            layers.append(hl)
    else:
        order = seg.sort_values("tti" if colour.startswith("Slowdown") else "lost_h", na_position="first")
        layers.append(pdk.Layer(
            "PathLayer", data=order[["path", "color", "width", "tip"]], get_path="path",
            get_color="color", get_width="width", width_min_pixels=1.5, width_max_pixels=10,
            cap_rounded=True, joint_rounded=True, pickable=True, auto_highlight=True,
        ))

    deck = pdk.Deck(
        map_provider=None,
        layers=layers,
        initial_view_state=pdk.ViewState(**STAVANGER),
        tooltip={"html": "{tip}", "style": {"fontSize": "12px"}},
    )
    return deck, seg, stops_, unit


@st.cache_resource
def warm_default_cache(dates_t, lines_t):
    """Pre-computes the default maps once for the first visitor so all subsequent visitors get instant loads."""
    try:
        build_road_map_deck(dates_t, (0, 23), lines_t, "all", 10, -60, 300, "Speed (km/h)", "Mean delay at stop")
        if HAS["gold_speed_hour"] and HAS["gold_busy_roads"]:
            build_busy_roads_deck(dates_t, (0, 23), lines_t, "all", 10, "Slowdown (travel time index)", "(all stops)")
    except Exception:
        pass
    return True


warm_default_cache(tuple(default_dates), tuple(routes_all.line_ref.tolist()))


NUM = st.column_config.NumberColumn
COLUMNS = {
    "service_date": st.column_config.DateColumn("Date", format="ddd D MMM YYYY"),
    "route_short_name": "Route", "lines": "Routes", "routes": "Routes",
    "direction_ref": "Direction", "start_hour": NUM("Start hour", format="%02d:00"),
    "n_stops": NUM("Stops", format="%d"), "start_delay_s": NUM("Delay at start (s)", format="%.0f"),
    "end_delay_s": NUM("Delay at end (s)", format="%.0f"), "delay_gained_s": NUM("Delay gained (s)", format="%+.0f"),
    "avg_kmh": NUM("Avg. speed (km/h)", format="%.1f"), "n_bunched": NUM("Times bunched", format="%d"),
    "n_headways": NUM("Headways judged", format="localized"),
    "from_stop_name": "From stop", "to_stop_name": "To stop", "rank_hours_lost": NUM("Rank", format="%d"),
    "n_runs": NUM("Runs", format="localized"), "n": NUM("Runs", format="localized"),
    "free_flow_kmh": NUM("Free-flow speed (km/h)", format="%.1f"), "peak_kmh": NUM("Peak speed (km/h)", format="%.1f"),
    "midday_kmh": NUM("Midday speed (km/h)", format="%.1f"), "night_kmh": NUM("Night speed (km/h)", format="%.1f"),
    "travel_time_index": NUM("Slowdown (×)", format="%.2f"), "vehicle_hours_lost": NUM("Bus-hours lost", format="%.1f"),
    "kmh": NUM("Avg. speed (km/h)", format="%.1f"), "tti": NUM("Slowdown (×)", format="%.2f"),
    "vs_tt": NUM("Vs. schedule (s per bus)", format="%+.0f"), "lost_h": NUM("Bus-hours lost", format="%.1f"),
}


# ------------------------------------------------------------------ pages
st.title("Kolumbus Bus Punctuality & Speed · Stavanger Region")
st.caption(f"{', '.join(sel_dates)} · {hours[0]:02d}:00–{hours[1]:02d}:59 · "
           f"{'all routes' if not sel_routes else str(len(sel_routes)) + ' route(s)'} · direction {sel_dir}")
tabs = st.tabs(["Overview", "Road map", "Routes", "Stops", "Time patterns", "Reliability", "Data",
                "Busy roads", "Bunching & speed heatmaps", "Trips"])


# ============ 0. Overview
with tabs[0]:
    if USE_DH:
        k = q(f"""SELECT SUM(n_valid) n,
                         {ONTIME_SUM} * 1.0 / NULLIF(SUM(n_valid), 0) otp,
                         {EARLY_SUM} * 1.0 / NULLIF(SUM(n_valid), 0) early,
                         {LATE_SUM} * 1.0 / NULLIF(SUM(n_valid), 0) late,
                         {MEAN_DELAY} mean_d,
                         COUNT(DISTINCT CASE WHEN n_valid > 0 THEN line_ref END) routes,
                         COUNT(DISTINCT CASE WHEN n_valid > 0 THEN stop_ref END) stops
                  FROM gold_delay_hour WHERE {W}""", P).iloc[0]
        if HAS["fact_stop_visits"]:
            kq = q(f"""SELECT MEDIAN(delay_s) med, QUANTILE_CONT(delay_s, 0.9) p90,
                              COUNT(DISTINCT (service_date, journey_ref)) trips
                       FROM fact_stop_visits WHERE valid AND {W}""", P).iloc[0]
            k["med"], k["p90"], k["trips"] = kq.med, kq.p90, kq.trips
        else:
            k["med"], k["p90"], k["trips"] = None, None, None
    else:
        k = q(f"""SELECT COUNT(*) n, AVG({ON_TIME}::INT) otp, AVG((delay_s < {int(early_s)})::INT) early,
                         AVG((delay_s >= {int(late_s)})::INT) late, AVG(delay_s) mean_d, MEDIAN(delay_s) med,
                         QUANTILE_CONT(delay_s, 0.9) p90, COUNT(DISTINCT line_ref) routes, COUNT(DISTINCT stop_ref) stops,
                         COUNT(DISTINCT (service_date, journey_ref)) trips
                  FROM fact_stop_visits WHERE valid AND {W}""", P).iloc[0]

    if not k.n or pd.isna(k.n):
        st.info("No bus visits match your current sidebar filters.")
    else:
        if USE_DH:
            hr = q(f"""SELECT local_hour, SUM(n_valid) n, {OTP_PCT} otp, {MEAN_DELAY} mean_delay
                       FROM gold_delay_hour WHERE {W} GROUP BY 1 HAVING SUM(n_valid) > 0 ORDER BY 1""", P)
            if HAS["fact_stop_visits"]:
                hr_p90 = q(f"""SELECT local_hour, QUANTILE_CONT(delay_s, 0.9) p90
                               FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1""", P)
                hr = hr.merge(hr_p90, on="local_hour", how="left")
            else:
                hr["p90"] = np.nan
        else:
            hr = q(f"""SELECT local_hour, COUNT(*) n, AVG({ON_TIME}::INT) * 100 otp, AVG(delay_s) mean_delay,
                              QUANTILE_CONT(delay_s, 0.9) p90
                       FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1 ORDER BY 1""", P)

        # Layman takeaway banner
        if not hr.empty:
            worst_hr_row = hr.loc[hr.mean_delay.idxmax()]
            best_hr_row = hr.loc[hr.otp.idxmax()]
            trip_txt = f" across **{int(k.trips):,} bus trips**" if pd.notna(k.trips) else ""
            insight(
                f"**What this shows:** Out of **{int(k.n):,} stop arrivals**{trip_txt}, **{k.otp:.0%}** were on time "
                f"and **{k.late:.0%}** ran more than {late_s // 60} minutes late. "
                f"**What it means for riders:** Service runs smoothest around **{int(best_hr_row.local_hour):02d}:00** "
                f"(**{best_hr_row.otp:.0f}%** on time), while delays peak around **{int(worst_hr_row.local_hour):02d}:00** "
                f"(averaging **{fmt_s(worst_hr_row.mean_delay)}** late) — leave a 5–10 minute buffer if travelling then."
            )

        c = st.columns(5)
        c[0].metric("Stop visits", f"{int(k.n):,}", help="Total recorded bus arrivals at stops")
        c[1].metric("On time", f"{k.otp:.1%}", help="Arrived within your selected punctuality window")
        c[2].metric("Late", f"{k.late:.1%}", help=f"More than {late_s // 60} min behind schedule")
        c[3].metric("Average delay", fmt_s(k.mean_d))
        c[4].metric("9 out of 10 buses within", fmt_s(k.p90), help="90% of all arrivals were less late than this")

        c = st.columns(5)
        c[0].metric("Bus trips", f"{int(k.trips):,}" if pd.notna(k.trips) else "–")
        c[1].metric("Routes / stops", f"{int(k.routes)} / {int(k.stops):,}")
        c[2].metric("Typical (median) delay", fmt_s(k.med))
        if HAS["gold_segment_hour"] and HAS["map_seg"]:
            sp = q(f"""SELECT 3.6 * SUM(m.length_m * s.n) / SUM(s.sum_actual_s) AS act,
                              3.6 * SUM(m.length_m * s.n) / NULLIF(SUM(s.sum_aimed_s), 0) AS sch,
                              SUM(s.sum_actual_s - s.sum_aimed_s) / 3600 AS lost_h
                       FROM gold_segment_hour s JOIN map_seg m USING (from_stop_ref, to_stop_ref)
                       WHERE {where(alias='s')[0]}""", where(alias="s")[1]).iloc[0]
            if pd.notna(sp.act):
                c[3].metric("Average speed", f"{sp.act:.1f} km/h", f"{sp.act - sp.sch:+.1f} vs schedule")
                c[4].metric("Extra time vs schedule", f"{sp.lost_h:,.0f} bus-hours")

        left, right = st.columns([3, 2])
        fig = go.Figure()
        fig.add_bar(x=hr.local_hour, y=hr.n, name="Bus visits", marker_color="#d9d9d9", yaxis="y2")
        fig.add_scatter(x=hr.local_hour, y=hr.mean_delay / 60, name="Average delay (min)", line=dict(color="#d73027", width=3))
        if hr.p90.notna().any():
            fig.add_scatter(x=hr.local_hour, y=hr.p90 / 60, name="90% of buses within (min)", line=dict(color="#fc8d59", dash="dot"))
        fig.update_layout(
            title="How delays build and clear through the day",
            xaxis=dict(title="Hour (local time)", dtick=1),
            yaxis=dict(title="Minutes late"),
            yaxis2=dict(overlaying="y", side="right", showgrid=False, title="Bus visits"),
            legend=dict(orientation="h", y=-0.2), height=380, margin=dict(t=50, b=10),
        )
        left.plotly_chart(fig, width="stretch")

        fig = px.bar(
            hr, x="local_hour", y="otp", color="otp", color_continuous_scale=OTP_SCALE, range_color=(50, 100),
            labels=dict(local_hour="Hour (local time)", otp="% on time"), title="Share of buses on time by hour",
        )
        fig.update_layout(height=380, coloraxis_showscale=False, margin=dict(t=50, b=10), xaxis_dtick=2)
        right.plotly_chart(fig, width="stretch")

        if USE_DH:
            sums = q(f"SELECT {', '.join(f'SUM(d{i}) AS d{i}' for i in range(52))} FROM gold_delay_hour WHERE {W}", P).iloc[0]
            bins = [-300 + 30 * j for j in range(51)]
            counts = [sums["d0"] + sums["d1"]] + [sums[f"d{j + 1}"] for j in range(1, 51)]
            dist = pd.DataFrame({"bin": bins, "n": counts})
            dist = dist[dist.n > 0]
        else:
            dist = q(f"""SELECT LEAST(GREATEST(FLOOR(delay_s / 30) * 30, -300), 1200) AS bin, COUNT(*) n
                         FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1 ORDER BY 1""", P)

        dist["Delay category"] = np.where(dist.bin < early_s, "Early", np.where(dist.bin >= late_s, "Late", "On time"))
        dist["Delay (minutes)"] = dist.bin / 60
        fig = px.bar(
            dist, x="Delay (minutes)", y="n", color="Delay category",
            color_discrete_map={"Early": "#4575b4", "On time": "#1a9850", "Late": "#d73027"},
            category_orders={"Delay category": ["Early", "On time", "Late"]},
            labels=dict(n="Stop visits"), title="How early or late do buses actually arrive?",
        )
        fig.update_layout(height=320, bargap=0.05, margin=dict(t=50, b=10))
        st.plotly_chart(fig, width="stretch")
        st.caption("Green bars show buses arriving comfortably on schedule; blue bars on the left are early departures "
                   "(watch out so you don't miss them!), and red bars on the right show delayed arrivals.")


# ============ 1. Road map (100% offline Kolumbus network, globally cached across all visitors)
with tabs[1]:
    geo = road_geometry()
    if geo is None:
        st.warning(f"No Kolumbus road map found in `{MAP_DIR}`. Build it once with `python dashboard/build_map.py --gtfs <gtfs folder>`.")
    else:
        c1, c2 = st.columns([1, 1])
        metric = c1.selectbox(
            "Colour Kolumbus roads by",
            ["Speed (km/h)", "Delay gained per bus (s)", "Buses (passes)",
             "Heatmap: where buses lose time", "Heatmap: where most buses drive", "Off"],
            help="Shows how fast buses move and where delays build up along the Kolumbus road network.",
        )
        points = c2.selectbox(
            "Colour stops by",
            ["Mean delay at stop", "Share on time at stop", "Lateness hot spots (heatmap)", "Off"],
        )

        deck, seg_tab, stop_df, stops_, unit, s_stops, s_unit = build_road_map_deck(
            tuple(sel_dates), tuple(hours), tuple(line_filter), sel_dir, int(min_n), int(early_s), int(late_s), metric, points
        )

        if seg_tab is not None and not seg_tab.empty:
            slowest = seg_tab.loc[seg_tab.speed.idxmin()]
            worst_gain = seg_tab.loc[seg_tab.gain.idxmax()]
            avg_net_spd = 3.6 * (seg_tab.length_m * seg_tab.n).sum() / seg_tab.act.sum()
            insight(
                f"**What this map shows:** Buses average **{avg_net_spd:.0f} km/h** across the active Kolumbus corridors. "
                f"**What it means for riders:** Green stretches move smoothly, while red stretches mark slow bottlenecks — "
                f"the slowest stretch in this view is **{slowest['name']}** (**{slowest['speed']:.0f} km/h**), and the biggest "
                f"schedule loss happens on **{worst_gain['name']}** (**{worst_gain['gain']:+.0f} s** per bus)."
            )

        st.pydeck_chart(deck, height=640, key="kolumbus_road_map")

        lc = st.columns(2)
        with lc[0]:
            if seg_tab is not None and metric.startswith("Heatmap"):
                st.caption("Brighter glow = more " + ("total bus-hours lost behind schedule" if "lose" in metric
                           else "bus traffic") + " along that corridor. Zoom in to inspect street-level bottlenecks.")
            elif seg_tab is not None and stops_:
                legend(metric, stops_, unit)
        with lc[1]:
            if points != "Off" and not points.startswith("Lateness") and not stop_df.empty and s_stops:
                legend(points, s_stops, s_unit)

        st.caption("Showing the offline Kolumbus bus road network (cached for instant loading with or without internet). "
                   "Hover over any road stretch or stop for route details.")

        if seg_tab is not None and not seg_tab.empty:
            st.subheader("Slowest road stretches in your selection")
            slow = seg_tab.sort_values("speed").head(15)
            st.dataframe(
                slow[["name", "routes", "n", "length_m", "speed", "gain"]],
                hide_index=True, width="stretch",
                column_config={
                    "name": "Road stretch", "routes": "Routes",
                    "n": st.column_config.NumberColumn("Buses", format="localized"),
                    "length_m": st.column_config.NumberColumn("Length (m)", format="%.0f"),
                    "speed": st.column_config.NumberColumn("Avg. speed (km/h)", format="%.1f"),
                    "gain": st.column_config.NumberColumn("Vs. schedule (s per bus)", format="%+.0f"),
                },
            )


# ============ 2. Routes
with tabs[2]:
    if USE_DH:
        rt = q(f"""SELECT line_ref, ANY_VALUE(route_short_name) route, SUM(n_valid) visits,
                          {OTP_PCT} otp, {MEAN_DELAY} mean_delay
                   FROM gold_delay_hour WHERE {W} GROUP BY 1 HAVING SUM(n_valid) >= ?""", [*P, min_n])
        if HAS["fact_stop_visits"]:
            rt_extra = q(f"""SELECT line_ref, COUNT(DISTINCT (service_date, journey_ref)) trips,
                                    QUANTILE_CONT(delay_s, 0.9) p90
                             FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1""", P)
            rt = rt.merge(rt_extra, on="line_ref", how="left")
    else:
        rt = q(f"""SELECT line_ref, ANY_VALUE(route_short_name) route, COUNT(*) visits, COUNT(DISTINCT (service_date, journey_ref)) trips,
                          AVG({ON_TIME}::INT) * 100 otp, AVG(delay_s) mean_delay, QUANTILE_CONT(delay_s, 0.9) p90
                   FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1 HAVING COUNT(*) >= ?""", [*P, min_n])

    if rt.empty:
        st.info("No routes match these filters.")
    else:
        if HAS["gold_segment_hour"] and HAS["map_seg"]:
            Ws, Ps = where(alias="s")
            rs = q(f"""SELECT line_ref, 3.6 * SUM(m.length_m * s.n) / SUM(s.sum_actual_s) speed,
                              SUM(s.sum_actual_s - s.sum_aimed_s) / 3600 lost_h
                       FROM gold_segment_hour s JOIN map_seg m USING (from_stop_ref, to_stop_ref) WHERE {Ws} GROUP BY 1""", Ps)
            rt = rt.merge(rs, on="line_ref", how="left")
        if HAS["gold_trip_summary"]:
            Wt, Pt = where("start_hour")
            ts = q(f"""SELECT line_ref, AVG((end_delay_s < {int(late_s)})::INT) * 100 term_otp,
                              MEDIAN(delay_gained_s) gained FROM gold_trip_summary WHERE n_stops >= 2 AND {Wt} GROUP BY 1""", Pt)
            rt = rt.merge(ts, on="line_ref", how="left")
        rt = rt.sort_values("visits", ascending=False)

        best_rt = rt.loc[rt.otp.idxmax()]
        worst_rt = rt.loc[rt.mean_delay.idxmax()]
        insight(
            f"**What this shows:** Across **{len(rt)} active routes**, **Route {best_rt.route}** is the most dependable "
            f"(**{best_rt.otp:.0f}%** on time), while **Route {worst_rt.route}** sees the heaviest delays "
            f"(averaging **{fmt_s(worst_rt.mean_delay)}** late). "
            f"**What it means for riders:** Compare your line below against the network average, then pick your route at the "
            f"bottom to see the exact stop where delays start piling up."
        )

        st.subheader("Route comparison")
        cfg = {
            "line_ref": None, "route": "Route",
            "visits": st.column_config.NumberColumn("Stop visits", format="localized"),
            "trips": st.column_config.NumberColumn("Trips", format="localized"),
            "otp": st.column_config.ProgressColumn("% on time", min_value=0, max_value=100, format="%.0f%%"),
            "mean_delay": st.column_config.NumberColumn("Avg. delay (s)", format="%.0f"),
            "p90": st.column_config.NumberColumn("90% within (s)", format="%.0f"),
            "speed": st.column_config.NumberColumn("Avg. speed (km/h)", format="%.1f"),
            "lost_h": st.column_config.NumberColumn("Bus-hours lost", format="%.1f"),
            "term_otp": st.column_config.ProgressColumn("% on time at final stop", min_value=0, max_value=100, format="%.0f%%"),
            "gained": st.column_config.NumberColumn("Typical delay gained per trip (s)", format="%+.0f"),
        }
        st.dataframe(rt, hide_index=True, width="stretch", height=360, column_config=cfg)

        top = rt.head(30).route.tolist()
        if USE_DH:
            rh = q(f"""SELECT ANY_VALUE(route_short_name) route, local_hour, SUM(n_valid) n,
                              {MEAN_DELAY} / 60 mean_delay, {OTP_PCT} otp
                       FROM gold_delay_hour WHERE {W} GROUP BY line_ref, local_hour""", P)
        else:
            rh = q(f"""SELECT ANY_VALUE(route_short_name) route, local_hour, COUNT(*) n, AVG(delay_s) / 60 mean_delay,
                              AVG({ON_TIME}::INT) * 100 otp
                       FROM fact_stop_visits WHERE valid AND {W} GROUP BY line_ref, local_hour""", P)
        rh = rh[rh.route.isin(top) & (rh.n >= min_n)]
        what = st.radio("Route × hour view", ["mean delay (min)", "% on time"], horizontal=True)
        z, scale, zr = ("mean_delay", DELAY_SCALE, (0, 10)) if what.startswith("mean") else ("otp", OTP_SCALE, (40, 100))
        st.plotly_chart(
            heatmap(rh, "local_hour", "route", z, f"{what.capitalize()} by route and hour (30 busiest routes)", what, scale, *zr, y_order=top),
            width="stretch",
        )

        if HAS["fact_stop_visits"]:
            st.subheader("Where delays build up along one route")
            c1, c2 = st.columns(2)
            pick = c1.selectbox("Pick a route", rt.line_ref.tolist(), format_func=dict(zip(rt.line_ref, rt.route)).get)
            rdirs = (q("SELECT DISTINCT direction_ref FROM gold_delay_hour WHERE line_ref = ? ORDER BY 1", [pick]).direction_ref.tolist()
                     if USE_DH else q("SELECT DISTINCT direction_ref FROM fact_stop_visits WHERE line_ref = ? ORDER BY 1", [pick]).direction_ref.tolist())
            pdir = c2.selectbox("Direction ", rdirs)
            Wr, Pr = where()
            prof = q(f"""SELECT stop_order, MODE(stop_name) stop, COUNT(*) n, AVG(delay_s) mean_delay,
                                QUANTILE_CONT(delay_s, 0.25) q25, QUANTILE_CONT(delay_s, 0.75) q75, AVG({ON_TIME}::INT) * 100 otp
                         FROM fact_stop_visits WHERE valid AND {Wr} AND line_ref = ? AND direction_ref = ?
                         GROUP BY 1 HAVING COUNT(*) >= ? ORDER BY 1""", [*Pr, pick, pdir, min(min_n, 3)])
            if prof.empty:
                st.info("Not enough visits for this route and direction.")
            else:
                peak_stop = prof.loc[prof.mean_delay.idxmax()]
                r_name = dict(zip(rt.line_ref, rt.route)).get(pick, pick)
                st.caption(
                    f"**Route {r_name} takeaway:** Delays peak around **{peak_stop.stop}** (stop #{int(peak_stop.stop_order)}, "
                    f"averaging **{peak_stop.mean_delay / 60:.1f} min** late). If you board after that point, expect buses to arrive a few minutes behind schedule."
                )
                fig = go.Figure()
                fig.add_scatter(x=prof.stop_order, y=prof.q75 / 60, line=dict(width=0), showlegend=False, hoverinfo="skip")
                fig.add_scatter(x=prof.stop_order, y=prof.q25 / 60, fill="tonexty", fillcolor="rgba(252,141,89,0.25)",
                                line=dict(width=0), name="Middle 50% of buses")
                fig.add_scatter(
                    x=prof.stop_order, y=prof.mean_delay / 60, name="Average delay", line=dict(color="#d73027", width=3),
                    customdata=np.column_stack([prof.stop, prof.n, prof.otp]),
                    hovertemplate="%{customdata[0]}<br>Stop %{x}: %{y:.1f} min<br>%{customdata[1]} visits · "
                                  "%{customdata[2]:.0f}% on time<extra></extra>",
                )
                fig.update_layout(
                    title="How delay builds up stop by stop", xaxis_title="Stop number along the route",
                    yaxis_title="Minutes late", height=380, legend=dict(orientation="h", y=-0.25), margin=dict(t=50),
                )
                st.plotly_chart(fig, width="stretch")


# ============ 3. Stops
with tabs[3]:
    oc = st.columns([2, 2, 3])
    by_name = oc[0].toggle(
        "Combine platforms of the same stop", value=True,
        help="Large hubs like Stavanger byterminal have several platforms. Keep on to view the hub as a whole, or turn off to inspect each platform.",
    )
    colour_by = oc[1].radio("Colour stops on the map by", ["Mean delay", "% on time"], horizontal=True)
    search = oc[2].text_input("Find a stop", placeholder="Type part of a stop name, e.g. Forus")

    if USE_DH and HAS["dim_stop"]:
        Wh, Ph = where(alias="h")
        key = "COALESCE(d.stop_name, h.stop_ref)" if by_name else "h.stop_ref"
        sd = q(f"""SELECT {key} k, ANY_VALUE(COALESCE(d.stop_name, h.stop_ref)) stop,
                          COUNT(DISTINCT h.stop_ref) platforms, SUM(h.n_valid) visits,
                          COUNT(DISTINCT CASE WHEN h.n_valid > 0 THEN h.route_short_name END) n_routes,
                          STRING_AGG(DISTINCT CASE WHEN h.n_valid > 0 THEN h.route_short_name END, ',') routes,
                          AVG(d.stop_lat) lat, AVG(d.stop_lon) lon,
                          {bucket_sum(_i_early + 1, _i_late + 1, 'h')} * 100.0 / NULLIF(SUM(h.n_valid), 0) otp,
                          {bucket_sum(_i_late + 1, 52, 'h')} * 100.0 / NULLIF(SUM(h.n_valid), 0) late_pct,
                          SUM(h.sum_delay_s) / NULLIF(SUM(h.n_valid), 0) / 60 mean_delay
                   FROM gold_delay_hour h LEFT JOIN dim_stop d USING (stop_ref)
                   WHERE {Wh} GROUP BY 1 HAVING SUM(h.n_valid) >= ?""", [*Ph, max(min_n, 20)])
        if HAS["fact_stop_visits"]:
            f_key = "COALESCE(stop_name, stop_ref)" if by_name else "stop_ref"
            sd_p90 = q(f"""SELECT {f_key} k, QUANTILE_CONT(delay_s, 0.9) / 60 p90
                           FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1""", P)
            sd = sd.merge(sd_p90, on="k", how="left")
        else:
            sd["p90"] = np.nan
    else:
        key = "COALESCE(stop_name, stop_ref)" if by_name else "stop_ref"
        sd = q(f"""SELECT {key} k, ANY_VALUE(COALESCE(stop_name, stop_ref)) stop, COUNT(DISTINCT stop_ref) platforms, COUNT(*) visits,
                          COUNT(DISTINCT route_short_name) n_routes,
                          STRING_AGG(DISTINCT route_short_name, ',') routes,
                          AVG(stop_lat) lat, AVG(stop_lon) lon,
                          AVG({ON_TIME}::INT) * 100 otp, AVG((delay_s >= {int(late_s)})::INT) * 100 late_pct,
                          AVG(delay_s) / 60 mean_delay, QUANTILE_CONT(delay_s, 0.9) / 60 p90
                   FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1 HAVING COUNT(*) >= ?""", [*P, max(min_n, 20)])

    if sd.empty:
        st.info("No stops match these filters.")
    else:
        nat = lambda r: (int(r) if r.isdigit() else 10 ** 6, r)
        sd["routes"] = [", ".join(sorted(str(x).split(","), key=nat)) if x else "" for x in sd.routes]
        if not by_name:
            sd["stop"] = sd.stop + " · platform " + sd.k.str.replace("NSR:Quay:", "", regex=False)
        worst, busiest_one = sd.loc[sd.mean_delay.idxmax()], sd.loc[sd.visits.idxmax()]

        insight(
            f"**What this shows:** Across **{len(sd):,} stops**, the typical stop sees **{sd.otp.median():.0f}%** of buses arrive on time. "
            f"**{busiest_one.stop}** is the busiest hub (**{int(busiest_one.visits):,} visits**), while **{worst.stop}** experiences "
            f"the longest delays (averaging **{worst.mean_delay:.1f} min** late). "
            f"**What it means for riders:** Search your regular boarding stop below to check its hourly reliability before heading out."
        )

        m = st.columns(4)
        m[0].metric("Stops shown", f"{len(sd):,}")
        m[1].metric("Typical stop: % on time", f"{sd.otp.median():.0f}%")
        m[2].metric("Most delayed stop", worst.stop, f"{worst.mean_delay:.1f} min avg. delay", delta_color="inverse")
        m[3].metric("Busiest stop", busiest_one.stop, f"{int(busiest_one.visits):,} visits", delta_color="off")

        map_box = st.container()

        st.subheader("Stop ranking")
        rc = st.columns([3, 1])
        rank_by = rc[0].radio("Rank by", ["Most delayed", "Lowest % on time", "Most often late", "Busiest"], horizontal=True)
        top_n = rc[1].selectbox("Show", [10, 20, 50, 100, len(sd)], index=1,
                                format_func=lambda n: "all" if n == len(sd) else f"top {n}")
        col, asc = {
            "Most delayed": ("mean_delay", False),
            "Lowest % on time": ("otp", True),
            "Most often late": ("late_pct", False),
            "Busiest": ("visits", False),
        }[rank_by]
        ranked = sd.sort_values(col, ascending=asc)
        if search:
            ranked = ranked[ranked.stop.str.contains(search, case=False, regex=False, na=False)]
        ranked = ranked.head(top_n).reset_index(drop=True)
        ranked.insert(0, "rank", ranked.index + 1)
        cols = ["rank", "stop"] + (["platforms"] if by_name else []) + ["visits", "n_routes", "routes", "otp", "late_pct", "mean_delay", "p90"]
        st.dataframe(
            ranked[cols], hide_index=True, width="stretch",
            height=min(36 * (len(ranked) + 1) + 2, 740),
            column_config={
                "rank": st.column_config.NumberColumn("#", width="small"),
                "stop": st.column_config.TextColumn("Stop", width="medium"),
                "platforms": st.column_config.NumberColumn("Platforms", width="small"),
                "visits": st.column_config.NumberColumn("Visits", format="localized"),
                "n_routes": st.column_config.NumberColumn("Routes", width="small"),
                "routes": st.column_config.TextColumn("Routes serving the stop", width="medium"),
                "otp": st.column_config.ProgressColumn("% on time", min_value=0, max_value=100, format="%.0f%%"),
                "late_pct": st.column_config.ProgressColumn("% late", min_value=0, max_value=100, format="%.0f%%"),
                "mean_delay": st.column_config.NumberColumn("Avg. delay (min)", format="%.1f"),
                "p90": st.column_config.NumberColumn("90% within (min)", format="%.1f"),
            },
        )
        if search and ranked.empty:
            st.caption(f"No stop name contains “{search}” in this selection.")

        st.subheader("Inspect a single stop")
        pick_opts = sd.sort_values("visits", ascending=False)
        if search:
            pick_opts = pd.concat([pick_opts[pick_opts.stop.str.contains(search, case=False, regex=False, na=False)], pick_opts])
        pick_opts = pick_opts.drop_duplicates("k")
        pick = st.selectbox("Stop", pick_opts.k.tolist(), format_func=dict(zip(sd.k, sd.stop)).get)
        one = sd[sd.k == pick].iloc[0]

        m = st.columns(5)
        m[0].metric("Visits", f"{int(one.visits):,}")
        m[1].metric("% on time", f"{one.otp:.0f}%")
        m[2].metric("% late", f"{one.late_pct:.0f}%")
        m[3].metric("Avg. delay", f"{one.mean_delay:.1f} min")
        m[4].metric("90% within", f"{one.p90:.1f} min" if pd.notna(one.p90) else "–")

        dc = st.columns([3, 2])
        if USE_DH and HAS["dim_stop"]:
            Wh, Ph = where(alias="h")
            oh = q(f"""SELECT h.local_hour, SUM(h.n_valid) n, SUM(h.sum_delay_s) / NULLIF(SUM(h.n_valid), 0) / 60 mean_delay,
                              {bucket_sum(_i_early + 1, _i_late + 1, 'h')} * 100.0 / NULLIF(SUM(h.n_valid), 0) otp
                       FROM gold_delay_hour h LEFT JOIN dim_stop d USING (stop_ref)
                       WHERE {Wh} AND {key} = ? GROUP BY 1 HAVING SUM(h.n_valid) > 0 ORDER BY 1""", [*Ph, pick])
            orr = q(f"""SELECT ANY_VALUE(h.route_short_name) route, SUM(h.n_valid) visits,
                               {bucket_sum(_i_early + 1, _i_late + 1, 'h')} * 100.0 / NULLIF(SUM(h.n_valid), 0) otp,
                               SUM(h.sum_delay_s) / NULLIF(SUM(h.n_valid), 0) / 60 mean_delay
                        FROM gold_delay_hour h LEFT JOIN dim_stop d USING (stop_ref)
                        WHERE {Wh} AND {key} = ? GROUP BY h.line_ref HAVING SUM(h.n_valid) > 0 ORDER BY visits DESC""", [*Ph, pick])
        else:
            oh = q(f"""SELECT local_hour, COUNT(*) n, AVG(delay_s) / 60 mean_delay, AVG({ON_TIME}::INT) * 100 otp
                       FROM fact_stop_visits WHERE valid AND {W} AND {key} = ? GROUP BY 1 ORDER BY 1""", [*P, pick])
            orr = q(f"""SELECT ANY_VALUE(route_short_name) route, COUNT(*) visits, AVG({ON_TIME}::INT) * 100 otp,
                               AVG(delay_s) / 60 mean_delay
                        FROM fact_stop_visits WHERE valid AND {W} AND {key} = ? GROUP BY line_ref ORDER BY visits DESC""", [*P, pick])

        fig = go.Figure()
        fig.add_bar(x=oh.local_hour, y=oh.n, name="Visits", yaxis="y2", marker_color="rgba(150,150,150,0.35)")
        fig.add_scatter(x=oh.local_hour, y=oh.mean_delay, name="Avg. delay (min)", mode="lines+markers",
                        line=dict(color="#d73027", width=3))
        fig.update_layout(
            title=f"{one.stop}: delay through the day", height=340, margin=dict(t=50, b=10),
            xaxis=dict(title="Hour (local time)", dtick=1), yaxis=dict(title="Minutes late"),
            yaxis2=dict(overlaying="y", side="right", showgrid=False, title="Visits"),
            legend=dict(orientation="h", y=-0.25),
        )
        dc[0].plotly_chart(fig, width="stretch")
        dc[1].markdown("**Routes serving this stop**")
        dc[1].dataframe(
            orr, hide_index=True, width="stretch", height=300,
            column_config={
                "route": "Route",
                "visits": st.column_config.NumberColumn("Visits", format="localized"),
                "otp": st.column_config.ProgressColumn("% on time", min_value=0, max_value=100, format="%.0f%%"),
                "mean_delay": st.column_config.NumberColumn("Avg. delay (min)", format="%.1f"),
            },
        )

        with map_box:
            stop_deck, s_stops, s_unit = build_stop_map_deck(sd, colour_by, pick, bool(search))
            st.pydeck_chart(stop_deck, height=480, key="kolumbus_stop_map")
            lc = st.columns([2, 3])
            with lc[0]:
                legend(colour_by, s_stops, s_unit)
            lc[1].caption("Offline Kolumbus network view: larger circles are busier stops, and the blue ring highlights "
                          "the stop selected under “Inspect a single stop”.")

        with st.expander("Delay by hour at the 25 busiest stops", expanded=False):
            busiest = sd.sort_values("visits", ascending=False).head(25)
            if USE_DH and HAS["dim_stop"]:
                Wh, Ph = where(alias="h")
                sh = q(f"""SELECT {key} k, h.local_hour, SUM(h.n_valid) n,
                                  SUM(h.sum_delay_s) / NULLIF(SUM(h.n_valid), 0) / 60 mean_delay
                           FROM gold_delay_hour h LEFT JOIN dim_stop d USING (stop_ref)
                           WHERE {Wh} GROUP BY 1, 2""", Ph)
            else:
                sh = q(f"""SELECT {key} k, local_hour, COUNT(*) n, AVG(delay_s) / 60 mean_delay
                           FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1, 2""", P)
            sh = sh[sh.k.isin(busiest.k) & (sh.n >= min_n)].merge(busiest[["k", "stop"]], on="k")
            st.plotly_chart(
                heatmap(sh, "local_hour", "stop", "mean_delay", "Average delay (min) at the 25 busiest stops, by hour",
                        "minutes", DELAY_SCALE, 0, 10, ".1f", y_order=busiest.stop.tolist()),
                width="stretch",
            )


# ============ 4. Time patterns
with tabs[4]:
    if USE_DH:
        dh = q(f"""SELECT CAST(service_date AS VARCHAR) d, local_hour, SUM(n_valid) n,
                          {OTP_PCT} otp, {MEAN_DELAY} / 60 mean_delay
                   FROM gold_delay_hour WHERE {W} GROUP BY 1, 2""", P)
    else:
        dh = q(f"""SELECT CAST(service_date AS VARCHAR) d, local_hour, COUNT(*) n, AVG({ON_TIME}::INT) * 100 otp, AVG(delay_s) / 60 mean_delay
                   FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1, 2""", P)
    dh = dh[dh.n >= min_n]

    if not dh.empty:
        by_hr = dh.groupby("local_hour", as_index=False).agg(otp=("otp", "mean"), mean_delay=("mean_delay", "mean"))
        best_t = by_hr.loc[by_hr.otp.idxmax()]
        worst_t = by_hr.loc[by_hr.mean_delay.idxmax()]
        insight(
            f"**What this shows:** Punctuality follows a clear daily rhythm — buses are most reliable around "
            f"**{int(best_t.local_hour):02d}:00** (**{best_t.otp:.0f}%** on time), while peak traffic around "
            f"**{int(worst_t.local_hour):02d}:00** pushes average delays up to **{worst_t.mean_delay:.1f} min**. "
            f"**What it means for riders:** If your schedule is flexible, travelling just 30–60 minutes outside the rush-hour "
            f"band dramatically improves your odds of a smooth, on-time connection."
        )

    hh = 230 + 40 * max(dh.d.nunique(), 1)
    st.plotly_chart(heatmap(dh, "local_hour", "d", "otp", "% on time: date × hour", "% on time", OTP_SCALE, 40, 100, height=hh),
                    width="stretch")
    st.plotly_chart(heatmap(dh, "local_hour", "d", "mean_delay", "Average delay (min): date × hour", "minutes", DELAY_SCALE, 0, 10,
                            ".1f", height=hh), width="stretch")

    if HAS["fact_stop_visits"]:
        qb = q(f"""SELECT local_quarter, ANY_VALUE(local_time_bin) t, COUNT(*) n, AVG(delay_s) / 60 mean_delay,
                          AVG({ON_TIME}::INT) * 100 otp
                   FROM fact_stop_visits WHERE valid AND {W} AND local_quarter >= 0 GROUP BY 1 ORDER BY 1""", P)
        qb = qb[qb.n >= min_n]
        fig = px.bar(
            qb, x="t", y="mean_delay", color="otp", color_continuous_scale=OTP_SCALE, range_color=(40, 100),
            labels=dict(t="Time of day (15-minute steps)", mean_delay="Average delay (min)", otp="% on time"),
            title="Exact 15-minute rush-hour peaks (colour = % on time)",
        )
        fig.update_layout(height=360, margin=dict(t=50), xaxis=dict(tickangle=-90, nticks=24))
        st.plotly_chart(fig, width="stretch")

    if USE_DH:
        wk = q(f"""SELECT weekday, local_hour, SUM(n_valid) n,
                          COUNT(DISTINCT CASE WHEN n_valid > 0 THEN service_date END) n_days,
                          {OTP_PCT} otp
                   FROM gold_delay_hour WHERE {W} GROUP BY 1, 2""", P)
    else:
        wk = q(f"""SELECT ISODOW(service_date) - 1 AS weekday, local_hour, COUNT(*) n, COUNT(DISTINCT service_date) n_days,
                          AVG({ON_TIME}::INT) * 100 otp
                   FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1, 2""", P)

    if wk.weekday.nunique() > 1:
        wk = wk[wk.n >= min_n]
        wk["day"] = wk.weekday.map(lambda w: WEEKDAYS[int(w)])
        st.plotly_chart(
            heatmap(wk, "local_hour", "day", "otp", "% on time: weekday × hour", "% on time", OTP_SCALE, 40, 100,
                    height=300, y_order=WEEKDAYS),
            width="stretch",
        )
        st.caption("Weekends typically stay green all day with minimal congestion, whereas weekday mornings and afternoons show clear commuter peaks.")
    else:
        st.caption("Select dates across multiple days of the week in the sidebar to compare weekday vs. weekend patterns.")


# ============ 5. Reliability
with tabs[5]:
    if not HAS["gold_headway"]:
        st.info("Headway regularity data (`gold_headway`) is not available in this gold folder.")
    else:
        cols = q("SELECT * FROM gold_headway LIMIT 0").columns
        has_sq = "sum_actual_headway_sq" in cols
        ewt = ("SUM(sum_actual_headway_sq) / (2 * SUM(sum_actual_headway_s)) - SUM(sum_aimed_headway_sq) / (2 * SUM(sum_aimed_headway_s))"
               if has_sq else "NULL")
        hw = q(f"""SELECT local_hour, SUM(n_pairs) pairs, SUM(n_bunched) * 100.0 / SUM(n_pairs) bunched,
                          SUM(n_large_gap) * 100.0 / SUM(n_pairs) gap, {ewt} AS ewt
                   FROM gold_headway WHERE {W} GROUP BY 1 ORDER BY 1""", P)
        if hw.empty:
            st.info("No bus headway pairs match these filters.")
        else:
            tot = q(f"""SELECT SUM(n_pairs) pairs, SUM(n_bunched) * 100.0 / SUM(n_pairs) bunched,
                               SUM(n_large_gap) * 100.0 / SUM(n_pairs) gap, {ewt} AS ewt FROM gold_headway WHERE {W}""", P).iloc[0]
            hr_ = q(f"""SELECT line_ref, SUM(n_pairs) pairs, SUM(n_bunched) * 100.0 / SUM(n_pairs) bunched,
                               SUM(n_large_gap) * 100.0 / SUM(n_pairs) gap, {ewt} AS ewt
                        FROM gold_headway WHERE {W} GROUP BY 1 HAVING SUM(n_pairs) >= ? ORDER BY bunched DESC""", [*P, max(min_n, 50)])
            hr_.insert(0, "route", hr_.line_ref.map(route_label))

            ewt_note = f"adding an average of **{fmt_s(tot.ewt)}** in extra waiting time at the stop" if has_sq and pd.notna(tot.ewt) else "causing uneven waits at the stop"
            worst_b_note = (f" **Route {hr_.iloc[0].route}** bunches most often (**{hr_.iloc[0].bunched:.1f}%**)."
                            if not hr_.empty else "")
            insight(
                f"**What this shows:** Even when average delays look modest, uneven spacing causes 'bus bunching' — two buses of the "
                f"same line arriving back-to-back after a long empty wait. Here, **{tot.bunched:.1f}%** of buses arrived bunched and "
                f"**{tot.gap:.1f}%** left an unusually long gap, {ewt_note}.{worst_b_note} "
                f"**What it means for riders:** On high-frequency lines, if you just missed a bus after a long wait, another one is often right behind it."
            )

            c = st.columns(4)
            c[0].metric("Consecutive bus pairs", f"{int(tot.pairs):,}")
            c[1].metric("Bunched arrivals", f"{tot.bunched:.1f}%", help="Arrived much closer together than scheduled")
            c[2].metric("Long gaps", f"{tot.gap:.1f}%", help="Arrived much further apart than scheduled")
            if has_sq:
                c[3].metric("Extra wait time", fmt_s(tot.ewt), help="Extra time a random rider waits due to uneven spacing")

            fig = go.Figure()
            fig.add_scatter(x=hw.local_hour, y=hw.bunched, name="% bunched", line=dict(color="#d73027", width=3))
            fig.add_scatter(x=hw.local_hour, y=hw.gap, name="% long gap", line=dict(color="#4575b4", width=3))
            if has_sq:
                fig.add_bar(x=hw.local_hour, y=hw.ewt, name="Extra wait time (s)", marker_color="#e0e0e0", yaxis="y2")
            fig.update_layout(
                title="Bus spacing regularity through the day",
                xaxis=dict(title="Hour (local time)", dtick=1),
                yaxis_title="% of bus pairs",
                yaxis2=dict(overlaying="y", side="right", showgrid=False, title="Extra wait (s)"),
                height=380, legend=dict(orientation="h", y=-0.2), margin=dict(t=50),
            )
            st.plotly_chart(fig, width="stretch")

            st.subheader("Routes most prone to bunching")
            st.dataframe(
                hr_.drop(columns="line_ref"), hide_index=True, width="stretch",
                column_config={
                    "route": "Route",
                    "pairs": st.column_config.NumberColumn("Bus pairs", format="localized"),
                    "bunched": st.column_config.NumberColumn("% bunched", format="%.1f"),
                    "gap": st.column_config.NumberColumn("% long gaps", format="%.1f"),
                    "ewt": st.column_config.NumberColumn("Extra wait time (s)", format="%.0f"),
                },
            )


# ============ 6. Data
with tabs[6]:
    full_count = len(full)
    total_visits = int(dates.n.sum())
    dq = q("SELECT * FROM gold_data_quality") if HAS["gold_data_quality"] else pd.DataFrame()
    if not dq.empty and "kind" in dq and "ok" in dq:
        checks = dq[dq.kind == "check"]
        check_summary = f" Automated data integrity checks: **{int(checks.ok.sum())}/{len(checks)} passed**."
    else:
        check_summary = ""

    insight(
        f"**What this shows:** The dashboard covers **{full_count} full days** of Kolumbus operations across the Stavanger region "
        f"(**{total_visits:,}** validated stop visits in total).{check_summary} "
        f"**What it means for you:** All charts and maps are powered by real GPS and timetable records, cached so every visitor gets instant answers."
    )

    st.subheader("Service coverage per day")
    cov = dates[["d", "n"]].assign(d=pd.to_datetime(dates.d).dt.date)
    st.dataframe(
        cov, hide_index=True,
        column_config={
            "d": st.column_config.DateColumn("Service date", format="ddd D MMM YYYY"),
            "n": st.column_config.NumberColumn("Recorded stop visits", format="localized"),
        },
    )
    st.caption("Dates with only a few hundred visits represent overnight trips running past midnight from the previous day.")

    if not dq.empty:
        with st.expander("Automated data quality checks"):
            st.dataframe(
                dq, hide_index=True, width="stretch",
                column_config={"kind": "Type", "label": "Check", "ok": "Passed", "detail": "Detail"},
            )
    if HAS["gold_meta"]:
        with st.expander("Build metadata"):
            st.dataframe(
                q("SELECT * FROM gold_meta"), hide_index=True, width="stretch",
                column_config={"key": "Setting", "value": "Value"},
            )
    with st.expander("Technical details"):
        st.markdown(f"**Gold data folder:** `{GOLD_DIR}`  \n**Road map folder:** `{MAP_DIR}`  \n"
                    f"**Tables loaded ({len(TABLES)}):** " + ", ".join(f"`{t}`" for t in sorted(TABLES)))


# ============ 7. Busy roads (100% offline Kolumbus network, globally cached)
with tabs[7]:
    if not (HAS["gold_speed_hour"] and HAS["gold_busy_roads"]):
        st.info("Busy-road tables (`gold_speed_hour` / `gold_busy_roads`) are missing: run `analysis/gold/build_gold.py`.")
    else:
        c1, c2 = st.columns([2, 3])
        colour = c1.selectbox(
            "Colour road stretches by",
            ["Slowdown (travel time index)", "Speed (km/h)", "Bus-hours lost", "Heatmap: bus-hours lost"],
            help="Compares actual travel speed against smooth free-flow traffic on the same stretch.",
        )
        stop_list = q("SELECT stop_ref, stop_name FROM dim_stop ORDER BY stop_name") if HAS["dim_stop"] else pd.DataFrame()
        stop_pick = c2.selectbox(
            "Filter to stretches starting or ending at stop",
            ["(all stops)"] + stop_list.stop_ref.tolist(),
            format_func=lambda x: x if x.startswith("(") else
            f"{stop_list.set_index('stop_ref').stop_name.get(x, x)} ({x.split(':')[-1]})",
        )

        deck_br, seg, stops_, unit = build_busy_roads_deck(
            tuple(sel_dates), tuple(hours), tuple(line_filter), sel_dir, int(min_n), colour, stop_pick
        )

        if deck_br is None or seg.empty:
            st.info("No road stretches match these filters.")
        else:
            top_seg = seg.sort_values("lost_h", ascending=False).iloc[0]
            insight(
                f"**What this shows:** Across **{len(seg):,} road stretches**, traffic and busy stops cost Kolumbus buses "
                f"**{seg.lost_h.sum():,.0f} extra hours** compared to free-flow driving. The biggest bottleneck is "
                f"**{top_seg.from_stop_name} → {top_seg.to_stop_name}** (routes {top_seg.routes}), where speeds drop from "
                f"**{top_seg.free_flow_kmh:.0f} km/h** to **{top_seg.kmh:.0f} km/h** (**{top_seg.tti:.1f}×** slower). "
                f"**What it means for riders:** Red corridors on the map are where buses get stuck in traffic or spend longer boarding passengers."
            )

            c = st.columns(4)
            c[0].metric("Road stretches", f"{len(seg):,}")
            c[1].metric("Bus runs", f"{int(seg.n.sum()):,}")
            c[2].metric("Average speed", f"{seg.dist.sum() / seg.run_s.sum() * 3.6:.1f} km/h")
            c[3].metric("Bus-hours lost to congestion", f"{seg.lost_h.sum():,.0f} h")

            st.pydeck_chart(deck_br, height=620, key="kolumbus_busy_roads_map")
            if colour.startswith("Heatmap"):
                st.caption("Brighter glow = more bus-hours lost compared to free-flow speeds. Zoom in for street-level bottlenecks.")
            elif stops_:
                legend(colour, stops_, unit)
            st.caption("Offline Kolumbus road network view. Stop-to-stop times include passenger boarding at the next stop, "
                       "so major hubs naturally show slower average speeds.")

            st.subheader("Road stretches losing the most time (your selection)")
            show = seg.sort_values("lost_h", ascending=False).head(25)
            st.dataframe(
                show[["from_stop_name", "to_stop_name", "routes", "n", "kmh", "free_flow_kmh", "tti", "vs_tt", "lost_h"]],
                hide_index=True, width="stretch", column_config=COLUMNS,
            )
            with st.expander("All-time bottleneck ranking (across all dates and hours)"):
                st.dataframe(
                    q("""SELECT rank_hours_lost, from_stop_name, to_stop_name, lines, n_runs, free_flow_kmh, peak_kmh,
                                midday_kmh, night_kmh, travel_time_index, vehicle_hours_lost
                         FROM gold_busy_roads WHERE ranked ORDER BY rank_hours_lost LIMIT 50"""),
                    hide_index=True, width="stretch", column_config=COLUMNS,
                )


# ============ 8. Bunching & speed heatmaps
with tabs[8]:
    bw = br_ = bs = sw = sr = sd_sp = pd.DataFrame()
    if HAS["gold_headway_hour"]:
        bw = q(f"""SELECT weekday, local_hour, SUM(n_headways) n, SUM(n_bunched) * 100.0 / SUM(n_headways) bunched,
                          SUM(n_large_gap) * 100.0 / SUM(n_headways) gap
                   FROM gold_headway_hour WHERE {W} GROUP BY 1, 2 HAVING SUM(n_headways) >= ?""", [*P, min_n])
        br_ = q(f"""SELECT route_short_name AS route, local_hour, SUM(n_headways) n,
                           SUM(n_bunched) * 100.0 / SUM(n_headways) bunched
                    FROM gold_headway_hour WHERE {W} GROUP BY 1, 2 HAVING SUM(n_headways) >= ?""", [*P, min_n])
        bs = q(f"""SELECT h.stop_ref, ANY_VALUE(d.stop_name) stop_name, local_hour, SUM(n_headways) n,
                          SUM(n_bunched) * 100.0 / SUM(n_headways) bunched
                   FROM gold_headway_hour h LEFT JOIN dim_stop d USING (stop_ref)
                   WHERE {where(alias='h')[0]} GROUP BY 1, 3 HAVING SUM(n_headways) >= ?""", [*where(alias="h")[1], min_n])

    if HAS["gold_speed_hour"]:
        sw = q(f"""SELECT weekday, local_hour, SUM(n_runs) n, SUM(sum_distance_m) / SUM(sum_run_s) * 3.6 kmh
                   FROM gold_speed_hour WHERE {W} GROUP BY 1, 2 HAVING SUM(n_runs) >= ?""", [*P, min_n])
        sr = q(f"""SELECT route_short_name AS route, local_hour, SUM(n_runs) n, SUM(sum_distance_m) / SUM(sum_run_s) * 3.6 kmh
                   FROM gold_speed_hour WHERE {W} GROUP BY 1, 2 HAVING SUM(n_runs) >= ?""", [*P, min_n])
        sd_sp = q(f"""SELECT CAST(service_date AS VARCHAR) AS date, local_hour, SUM(n_runs) n,
                             SUM(sum_distance_m) / SUM(sum_run_s) * 3.6 kmh
                      FROM gold_speed_hour WHERE {W} GROUP BY 1, 2 HAVING SUM(n_runs) >= ?""", [*P, min_n])

    if not sw.empty:
        hr_spd = sw.groupby("local_hour", as_index=False)["kmh"].mean()
        slow_h = hr_spd.loc[hr_spd.kmh.idxmin()]
        fast_h = hr_spd.loc[hr_spd.kmh.idxmax()]
        insight(
            f"**What this shows:** These grids reveal when buses move briskly and stay evenly spaced (green) versus when "
            f"traffic slows them down and causes bunching (red). Across your selection, average bus speed ranges from "
            f"**{slow_h.kmh:.0f} km/h** at **{int(slow_h.local_hour):02d}:00** up to **{fast_h.kmh:.0f} km/h** at **{int(fast_h.local_hour):02d}:00**. "
            f"**What it means for riders:** Red cells mark the exact hours and lines where journey times stretch out and spacing becomes unpredictable."
        )

    if HAS["gold_headway_hour"]:
        st.subheader("Bus bunching (when buses arrive back-to-back)")
        if not bw.empty:
            bw["weekday"] = bw.weekday.map(lambda w: WEEKDAYS[int(w)])
            st.plotly_chart(heatmap(bw, "local_hour", "weekday", "bunched", "% bunched: weekday × hour", "%",
                                    DELAY_SCALE, zmin=0, fmt=".1f", y_order=WEEKDAYS), width="stretch")
        if not br_.empty:
            top_r = br_.groupby("route").n.sum().nlargest(25).index.tolist()
            st.plotly_chart(heatmap(br_[br_.route.isin(top_r)], "local_hour", "route", "bunched",
                                    "% bunched: route × hour (25 busiest routes)", "%", DELAY_SCALE, zmin=0, fmt=".0f",
                                    y_order=top_r), width="stretch")
        if not bs.empty:
            top_s = bs.groupby("stop_ref").n.sum().nlargest(25).index.tolist()
            bs["stop"] = bs.stop_name.fillna(bs.stop_ref) + " (" + bs.stop_ref.str.split(":").str[-1] + ")"
            order_s = bs.drop_duplicates("stop_ref").set_index("stop_ref").loc[top_s, "stop"].tolist()
            st.plotly_chart(heatmap(bs[bs.stop_ref.isin(top_s)], "local_hour", "stop", "bunched",
                                    "% bunched: stop × hour (25 busiest stops)", "%", DELAY_SCALE, zmin=0, fmt=".0f",
                                    y_order=order_s), width="stretch")

    if HAS["gold_speed_hour"]:
        st.subheader("Bus speed (km/h including stop time)")
        if not sw.empty:
            sw["weekday"] = sw.weekday.map(lambda w: WEEKDAYS[int(w)])
            st.plotly_chart(heatmap(sw, "local_hour", "weekday", "kmh", "Average speed (km/h): weekday × hour", "km/h",
                                    OTP_SCALE, fmt=".0f", y_order=WEEKDAYS), width="stretch")
        if not sr.empty:
            top_r = sr.groupby("route").n.sum().nlargest(25).index.tolist()
            st.plotly_chart(heatmap(sr[sr.route.isin(top_r)], "local_hour", "route", "kmh",
                                    "Average speed (km/h): route × hour (25 busiest routes)", "km/h", OTP_SCALE, fmt=".0f",
                                    y_order=top_r), width="stretch")
        if not sd_sp.empty:
            st.plotly_chart(heatmap(sd_sp, "local_hour", "date", "kmh", "Average speed (km/h): date × hour", "km/h",
                                    OTP_SCALE, fmt=".0f"), width="stretch")


# ============ 9. Trips
with tabs[9]:
    if not HAS["gold_trip_metrics"]:
        st.info("Trip-level metrics (`gold_trip_metrics`) are missing: run `analysis/gold/build_gold.py`.")
    else:
        Wt, Pt = where("start_hour")
        tm = q(f"""SELECT * FROM gold_trip_metrics WHERE n_stops >= 2 AND {Wt}""", Pt)
        if tm.empty:
            st.info("No trips match these filters.")
        else:
            med_spd = f"{tm.avg_kmh.median():.1f} km/h" if "avg_kmh" in tm and tm.avg_kmh.notna().any() else "–"
            bunch_share = f"{(tm.n_bunched > 0).mean():.1%}" if "n_bunched" in tm else "–"
            insight(
                f"**What this shows:** Looking at **{len(tm):,} complete journeys** from start to finish, the typical trip "
                f"changes its delay by **{fmt_s(tm.delay_gained_s.median())}** along the way (median speed **{med_spd}**), "
                f"and **{bunch_share}** of trips ended up bunching with another bus. "
                f"**What it means for riders:** Slower journeys lose time steadily across stops — the table below highlights the "
                f"individual runs that fell furthest behind schedule."
            )

            c = st.columns(4)
            c[0].metric("Complete trips", f"{len(tm):,}")
            if "avg_kmh" in tm:
                c[1].metric("Typical trip speed", med_spd)
            c[2].metric("Typical delay gained per trip", fmt_s(tm.delay_gained_s.median()))
            if "n_bunched" in tm:
                c[3].metric("Trips bunched at least once", bunch_share)

            if "avg_kmh" in tm:
                sample = tm.dropna(subset=["avg_kmh"]).sample(min(len(tm), 5000), random_state=0)
                fig = px.scatter(
                    sample, x="avg_kmh", y="delay_gained_s", color="route_short_name", opacity=0.5,
                    labels=dict(avg_kmh="Trip speed (km/h)", delay_gained_s="Delay gained along the trip (s)",
                                route_short_name="Route"),
                    title="Slower trips lose more time along the route (up to 5,000 trips shown)",
                )
                fig.update_layout(height=420, showlegend=False)
                st.plotly_chart(fig, width="stretch")

            st.subheader("Individual trips that lost the most time")
            cols = [c_ for c_ in ["service_date", "route_short_name", "direction_ref", "start_hour", "n_stops", "start_delay_s",
                                  "end_delay_s", "delay_gained_s", "avg_kmh", "n_bunched", "n_headways"] if c_ in tm]
            top_trips = tm.sort_values("delay_gained_s", ascending=False).head(50)[cols].copy()
            top_trips["service_date"] = pd.to_datetime(top_trips["service_date"]).dt.date
            st.dataframe(top_trips, hide_index=True, width="stretch", column_config=COLUMNS)
