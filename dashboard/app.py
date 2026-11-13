"""
BusPulse dashboard: punctuality, delays and speeds of Kolumbus buses, on the Stavanger road network.

    streamlit run dashboard/app.py -- --gold <gold folder> --map dashboard/map

Reads the gold tables (parquet, from analysis/gold/build_gold.ipynb) and the static road map
(from dashboard/build_map.py). Everything is queried with DuckDB straight from the parquet files,
so filters run in milliseconds and nothing is loaded that a page does not show.
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

st.set_page_config(page_title="BusPulse", page_icon="🚌", layout="wide")
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
STAVANGER = dict(latitude=58.955, longitude=5.725, zoom=11.2)


# ------------------------------------------------------------------ data access
@st.cache_resource
def connect():
    """In-memory DuckDB with one view per gold parquet table and the road map."""
    con = duckdb.connect()
    found = []
    for p in sorted(GOLD_DIR.glob("*.parquet")):
        src = f"{p.as_posix()}/*.parquet" if p.is_dir() else p.as_posix()     # DuckDB file or Spark folder
        con.execute(f"CREATE VIEW {p.stem} AS SELECT * FROM read_parquet('{src}')")
        found.append(p.stem)
    seg = MAP_DIR / "road_segments.parquet"
    if seg.exists():
        con.execute(f"CREATE VIEW map_seg AS SELECT from_stop_ref, to_stop_ref, length_m, n_trips FROM read_parquet('{seg.as_posix()}')")
        found.append("map_seg")
    return con, set(found)


def q(sql, params=()):
    return _q(sql, tuple(params))


@st.cache_data(show_spinner=False, max_entries=512)
def _q(sql, params):
    con, _ = connect()
    return con.cursor().execute(sql, list(params)).df()


@st.cache_data(show_spinner=False)
def road_geometry():
    """All road segments with geometry (static, loaded once)."""
    p = MAP_DIR / "road_segments.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    df["path"] = df["path"].map(lambda a: [list(x) for x in a])
    return df


@st.cache_data(show_spinner=False)
def stop_names():
    p = MAP_DIR / "stops.parquet"
    return pd.read_parquet(p).set_index("stop_ref")["stop_name"] if p.exists() else pd.Series(dtype=str)


con, TABLES = connect()
if "fact_stop_visits" not in TABLES:
    st.error(f"No gold tables found in `{GOLD_DIR}`. Run analysis/gold/build_gold.ipynb first, or start with "
             "`streamlit run dashboard/app.py -- --gold <folder with the gold parquet files>`.")
    st.stop()
HAS = {t: t in TABLES for t in ["gold_segment_hour", "gold_trip_summary", "gold_headway", "map_seg", "dim_line",
                                "gold_data_quality", "gold_meta", "gold_speed_hour", "gold_busy_roads",
                                "gold_headway_hour", "gold_trip_metrics", "gold_thresholds", "dim_stop"]}
RULES = (q("SELECT rule, value FROM gold_thresholds").set_index("rule")["value"].to_dict()
         if HAS["gold_thresholds"] else {})
BUNCH, GAP = RULES.get("bunch_ratio", 0.5), RULES.get("gap_ratio", 1.5)


# ------------------------------------------------------------------ sidebar filters
dates = q("""SELECT CAST(service_date AS VARCHAR) AS d, COUNT(*) AS n, ISODOW(ANY_VALUE(service_date)) - 1 AS wd
             FROM fact_stop_visits WHERE valid GROUP BY 1 ORDER BY 1""")
if dates.empty:
    st.error("The gold fact table has no valid stop visits.")
    st.stop()
dates["label"] = [f"{d} ({WEEKDAYS[int(w)][:3] if 0 <= w < 7 else '?'}): {n:,} visits" for d, n, w in zip(dates.d, dates.n, dates.wd)]
full = dates[dates.n >= 0.2 * dates.n.max()]      # leftovers of neighbouring days have only a few hundred visits
dates["week"] = pd.to_datetime(dates.d).dt.isocalendar().week.astype(int)

st.sidebar.title("🚌 BusPulse")
st.sidebar.caption("Kolumbus buses · Stavanger region")
weeks_all = sorted(dates.week.unique())
sel_weeks = st.sidebar.multiselect("Weeks (ISO)", weeks_all, default=weeks_all,
                                   help="Narrows the list of service dates below to these calendar weeks")
date_opts = dates[dates.week.isin(sel_weeks)]
sel_dates = st.sidebar.multiselect("Service dates", date_opts.d.tolist(),
                                   default=[d for d in full.d if d in set(date_opts.d)],
                                   format_func=dict(zip(dates.d, dates.label)).get,
                                   help="Dates with very few visits are fragments of the neighbouring day "
                                        "(trips after midnight) and are not selected by default.")
hours = st.sidebar.slider("Hours (local time)", 0, 23, (0, 23))

routes_all = q("""SELECT f.line_ref, ANY_VALUE(f.route_short_name) AS route, COUNT(*) AS n
                  FROM fact_stop_visits f WHERE valid GROUP BY 1 ORDER BY n DESC""")
if HAS["dim_line"]:
    routes_all = routes_all.merge(q("SELECT line_ref, route_type_name FROM dim_line"), on="line_ref", how="left")
    types = sorted(routes_all["route_type_name"].dropna().unique())
    sel_types = st.sidebar.multiselect("Route types", types, default=types)
    routes_all = routes_all[routes_all["route_type_name"].isin(sel_types)]
route_label = {r.line_ref: f"{r.route}" for r in routes_all.itertuples()}
sel_routes = st.sidebar.multiselect("Routes (empty = all)", routes_all.line_ref.tolist(), format_func=route_label.get)
dirs = q("SELECT DISTINCT direction_ref FROM fact_stop_visits ORDER BY 1").direction_ref.tolist()
sel_dir = st.sidebar.selectbox("Direction", ["all"] + dirs)

with st.sidebar.expander("Definitions", expanded=False):
    early_s, late_s = st.slider("On time = between (seconds)", -300, 900, (-60, 300), step=30,
                                help="Project rule (gold and silver): 60 s early to under 5 min late.")
    min_n = st.slider("Hide cells / segments with fewer observations than", 1, 50, 10)
st.sidebar.caption(f"On time: {early_s} s to {late_s} s · min. {min_n} observations")

if not sel_dates:
    st.warning("Select at least one service date.")
    st.stop()
line_filter = sel_routes or routes_all.line_ref.tolist()
if not line_filter:
    st.warning("No routes match the selected route types.")
    st.stop()


def where(hour_col="local_hour", alias=""):
    """WHERE clause + parameters for the sidebar filters, for any gold table with these columns."""
    a = f"{alias}." if alias else ""
    sql = [f"CAST({a}service_date AS VARCHAR) IN ({','.join('?' * len(sel_dates))})", f"{a}{hour_col} BETWEEN ? AND ?",
           f"{a}line_ref IN ({','.join('?' * len(line_filter))})"]
    params = [*sel_dates, hours[0], hours[1], *line_filter]
    if sel_dir != "all":
        sql.append(f"{a}direction_ref = ?")
        params.append(sel_dir)
    return " AND ".join(sql), params


W, P = where()
ON_TIME = f"(delay_s >= {int(early_s)} AND delay_s < {int(late_s)})"


# ------------------------------------------------------------------ helpers for charts
def heatmap(df, x, y, z, title, zlabel, scale, zmin=None, zmax=None, fmt=".0f", height=None, y_order=None):
    pv = df.pivot_table(index=y, columns=x, values=z, aggfunc="first")
    if y_order is not None:
        pv = pv.reindex([r for r in y_order if r in pv.index])
    fig = px.imshow(pv, aspect="auto", color_continuous_scale=scale, zmin=zmin, zmax=zmax,
                    labels=dict(color=zlabel, x={"local_hour": "Hour (local time)", "local_time_bin": "Time of day"}.get(x, x.replace("_", " ").capitalize()), y=""), text_auto=fmt if pv.size <= 700 else False)
    fig.update_layout(title=title, height=height or max(260, 22 * len(pv) + 120), margin=dict(l=10, r=10, t=50, b=10),
                      coloraxis_colorbar=dict(title=zlabel))
    fig.update_xaxes(type="category")
    fig.update_yaxes(type="category")      # route numbers like "20" are names, not values
    return fig


DELAY_SCALE = [(0, "#1a9850"), (0.2, "#91cf60"), (0.4, "#fee08b"), (0.7, "#fc8d59"), (1, "#d73027")]
OTP_SCALE = [(0, "#d73027"), (0.3, "#fc8d59"), (0.6, "#fee08b"), (0.8, "#91cf60"), (1, "#1a9850")]


def ramp(values, stops):
    """Colour each value by linear interpolation between (value, (r, g, b)) stops."""
    xs = np.array([s[0] for s in stops], float)
    cols = np.array([s[1] for s in stops], float)
    v = np.clip(np.asarray(values, float), xs[0], xs[-1])
    return np.column_stack([np.interp(v, xs, cols[:, i]) for i in range(3)]).astype(int).tolist()


def road_heat_layer(seg, weight_col, radius=35):
    """Heatmap along the roads: each segment's weight is spread evenly over the points of its path,
    so long segments do not count more than short ones and the heat follows the streets."""
    pts = seg[["path", weight_col]].dropna()
    pts = pts[pts[weight_col] > 0]
    if pts.empty:
        return None
    rows = [(x, y, w / len(path)) for path, w in zip(pts["path"], pts[weight_col]) for x, y in path]
    heat = pd.DataFrame(rows, columns=["lon", "lat", "w"])
    return pdk.Layer("HeatmapLayer", data=heat, get_position=["lon", "lat"], get_weight="w",
                     radius_pixels=radius, intensity=1.2, threshold=0.03, opacity=0.8, aggregation="SUM")


def legend(title, stops, unit):
    grad = ", ".join(f"rgb{tuple(c)} {100 * i / (len(stops) - 1):.0f}%" for i, (_, c) in enumerate(stops))
    ticks = "".join(f"<span>{v:g}{unit}</span>" for v, _ in stops)
    st.markdown(f"""<div style="font-size:0.8rem;margin:-0.3rem 0 0.6rem 0"><b>{title}</b>
      <div style="height:10px;border-radius:3px;background:linear-gradient(90deg,{grad})"></div>
      <div style="display:flex;justify-content:space-between">{ticks}</div></div>""", unsafe_allow_html=True)


SPEED_STOPS = [(10, (215, 48, 39)), (20, (252, 141, 89)), (30, (254, 224, 139)), (40, (145, 207, 96)), (55, (26, 152, 80))]
DELAY_STOPS = [(0, (26, 152, 80)), (60, (145, 207, 96)), (180, (254, 224, 139)), (300, (252, 141, 89)), (600, (215, 48, 39))]
GAIN_STOPS = [(-30, (69, 117, 180)), (0, (240, 240, 240)), (15, (254, 224, 139)), (30, (252, 141, 89)), (60, (215, 48, 39))]
OTP_STOPS = [(50, (215, 48, 39)), (65, (252, 141, 89)), (75, (254, 224, 139)), (85, (145, 207, 96)), (95, (26, 152, 80))]


NUM = st.column_config.NumberColumn
COLUMNS = {   # readable headers + formats for the tables of the new pages
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
    "vs_tt": NUM("Vs. timetable (s per bus)", format="%+.0f"), "lost_h": NUM("Bus-hours lost", format="%.1f"),
}


def fmt_s(x):
    return "–" if x is None or pd.isna(x) else f"{x / 60:.1f} min" if abs(x) >= 120 else f"{x:.0f} s"


# ------------------------------------------------------------------ pages
st.title("Bus punctuality and speed · Stavanger region")
st.caption(f"{', '.join(sel_dates)} · {hours[0]:02d}:00–{hours[1]:02d}:59 · "
           f"{'all routes' if not sel_routes else str(len(sel_routes)) + ' route(s)'} · direction {sel_dir}")
tabs = st.tabs(["Overview", "Road map", "Routes", "Stops", "Time patterns", "Reliability", "Data",
                "Busy roads", "Bunching & speed heatmaps", "Trips"])

# ============ Overview
with tabs[0]:
    k = q(f"""SELECT COUNT(*) n, AVG({ON_TIME}::INT) otp, AVG((delay_s < {int(early_s)})::INT) early,
                     AVG((delay_s >= {int(late_s)})::INT) late, AVG(delay_s) mean_d, MEDIAN(delay_s) med,
                     QUANTILE_CONT(delay_s, 0.9) p90, COUNT(DISTINCT line_ref) routes, COUNT(DISTINCT stop_ref) stops,
                     COUNT(DISTINCT (service_date, journey_ref)) trips
              FROM fact_stop_visits WHERE valid AND {W}""", P).iloc[0]
    if not k.n:
        st.info("No stop visits match these filters.")
    else:
        c = st.columns(5)
        c[0].metric("Stop visits", f"{int(k.n):,}", help="Observed bus stop visits with an actual time (valid)")
        c[1].metric("On time", f"{k.otp:.1%}", help=f"Share of visits {early_s} s to {late_s} s from the timetable")
        c[2].metric("Late", f"{k.late:.1%}", help=f"{late_s} s or more behind the timetable")
        c[3].metric("Mean delay", fmt_s(k.mean_d))
        c[4].metric("90% of visits within", fmt_s(k.p90), help="90th percentile of delay")
        c = st.columns(5)
        c[0].metric("Trips", f"{int(k.trips):,}")
        c[1].metric("Routes / stops", f"{int(k.routes)} / {int(k.stops):,}")
        c[2].metric("Median delay", fmt_s(k.med))
        if HAS["gold_segment_hour"] and HAS["map_seg"]:
            sp = q(f"""SELECT 3.6 * SUM(m.length_m * s.n) / SUM(s.sum_actual_s) AS act,
                              3.6 * SUM(m.length_m * s.n) / NULLIF(SUM(s.sum_aimed_s), 0) AS sch,
                              SUM(s.sum_actual_s - s.sum_aimed_s) / 3600 AS lost_h
                       FROM gold_segment_hour s JOIN map_seg m USING (from_stop_ref, to_stop_ref)
                       WHERE {where(alias='s')[0]}""", where(alias="s")[1]).iloc[0]
            if pd.notna(sp.act):
                c[3].metric("Average speed", f"{sp.act:.1f} km/h", f"{sp.act - sp.sch:+.1f} vs timetable",
                            help="Stop-to-stop speed incl. time at stops: road length (GTFS) / actual time")
                c[4].metric("Time lost vs timetable", f"{sp.lost_h:,.0f} bus-hours",
                            help="Sum over all driven segments of (actual - scheduled) time")
        if HAS["gold_trip_summary"]:
            Wt, Pt = where("start_hour")
            t = q(f"""SELECT AVG((end_delay_s < {int(late_s)})::INT) term_otp,
                             MEDIAN(delay_gained_s) gained FROM gold_trip_summary WHERE n_stops >= 2 AND {Wt}""", Pt).iloc[0]
            if pd.notna(t.term_otp):
                st.caption(f"Trips not late (< {late_s} s) at their last observed stop: **{t.term_otp:.1%}** · "
                           f"median delay gained along a trip: **{fmt_s(t.gained)}**")

        left, right = st.columns([3, 2])
        hr = q(f"""SELECT local_hour, COUNT(*) n, AVG({ON_TIME}::INT) * 100 otp, AVG(delay_s) mean_delay,
                          QUANTILE_CONT(delay_s, 0.9) p90
                   FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1 ORDER BY 1""", P)
        fig = go.Figure()
        fig.add_bar(x=hr.local_hour, y=hr.n, name="stop visits", marker_color="#d9d9d9", yaxis="y2")
        fig.add_scatter(x=hr.local_hour, y=hr.mean_delay / 60, name="mean delay (min)", line=dict(color="#d73027", width=3))
        fig.add_scatter(x=hr.local_hour, y=hr.p90 / 60, name="90th percentile (min)", line=dict(color="#fc8d59", dash="dot"))
        fig.update_layout(title="Delay through the day", xaxis=dict(title="Hour (local time)", dtick=1),
                          yaxis=dict(title="Minutes late"), yaxis2=dict(overlaying="y", side="right", showgrid=False, title="Stop visits"),
                          legend=dict(orientation="h", y=-0.2), height=380, margin=dict(t=50, b=10))
        left.plotly_chart(fig, width="stretch")
        fig = px.bar(hr, x="local_hour", y="otp", color="otp", color_continuous_scale=OTP_SCALE, range_color=(50, 100),
                     labels=dict(local_hour="Hour (local time)", otp="% on time"), title="Share on time by hour")
        fig.update_layout(height=380, coloraxis_showscale=False, margin=dict(t=50, b=10), xaxis_dtick=2)
        right.plotly_chart(fig, width="stretch")

        dist = q(f"""SELECT LEAST(GREATEST(FLOOR(delay_s / 30) * 30, -300), 1200) AS bin, COUNT(*) n
                     FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1 ORDER BY 1""", P)
        dist["Delay category"] = np.where(dist.bin < early_s, "Early", np.where(dist.bin >= late_s, "Late", "On time"))
        dist["Delay (minutes)"] = dist.bin / 60
        fig = px.bar(dist, x="Delay (minutes)", y="n", color="Delay category",
                     color_discrete_map={"Early": "#4575b4", "On time": "#1a9850", "Late": "#d73027"},
                     category_orders={"Delay category": ["Early", "On time", "Late"]},
                     labels=dict(n="Stop visits"), title="How late are buses?")
        fig.update_layout(height=320, bargap=0.05, margin=dict(t=50, b=10))
        st.plotly_chart(fig, width="stretch")
        st.caption("Bars are 30-second steps. The first and last bar collect everything more than 5 minutes early "
                   "or more than 20 minutes late.")

# ============ Road map
with tabs[1]:
    geo = road_geometry()
    if geo is None:
        st.warning(f"No road map in `{MAP_DIR}`. Build it once with `python dashboard/build_map.py --gtfs <gtfs folder>`.")
    else:
        c1, c2, c3 = st.columns([2, 2, 1.3])
        metric = c1.selectbox("Colour roads by", ["Speed (km/h)", "Delay gained per bus (s)", "Buses (passes)",
                                                  "Heatmap: where buses lose time", "Heatmap: where most buses drive", "Off"],
                              help="Computed per stop-to-stop road segment for the selected filters. The heatmaps show "
                                   "hot spots: total bus-hours lost against the timetable, or the number of buses.")
        points = c2.selectbox("Stops", ["Mean delay at stop", "Share on time at stop", "Lateness hot spots (heatmap)", "Off"])
        base = c3.selectbox("Background", ["Bus roads only (offline)", "Street map (online)", "Dark street map (online)"],
                            help="The street maps are loaded by your browser from CARTO; the bus road network is our own static file")
        layers = [pdk.Layer("PathLayer", data=geo[["path"]], get_path="path", get_color=[150, 150, 150, 110],
                            get_width=8, width_min_pixels=1, width_max_pixels=2, pickable=False)]
        seg_tab = None
        if metric != "Off" and HAS["gold_segment_hour"]:
            Ws, Ps = where(alias="s")
            seg = q(f"""SELECT from_stop_ref, to_stop_ref, SUM(n) n, SUM(sum_actual_s) act, SUM(sum_aimed_s) aim,
                               COUNT(DISTINCT route_short_name) n_routes, STRING_AGG(DISTINCT route_short_name, ', ') routes
                        FROM gold_segment_hour s WHERE {Ws} GROUP BY 1, 2 HAVING SUM(n) >= ?""", [*Ps, min_n])
            seg = seg.merge(geo[["from_stop_ref", "to_stop_ref", "length_m", "path"]], on=["from_stop_ref", "to_stop_ref"])
            if not seg.empty:
                seg["speed"] = 3.6 * seg.length_m * seg.n / seg.act
                seg["gain"] = (seg.act - seg.aim) / seg.n
                names = stop_names()
                seg["name"] = seg.from_stop_ref.map(names).fillna(seg.from_stop_ref) + " → " + seg.to_stop_ref.map(names).fillna(seg.to_stop_ref)
                seg["tip"] = [f"<b>{nm}</b><br/>routes {r}<br/>{int(n):,} buses ·{l:,.0f} m<br/>speed {s:.0f} km/h · "
                              f"{g:+.0f} s vs timetable per bus" for nm, r, n, l, s, g in
                              zip(seg.name, seg.routes, seg.n, seg.length_m, seg.speed, seg.gain)]
                if metric.startswith("Heatmap"):
                    seg["lost_h"] = (seg.act - seg.aim).clip(lower=0) / 3600       # bus-hours behind the timetable
                    hl = road_heat_layer(seg, "lost_h" if "lose" in metric else "n")
                    if hl is not None:
                        layers.append(hl)
                    seg_tab = seg
                elif metric.startswith("Speed"):
                    seg["color"], stops_, unit = ramp(seg.speed, SPEED_STOPS), SPEED_STOPS, " km/h"
                elif metric.startswith("Delay gained"):
                    seg["color"], stops_, unit = ramp(seg.gain, GAIN_STOPS), GAIN_STOPS, " s"
                else:
                    lo, hi = np.log10(min_n), np.log10(max(seg.n.max(), min_n * 10))
                    vstops = [(round(10 ** (lo + i * (hi - lo) / 4)), c) for i, (_, c) in enumerate(SPEED_STOPS[::-1])]
                    seg["color"], stops_, unit = ramp(seg.n, vstops), vstops, ""
                if not metric.startswith("Heatmap"):
                    seg["width"] = 6 + 8 * np.log10(seg.n)          # metres, limited to 1.5..10 px below
                    layers.append(pdk.Layer("PathLayer", data=seg[["path", "color", "width", "tip"]], get_path="path",
                                            get_color="color", get_width="width", width_min_pixels=1.5, width_max_pixels=10,
                                            cap_rounded=True, joint_rounded=True, pickable=True, auto_highlight=True))
                    seg_tab = seg
        stop_df = q(f"""SELECT stop_ref, ANY_VALUE(stop_name) stop_name, AVG(stop_lat) lat, AVG(stop_lon) lon, COUNT(*) n,
                               AVG(delay_s) mean_delay, AVG({ON_TIME}::INT) * 100 otp,
                               SUM((delay_s >= {int(late_s)})::INT) n_late
                        FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1 HAVING COUNT(*) >= ?""", [*P, min_n])
        if points != "Off" and not stop_df.empty:
            stop_df["tip"] = [f"<b>{s}</b><br/>{int(n):,} visits<br/>mean delay {fmt_s(d)}<br/>{o:.0f}% on time"
                              for s, n, d, o in zip(stop_df.stop_name, stop_df.n, stop_df.mean_delay, stop_df.otp)]
            if points.startswith("Lateness"):
                layers.append(pdk.Layer("HeatmapLayer", data=stop_df[["lon", "lat", "n_late"]], get_position=["lon", "lat"],
                                        get_weight="n_late", radius_pixels=40, intensity=1, threshold=0.05, opacity=0.75))
            else:
                if points.startswith("Mean"):
                    stop_df["color"], s_stops, s_unit = ramp(stop_df.mean_delay, DELAY_STOPS), DELAY_STOPS, " s"
                else:
                    stop_df["color"], s_stops, s_unit = ramp(stop_df.otp, OTP_STOPS), OTP_STOPS, "%"
                stop_df["r"] = 15 + 12 * np.log10(stop_df.n)    # metres, limited to 2..12 px below
                layers.append(pdk.Layer("ScatterplotLayer", data=stop_df[["lon", "lat", "color", "r", "tip"]],
                                        get_position=["lon", "lat"], get_fill_color="color", get_radius="r",
                                        radius_min_pixels=2, radius_max_pixels=12, stroked=True, get_line_color=[40, 40, 40], line_width_min_pixels=0.5,
                                        pickable=True))
        view = dict(layers=layers, initial_view_state=pdk.ViewState(**STAVANGER), tooltip={"html": "{tip}", "style": {"fontSize": "12px"}})
        if base.startswith("Bus"):
            deck = pdk.Deck(map_provider=None, **view)   # no basemap at all: nothing is fetched from the internet
        else:
            deck = pdk.Deck(map_provider="carto", map_style="dark" if base.startswith("Dark") else "light", **view)
        st.pydeck_chart(deck, height=640)
        lc = st.columns(2)
        with lc[0]:
            if seg_tab is not None and metric.startswith("Heatmap"):
                st.caption("Heatmap: brighter = more " + ("bus-hours lost against the timetable" if "lose" in metric
                           else "buses") + " on the roads there (summed over the selection). Zoom in for street level.")
            elif seg_tab is not None:
                legend(metric, stops_, unit)
        with lc[1]:
            if points != "Off" and not points.startswith("Lateness") and not stop_df.empty:
                legend(points, s_stops, s_unit)
        st.caption("Roads are the exact paths of the bus trips (GTFS shapes), cut at the stops. Speed = road length / "
                   "actual stop-to-stop time, including time spent at the stop. The timetable has one-minute resolution, "
                   "so 'delay gained' on one short segment can be off by up to a minute; it is reliable summed along a route.")
        if seg_tab is not None:
            st.subheader("Slowest road segments")
            slow = seg_tab.sort_values("speed").head(15)
            st.dataframe(slow[["name", "routes", "n", "length_m", "speed", "gain"]], hide_index=True, width="stretch",
                         column_config={"name": "Road segment", "routes": "Routes",
                                        "n": st.column_config.NumberColumn("Buses", format="localized"),
                                        "length_m": st.column_config.NumberColumn("Length (m)", format="%.0f"),
                                        "speed": st.column_config.NumberColumn("Avg. speed (km/h)", format="%.1f"),
                                        "gain": st.column_config.NumberColumn("Vs. timetable (s per bus)", format="%+.0f")})

# ============ Routes
with tabs[2]:
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
        st.subheader("Routes")
        cfg = {"line_ref": None, "route": "Route",
               "visits": st.column_config.NumberColumn("Stop visits", format="localized"), "trips": st.column_config.NumberColumn("Trips", format="localized"),
               "otp": st.column_config.ProgressColumn("% on time", min_value=0, max_value=100, format="%.0f%%"),
               "mean_delay": st.column_config.NumberColumn("Mean delay (s)", format="%.0f"),
               "p90": st.column_config.NumberColumn("90th percentile delay (s)", format="%.0f"),
               "speed": st.column_config.NumberColumn("Avg. speed (km/h)", format="%.1f"),
               "lost_h": st.column_config.NumberColumn("Bus-hours lost vs. timetable", format="%.1f"),
               "term_otp": st.column_config.ProgressColumn("% not late at last stop", min_value=0, max_value=100, format="%.0f%%"),
               "gained": st.column_config.NumberColumn("Median delay gained per trip (s)", format="%+.0f")}
        st.dataframe(rt, hide_index=True, width="stretch", height=360, column_config=cfg)

        top = rt.head(30).route.tolist()
        rh = q(f"""SELECT ANY_VALUE(route_short_name) route, local_hour, COUNT(*) n, AVG(delay_s) / 60 mean_delay,
                          AVG({ON_TIME}::INT) * 100 otp
                   FROM fact_stop_visits WHERE valid AND {W} GROUP BY line_ref, local_hour""", P)
        rh = rh[rh.route.isin(top) & (rh.n >= min_n)]
        what = st.radio("Route × hour", ["mean delay (min)", "% on time"], horizontal=True)
        z, scale, zr = ("mean_delay", DELAY_SCALE, (0, 10)) if what.startswith("mean") else ("otp", OTP_SCALE, (40, 100))
        st.plotly_chart(heatmap(rh, "local_hour", "route", z, f"{what} by route and hour (30 busiest routes, cells with "
                                f"< {min_n} visits hidden)", what, scale, *zr, y_order=top), width="stretch")

        st.subheader("Along one route")
        c1, c2 = st.columns(2)
        pick = c1.selectbox("Route", rt.line_ref.tolist(), format_func=dict(zip(rt.line_ref, rt.route)).get)
        rdirs = q("SELECT DISTINCT direction_ref FROM fact_stop_visits WHERE line_ref = ? ORDER BY 1", [pick]).direction_ref.tolist()
        pdir = c2.selectbox("Direction ", rdirs)
        Wr, Pr = where()
        prof = q(f"""SELECT stop_order, MODE(stop_name) stop, COUNT(*) n, AVG(delay_s) mean_delay,
                            QUANTILE_CONT(delay_s, 0.25) q25, QUANTILE_CONT(delay_s, 0.75) q75, AVG({ON_TIME}::INT) * 100 otp
                     FROM fact_stop_visits WHERE valid AND {Wr} AND line_ref = ? AND direction_ref = ?
                     GROUP BY 1 HAVING COUNT(*) >= ? ORDER BY 1""", [*Pr, pick, pdir, min(min_n, 3)])
        if prof.empty:
            st.info("Not enough visits for this route and direction.")
        else:
            fig = go.Figure()
            fig.add_scatter(x=prof.stop_order, y=prof.q75 / 60, line=dict(width=0), showlegend=False, hoverinfo="skip")
            fig.add_scatter(x=prof.stop_order, y=prof.q25 / 60, fill="tonexty", fillcolor="rgba(252,141,89,0.25)",
                            line=dict(width=0), name="middle 50% of buses")
            fig.add_scatter(x=prof.stop_order, y=prof.mean_delay / 60, name="mean delay", line=dict(color="#d73027", width=3),
                            customdata=np.column_stack([prof.stop, prof.n, prof.otp]),
                            hovertemplate="%{customdata[0]}<br>stop %{x}: %{y:.1f} min<br>%{customdata[1]} visits · "
                                          "%{customdata[2]:.0f}% on time<extra></extra>")
            fig.update_layout(title="How delay builds up along the route", xaxis_title="Stop number along the route",
                              yaxis_title="Minutes late", height=380, legend=dict(orientation="h", y=-0.25), margin=dict(t=50))
            st.plotly_chart(fig, width="stretch")

# ============ Stops
with tabs[3]:
    oc = st.columns([2, 2, 3])
    by_name = oc[0].toggle("Combine platforms of the same stop", value=True,
                           help="A stop such as Stavanger byterminal has many platforms (quays). On: one row per stop name. "
                                "Off: one row per platform.")
    colour_by = oc[1].radio("Colour stops on the map by", ["Mean delay", "% on time"], horizontal=True)
    search = oc[2].text_input("Find a stop", placeholder="type part of a stop name, e.g. Forus")
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
        nat = lambda r: (int(r) if r.isdigit() else 10 ** 6, r)      # routes in natural order: 2, 3, 10, X60
        sd["routes"] = [", ".join(sorted(str(x).split(","), key=nat)) if x else "" for x in sd.routes]
        if not by_name:   # one name can have several platforms: add the platform number
            sd["stop"] = sd.stop + " · platform " + sd.k.str.replace("NSR:Quay:", "", regex=False)
        worst, busiest_one = sd.loc[sd.mean_delay.idxmax()], sd.loc[sd.visits.idxmax()]
        m = st.columns(4)
        m[0].metric("Stops shown", f"{len(sd):,}", help=f"Stops with at least {max(min_n, 20)} visits in the selection")
        m[1].metric("Typical stop: % on time", f"{sd.otp.median():.0f}%", help="Median over the stops shown")
        m[2].metric("Most delayed stop", worst.stop, f"{worst.mean_delay:.1f} min mean delay", delta_color="inverse")
        m[3].metric("Busiest stop", busiest_one.stop, f"{int(busiest_one.visits):,} visits", delta_color="off")

        map_box = st.container()      # filled after the stop picker below, so the picked stop can be highlighted

        st.subheader("Stop ranking")
        rc = st.columns([3, 1])
        rank_by = rc[0].radio("Rank by", ["Most delayed", "Lowest % on time", "Most often late", "Busiest"], horizontal=True)
        top_n = rc[1].selectbox("Show", [10, 20, 50, 100, len(sd)], index=1,
                                format_func=lambda n: "all" if n == len(sd) else f"top {n}")
        col, asc = {"Most delayed": ("mean_delay", False), "Lowest % on time": ("otp", True),
                    "Most often late": ("late_pct", False), "Busiest": ("visits", False)}[rank_by]
        ranked = sd.sort_values(col, ascending=asc)
        if search:
            ranked = ranked[ranked.stop.str.contains(search, case=False, regex=False, na=False)]
        ranked = ranked.head(top_n).reset_index(drop=True)
        ranked.insert(0, "rank", ranked.index + 1)
        cols = ["rank", "stop"] + (["platforms"] if by_name else []) + ["visits", "n_routes", "routes", "otp", "late_pct",
                                                                        "mean_delay", "p90"]
        st.dataframe(ranked[cols], hide_index=True, width="stretch",
                     height=min(36 * (len(ranked) + 1) + 2, 740),
                     column_config={
                         "rank": st.column_config.NumberColumn("#", width="small"),
                         "stop": st.column_config.TextColumn("Stop", width="medium"),
                         "platforms": st.column_config.NumberColumn("Platforms", width="small"),
                         "visits": st.column_config.NumberColumn("Visits", format="localized"),
                         "n_routes": st.column_config.NumberColumn("Routes", width="small"),
                         "routes": st.column_config.TextColumn("Routes serving the stop", width="medium"),
                         "otp": st.column_config.ProgressColumn("% on time", min_value=0, max_value=100, format="%.0f%%"),
                         "late_pct": st.column_config.ProgressColumn("% late", min_value=0, max_value=100, format="%.0f%%",
                                                                     help=f"Share of visits {int(late_s) // 60} min or more late"),
                         "mean_delay": st.column_config.NumberColumn("Mean delay (min)", format="%.1f"),
                         "p90": st.column_config.NumberColumn("90th percentile delay (min)", format="%.1f",
                                                              help="9 out of 10 buses are less late than this")})
        if search and ranked.empty:
            st.caption(f"No stop name contains “{search}” in this selection.")

        st.subheader("Look at one stop")
        pick_opts = sd.sort_values("visits", ascending=False)
        if search:
            pick_opts = pd.concat([pick_opts[pick_opts.stop.str.contains(search, case=False, regex=False, na=False)], pick_opts])
        pick_opts = pick_opts.drop_duplicates("k")
        pick = st.selectbox("Stop", pick_opts.k.tolist(), format_func=dict(zip(sd.k, sd.stop)).get,
                            help="Sorted by number of visits. Typing in “Find a stop” above puts matches first.")
        one = sd[sd.k == pick].iloc[0]
        m = st.columns(5)
        m[0].metric("Visits", f"{int(one.visits):,}")
        m[1].metric("% on time", f"{one.otp:.0f}%")
        m[2].metric("% late", f"{one.late_pct:.0f}%")
        m[3].metric("Mean delay", f"{one.mean_delay:.1f} min")
        m[4].metric("90th percentile delay", f"{one.p90:.1f} min")
        dc = st.columns([3, 2])
        oh = q(f"""SELECT local_hour, COUNT(*) n, AVG(delay_s) / 60 mean_delay, AVG({ON_TIME}::INT) * 100 otp
                   FROM fact_stop_visits WHERE valid AND {W} AND {key} = ? GROUP BY 1 ORDER BY 1""", [*P, pick])
        fig = go.Figure()
        fig.add_bar(x=oh.local_hour, y=oh.n, name="Visits", yaxis="y2", marker_color="rgba(150,150,150,0.35)")
        fig.add_scatter(x=oh.local_hour, y=oh.mean_delay, name="Mean delay (min)", mode="lines+markers",
                        line=dict(color="#d73027", width=3))
        fig.update_layout(title=f"{one.stop}: delay through the day", height=340, margin=dict(t=50, b=10),
                          xaxis=dict(title="Hour (local time)", dtick=1), yaxis=dict(title="Minutes late"),
                          yaxis2=dict(overlaying="y", side="right", showgrid=False, title="Visits"),
                          legend=dict(orientation="h", y=-0.25))
        dc[0].plotly_chart(fig, width="stretch")
        orr = q(f"""SELECT ANY_VALUE(route_short_name) route, COUNT(*) visits, AVG({ON_TIME}::INT) * 100 otp,
                           AVG(delay_s) / 60 mean_delay
                    FROM fact_stop_visits WHERE valid AND {W} AND {key} = ? GROUP BY line_ref ORDER BY visits DESC""",
                [*P, pick])
        dc[1].markdown("**Routes at this stop**")
        dc[1].dataframe(orr, hide_index=True, width="stretch", height=300,
                        column_config={"route": "Route", "visits": st.column_config.NumberColumn("Visits", format="localized"),
                                       "otp": st.column_config.ProgressColumn("% on time", min_value=0, max_value=100,
                                                                              format="%.0f%%"),
                                       "mean_delay": st.column_config.NumberColumn("Mean delay (min)", format="%.1f")})

        with map_box:
            mp = sd.dropna(subset=["lat", "lon"]).copy()
            if colour_by == "Mean delay":
                mp["color"], s_stops, s_unit = ramp(mp.mean_delay * 60, DELAY_STOPS), DELAY_STOPS, " s"
            else:
                mp["color"], s_stops, s_unit = ramp(mp.otp, OTP_STOPS), OTP_STOPS, "%"
            mp["r"] = 15 + 12 * np.log10(mp.visits)
            mp["tip"] = [f"<b>{s}</b><br/>{int(n):,} visits · routes {r}<br/>mean delay {d:.1f} min · {o:.0f}% on time"
                         for s, n, r, d, o in zip(mp.stop, mp.visits, mp.routes, mp.mean_delay, mp.otp)]
            layers = [pdk.Layer("ScatterplotLayer", data=mp[["lon", "lat", "color", "r", "tip"]], get_position=["lon", "lat"],
                                get_fill_color="color", get_radius="r", radius_min_pixels=3, radius_max_pixels=14,
                                stroked=True, get_line_color=[40, 40, 40], line_width_min_pixels=0.5,
                                pickable=True, auto_highlight=True)]
            sel = mp[mp.k == pick]
            if not sel.empty:
                layers.append(pdk.Layer("ScatterplotLayer", data=sel[["lon", "lat", "tip"]], get_position=["lon", "lat"],
                                        get_radius=60, radius_min_pixels=12, filled=False, stroked=True,
                                        get_line_color=[20, 20, 255], line_width_min_pixels=3, pickable=True))
            view = pdk.ViewState(latitude=float(sel.lat.iloc[0]), longitude=float(sel.lon.iloc[0]), zoom=13) \
                if not sel.empty and search else pdk.ViewState(**STAVANGER)
            st.pydeck_chart(pdk.Deck(map_provider="carto", map_style="light", layers=layers, initial_view_state=view,
                                     tooltip={"html": "{tip}", "style": {"fontSize": "12px"}}), height=480)
            lc = st.columns([2, 3])
            with lc[0]:
                legend(colour_by, s_stops, s_unit)
            lc[1].caption("Bigger circle = more bus visits. The blue ring is the stop picked under “Look at one stop”. "
                          "Hover a stop for details.")

        with st.expander("Delay by hour at the 25 busiest stops", expanded=False):
            busiest = sd.sort_values("visits", ascending=False).head(25)
            sh = q(f"""SELECT {key} k, local_hour, COUNT(*) n, AVG(delay_s) / 60 mean_delay
                       FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1, 2""", P)
            sh = sh[sh.k.isin(busiest.k) & (sh.n >= min_n)].merge(busiest[["k", "stop"]], on="k")
            st.plotly_chart(heatmap(sh, "local_hour", "stop", "mean_delay", "Mean delay (min) at the 25 busiest stops, by hour",
                                    "minutes", DELAY_SCALE, 0, 10, ".1f", y_order=busiest.stop.tolist()), width="stretch")

# ============ Time patterns
with tabs[4]:
    dh = q(f"""SELECT CAST(service_date AS VARCHAR) d, local_hour, COUNT(*) n, AVG({ON_TIME}::INT) * 100 otp, AVG(delay_s) / 60 mean_delay
               FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1, 2""", P)
    dh = dh[dh.n >= min_n]
    hh = 230 + 40 * dh.d.nunique()
    st.plotly_chart(heatmap(dh, "local_hour", "d", "otp", "% on time: date × hour", "% on time", OTP_SCALE, 40, 100, height=hh),
                    width="stretch")
    st.plotly_chart(heatmap(dh, "local_hour", "d", "mean_delay", "Mean delay (min): date × hour", "minutes", DELAY_SCALE, 0, 10,
                            ".1f", height=hh), width="stretch")
    qb = q(f"""SELECT local_quarter, ANY_VALUE(local_time_bin) t, COUNT(*) n, AVG(delay_s) / 60 mean_delay,
                      AVG({ON_TIME}::INT) * 100 otp
               FROM fact_stop_visits WHERE valid AND {W} AND local_quarter >= 0 GROUP BY 1 ORDER BY 1""", P)
    qb = qb[qb.n >= min_n]
    fig = px.bar(qb, x="t", y="mean_delay", color="otp", color_continuous_scale=OTP_SCALE, range_color=(40, 100),
                 labels=dict(t="Time of day (15-minute steps)", mean_delay="Mean delay (min)", otp="% on time"),
                 title="Delay in 15-minute steps (colour = % on time)")
    fig.update_layout(height=360, margin=dict(t=50), xaxis=dict(tickangle=-90, nticks=24))
    st.plotly_chart(fig, width="stretch")
    # weekday of the service date (a Friday trip running after midnight still counts as Friday)
    wk = q(f"""SELECT ISODOW(service_date) - 1 AS weekday, local_hour, COUNT(*) n, COUNT(DISTINCT service_date) n_days,
                      AVG({ON_TIME}::INT) * 100 otp
               FROM fact_stop_visits WHERE valid AND {W} GROUP BY 1, 2""", P)
    if wk.weekday.nunique() > 1:
        wk = wk[wk.n >= min_n]
        wk["day"] = wk.weekday.map(lambda w: WEEKDAYS[int(w)])
        st.plotly_chart(heatmap(wk, "local_hour", "day", "otp", "% on time: weekday × hour", "% on time", OTP_SCALE, 40, 100,
                                height=300, y_order=WEEKDAYS), width="stretch")
        st.caption("Days per weekday in the selection: " + ", ".join(
            f"{WEEKDAYS[int(w)]} {int(d)}" for w, d in wk.groupby("weekday").n_days.max().items()))
    else:
        st.caption("Weekday × hour needs more than one weekday in the selection.")

# ============ Reliability
with tabs[5]:
    if not HAS["gold_headway"]:
        st.info("gold_headway is missing.")
    else:
        cols = q("SELECT * FROM gold_headway LIMIT 0").columns
        has_sq = "sum_actual_headway_sq" in cols
        ewt = ("SUM(sum_actual_headway_sq) / (2 * SUM(sum_actual_headway_s)) - SUM(sum_aimed_headway_sq) / (2 * SUM(sum_aimed_headway_s))"
               if has_sq else "NULL")
        hw = q(f"""SELECT local_hour, SUM(n_pairs) pairs, SUM(n_bunched) * 100.0 / SUM(n_pairs) bunched,
                          SUM(n_large_gap) * 100.0 / SUM(n_pairs) gap, {ewt} AS ewt
                   FROM gold_headway WHERE {W} GROUP BY 1 ORDER BY 1""", P)
        st.caption("Pairs of consecutive buses of the same route at the same stop, scheduled 2 to 60 minutes apart. "
                   f"**Bunched**: arrived less than {BUNCH:g} × the scheduled gap apart. **Large gap**: more than {GAP:g} × the scheduled gap. "
                   + ("**Excess waiting time**: extra seconds an average passenger arriving at random waits because buses are irregular."
                      if has_sq else "Excess waiting time needs the updated gold notebook (sums of squared headways)."))
        if hw.empty:
            st.info("No headway pairs match these filters.")
        else:
            tot = q(f"""SELECT SUM(n_pairs) pairs, SUM(n_bunched) * 100.0 / SUM(n_pairs) bunched,
                               SUM(n_large_gap) * 100.0 / SUM(n_pairs) gap, {ewt} AS ewt FROM gold_headway WHERE {W}""", P).iloc[0]
            c = st.columns(4)
            c[0].metric("Bus pairs analysed", f"{int(tot.pairs):,}")
            c[1].metric("Bunched", f"{tot.bunched:.1f}%")
            c[2].metric("Large gaps", f"{tot.gap:.1f}%")
            if has_sq:
                c[3].metric("Excess waiting time", fmt_s(tot.ewt))
            fig = go.Figure()
            fig.add_scatter(x=hw.local_hour, y=hw.bunched, name="% bunched", line=dict(color="#d73027", width=3))
            fig.add_scatter(x=hw.local_hour, y=hw.gap, name="% large gap", line=dict(color="#4575b4", width=3))
            if has_sq:
                fig.add_bar(x=hw.local_hour, y=hw.ewt, name="excess waiting time (s)", marker_color="#e0e0e0", yaxis="y2")
            fig.update_layout(title="Regularity through the day", xaxis=dict(title="Hour (local time)", dtick=1), yaxis_title="% of bus pairs",
                              yaxis2=dict(overlaying="y", side="right", showgrid=False, title="Seconds"), height=380,
                              legend=dict(orientation="h", y=-0.2), margin=dict(t=50))
            st.plotly_chart(fig, width="stretch")
            hr_ = q(f"""SELECT line_ref, SUM(n_pairs) pairs, SUM(n_bunched) * 100.0 / SUM(n_pairs) bunched,
                               SUM(n_large_gap) * 100.0 / SUM(n_pairs) gap, {ewt} AS ewt
                        FROM gold_headway WHERE {W} GROUP BY 1 HAVING SUM(n_pairs) >= ? ORDER BY bunched DESC""", [*P, max(min_n, 50)])
            st.caption("Routes with at least 50 bus pairs in the selection (fewer pairs give unstable percentages).")
            hr_.insert(0, "route", hr_.line_ref.map(route_label))
            st.dataframe(hr_.drop(columns="line_ref"), hide_index=True, width="stretch",
                         column_config={"route": "Route", "pairs": st.column_config.NumberColumn("Bus pairs", format="localized"),
                                        "bunched": st.column_config.NumberColumn("% bunched", format="%.1f"),
                                        "gap": st.column_config.NumberColumn("% large gaps", format="%.1f"),
                                        "ewt": st.column_config.NumberColumn("Excess waiting time (s)", format="%.0f")})

# ============ Data
with tabs[6]:
    st.subheader("Coverage per service date")
    cov = dates[["d", "n"]].assign(d=pd.to_datetime(dates.d).dt.date)
    st.dataframe(cov, hide_index=True,
                 column_config={"d": st.column_config.DateColumn("Service date", format="ddd D MMM YYYY"),
                                "n": st.column_config.NumberColumn("Valid stop visits", format="localized")})
    st.caption("A service date with only a few hundred visits is the part of a neighbouring day's trips that ran after midnight.")
    if HAS["gold_data_quality"]:
        st.subheader("Gold build checks")
        st.dataframe(q("SELECT * FROM gold_data_quality"), hide_index=True, width="stretch",
                     column_config={"kind": "Type", "label": "Check", "ok": "Passed", "detail": "Detail"})
    if HAS["gold_meta"]:
        with st.expander("Build metadata"):
            st.dataframe(q("SELECT * FROM gold_meta"), hide_index=True, width="stretch",
                         column_config={"key": "Setting", "value": "Value"})
    with st.expander("Technical details"):
        st.markdown(f"**Gold data folder:** `{GOLD_DIR}`  \n**Road map files:** `{MAP_DIR}`  \n"
                    f"**Tables loaded ({len(TABLES)}):** " + ", ".join(f"`{t}`" for t in sorted(TABLES)))


# ============ Busy roads (gold_speed_hour: sums per day x hour x line x road segment; gold_busy_roads: whole period)
TTI_STOPS = [(1.0, (26, 152, 80)), (1.2, (145, 207, 96)), (1.4, (254, 224, 139)), (1.7, (252, 141, 89)), (2.0, (215, 48, 39))]

with tabs[7]:
    if not (HAS["gold_speed_hour"] and HAS["gold_busy_roads"]):
        st.info("gold_speed_hour / gold_busy_roads are missing: run the current analysis/gold/run_gold.py.")
    else:
        c1, c2, c3 = st.columns([2, 3, 1.3])
        colour = c1.selectbox("Colour segments by", ["Slowdown (travel time index)", "Speed (km/h)", "Bus-hours lost",
                                                     "Heatmap: bus-hours lost"],
                              help="Travel time index = free-flow speed / actual speed; 1.0 = no slowdown")
        stop_list = q("SELECT stop_ref, stop_name FROM dim_stop ORDER BY stop_name") if HAS["dim_stop"] else pd.DataFrame()
        stop_pick = c2.selectbox("Only segments starting or ending at stop", ["(all stops)"] + stop_list.stop_ref.tolist(),
                                 format_func=lambda x: x if x.startswith("(") else
                                 f"{stop_list.set_index('stop_ref').stop_name.get(x, x)} ({x.split(':')[-1]})")
        base_map = c3.selectbox("Background ", ["Bus roads only (offline)", "Street map (online)"])
        Ws, Ps = where(alias="s")
        stop_sql, stop_par = ("", []) if stop_pick.startswith("(") else (" AND (s.from_stop_ref = ? OR s.to_stop_ref = ?)", [stop_pick, stop_pick])
        seg = q(f"""SELECT s.from_stop_ref, s.to_stop_ref, SUM(n_runs) n, SUM(sum_distance_m) dist, SUM(sum_run_s) run_s,
                           SUM(sum_sched_s) sched_s, SUM(sum_excess_s) excess_s,
                           STRING_AGG(DISTINCT route_short_name, ', ') routes
                    FROM gold_speed_hour s WHERE {Ws}{stop_sql}
                    GROUP BY 1, 2 HAVING SUM(n_runs) >= ?""", [*Ps, *stop_par, min_n])
        if seg.empty:
            st.info("No road segments match these filters.")
        else:
            br = q("""SELECT from_stop_ref, to_stop_ref, from_stop_name, to_stop_name, free_flow_kmh,
                             from_lat, from_lon, to_lat, to_lon FROM gold_busy_roads""")
            seg = seg.merge(br, on=["from_stop_ref", "to_stop_ref"], how="left")
            seg["kmh"] = seg.dist / seg.run_s * 3.6
            seg["tti"] = seg.free_flow_kmh / seg.kmh
            seg["lost_h"] = seg.excess_s / 3600
            seg["vs_tt"] = (seg.run_s - seg.sched_s) / seg.n
            c = st.columns(4)
            c[0].metric("Road segments", f"{len(seg):,}")
            c[1].metric("Bus runs", f"{int(seg.n.sum()):,}")
            c[2].metric("Average speed", f"{seg.dist.sum() / seg.run_s.sum() * 3.6:.1f} km/h",
                        help="Distance-weighted, stop to stop, including time at the stop")
            c[3].metric("Bus-hours lost vs free flow", f"{seg.lost_h.sum():,.0f} h",
                        help="Sum over all runs of the time beyond driving each segment at its free-flow speed")

            geo = road_geometry()
            if geo is not None:
                seg = seg.merge(geo[["from_stop_ref", "to_stop_ref", "path"]], on=["from_stop_ref", "to_stop_ref"], how="left")
            else:
                seg["path"] = None
            straight = [[[a, b], [c_, d]] for a, b, c_, d in zip(seg.from_lon, seg.from_lat, seg.to_lon, seg.to_lat)]
            seg["path"] = [p_ if isinstance(p_, list) else s_ for p_, s_ in zip(seg["path"], straight)]
            seg = seg[seg.from_lat.notna() & seg.to_lat.notna()]
            if colour.startswith("Slowdown"):
                seg["color"], stops_, unit = ramp(seg.tti.fillna(1), TTI_STOPS), TTI_STOPS, "×"
            elif colour.startswith("Speed"):
                seg["color"], stops_, unit = ramp(seg.kmh, SPEED_STOPS), SPEED_STOPS, " km/h"
            else:
                hi = max(float(seg.lost_h.quantile(0.98)), 1.0)
                stops_ = [(round(hi * f, 1), col) for f, (_, col) in zip([0, .25, .5, .75, 1], TTI_STOPS)]
                seg["color"], unit = ramp(seg.lost_h, stops_), " h"
            seg["width"] = 6 + 8 * np.log10(seg.n)              # metres, limited to 1.5..10 px below
            seg["tip"] = [f"<b>{a} → {b}</b><br/>routes {r}<br/>{int(n):,} runs · {k:.1f} km/h (free flow {ff:.1f})<br/>"
                          f"slowdown {t:.2f}× · {l:.1f} bus-hours lost · {v:+.0f} s vs timetable per bus"
                          for a, b, r, n, k, ff, t, l, v in zip(seg.from_stop_name, seg.to_stop_name, seg.routes, seg.n,
                                                                 seg.kmh, seg.free_flow_kmh, seg.tti, seg.lost_h, seg.vs_tt)]
            layers = []
            if geo is not None:
                layers.append(pdk.Layer("PathLayer", data=geo[["path"]], get_path="path", get_color=[150, 150, 150, 90],
                                        get_width=8, width_min_pixels=1, width_max_pixels=2, pickable=False))
            if colour.startswith("Heatmap"):
                hl = road_heat_layer(seg, "lost_h")
                if hl is not None:
                    layers.append(hl)
            else:
                order = seg.sort_values("tti" if colour.startswith("Slowdown") else "lost_h", na_position="first")
                layers.append(pdk.Layer("PathLayer", data=order[["path", "color", "width", "tip"]], get_path="path",
                                        get_color="color", get_width="width", width_min_pixels=1.5, width_max_pixels=10,
                                        cap_rounded=True, joint_rounded=True, pickable=True, auto_highlight=True))
            view = dict(layers=layers, initial_view_state=pdk.ViewState(**STAVANGER),
                        tooltip={"html": "{tip}", "style": {"fontSize": "12px"}})
            deck = pdk.Deck(map_provider=None, **view) if base_map.startswith("Bus") else pdk.Deck(map_provider="carto", map_style="light", **view)
            st.pydeck_chart(deck, height=620)
            if colour.startswith("Heatmap"):
                st.caption("Heatmap: brighter = more bus-hours lost against free-flow driving on the roads there "
                           "(summed over the selection). Zoom in for street level.")
            else:
                legend(colour, stops_, unit)
            st.caption("A run time includes the dwell at the next stop (Kolumbus reports one time per stop), so segments "
                       "ending at big hubs or terminals partly measure waiting at the stop, not traffic.")
            st.subheader("Segments losing the most time (selection)")
            show = seg.sort_values("lost_h", ascending=False).head(25)
            st.dataframe(show[["from_stop_name", "to_stop_name", "routes", "n", "kmh", "free_flow_kmh", "tti", "vs_tt", "lost_h"]],
                         hide_index=True, width="stretch", column_config=COLUMNS)
            with st.expander("Whole-period ranking (gold_busy_roads, all dates and hours)"):
                st.dataframe(q("""SELECT rank_hours_lost, from_stop_name, to_stop_name, lines, n_runs, free_flow_kmh, peak_kmh,
                                         midday_kmh, night_kmh, travel_time_index, vehicle_hours_lost
                                  FROM gold_busy_roads WHERE ranked ORDER BY rank_hours_lost LIMIT 50"""),
                             hide_index=True, width="stretch", column_config=COLUMNS)

# ============ Bunching & speed heatmaps (gold_headway_hour / gold_speed_hour, filtered by the sidebar)
with tabs[8]:
    st.caption("Delay heatmaps are on the Routes, Stops and Time patterns pages. Here: how regular and how fast buses are.")
    if HAS["gold_headway_hour"]:
        st.subheader("Bunching")
        st.caption(f"Bunched: a bus arrives less than {BUNCH:g} × the scheduled gap after the previous bus of the same "
                   f"route at the same stop. Large gap: more than {GAP:g} ×.")
        bw = q(f"""SELECT weekday, local_hour, SUM(n_headways) n, SUM(n_bunched) * 100.0 / SUM(n_headways) bunched,
                          SUM(n_large_gap) * 100.0 / SUM(n_headways) gap
                   FROM gold_headway_hour WHERE {W} GROUP BY 1, 2 HAVING SUM(n_headways) >= ?""", [*P, min_n])
        if not bw.empty:
            bw["weekday"] = bw.weekday.map(lambda w: WEEKDAYS[int(w)])
            st.plotly_chart(heatmap(bw, "local_hour", "weekday", "bunched", "% bunched: weekday × hour", "%",
                                    DELAY_SCALE, zmin=0, fmt=".1f", y_order=WEEKDAYS), width="stretch")
        br_ = q(f"""SELECT route_short_name AS route, local_hour, SUM(n_headways) n,
                           SUM(n_bunched) * 100.0 / SUM(n_headways) bunched
                    FROM gold_headway_hour WHERE {W} GROUP BY 1, 2 HAVING SUM(n_headways) >= ?""", [*P, min_n])
        if not br_.empty:
            top_r = br_.groupby("route").n.sum().nlargest(25).index.tolist()
            st.plotly_chart(heatmap(br_[br_.route.isin(top_r)], "local_hour", "route", "bunched",
                                    "% bunched: route × hour (25 busiest routes)", "%", DELAY_SCALE, zmin=0, fmt=".0f",
                                    y_order=top_r), width="stretch")
        bs = q(f"""SELECT h.stop_ref, ANY_VALUE(d.stop_name) stop_name, local_hour, SUM(n_headways) n,
                          SUM(n_bunched) * 100.0 / SUM(n_headways) bunched
                   FROM gold_headway_hour h LEFT JOIN dim_stop d USING (stop_ref)
                   WHERE {where(alias='h')[0]} GROUP BY 1, 3 HAVING SUM(n_headways) >= ?""", [*where(alias="h")[1], min_n])
        if not bs.empty:
            top_s = bs.groupby("stop_ref").n.sum().nlargest(25).index.tolist()
            bs["stop"] = bs.stop_name.fillna(bs.stop_ref) + " (" + bs.stop_ref.str.split(":").str[-1] + ")"
            order_s = bs.drop_duplicates("stop_ref").set_index("stop_ref").loc[top_s, "stop"].tolist()
            st.plotly_chart(heatmap(bs[bs.stop_ref.isin(top_s)], "local_hour", "stop", "bunched",
                                    "% bunched: stop × hour (25 busiest stops)", "%", DELAY_SCALE, zmin=0, fmt=".0f",
                                    y_order=order_s), width="stretch")
    if HAS["gold_speed_hour"]:
        st.subheader("Speed")
        st.caption("Average stop-to-stop speed, distance-weighted, including time at the stop.")
        sw = q(f"""SELECT weekday, local_hour, SUM(n_runs) n, SUM(sum_distance_m) / SUM(sum_run_s) * 3.6 kmh
                   FROM gold_speed_hour WHERE {W} GROUP BY 1, 2 HAVING SUM(n_runs) >= ?""", [*P, min_n])
        if not sw.empty:
            sw["weekday"] = sw.weekday.map(lambda w: WEEKDAYS[int(w)])
            st.plotly_chart(heatmap(sw, "local_hour", "weekday", "kmh", "Average speed (km/h): weekday × hour", "km/h",
                                    OTP_SCALE, fmt=".0f", y_order=WEEKDAYS), width="stretch")
        sr = q(f"""SELECT route_short_name AS route, local_hour, SUM(n_runs) n, SUM(sum_distance_m) / SUM(sum_run_s) * 3.6 kmh
                   FROM gold_speed_hour WHERE {W} GROUP BY 1, 2 HAVING SUM(n_runs) >= ?""", [*P, min_n])
        if not sr.empty:
            top_r = sr.groupby("route").n.sum().nlargest(25).index.tolist()
            st.plotly_chart(heatmap(sr[sr.route.isin(top_r)], "local_hour", "route", "kmh",
                                    "Average speed (km/h): route × hour (25 busiest routes)", "km/h", OTP_SCALE, fmt=".0f",
                                    y_order=top_r), width="stretch")
        sd = q(f"""SELECT CAST(service_date AS VARCHAR) AS date, local_hour, SUM(n_runs) n,
                          SUM(sum_distance_m) / SUM(sum_run_s) * 3.6 kmh
                   FROM gold_speed_hour WHERE {W} GROUP BY 1, 2 HAVING SUM(n_runs) >= ?""", [*P, min_n])
        if not sd.empty:
            st.plotly_chart(heatmap(sd, "local_hour", "date", "kmh", "Average speed (km/h): date × hour", "km/h",
                                    OTP_SCALE, fmt=".0f"), width="stretch")

# ============ Trips (gold_trip_metrics: one row per observed trip)
with tabs[9]:
    if not HAS["gold_trip_metrics"]:
        st.info("gold_trip_metrics is missing: run the current analysis/gold/run_gold.py.")
    else:
        Wt, Pt = where("start_hour")
        tm = q(f"""SELECT * FROM gold_trip_metrics WHERE n_stops >= 2 AND {Wt}""", Pt)
        if tm.empty:
            st.info("No trips match these filters.")
        else:
            c = st.columns(4)
            c[0].metric("Trips", f"{len(tm):,}")
            if "avg_kmh" in tm:
                c[1].metric("Median trip speed", f"{tm.avg_kmh.median():.1f} km/h")
            c[2].metric("Median delay gained per trip", fmt_s(tm.delay_gained_s.median()))
            if "n_bunched" in tm:
                c[3].metric("Trips bunched at least once", f"{(tm.n_bunched > 0).mean():.1%}")
            if "avg_kmh" in tm:
                sample = tm.dropna(subset=["avg_kmh"]).sample(min(len(tm), 5000), random_state=0)
                fig = px.scatter(sample, x="avg_kmh", y="delay_gained_s", color="route_short_name", opacity=0.5,
                                 labels=dict(avg_kmh="Trip speed (km/h)", delay_gained_s="Delay gained along the trip (s)",
                                             route_short_name="Route"),
                                 title="Slower trips gain more delay (up to 5,000 trips shown)")
                fig.update_layout(height=420, showlegend=False)
                st.plotly_chart(fig, width="stretch")
            st.subheader("Trips that gained the most delay")
            cols = [c_ for c_ in ["service_date", "route_short_name", "direction_ref", "start_hour", "n_stops", "start_delay_s",
                                  "end_delay_s", "delay_gained_s", "avg_kmh", "n_bunched", "n_headways"] if c_ in tm]
            top_trips = tm.sort_values("delay_gained_s", ascending=False).head(50)[cols].copy()
            top_trips["service_date"] = pd.to_datetime(top_trips["service_date"]).dt.date     # date only, no 00:00:00
            st.dataframe(top_trips, hide_index=True, width="stretch", column_config=COLUMNS)
