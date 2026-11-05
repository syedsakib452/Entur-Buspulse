"""Kolumbus transit performance dashboard (Streamlit + deck.gl via pydeck).

Run:  streamlit run dashboard_app.py
DB :  set GOLD_DB to your gold.duckdb (default below is relative to where you start streamlit).
"""
import os
from datetime import date

import duckdb
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

DB_PATH = os.environ.get("GOLD_DB", "../../tables/parquet_2026-10-24/gold-layer-parquet/gold.duckdb")
MAX_SEG_M = 3000            # drop stop-to-stop jumps longer than this (data gaps look like fake roads)
MAP_HEIGHT = 640

st.set_page_config(page_title="Kolumbus Transit", layout="wide", initial_sidebar_state="collapsed")
st.markdown("""
<style>
  .block-container {padding-top: 0.6rem; padding-bottom: 0.4rem; max-width: 100%;}
  header[data-testid="stHeader"] {display: none;}
  .topbar {font-size: 1.5rem; font-weight: 600; padding: 0.2rem 0 0.6rem 0; letter-spacing: .3px;}
  .topbar span {font-weight: 300; opacity: .7; font-size: 1.05rem; margin-left: .8rem;}
  div[data-testid="stMetricValue"] {font-size: 1.6rem;}
</style>
<div class="topbar">Kolumbus Transit <span>Stavanger, Rogaland</span></div>
""", unsafe_allow_html=True)


# ------------------------------------------------------------------ data access
@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data(show_spinner=False)
def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    return get_con().cursor().execute(sql, list(params)).fetchdf()


try:
    tables = set(q("SELECT table_name FROM information_schema.tables").table_name)
except Exception as e:
    st.error(f"Could not open gold database at `{DB_PATH}`.\n\n{e}")
    st.stop()
need = {"gold_segments_hour", "dim_line", "dim_stop"}
if not need <= tables:
    st.error(f"Missing tables {sorted(need - tables)}. Run gold_geo_cell.py in the gold notebook first.")
    st.stop()
HAS_HEX = "gold_hex_hour" in tables

dates = q("SELECT MIN(service_date) lo, MAX(service_date) hi FROM gold_segments_hour").iloc[0]
routes = q("SELECT line_ref, route_short_name FROM dim_line ORDER BY TRY_CAST(route_short_name AS INTEGER), route_short_name")
bounds = q("SELECT MIN(stop_lat) a, MAX(stop_lat) b, MIN(stop_lon) c, MAX(stop_lon) d FROM dim_stop WHERE stop_lat IS NOT NULL").iloc[0]


# ------------------------------------------------------------------ colour helpers
GREEN, YELLOW, RED = np.array([46, 204, 113]), np.array([241, 196, 15]), np.array([231, 76, 60])
RAMP = np.stack([GREEN, YELLOW, RED]).astype(float)


def ramp(t: np.ndarray) -> np.ndarray:
    t = np.clip(np.nan_to_num(t, nan=0.0), 0, 1) * 2
    i = np.minimum(t.astype(int), 1)
    f = (t - i)[:, None]
    return (RAMP[i] * (1 - f) + RAMP[i + 1] * f).astype(int)


def colorize(values: pd.Series, metric: str, alpha: int) -> list:
    lo, hi = (0.0, 300.0) if metric == "Avg delay (s)" else (0.0, 0.40)      # fixed domain = stable colours
    rgb = ramp(((values - lo) / (hi - lo)).to_numpy(dtype=float))
    a = np.full((len(rgb), 1), alpha)
    out = np.hstack([rgb, a])
    out[values.isna().to_numpy()] = [120, 120, 120, 90]                       # no valid calls -> grey
    return out.tolist()


# ------------------------------------------------------------------ filters (right-hand panel, like the reference)
map_col, side_col = st.columns([5, 1.35], gap="medium")

with side_col:
    st.markdown("##### Date range")
    dsel = st.date_input("Date range", value=(dates.lo, dates.hi), min_value=dates.lo, max_value=dates.hi,
                         label_visibility="collapsed")
    d0, d1 = (dsel[0], dsel[-1]) if isinstance(dsel, (list, tuple)) and len(dsel) else (dsel, dsel)

    st.markdown("##### Routes")
    picked = st.multiselect("Routes", routes.route_short_name.tolist(), placeholder="All routes",
                            label_visibility="collapsed")
    line_refs = routes[routes.route_short_name.isin(picked)].line_ref.tolist()
    route_sql = "AND line_ref IN (" + ",".join("'" + str(x).replace("'", "''") + "'" for x in line_refs) + ")" if line_refs else ""

    st.markdown("##### Hours (local time)")
    h0, h1 = st.select_slider("Hours", options=list(range(24)), value=(0, 23),
                              format_func=lambda h: f"{h:02d}:00", label_visibility="collapsed")

    st.markdown("##### Map view")
    view = st.radio("Map view", ["Roads", "Hexagon heat"], horizontal=True, label_visibility="collapsed",
                    help="Hexagon heat aggregates delay into hexagons along the roads where buses stop."
                         + ("" if HAS_HEX else " (unavailable: gold_hex_hour not built)"))
    if view == "Hexagon heat" and not HAS_HEX:
        st.warning("Hexagon table missing, showing roads.")
        view = "Roads"
    metric = st.radio("Colour by", ["Avg delay (s)", "% late"], horizontal=True)
    if view == "Roads":
        min_n = st.slider("Hide thin segments (min trips)", 1, 30, 3)
    else:
        res = st.radio("Hexagon size", ["Fine (r9)", "Coarse (r8)"], horizontal=True)
        min_n = st.slider("Min valid calls per hexagon", 1, 100, 10)

base_params = (d0, d1, int(h0), int(h1))


# ------------------------------------------------------------------ queries
def roads_df(min_trips: int) -> pd.DataFrame:
    sql = f"""
    WITH s AS (
      SELECT CASE WHEN from_stop_ref <= to_stop_ref THEN from_stop_ref ELSE to_stop_ref END AS a_ref,
             CASE WHEN from_stop_ref <= to_stop_ref THEN to_stop_ref ELSE from_stop_ref END AS b_ref,
             CASE WHEN from_stop_ref <= to_stop_ref THEN from_lat ELSE to_lat END AS a_lat,
             CASE WHEN from_stop_ref <= to_stop_ref THEN from_lon ELSE to_lon END AS a_lon,
             CASE WHEN from_stop_ref <= to_stop_ref THEN to_lat ELSE from_lat END AS b_lat,
             CASE WHEN from_stop_ref <= to_stop_ref THEN to_lon ELSE from_lon END AS b_lon,
             CASE WHEN from_stop_ref <= to_stop_ref THEN from_stop_name ELSE to_stop_name END AS a_name,
             CASE WHEN from_stop_ref <= to_stop_ref THEN to_stop_name ELSE from_stop_name END AS b_name,
             route_short_name, n_trips, n_valid, sum_delay_s, n_late
      FROM gold_segments_hour
      WHERE service_date BETWEEN ? AND ? AND local_hour BETWEEN ? AND ? AND dist_m <= {MAX_SEG_M} {route_sql})
    SELECT a_ref, b_ref, ANY_VALUE(a_lat) a_lat, ANY_VALUE(a_lon) a_lon, ANY_VALUE(b_lat) b_lat, ANY_VALUE(b_lon) b_lon,
           ANY_VALUE(a_name) a_name, ANY_VALUE(b_name) b_name,
           SUM(n_trips) n_trips, SUM(n_valid) n_valid, SUM(sum_delay_s) sum_delay_s, SUM(n_late) n_late,
           string_agg(DISTINCT route_short_name, ', ') routes
    FROM s GROUP BY a_ref, b_ref HAVING SUM(n_trips) >= ?"""
    return q(sql, base_params + (int(min_trips),))


def hex_df(col: str, min_valid: int) -> pd.DataFrame:
    sql = f"""
    SELECT {col} AS hex, SUM(n_valid) n_valid, SUM(sum_delay_s) sum_delay_s, SUM(n_late) n_late,
           string_agg(DISTINCT route_short_name, ', ') routes
    FROM gold_hex_hour
    WHERE service_date BETWEEN ? AND ? AND local_hour BETWEEN ? AND ? {route_sql}
    GROUP BY 1 HAVING SUM(n_valid) >= ?"""
    return q(sql, base_params + (int(min_valid),))


def with_metrics(df: pd.DataFrame) -> pd.DataFrame:
    n = df.n_valid.replace(0, np.nan)
    df = df.copy()
    df["avg_delay"] = df.sum_delay_s / n
    df["pct_late"] = df.n_late / n
    df["m"] = df.avg_delay if metric == "Avg delay (s)" else df.pct_late
    return df


def fmt_tip(df: pd.DataFrame, title: pd.Series) -> list:
    return [f"<b>{t}</b><br/>Avg delay: {a:.0f} s<br/>Late: {p:.0%}<br/>Valid calls: {n:,.0f}<br/>Routes: {r}"
            if pd.notna(a) else f"<b>{t}</b><br/>No valid calls"
            for t, a, p, n, r in zip(title, df.avg_delay, df.pct_late, df.n_valid, df.routes)]


# ------------------------------------------------------------------ map
span_lon = max(float(bounds.d - bounds.c), 0.01)
span_lat = max(float(bounds.b - bounds.a), 0.01)
zoom = float(np.clip(np.log2(703 / (max(span_lon, span_lat * 1.6) * 1.15)), 6, 14))
VIEW = pdk.ViewState(latitude=float((bounds.a + bounds.b) / 2), longitude=float((bounds.c + bounds.d) / 2),
                     zoom=zoom, pitch=0, bearing=0)      # constant => the camera is not reset when filters change


def path_layer(df: pd.DataFrame, rgba, width, layer_id: str, pickable: bool) -> pdk.Layer:
    d = pd.DataFrame({
        "path": [[[a, b], [c, d_]] for a, b, c, d_ in zip(df.a_lon, df.a_lat, df.b_lon, df.b_lat)],
        "color": rgba, "w": width,
        "tip": fmt_tip(df, df.a_name + " \u2192 " + df.b_name) if pickable else "",
    })
    return pdk.Layer("PathLayer", d, id=layer_id, get_path="path", get_color="color", get_width="w",
                     width_units="pixels", width_min_pixels=1, cap_rounded=True, joint_rounded=True,
                     pickable=pickable, auto_highlight=pickable, highlight_color=[255, 255, 255, 200],
                     transitions={"getColor": 500, "getWidth": 500})


with map_col:
    roads = with_metrics(roads_df(min_n if view == "Roads" else 1))
    if view == "Roads":
        if roads.empty:
            st.info("No road segments for this selection.")
            st.stop()
        w = 1.5 + 4.0 * np.sqrt(roads.n_trips / roads.n_trips.max())
        layers = [path_layer(roads, colorize(roads.m, metric, 235), w.tolist(), "roads", True)]
        shown = f"{len(roads):,} road segments"
    else:
        col = "h3_r9" if res.startswith("Fine") else "h3_r8"
        hx = with_metrics(hex_df(col, min_n))
        if hx.empty:
            st.info("No hexagons meet the minimum-calls filter. Lower it on the right.")
            st.stop()
        hd = pd.DataFrame({"hex": hx.hex, "color": colorize(hx.m, metric, 175),
                           "tip": fmt_tip(hx, pd.Series(["Zone"] * len(hx)))})
        layers = [
            path_layer(roads, [[90, 90, 95, 110]] * len(roads), 1.2, "roads_faint", False),   # roads stay visible underneath
            pdk.Layer("H3HexagonLayer", hd, id="hex", get_hexagon="hex", get_fill_color="color",
                      pickable=True, auto_highlight=True, highlight_color=[255, 255, 255, 120],
                      extruded=False, filled=True, stroked=True, get_line_color=[255, 255, 255, 40],
                      line_width_min_pixels=1, coverage=0.96, transitions={"getFillColor": 500}),
        ]
        shown = f"{len(hx):,} hexagons"
    st.pydeck_chart(pdk.Deck(layers=layers, initial_view_state=VIEW, map_style="dark",
                             tooltip={"html": "{tip}", "style": {"backgroundColor": "#111", "color": "#eee",
                                                                  "fontSize": "12px"}}),
                    height=MAP_HEIGHT)
    lo, hi = ("0 s", "300+ s") if metric == "Avg delay (s)" else ("0%", "40%+")
    st.caption(f"{shown} \u00b7 colour: {metric}, green {lo} \u2192 red {hi}. "
               "Segments are straight stop-to-stop lines (add GTFS shapes for true road curves).")


# ------------------------------------------------------------------ gauges (same filters as the map)
gsrc = "gold_hex_hour" if HAS_HEX else "gold_segments_hour"
g = q(f"""SELECT SUM(n_valid) n_valid, SUM(sum_delay_s) sd, SUM(n_on_time) ot, SUM(n_late) late
          FROM {gsrc} WHERE service_date BETWEEN ? AND ? AND local_hour BETWEEN ? AND ? {route_sql}""", base_params).iloc[0]
sp = q(f"""SELECT SUM(sum_dist_m) dist, SUM(sum_runtime_s) rt FROM gold_segments_hour
           WHERE service_date BETWEEN ? AND ? AND local_hour BETWEEN ? AND ? AND dist_m <= {MAX_SEG_M} {route_sql}""",
       base_params).iloc[0]

n_valid = float(g.n_valid or 0)
on_time = 100 * g.ot / n_valid if n_valid else None
avg_delay = g.sd / n_valid if n_valid else None
pct_late = 100 * g.late / n_valid if n_valid else None
speed = 3.6 * sp.dist / sp.rt if sp.rt else None


def gauge(title, value, lo, hi, suffix, good_high=True, steps=None):
    ok = value is not None and not pd.isna(value)
    frac = (value - lo) / (hi - lo) if ok else 0
    colour = "#2ecc71" if (frac >= 0.7 if good_high else frac <= 0.55) else ("#f1c40f" if 0.4 <= frac <= 0.7 else "#e74c3c")
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=float(value) if ok else 0, number={"suffix": suffix, "valueformat": ".0f"},
        title={"text": title, "font": {"size": 16}},
        gauge={"axis": {"range": [lo, hi]}, "bar": {"color": colour}, "bgcolor": "rgba(255,255,255,0.08)",
               "borderwidth": 0}))
    fig.update_layout(height=210, margin=dict(l=20, r=20, t=50, b=10), paper_bgcolor="rgba(0,0,0,0)",
                      font={"color": "#ddd"})
    return fig


c1, c2, c3, c4 = st.columns([1, 1, 1, 1.1])
c1.plotly_chart(gauge("Avg. on-time performance", on_time, 0, 100, " %"))
c2.plotly_chart(gauge("Avg. segment speed (approx.)", speed, 0, 80, " km/h"))
c3.plotly_chart(gauge("Avg. delay", avg_delay, -180, 360, " s", good_high=False))
with c4:
    st.metric("Valid stop calls", f"{n_valid:,.0f}")
    st.metric("Late (\u2265 5 min)", f"{pct_late:.1f} %" if pct_late is not None else "n/a")
    st.caption("On-time = between 1 min early and 5 min late. Speed uses straight-line distance and includes "
               "dwell time, so it underestimates true road speed.")