"""
BusPulse · Kolumbus buses, Stavanger.   streamlit run app_v3.py -- --gold <gold folder> [--map <road map folder>]

Design: a white canvas, one deep fjord-navy ink, and red / amber / green kept for service quality only.
Every colour and font lives in the DESIGN block, so swapping the palette is a few lines.
Pages render lazily (only the open page runs), queries are cached, stops are combined by name,
and any row, road or dot opens a detail popup.
"""
import argparse, html, itertools, os, sys
from contextlib import contextmanager
from pathlib import Path
import duckdb, numpy as np, pandas as pd
import plotly.graph_objects as go, plotly.io as pio, pydeck as pdk, streamlit as st

HERE = Path(__file__).resolve().parent
ap = argparse.ArgumentParser()
_g = HERE.parent / "tables" / "gold-layer"
if not _g.exists():
    _g = HERE.parent / "tables/parquet_2026-10-24/gold-layer-parquet"
ap.add_argument("--gold", default=os.environ.get("BUSPULSE_GOLD_DIR", str(_g)))
ap.add_argument("--map", default=os.environ.get("BUSPULSE_MAP_DIR", str(HERE / "map")))
A, _ = ap.parse_known_args(sys.argv[1:])
GOLD, MAPD = Path(A.gold), Path(A.map)
st.set_page_config(page_title="BusPulse", page_icon=":material/directions_bus:", layout="wide", initial_sidebar_state="expanded")

DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
CENTER = dict(latitude=58.955, longitude=5.725, zoom=11)
MIN_N = 20
BASES = ["Bus roads only (offline)", "Street map (online)"]
PRESETS = [("All day", (0, 23)), ("Early", (0, 5)), ("Morning rush", (6, 8)), ("Midday", (9, 14)), ("Evening rush", (15, 17)), ("Evening", (18, 23))]
nat = lambda r: (0, int(r), "") if str(r).isdigit() else (1, 0, str(r))

# ================================================================ DESIGN
INK, MUTED, FAINT, TILE, LINE = "#13283B", "#5F7185", "#9AA9B8", "#F2F5F8", "#E3E9EF"
GOOD, WARN, BAD, INFO = "#1F9D6C", "#F0A12B", "#E4474B", "#2F6FB5"
HEAD = "'Bricolage Grotesque', 'Instrument Sans', system-ui, sans-serif"
BODY = "'Instrument Sans', system-ui, -apple-system, 'Segoe UI', sans-serif"

pio.templates["bp"] = go.layout.Template(layout=go.Layout(
    font=dict(family=BODY, color=INK, size=12), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    colorway=[INK, BAD, INFO, WARN, GOOD],
    hoverlabel=dict(bgcolor=INK, bordercolor=INK, font=dict(color="#fff", family=BODY, size=12)),
    xaxis=dict(showgrid=False, zeroline=False, linecolor="#D3DCE5", ticks="outside", tickcolor="#D3DCE5", ticklen=3),
    yaxis=dict(gridcolor="#E6ECF2", zeroline=False, ticks="", showline=False),
    legend=dict(font=dict(size=11, color=MUTED), bgcolor="rgba(0,0,0,0)")))
pio.templates.default = "bp"

st.markdown("<style>@import url('https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:wght@600;700;800&family=Instrument+Sans:wght@400;500;600&display=swap');</style>",
            unsafe_allow_html=True)
CSS = """
.stApp,[data-testid="stAppViewContainer"],[data-testid="stMain"]{background:#fff}
.stApp,.stApp p,.stApp label,.stApp button,.stApp input,.stApp [data-baseweb]{font-family:var(--body)!important}
[data-testid="stHeader"]{background:transparent}
[data-testid="stToolbar"],[data-testid="stDecoration"],footer,#MainMenu{display:none!important}
.block-container{max-width:1360px;padding:1.1rem 2.2rem 4rem!important}
.stApp p,.stApp label{font-weight:400;color:var(--ink)}

/* type: bold titles, fine body */
.brand{display:flex;align-items:center;gap:10px;font:800 1.35rem var(--head);letter-spacing:-.03em;color:var(--ink)}
.pt{font:800 2.3rem/1.05 var(--head);letter-spacing:-.04em;color:var(--ink);margin:1rem 0 .3rem}
.psub{font-size:.95rem;color:var(--muted);margin-bottom:1rem;max-width:64ch;line-height:1.45}
.bt{font:700 1.08rem var(--head);letter-spacing:-.02em;color:var(--ink)}
.bs{font-size:.82rem;color:var(--muted);margin:2px 0 8px;line-height:1.4}

/* centred nav + sub-controls */
[data-testid="stButtonGroup"]{justify-content:center;gap:4px}
button[data-testid^="stBaseButton-segmented_control"]{border:none!important;border-radius:99px!important;background:transparent!important;color:var(--muted)!important;font-weight:500!important;padding:.45rem 1.05rem!important;box-shadow:none!important}
button[data-testid^="stBaseButton-segmented_control"]:hover{background:var(--tile)!important;color:var(--ink)!important}
button[data-testid="stBaseButton-segmented_controlActive"]{background:var(--ink)!important;color:#fff!important;font-weight:600!important}
button[data-testid^="stBaseButton-pills"]{border:none!important;border-radius:99px!important;background:var(--tile)!important;color:var(--muted)!important;font-weight:500!important;padding:.35rem .9rem!important}
button[data-testid="stBaseButton-pillsActive"]{background:#fff!important;color:var(--ink)!important;font-weight:600!important;box-shadow:inset 0 0 0 1.5px var(--ink)!important}
.block-container [data-testid="stButtonGroup"]:has(button[data-testid^="stBaseButton-pills"]){justify-content:flex-start}

/* search + selects */
div[data-baseweb="select"]>div{background:var(--tile)!important;border:none!important;border-radius:14px!important;min-height:44px}
span[data-baseweb="tag"]{background:var(--ink)!important;border-radius:99px!important}
span[data-baseweb="tag"] *{color:#fff!important}
div[data-testid="stToggle"] label p{font-size:.85rem}
.block-container button[data-testid="stBaseButton-secondary"]{border:none;background:var(--tile);border-radius:14px;color:var(--ink);font-weight:500;min-height:44px}
.block-container button[data-testid="stBaseButton-secondary"]:hover{background:#E8EEF4;color:var(--ink)}

/* filter chips */
.fc{display:inline-block;background:var(--tile);color:var(--ink);font-size:.78rem;font-weight:500;padding:6px 12px;border-radius:99px;margin:0 6px 6px 0}

/* widgets */
.grid{display:grid;gap:14px;margin:6px 0 16px}
.g5{grid-template-columns:repeat(5,minmax(0,1fr))}.g4{grid-template-columns:repeat(4,minmax(0,1fr))}.g3{grid-template-columns:repeat(3,minmax(0,1fr))}
@media(max-width:1180px){.g5{grid-template-columns:repeat(3,minmax(0,1fr))}.g4{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:760px){.g5,.g4,.g3{grid-template-columns:repeat(1,minmax(0,1fr))}}
.tile{background:var(--tile);border-radius:20px;padding:18px 20px 14px;display:flex;flex-direction:column;min-height:186px;box-sizing:border-box}
.tile.sm{min-height:116px}
.tile.ins{min-height:140px}
.tl{display:flex;justify-content:space-between;align-items:center;gap:8px;font-size:.84rem;font-weight:500;color:var(--muted)}
.tv{font:800 2.35rem/1.05 var(--head);letter-spacing:-.04em;color:var(--ink);margin-top:12px}
.tv .u{font-size:.95rem;font-weight:600;color:var(--muted);margin-left:5px;letter-spacing:0}
.ins .tv{font-size:1.3rem;line-height:1.2;letter-spacing:-.02em}
.ts{font-size:.83rem;color:var(--muted);margin-top:4px;line-height:1.4}
.tsp{margin-top:auto;padding-top:10px}
.sp{display:block}
.cap{font-size:.72rem;color:var(--faint);margin-top:3px}
.chip{font-size:.72rem;font-weight:600;padding:3px 9px;border-radius:99px;background:#fff;color:var(--ink);white-space:nowrap}
.chip.good{background:#DCF2E8;color:#14774F}.chip.warn{background:#FCEBCF;color:#8A5A00}.chip.bad{background:#FADCDD;color:#A52A2E}

/* chart boxes */
[class*="st-key-box_"]{background:var(--tile);border-radius:16px;padding:18px 20px 12px;gap:.25rem!important;margin-bottom:6px}
[class*="st-key-box_"] [data-testid="stPlotlyChart"]{margin-top:4px}

/* clickable rows: no lines, whole row is the button */
.hd,.rw{display:grid;align-items:center;column-gap:16px;padding:0 18px}
.hd{height:36px;font-size:.76rem;font-weight:500;color:var(--faint)}
.rw{height:58px;font-size:.9rem;color:var(--ink)}
.rw .n{display:block;font-weight:600}
.rw .s{display:block;font-size:.76rem;color:var(--muted);margin-top:1px}
.mv{display:block;font-variant-numeric:tabular-nums}
.bar{display:block;height:4px;border-radius:9px;background:#E3E9EF;margin-top:5px;overflow:hidden;max-width:120px}
.bar i{display:block;height:100%;border-radius:9px}
.badge{display:inline-flex;min-width:44px;height:30px;padding:0 10px;align-items:center;justify-content:center;border-radius:10px;background:var(--ink);color:#fff;font:700 .88rem var(--head)}
[class*="st-key-row_"]{position:relative;gap:0!important;border-radius:14px;transition:background .12s}
[class*="st-key-row_"]:hover{background:var(--tile)}
[class*="st-key-row_"] [data-testid="stElementContainer"]:has(button){position:absolute;inset:0;z-index:5;width:100%!important;height:100%!important}
[class*="st-key-row_"] [data-testid="stButton"],[class*="st-key-row_"] button{width:100%!important;height:100%!important;min-height:0!important;opacity:0;cursor:pointer}

/* map legend */
.lg{font-size:.78rem;color:var(--muted);margin-top:6px}
.lg b{display:block;font-weight:600;color:var(--ink);margin-bottom:5px}
.lgb{height:9px;border-radius:9px}
.lgt{display:flex;justify-content:space-between;margin-top:3px;font-size:.72rem}

/* filter drawer */
section[data-testid="stSidebar"]{background:#fff;border-right:1px solid var(--line);box-shadow:14px 0 36px rgba(19,40,59,.10);width:350px!important}
.sb-h{font:700 .98rem var(--head);letter-spacing:-.01em;color:var(--ink);margin:22px 0 6px}
section[data-testid="stSidebar"] button[data-testid="stBaseButton-secondary"],section[data-testid="stSidebar"] button[data-testid="stBaseButton-primary"]{width:100%;justify-content:flex-start!important;text-align:left!important;border:none!important;border-radius:12px!important;padding:8px 12px!important;min-height:0!important;box-shadow:none!important;font-size:.86rem!important;margin-bottom:2px}
section[data-testid="stSidebar"] button[data-testid="stBaseButton-secondary"]{background:#fff!important;color:var(--ink)!important}
section[data-testid="stSidebar"] button[data-testid="stBaseButton-secondary"]:hover{background:var(--tile)!important}
section[data-testid="stSidebar"] button[data-testid="stBaseButton-primary"]{background:var(--ink)!important;color:#fff!important;font-weight:600!important}
section[data-testid="stSidebar"] button[data-testid="stBaseButton-primary"] p{color:#fff!important}
[data-testid="stSidebarCollapseButton"],[data-testid="collapsedControl"]{display:none!important}
div[role="dialog"]{border-radius:24px!important}
"""
st.markdown(f"<style>:root{{--ink:{INK};--muted:{MUTED};--faint:{FAINT};--tile:{TILE};--line:{LINE};--head:{HEAD};--body:{BODY}}}{CSS}</style>",
            unsafe_allow_html=True)


# ================================================================ data (cached)
@st.cache_resource
def connect():
    con = duckdb.connect()
    for p in sorted(GOLD.glob("*.parquet")):
        src = f"{p.as_posix()}/*.parquet" if p.is_dir() else p.as_posix()
        con.execute(f"CREATE VIEW {p.stem} AS SELECT * FROM read_parquet('{src}')")
    con.execute("CREATE TABLE stop_map AS SELECT stop_ref, COALESCE(stop_name, stop_ref) AS stop, stop_lat lat, stop_lon lon FROM dim_stop")
    return con


@st.cache_data(show_spinner=False, max_entries=2048)
def _q(sql, params):
    return connect().cursor().execute(sql, list(params)).df()


def q(sql, params=()):
    return _q(sql, tuple(params))


@st.cache_data(show_spinner=False)
def geometry():
    p = MAPD / "road_segments.parquet"
    if not p.exists():
        return None
    g = pd.read_parquet(p)
    g["path"] = g["path"].map(lambda a: [list(x) for x in a])
    return g[["from_stop_ref", "to_stop_ref", "path"]]


try:
    connect()
except Exception as e:
    st.error(f"Could not read the gold tables in `{GOLD}`: {e}. Start with `-- --gold <folder>`.")
    st.stop()


@st.cache_data(show_spinner=False)
def load_meta():
    m = q("SELECT DISTINCT iso_week, CAST(service_date AS VARCHAR) d, weekday_name, route_type_name, route_short_name "
          "FROM gold_filters WHERE is_full_date")
    ds = q("SELECT DISTINCT CAST(service_date AS VARCHAR) d, stop FROM gold_delay_hour JOIN stop_map USING (stop_ref)")
    hs = q("SELECT DISTINCT local_hour h, stop FROM gold_delay_hour JOIN stop_map USING (stop_ref)")
    return m, sorted(q("SELECT DISTINCT stop FROM stop_map").stop.tolist()), ds, hs


meta, stops_all, ds_pairs, hs_pairs = load_meta()
dates_df = meta[["d", "iso_week", "weekday_name"]].drop_duplicates()
lines_df = meta[["route_short_name", "route_type_name"]].drop_duplicates()
names_all = sorted(lines_df.route_short_name.unique(), key=nat)
week_ids = sorted(dates_df.iso_week.unique())
wk_stops = {w: ds_pairs[ds_pairs.d.isin(dates_df[dates_df.iso_week == w].d)].stop.nunique() for w in week_ids}
wd_stops = {d: ds_pairs[ds_pairs.d.isin(dates_df[dates_df.weekday_name == d].d)].stop.nunique() for d in DAYS if d in set(dates_df.weekday_name)}
hour_stops = lambda a, b: hs_pairs[(hs_pairs.h >= a) & (hs_pairs.h <= b)].stop.nunique()
day_label = lambda s: f"{pd.Timestamp(s).day} {pd.Timestamp(s).strftime('%b')}"

# ================================================================ filter state + drawer
st.session_state.setdefault("open", True)
st.session_state.setdefault("hrs_v", 0)
FLT0 = dict(weeks=[], wd=[], types=[], hours=(0, 23))
flt = st.session_state.setdefault("flt", dict(FLT0, weeks=[], wd=[], types=[]))
route_opts = {f"Route {r}": r for r in names_all}
stop_opts = {f"Stop {s}": s for s in stops_all}


def flip_drawer():
    st.session_state.open = not st.session_state.open


def toggle(field, v):
    cur = st.session_state.flt[field]
    cur.remove(v) if v in cur else cur.append(v)


def set_hours(h):
    st.session_state.flt["hours"] = h
    st.session_state.hrs_v += 1  # new slider key, so it restarts from the preset without touching widget state


def clear_all():
    st.session_state.flt = dict(FLT0, weeks=[], wd=[], types=[])
    st.session_state["search"] = []
    st.session_state.hrs_v += 1


def option_list(field, opts):
    cur = flt[field]
    for v, label in opts.items():
        st.button(label, key=f"{field}_{v}", type="primary" if v in cur else "secondary", use_container_width=True, on_click=toggle, args=(field, v))


def hours_name(h):
    h = tuple(h)
    return next((n for n, r in PRESETS if r == h), f"{h[0]:02d}:00 to {h[1]:02d}:59")


n_active = sum([bool(flt["weeks"]), bool(flt["wd"]), bool(flt["types"]), tuple(flt["hours"]) != (0, 23), bool(st.session_state.get("search"))])
c_logo, c_nav, c_btn = st.columns([1.5, 6, 1.5], vertical_alignment="center")
c_logo.markdown('<div class="brand"><svg width="30" height="30" viewBox="0 0 30 30"><rect width="30" height="30" rx="9" fill="#13283B"/>'
                '<path d="M4.5 16h5l3-7.5 4 13.5 3-6h6" fill="none" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/></svg>'
                '<span>BusPulse</span></div>', unsafe_allow_html=True)
PAGES = ["Overview", "Stops", "Routes", "Maps", "Heatmaps", "Trips", "Data"]
page = c_nav.segmented_control("Page", PAGES, default="Overview", key="nav", label_visibility="collapsed") or "Overview"
c_btn.button(f"Filters ({n_active})" if n_active else "Filters", icon=":material/tune:", use_container_width=True, on_click=flip_drawer)

c_s, c_chips = st.columns([5, 7], vertical_alignment="center")
picked = c_s.multiselect("Search", list(route_opts) + list(stop_opts), key="search", placeholder="Search routes and stops",
                         label_visibility="collapsed")
chip_slot = c_chips.empty()

if st.session_state.open:
    with st.sidebar:
        a, b = st.columns([5, 1], vertical_alignment="center")
        a.markdown('<div class="pt" style="font-size:1.7rem;margin:.2rem 0 0">Filters</div>', unsafe_allow_html=True)
        b.button("", icon=":material/close:", key="x", on_click=flip_drawer)
        st.caption("Narrow every page to the days, hours and buses you care about.")

        st.markdown('<div class="sb-h">Time of day</div>', unsafe_allow_html=True)
        for name, (a_, b_) in PRESETS:
            lab = f"{name} ({a_:02d}:00 to {b_:02d}:59), {hour_stops(a_, b_):,} stops" if name != "All day" else f"All day, {hour_stops(0, 23):,} stops"
            st.button(lab, key=f"pre_{name}", type="primary" if tuple(flt["hours"]) == (a_, b_) else "secondary", use_container_width=True,
                      on_click=set_hours, args=((a_, b_),))
        flt["hours"] = st.slider("Custom hours", 0, 23, value=tuple(flt["hours"]), key=f"hrs_{st.session_state.hrs_v}")

        st.markdown('<div class="sb-h">Week</div>', unsafe_allow_html=True)
        option_list("weeks", {w: f"Week {w} ({day_label(dates_df[dates_df.iso_week == w].d.min())} to {day_label(dates_df[dates_df.iso_week == w].d.max())}), {wk_stops[w]:,} stops"
                              for w in week_ids})
        st.markdown('<div class="sb-h">Day of the week</div>', unsafe_allow_html=True)
        option_list("wd", {d: f"{d}, {n:,} stops" for d, n in wd_stops.items()})
        st.markdown('<div class="sb-h">Bus type</div>', unsafe_allow_html=True)
        option_list("types", {t: t for t in sorted(lines_df.route_type_name.dropna().unique())})
        st.write("")
        ca, cc = st.columns([1.4, 1])
        ca.button("Apply filters", type="primary", use_container_width=True, on_click=flip_drawer)
        cc.button("Clear", use_container_width=True, on_click=clear_all)

# ================================================================ resolve filters
d = dates_df
if flt["weeks"]:
    d = d[d.iso_week.isin(flt["weeks"])]
if flt["wd"]:
    d = d[d.weekday_name.isin(flt["wd"])]
sel_dates, all_dates, hours = d.d.tolist(), dates_df.d.tolist(), tuple(flt["hours"])
names = sorted(set(lines_df[lines_df.route_type_name.isin(flt["types"])].route_short_name) if flt["types"] else set(names_all), key=nat)
r_pick = [route_opts[p] for p in picked if p in route_opts]
if r_pick:
    names = [n for n in names if n in r_pick]
s_pick = [stop_opts[p] for p in picked if p in stop_opts]
sel_refs = q(f"SELECT stop_ref FROM stop_map WHERE stop IN ({','.join('?' * len(s_pick))})", s_pick).stop_ref.tolist() if s_pick else []

chips = [f"{len(sel_dates)} day{'s' if len(sel_dates) != 1 else ''}", hours_name(hours),
         "All routes" if len(names) == len(names_all) else f"{len(names)} route{'s' if len(names) != 1 else ''}"]
if s_pick:
    chips.append(f"{len(s_pick)} stop{'s' if len(s_pick) != 1 else ''}")
chip_slot.markdown("".join(f'<span class="fc">{html.escape(c)}</span>' for c in chips), unsafe_allow_html=True)

if not sel_dates or not names:
    st.warning("No data matches these filters. Clear a filter to see results again.")
    st.stop()


def W(stop=True, h="local_hour"):
    c, p = [], []
    if len(sel_dates) < len(all_dates):
        c.append(f"CAST(service_date AS VARCHAR) IN ({','.join('?' * len(sel_dates))})"); p += sel_dates
    c.append(f"{h} BETWEEN ? AND ?"); p += list(hours)
    if len(names) < len(names_all):
        c.append(f"route_short_name IN ({','.join('?' * len(names))})"); p += names
    if stop and sel_refs:
        c.append(f"stop_ref IN ({','.join('?' * len(sel_refs))})"); p += sel_refs
    return " AND ".join(c), p


DEL, HW = "gold_delay_hour JOIN stop_map USING (stop_ref)", "gold_headway_hour JOIN stop_map USING (stop_ref)"
D_AGG = ("SUM(n_valid) n, SUM(n_on_time) * 100.0 / NULLIF(SUM(n_valid), 0) otp, SUM(n_late) * 100.0 / NULLIF(SUM(n_valid), 0) late, "
         "SUM(sum_delay_s) / NULLIF(SUM(n_valid), 0) mean_d")
H_AGG = ("SUM(n_headways) nh, SUM(n_bunched) * 100.0 / NULLIF(SUM(n_headways), 0) bunched, SUM(n_large_gap) * 100.0 / NULLIF(SUM(n_headways), 0) gap, "
         "SUM(sum_actual_headway_sq) / NULLIF(2 * SUM(sum_actual_headway_s), 0) - SUM(sum_aimed_headway_sq) / NULLIF(2 * SUM(sum_aimed_headway_s), 0) ewt")
S_AGG = "SUM(sum_distance_m) / NULLIF(SUM(sum_run_s), 0) * 3.6 kmh, SUM(n_runs) runs, SUM(sum_excess_s) / 3600 lost_h"
WS, PS = W()
WN, PN = W(stop=False)

# ================================================================ formatting + colour ramps
isna = lambda x: x is None or pd.isna(x)
mins = lambda s: "–" if isna(s) else f"{s / 60:.1f} min"
pct = lambda x: "–" if isna(x) else f"{x:.1f}%"
kmh = lambda x: "–" if isna(x) else f"{x:.1f} km/h"
num = lambda x: "–" if isna(x) else f"{int(x):,}"
hh = lambda h: f"{int(h):02d}:00"
esc = lambda x: html.escape(str(x))

SPEED_STOPS = [(10, (215, 48, 39)), (20, (252, 141, 89)), (30, (254, 224, 139)), (40, (145, 207, 96)), (55, (26, 152, 80))]
DELAY_STOPS = [(0, (26, 152, 80)), (60, (145, 207, 96)), (180, (254, 224, 139)), (300, (252, 141, 89)), (600, (215, 48, 39))]
TTI_STOPS = [(1.0, (26, 152, 80)), (1.2, (145, 207, 96)), (1.4, (254, 224, 139)), (1.7, (252, 141, 89)), (2.0, (215, 48, 39))]
OTP_STOPS = [(50, (215, 48, 39)), (65, (252, 141, 89)), (75, (254, 224, 139)), (85, (145, 207, 96)), (95, (26, 152, 80))]
BUNCH_STOPS = [(0, (26, 152, 80)), (8, (145, 207, 96)), (15, (254, 224, 139)), (25, (252, 141, 89)), (40, (215, 48, 39))]
STOP_METRICS = {"Mean delay": ("mean_d", DELAY_STOPS, " s"), "% on time": ("otp", OTP_STOPS, "%"), "% bunched": ("bunched", BUNCH_STOPS, "%")}
HEAT_RANGE = [[254, 240, 217], [253, 212, 158], [253, 187, 132], [252, 141, 89], [227, 74, 51], [179, 0, 0]]
HEAT_BAD = [[0, GOOD], [.5, "#FFE7A3"], [1, BAD]]
HEAT_GOOD = [[0, BAD], [.5, "#FFE7A3"], [1, GOOD]]
TONE = {"good": GOOD, "warn": WARN, "bad": BAD, "": INK}


def ramp(v, stops):
    xs, cols = np.array([s[0] for s in stops], float), np.array([s[1] for s in stops], float)
    v = np.clip(np.asarray(v, float), xs[0], xs[-1])
    return np.column_stack([np.interp(v, xs, cols[:, i]) for i in range(3)]).astype(int).tolist()


def tone(v, stops):
    return FAINT if isna(v) else "rgb({},{},{})".format(*ramp([v], stops)[0])


def grade(v, good, warn, high_bad=True):
    if isna(v):
        return ""
    if high_bad:
        return "good" if v <= good else "warn" if v <= warn else "bad"
    return "good" if v >= good else "warn" if v >= warn else "bad"


def legend(title, stops, unit):
    g = ", ".join(f"rgb{tuple(c)} {100 * i / (len(stops) - 1):.0f}%" for i, (_, c) in enumerate(stops))
    t = "".join(f"<span>{v:g}{unit}</span>" for v, _ in stops)
    st.markdown(f'<div class="lg"><b>{title}</b><div class="lgb" style="background:linear-gradient(90deg,{g})"></div><div class="lgt">{t}</div></div>',
                unsafe_allow_html=True)


# ================================================================ widgets
_ids = itertools.count()


@contextmanager
def box(title, sub=""):
    with st.container(key=f"box_{next(_ids)}"):
        st.markdown(f'<div class="bt">{title}</div>' + (f'<div class="bs">{sub}</div>' if sub else ""), unsafe_allow_html=True)
        yield


def head(title, sub=""):
    st.markdown(f'<div class="pt">{title}</div>' + (f'<div class="psub">{sub}</div>' if sub else ""), unsafe_allow_html=True)


def spark(vals, color=INK, h=38):
    v = [float(x) for x in vals if not isna(x)]
    if len(v) < 2:
        return ""
    lo, hi = min(v), max(v)
    span = (hi - lo) or 1.0
    line = " ".join(f"{100 * i / (len(v) - 1):.2f},{h - 4 - (x - lo) / span * (h - 10):.2f}" for i, x in enumerate(v))
    return (f'<svg class="sp" viewBox="0 0 100 {h}" preserveAspectRatio="none" width="100%" height="{h}">'
            f'<polygon points="0,{h} {line} 100,{h}" fill="{color}" opacity=".10"/>'
            f'<polyline points="{line}" fill="none" stroke="{color}" stroke-width="2" vector-effect="non-scaling-stroke" '
            f'stroke-linejoin="round" stroke-linecap="round"/></svg>')


def tile(label, value, sub="", chip="", tn="", sp="", cap="", cls=""):
    c = f'<span class="chip {tn}">{chip}</span>' if chip else ""
    s = f'<div class="tsp">{sp}<div class="cap">{cap}</div></div>' if sp else ""
    return f'<div class="tile {cls}"><div class="tl"><span>{label}</span>{c}</div><div class="tv">{value}</div><div class="ts">{sub}</div>{s}</div>'


def grid(tiles, n):
    st.markdown(f'<div class="grid g{n}">{"".join(tiles)}</div>', unsafe_allow_html=True)


unit = lambda v, u: f'{v}<span class="u">{u}</span>'


def show(f, h=300):
    f.update_layout(height=h, margin=dict(t=6, b=6, l=2, r=2))
    st.plotly_chart(f, width="stretch", theme=None, config={"displayModeBar": False})


def hour_chart(df, h=300):
    f = go.Figure()
    f.add_bar(x=df.local_hour, y=df.n, yaxis="y2", marker_color="#D9E1E9", name="Visits", hovertemplate="%{x}:00, %{y:,} visits<extra></extra>")
    f.add_scatter(x=df.local_hour, y=df.mean_d / 60, mode="lines+markers", name="Mean delay (min)", line=dict(color=BAD, width=3, shape="spline", smoothing=.5),
                  marker=dict(size=7, color="#fff", line=dict(color=BAD, width=2)), hovertemplate="%{x}:00, %{y:.1f} min late<extra></extra>")
    f.update_layout(xaxis=dict(dtick=2, title=""), yaxis=dict(title="Minutes late"), bargap=.25,
                    yaxis2=dict(overlaying="y", side="right", showgrid=False, showticklabels=False), legend=dict(orientation="h", y=1.14, x=0))
    show(f, h)


def hbar(df, y, x, color=BAD, fmt=".1f", suffix=""):
    if df.empty:
        st.caption("Not enough data for this selection.")
        return
    d = df.sort_values(x)
    f = go.Figure(go.Bar(x=d[x], y=d[y].astype(str), orientation="h", marker_color=color, text=[f"{v:{fmt}}{suffix}" for v in d[x]],
                         textposition="outside", cliponaxis=False, hovertemplate="%{y}: %{x:.1f}<extra></extra>"))
    f.update_layout(xaxis=dict(visible=False), yaxis=dict(showgrid=False, type="category", automargin=True), bargap=.4)
    show(f, max(220, 34 * len(d) + 30))


def heat(pv, scale, zmin, zmax, fmt=".1f"):
    f = go.Figure(go.Heatmap(z=pv.values, x=[f"{int(c):02d}" for c in pv.columns], y=[str(i) for i in pv.index], colorscale=scale, zmin=zmin, zmax=zmax,
                             xgap=2, ygap=2, hoverongaps=False, texttemplate=("%{z:" + fmt + "}") if pv.size <= 700 else None, textfont=dict(size=10),
                             colorbar=dict(thickness=8, len=.7, outlinewidth=0), hovertemplate="%{y}, %{x}:00: %{z:" + fmt + "}<extra></extra>"))
    f.update_xaxes(type="category", side="top", title="")
    f.update_yaxes(type="category", autorange="reversed", showgrid=False)
    show(f, max(260, 26 * len(pv) + 90))


def meter(v, stops, text, hi=100):
    if isna(v):
        return "–"
    w = max(0, min(100, 100 * v / hi))
    return f'<span class="mv">{text}</span><span class="bar"><i style="width:{w:.0f}%;background:{tone(v, stops)}"></i></span>'


def rows(df, key, spec, opener):
    """Clickable rows without lines. spec = [(header, css width, cell(row) -> html)]; the first column names the row."""
    cols = " ".join(w for _, w, _ in spec)
    st.markdown(f'<div class="hd" style="grid-template-columns:{cols}">' + "".join(f"<div>{h}</div>" for h, _, _ in spec) + "</div>", unsafe_allow_html=True)
    for i, r in enumerate(df.itertuples(index=False)):
        with st.container(key=f"row_{key}_{i}"):
            st.markdown(f'<div class="rw" style="grid-template-columns:{cols}">' + "".join(f"<div>{fn(r)}</div>" for _, _, fn in spec) + "</div>",
                        unsafe_allow_html=True)
            if st.button("Open", key=f"go_{key}_{i}"):
                opener(r)


def fire(key, ident, fn):
    """Open the popup once per new map selection (selections persist across reruns)."""
    k = "_o_" + key
    if ident is None:
        st.session_state.pop(k, None)
    elif st.session_state.get(k) != ident:
        st.session_state[k] = ident
        fn(ident)


# ================================================================ maps
TIP = {"html": "<div style='padding:2px 4px;font-size:12px'>{tip}</div>", "style": {"backgroundColor": INK, "color": "#fff", "borderRadius": "10px"}}


def deck(layers, key, height=520, select=True, view=None):
    base = st.session_state.get("base", BASES[0])
    d = pdk.Deck(layers=layers, initial_view_state=pdk.ViewState(**(view or CENTER), pitch=0), map_style=None if base == BASES[0] else "light", tooltip=TIP)
    if not select:
        return st.pydeck_chart(d, height=height, key=key)
    return st.pydeck_chart(d, height=height, on_select="rerun", selection_mode="single-object", key=key)


def picked_object(ev):
    try:
        o = ev.selection.objects
    except Exception:
        return None
    if o.get("stops"):
        return "s:" + o["stops"][0]["name"]
    if o.get("roads"):
        return "r:" + o["roads"][0]["routes_key"]
    return None


def net_layer():
    g = geometry()
    if g is None:
        return []
    return [pdk.Layer("PathLayer", id="net", data=g[["path"]], get_path="path", get_color=[214, 221, 229], get_width=3, width_units="pixels",
                      cap_rounded=True, joint_rounded=True)]


def road_frame(route=None):
    c, p = WN, list(PN)
    if route:
        c += " AND route_short_name = ?"
        p.append(route)
    seg = q(f"""SELECT from_stop_ref, to_stop_ref, SUM(n_runs) n, SUM(sum_distance_m) dist, SUM(sum_run_s) run_s, SUM(sum_excess_s) excess_s,
                       STRING_AGG(DISTINCT route_short_name, ', ') routes
                FROM gold_speed_hour WHERE {c} GROUP BY 1, 2 HAVING SUM(n_runs) >= {1 if route else MIN_N // 2}""", p)
    if seg.empty:
        return seg
    br = q("SELECT from_stop_ref, to_stop_ref, from_stop_name, to_stop_name, free_flow_kmh, from_lat, from_lon, to_lat, to_lon FROM gold_busy_roads")
    seg = seg.merge(br, on=["from_stop_ref", "to_stop_ref"], how="left")
    seg = seg[seg.from_lat.notna()].copy()
    if seg.empty:
        return seg
    seg["kmh"], seg["lost_h"] = seg.dist / seg.run_s * 3.6, seg.excess_s / 3600
    seg["tti"] = (seg.free_flow_kmh / seg.kmh).replace([np.inf, -np.inf], np.nan)
    g = geometry()
    seg = seg.merge(g, on=["from_stop_ref", "to_stop_ref"], how="left") if g is not None else seg.assign(path=None)
    seg["path"] = [p if isinstance(p, list) else [[a, b], [c_, d_]] for p, a, b, c_, d_ in zip(seg.path, seg.from_lon, seg.from_lat, seg.to_lon, seg.to_lat)]
    seg["routes_key"] = seg.routes.str.replace(", ", "|")
    seg["width"] = 4 + 2 * np.log10(seg.n.clip(lower=1))
    seg["tip"] = [f"{a} to {b}, routes {r}: {k:.0f} km/h, {t:.2f}x slower than free flow" if not isna(t) else f"{a} to {b}, routes {r}: {k:.0f} km/h"
                  for a, b, r, k, t in zip(seg.from_stop_name, seg.to_stop_name, seg.routes, seg.kmh, seg.tti)]
    return seg.reset_index(drop=True)


def road_layers(seg, how, focus=()):
    """Broad, rounded road lines coloured by slowdown, speed or hours lost. Routes outside `focus` fade to grey."""
    if seg.empty or how == "Off":
        return [], None
    if how.startswith("Slow"):
        col, stops_, u = ramp(seg.tti.fillna(1), TTI_STOPS), TTI_STOPS, "×"
    elif how.startswith("Speed"):
        col, stops_, u = ramp(seg.kmh, SPEED_STOPS), SPEED_STOPS, " km/h"
    else:
        hi = max(float(seg.lost_h.quantile(.98)), 1.0)
        stops_ = [(round(hi * f, 1), c) for f, (_, c) in zip([0, .25, .5, .75, 1], TTI_STOPS)]
        col, u = ramp(seg.lost_h, stops_), " h"
    on = np.array([any(r in focus for r in s.split(", ")) for s in seg.routes]) if focus else np.ones(len(seg), bool)
    d = seg[["path", "width", "tip", "routes_key"]].copy()
    d["color"] = [c + [235] if o else [214, 221, 229, 190] for c, o in zip(col, on)]
    d["width"] = np.where(on, d.width, 3)
    d = d.iloc[np.lexsort((seg.lost_h.fillna(0).values, on))]
    lyr = pdk.Layer("PathLayer", id="roads", data=d, get_path="path", get_color="color", get_width="width", width_units="pixels", width_min_pixels=3,
                    width_max_pixels=14, cap_rounded=True, joint_rounded=True, pickable=True, auto_highlight=True)
    return [lyr], (stops_, u)


def road_heat(seg):
    pts = [(x, y, w / len(p)) for p, w in zip(seg.path, seg.lost_h) if w > 0 for x, y in p]
    if not pts:
        return []
    return [pdk.Layer("HeatmapLayer", id="heat_roads", data=pd.DataFrame(pts, columns=["lon", "lat", "w"]), get_position=["lon", "lat"], get_weight="w",
                      radius_pixels=40, intensity=1.2, threshold=.04, opacity=.85, aggregation="SUM", color_range=HEAT_RANGE)]


def stop_table():
    a = q(f"SELECT stop, AVG(lat) lat, AVG(lon) lon, COUNT(DISTINCT stop_ref) plats, COUNT(DISTINCT route_short_name) routes, {D_AGG} "
          f"FROM {DEL} WHERE {WS} GROUP BY 1 HAVING SUM(n_valid) >= {MIN_N}", PS)
    b = q(f"SELECT stop, {H_AGG} FROM {HW} WHERE {WS} GROUP BY 1", PS)
    return a.merge(b[["stop", "bunched", "ewt"]], on="stop", how="left")


def stop_layers(sd, met, ring=True, pick=True):
    col = STOP_METRICS[met][0]
    d = sd.dropna(subset=["lat", "lon", col]).copy()
    if d.empty:
        return []
    d["color"] = [c + [230] for c in ramp(d[col], STOP_METRICS[met][1])]
    d["r"] = 24 + 14 * np.log10(d.n.clip(lower=1))
    d["tip"] = [f"{a}: {mins(m)} late, {o:.0f}% on time" for a, m, o in zip(d.stop, d.mean_d, d.otp)]
    d["name"] = d.stop
    out = [pdk.Layer("ScatterplotLayer", id="stops", data=d[["lon", "lat", "color", "r", "tip", "name"]], get_position=["lon", "lat"], get_fill_color="color",
                     get_radius="r", radius_min_pixels=5, radius_max_pixels=15, stroked=True, get_line_color=[255, 255, 255], line_width_min_pixels=1.5,
                     pickable=pick, auto_highlight=True)]
    if ring:
        worst = d.nsmallest(10, col) if col == "otp" else d.nlargest(10, col)
        out.append(pdk.Layer("ScatterplotLayer", id="hot", data=worst[["lon", "lat", "r"]], get_position=["lon", "lat"], get_radius="r", radius_min_pixels=12,
                             radius_max_pixels=26, filled=False, stroked=True, get_line_color=[228, 71, 75], line_width_min_pixels=3))
    return out


def stop_heat(sd, col):
    d = sd.dropna(subset=["lat", "lon", col]).copy()
    d["w"] = np.clip(d[col], 0, None) * np.log10(d.n.clip(lower=1) + 1)
    d = d[d.w > 0]
    if d.empty:
        return []
    return [pdk.Layer("HeatmapLayer", id="heat_stops", data=d[["lon", "lat", "w"]], get_position=["lon", "lat"], get_weight="w", radius_pixels=45,
                      intensity=1.1, threshold=.04, opacity=.85, aggregation="SUM", color_range=HEAT_RANGE)]


# ================================================================ popups (route / stop detail)
def route_body(name):
    c, p = f"{WS} AND route_short_name = ?", [*PS, name]
    k = q(f"SELECT {D_AGG}, COUNT(DISTINCT stop) stops FROM {DEL} WHERE {c}", p).iloc[0]
    if isna(k.n) or not k.n:
        st.info("No data for this route in the current filters.")
        return
    h = q(f"SELECT {H_AGG} FROM {HW} WHERE {c}", p).iloc[0]
    sp = q(f"SELECT {S_AGG} FROM gold_speed_hour WHERE {WN} AND route_short_name = ?", [*PN, name]).iloc[0]
    grid([tile("Stops served", num(k.stops), f"{num(k.n)} stop visits", cls="sm"),
          tile("On time", pct(k.otp), f"{pct(k.late)} of visits were late", chip="", tn=grade(k.otp, 85, 70, False), cls="sm"),
          tile("Mean delay", mins(k.mean_d), "Average lateness at stops", cls="sm"),
          tile("Bunched pairs", pct(h.bunched), f"{pct(h.gap)} large gaps", cls="sm"),
          tile("Extra wait", mins(h.ewt), "On top of the timetable", cls="sm"),
          tile("Average speed", kmh(sp.kmh), f"{num(sp.runs)} bus runs", cls="sm")], 3)
    hr = q(f"SELECT local_hour, {D_AGG} FROM {DEL} WHERE {c} GROUP BY 1 ORDER BY 1", p)
    hr = hr[hr.n >= MIN_N]
    bh = q(f"SELECT local_hour, {H_AGG} FROM {HW} WHERE {c} GROUP BY 1 HAVING SUM(n_headways) >= {MIN_N} ORDER BY 1", p)
    tips = []
    if not hr.empty:
        w, b = hr.loc[hr.mean_d.idxmax()], hr.loc[hr.mean_d.idxmin()]
        tips.append(f"Rush hour is **{hh(w.local_hour)}**: buses run {mins(w.mean_d)} behind and only {pct(w.otp)} are on time. Calmest is **{hh(b.local_hour)}** ({mins(b.mean_d)}).")
    if not bh.empty:
        t = bh.loc[bh.bunched.idxmax()]
        tips.append(f"Bunching peaks at **{hh(t.local_hour)}** ({pct(t.bunched)} of bus pairs): two buses arrive together, then a long wait.")
    if not isna(h.ewt):
        tips.append(f"The average rider waits about **{mins(h.ewt)}** longer than the timetable promises.")
    for t in tips:
        st.markdown(f"- {t}")

    seg = road_frame(name)
    sd = q(f"SELECT stop, AVG(lat) lat, AVG(lon) lon, {D_AGG} FROM {DEL} WHERE {c} GROUP BY 1", p)
    sd = sd[sd.n >= 3]
    with box("Where it runs", "Road colour shows slowdown against free flow. Dots are stops coloured by delay, with red rings on the worst."):
        layers = net_layer() + road_layers(seg, "Slowdown")[0] + (stop_layers(sd.assign(bunched=np.nan), "Mean delay", pick=False) if not sd.empty else [])
        try:
            view = pdk.data_utils.compute_view(sd[["lon", "lat"]].dropna().values.tolist(), .9) if not sd.empty else None
            view = dict(latitude=view.latitude, longitude=view.longitude, zoom=view.zoom)
        except Exception:
            view = None
        deck(layers, key=f"rmap_{name}", height=340, select=False, view=view)
    l, r = st.columns(2)
    with l:
        with box("Delay through the day"):
            if not hr.empty:
                hour_chart(hr)
    with r:
        with box("Average speed by hour"):
            sh = q(f"SELECT local_hour, {S_AGG} FROM gold_speed_hour WHERE {WN} AND route_short_name = ? GROUP BY 1 ORDER BY 1", [*PN, name])
            if not sh.empty:
                f = go.Figure(go.Scatter(x=sh.local_hour, y=sh.kmh, mode="lines+markers", line=dict(color=INFO, width=3, shape="spline", smoothing=.5),
                                         marker=dict(size=7, color="#fff", line=dict(color=INFO, width=2)), hovertemplate="%{x}:00, %{y:.1f} km/h<extra></extra>"))
                f.update_layout(xaxis=dict(dtick=2, title=""), yaxis=dict(title="km/h"))
                show(f)
    l, r = st.columns(2)
    with l:
        with box("Most delayed stops", "Mean minutes late"):
            ws = q(f"SELECT stop, {D_AGG} FROM {DEL} WHERE {c} GROUP BY 1 HAVING SUM(n_valid) >= {MIN_N} ORDER BY mean_d DESC LIMIT 8", p)
            ws["min"] = ws.mean_d / 60
            hbar(ws, "stop", "min", BAD)
    with r:
        with box("Stops with most bunching", "Share of bus pairs that arrive together"):
            bs = q(f"SELECT stop, {H_AGG} FROM {HW} WHERE {c} GROUP BY 1 HAVING SUM(n_headways) >= {MIN_N} ORDER BY bunched DESC LIMIT 8", p)
            hbar(bs, "stop", "bunched", INFO, suffix="%")


def stop_body(stop):
    c, p = f"{WS} AND stop = ?", [*PS, stop]
    k = q(f"SELECT {D_AGG}, COUNT(DISTINCT route_short_name) routes, COUNT(DISTINCT stop_ref) plats, AVG(lat) lat, AVG(lon) lon FROM {DEL} WHERE {c}", p).iloc[0]
    if isna(k.n) or not k.n:
        st.info("No data for this stop in the current filters.")
        return
    h = q(f"SELECT {H_AGG} FROM {HW} WHERE {c}", p).iloc[0]
    grid([tile("Routes serving it", num(k.routes), f"{num(k.plats)} platform{'s' if k.plats != 1 else ''}", cls="sm"),
          tile("On time", pct(k.otp), f"{pct(k.late)} of visits were late", tn=grade(k.otp, 85, 70, False), cls="sm"),
          tile("Mean delay", mins(k.mean_d), f"{num(k.n)} visits", cls="sm"),
          tile("Bunched pairs", pct(h.bunched), f"{pct(h.gap)} large gaps", cls="sm"),
          tile("Extra wait", mins(h.ewt), "On top of the timetable", cls="sm")], 3)
    rt = q(f"SELECT route_short_name Route, {D_AGG} FROM {DEL} WHERE {c} GROUP BY 1 HAVING SUM(n_valid) >= 5", p)
    rb = q(f"SELECT route_short_name Route, {H_AGG} FROM {HW} WHERE {c} GROUP BY 1", p)
    rt = rt.merge(rb[["Route", "bunched"]], on="Route", how="left").sort_values("mean_d", ascending=False)
    hr = q(f"SELECT local_hour, {D_AGG} FROM {DEL} WHERE {c} GROUP BY 1 ORDER BY 1", p)
    hr = hr[hr.n >= 5]
    bh = q(f"SELECT local_hour, {H_AGG} FROM {HW} WHERE {c} GROUP BY 1 HAVING SUM(n_headways) >= 5 ORDER BY 1", p)
    if not rt.empty and not hr.empty:
        w, wh = rt.iloc[0], hr.loc[hr.mean_d.idxmax()]
        st.markdown(f"- **Route {w.Route}** is the most delayed here ({mins(w.mean_d)}, {pct(w.otp)} on time).\n"
                    f"- The stop is worst at **{hh(wh.local_hour)}** ({mins(wh.mean_d)} behind).")
    if not bh.empty:
        t = bh.loc[bh.bunched.idxmax()]
        st.markdown(f"- Bunching happens most at **{hh(t.local_hour)}** ({pct(t.bunched)} of bus pairs).")
    l, r = st.columns([3, 2])
    with l:
        with box("Delay through the day"):
            if not hr.empty:
                hour_chart(hr)
    with r:
        with box("Routes at this stop"):
            st.dataframe(rt.rename(columns={"n": "Visits", "otp": "% on time", "mean_d": "Delay (s)", "bunched": "% bunched"})[["Route", "Visits", "% on time", "Delay (s)", "% bunched"]],
                         hide_index=True, width="stretch", height=300,
                         column_config={c_: st.column_config.NumberColumn(format="%.0f") for c_ in ["% on time", "Delay (s)"]})
    pl = q(f"SELECT stop_ref, STRING_AGG(DISTINCT route_short_name, ', ') routes, {D_AGG} FROM {DEL} WHERE {c} GROUP BY 1", p)
    if len(pl) > 1:
        pb = q(f"SELECT stop_ref, {H_AGG} FROM {HW} WHERE {c} GROUP BY 1", p)
        pl = pl.merge(pb[["stop_ref", "bunched"]], on="stop_ref", how="left")
        pl["Platform"] = pl.stop_ref.str.split(":").str[-1]
        with box("Each platform on its own"):
            st.dataframe(pl[["Platform", "routes", "n", "otp", "mean_d", "bunched"]], hide_index=True, width="stretch",
                         column_config={"routes": "Routes", "n": st.column_config.NumberColumn("Visits", format="localized"),
                                        "otp": st.column_config.NumberColumn("% on time", format="%.0f"),
                                        "mean_d": st.column_config.NumberColumn("Mean delay (s)", format="%.0f"),
                                        "bunched": st.column_config.NumberColumn("% bunched", format="%.1f")})


@st.dialog("Route details", width="large")
def route_dialog(key):
    ns = key.split("|")
    name = ns[0] if len(ns) == 1 else st.selectbox("Routes on this road", ns)
    st.markdown(f'<div class="pt" style="font-size:1.8rem;margin:0 0 .6rem">Route {esc(name)}</div>', unsafe_allow_html=True)
    route_body(name)


@st.dialog("Stop details", width="large")
def stop_dialog(stop):
    st.markdown(f'<div class="pt" style="font-size:1.8rem;margin:0 0 .6rem">{esc(stop)}</div>', unsafe_allow_html=True)
    stop_body(stop)


def open_map(ident):
    kind, val = ident.split(":", 1)
    (stop_dialog if kind == "s" else route_dialog)(val)


# ================================================================ row layouts
BADGE = lambda r: f'<span class="badge">{esc(r.Route)}</span>'
STOP_SPEC = [
    ("Stop", "minmax(0,2.6fr)", lambda r: f'<span class="n">{esc(r.stop)}</span><span class="s">{int(r.plats)} platform{"s" if r.plats != 1 else ""}, {int(r.routes)} routes</span>'),
    ("Visits", "minmax(0,1fr)", lambda r: f'<span class="mv">{int(r.n):,}</span>'),
    ("On time", "minmax(0,1.3fr)", lambda r: meter(r.otp, OTP_STOPS, pct(r.otp))),
    ("Mean delay", "minmax(0,1fr)", lambda r: f'<span class="mv">{mins(r.mean_d)}</span>'),
    ("Bunched", "minmax(0,1.3fr)", lambda r: meter(r.bunched, BUNCH_STOPS, pct(r.bunched), hi=40)),
    ("Extra wait", "minmax(0,1fr)", lambda r: f'<span class="mv">{mins(r.ewt)}</span>'),
]
ROUTE_SPEC = [
    ("Route", "minmax(0,1.2fr)", BADGE),
    ("Stops", "minmax(0,.8fr)", lambda r: f'<span class="mv">{int(r.Stops):,}</span>'),
    ("Visits", "minmax(0,1fr)", lambda r: f'<span class="mv">{int(r.n):,}</span>'),
    ("On time", "minmax(0,1.3fr)", lambda r: meter(r.otp, OTP_STOPS, pct(r.otp))),
    ("Mean delay", "minmax(0,1fr)", lambda r: f'<span class="mv">{mins(r.mean_d)}</span>'),
    ("Bunched", "minmax(0,1.3fr)", lambda r: meter(r.bunched, BUNCH_STOPS, pct(r.bunched), hi=40)),
    ("Extra wait", "minmax(0,1fr)", lambda r: f'<span class="mv">{mins(r.ewt)}</span>'),
    ("Speed", "minmax(0,1fr)", lambda r: f'<span class="mv">{kmh(r.kmh)}</span>'),
]


# ================================================================ pages
def page_overview():
    k = q(f"SELECT {D_AGG}, COUNT(DISTINCT stop) stops, COUNT(DISTINCT stop_ref) plats, COUNT(DISTINCT route_short_name) routes FROM {DEL} WHERE {WS}", PS).iloc[0]
    if isna(k.n) or not k.n:
        st.info("No stop visits match these filters.")
        return
    h = q(f"SELECT {H_AGG} FROM {HW} WHERE {WS}", PS).iloc[0]
    sp = q(f"SELECT {S_AGG} FROM gold_speed_hour WHERE {WN}", PN).iloc[0]
    Wt, Pt = W(stop=False, h="start_hour")
    tb = f"FROM gold_trip_metrics WHERE n_stops >= 2 AND {Wt}"
    tr = q(f"SELECT COUNT(*) trips, AVG((n_bunched > 0)::INT) * 100 bunched_trips, MEDIAN(delay_gained_s) gained {tb}", Pt).iloc[0]
    trh = q(f"SELECT start_hour h, COUNT(*) trips {tb} GROUP BY 1 ORDER BY 1", Pt)
    hr = q(f"SELECT local_hour, {D_AGG}, COUNT(DISTINCT stop) stops, COUNT(DISTINCT route_short_name) routes FROM {DEL} WHERE {WS} GROUP BY 1 ORDER BY 1", PS)
    bh = q(f"SELECT local_hour, {H_AGG} FROM {HW} WHERE {WS} GROUP BY 1 ORDER BY 1", PS)
    sph = q(f"SELECT local_hour, {S_AGG} FROM gold_speed_hour WHERE {WN} GROUP BY 1 ORDER BY 1", PN)
    wd = q(f"SELECT weekday, {D_AGG} FROM {DEL} WHERE {WS} GROUP BY 1 ORDER BY 1", PS)
    rr = q(f"SELECT route_short_name r, {D_AGG} FROM {DEL} WHERE {WS} GROUP BY 1 HAVING SUM(n_valid) >= {MIN_N * 5}", PS)
    ss = q(f"SELECT stop, {D_AGG} FROM {DEL} WHERE {WS} GROUP BY 1 HAVING SUM(n_valid) >= {MIN_N * 5} ORDER BY mean_d DESC LIMIT 8", PS)

    head("Overview", "How the network is running right now in your selection: what is served, how late it is, and where buses bunch up.")
    n_types = lines_df[lines_df.route_short_name.isin(names)].route_type_name.nunique()
    gd, gb = grade(k.mean_d, 60, 180), grade(h.bunched, 8, 15)
    grid([
        tile("Stops", num(k.stops), f"{num(k.plats)} platforms, {num(k.n)} visits", sp=spark(hr.stops), cap="Active stops by hour"),
        tile("Trips", num(tr.trips), f"{pct(tr.bunched_trips)} contain bunching", chip="" if isna(tr.gained) else f"median {tr.gained / 60:+.1f} min",
             sp=spark(trh.trips), cap="Trips by start hour"),
        tile("Routes", num(k.routes), f"{n_types} bus type{'s' if n_types != 1 else ''}, {len(names_all)} in the network", sp=spark(hr.routes), cap="Routes running by hour"),
        tile("Delays", "–" if isna(k.mean_d) else unit(f"{k.mean_d / 60:.1f}", "min"), f"{pct(k.otp)} on time", chip=f"{pct(k.late)} late", tn=gd,
             sp=spark(hr.mean_d, TONE[gd]), cap="Mean delay by hour"),
        tile("Bunched", pct(h.bunched), f"{pct(h.gap)} large gaps", chip="" if isna(h.ewt) else f"+{mins(h.ewt)} wait", tn=gb,
             sp=spark(bh.bunched, TONE[gb]), cap="Bunched share by hour"),
    ], 5)
    grid([tile("On time", pct(k.otp), f"of {num(k.n)} stop visits", tn=grade(k.otp, 85, 70, False), cls="sm"),
          tile("Extra wait", mins(h.ewt), "Riders wait this much more than scheduled", cls="sm"),
          tile("Average speed", kmh(sp.kmh), f"{num(sp.runs)} bus runs", cls="sm"),
          tile("Bus-hours lost", "–" if isna(sp.lost_h) else f"{sp.lost_h:,.0f} h", "Compared with free-flowing traffic", cls="sm")], 4)

    ins = []
    hq = hr[hr.n >= MIN_N]
    if not hq.empty:
        w, b = hq.loc[hq.mean_d.idxmax()], hq.loc[hq.otp.idxmax()]
        ins.append(("Worst hour", hh(w.local_hour), f"Buses average {mins(w.mean_d)} late. Timetables hold best at {hh(b.local_hour)} ({pct(b.otp)} on time)."))
    if len(wd) > 1:
        w = wd.loc[wd.mean_d.idxmax()]
        ins.append(("Hardest day", DAYS[int(w.weekday)], f"{mins(w.mean_d)} mean delay and {pct(w.otp)} on time."))
    if not rr.empty:
        w, b = rr.loc[rr.otp.idxmin()], rr.loc[rr.otp.idxmax()]
        ins.append(("Route gap", f"{b.otp - w.otp:.0f} points", f"Route {b.r} is the most punctual ({pct(b.otp)}), route {w.r} the least ({pct(w.otp)}). Which route you take matters more than when."))
    if not ss.empty:
        ins.append(("Most delayed busy stop", esc(ss.stop.iloc[0]), f"{mins(ss.mean_d.iloc[0])} late on average, which hits every route that serves it."))
    try:
        bn = q("SELECT stop_name, pct_bunched FROM gold_bunching_stops WHERE n_headways >= 100 ORDER BY pct_bunched DESC LIMIT 1").iloc[0]
        ins.append(("Bunching hotspot", esc(bn.stop_name), f"{pct(bn.pct_bunched)} of bus pairs bunch here across the whole period."))
        br = q("SELECT from_stop_name, to_stop_name, travel_time_index, vehicle_hours_lost FROM gold_busy_roads WHERE ranked ORDER BY rank_hours_lost LIMIT 1").iloc[0]
        ins.append(("Costliest road", f"{esc(br.from_stop_name)} to {esc(br.to_stop_name)}",
                    f"{br.vehicle_hours_lost:,.0f} bus-hours lost, {br.travel_time_index:.2f}x slower than free flow."))
    except Exception:
        pass
    if ins:
        st.markdown('<div class="bt" style="margin:14px 0 4px;font-size:1.3rem">What stands out</div>', unsafe_allow_html=True)
        grid([tile(a, b, c, cls="ins") for a, b, c in ins], 3)

    l, r = st.columns([3, 2])
    with l:
        with box("Delay through the day", "Lateness (line) against how many stop visits there were (bars)."):
            hour_chart(hr[hr.n >= MIN_N])
    with r:
        with box("Share on time by hour", "Green is reliable, red is not."):
            f = go.Figure(go.Bar(x=hr.local_hour, y=hr.otp, marker_color=[tone(v, OTP_STOPS) for v in hr.otp], hovertemplate="%{x}:00, %{y:.0f}% on time<extra></extra>"))
            f.update_layout(xaxis=dict(dtick=2, title=""), yaxis=dict(title="% on time", range=[0, 100]), bargap=.2)
            show(f)
    l, r = st.columns(2)
    with l:
        with box("Bunching through the day", "Bus pairs arriving together, and pairs leaving a big gap."):
            bq = bh[bh.nh >= MIN_N]
            f = go.Figure()
            f.add_scatter(x=bq.local_hour, y=bq.bunched, name="Bunched", mode="lines", fill="tozeroy", fillcolor="rgba(47,111,181,.12)",
                          line=dict(color=INFO, width=3, shape="spline", smoothing=.5), hovertemplate="%{x}:00, %{y:.1f}% bunched<extra></extra>")
            f.add_scatter(x=bq.local_hour, y=bq.gap, name="Large gap", mode="lines", line=dict(color=WARN, width=3, shape="spline", smoothing=.5),
                          hovertemplate="%{x}:00, %{y:.1f}% large gaps<extra></extra>")
            f.update_layout(xaxis=dict(dtick=2, title=""), yaxis=dict(title="% of bus pairs"), legend=dict(orientation="h", y=1.14, x=0))
            show(f)
    with r:
        with box("Average speed by hour", "Slower hours mean congestion on the roads."):
            f = go.Figure(go.Scatter(x=sph.local_hour, y=sph.kmh, mode="lines", fill="tozeroy", fillcolor="rgba(19,40,59,.07)",
                                     line=dict(color=INK, width=3, shape="spline", smoothing=.5), hovertemplate="%{x}:00, %{y:.1f} km/h<extra></extra>"))
            f.update_layout(xaxis=dict(dtick=2, title=""), yaxis=dict(title="km/h", rangemode="tozero"))
            show(f)
    l, r = st.columns(2)
    with l:
        with box("Delay by weekday", "Mean minutes late"):
            if len(wd) > 1:
                f = go.Figure(go.Bar(x=[DAYS[int(x)][:3] for x in wd.weekday], y=wd.mean_d / 60, marker_color=[tone(v, DELAY_STOPS) for v in wd.mean_d],
                                     text=[f"{v / 60:.1f}" for v in wd.mean_d], textposition="outside", cliponaxis=False, hovertemplate="%{x}: %{y:.1f} min<extra></extra>"))
                f.update_layout(yaxis=dict(title="Minutes late", rangemode="tozero"), bargap=.35)
                show(f)
            else:
                st.caption("Select more than one day to compare weekdays.")
    with r:
        with box("On time by bus type", "Share of stop visits that were on time"):
            bt = rr.merge(lines_df.drop_duplicates("route_short_name").rename(columns={"route_short_name": "r"}), on="r").assign(w=lambda x: x.otp * x.n)
            bt = bt.groupby("route_type_name").agg(w=("w", "sum"), n=("n", "sum")).reset_index()
            bt["otp"] = bt.w / bt.n
            hbar(bt, "route_type_name", "otp", INK, fmt=".0f", suffix="%")
    l, r = st.columns(2)
    with l:
        with box("Most delayed routes", "Mean minutes late"):
            t = rr.nlargest(8, "mean_d").assign(min=lambda x: x.mean_d / 60)
            t["r"] = "Route " + t.r.astype(str)
            hbar(t, "r", "min", BAD)
    with r:
        with box("Most delayed stops", "Mean minutes late"):
            hbar(ss.assign(min=ss.mean_d / 60), "stop", "min", BAD)


def page_stops():
    s = stop_table()
    head("Stops", "Every stop with platforms combined. Click a row or a dot on the map to open its full breakdown.")
    if s.empty:
        st.info("No stops match these filters.")
        return
    with box("Stop map", "Dots are sized by visits and coloured by the chosen measure. Red rings mark the ten worst."):
        met = st.pills("Colour dots by", list(STOP_METRICS), default="Mean delay", key="st_met") or "Mean delay"
        ev = deck(net_layer() + stop_layers(s, met), "stops_map", 440)
        legend(met, STOP_METRICS[met][1], STOP_METRICS[met][2])
    fire("stops_map", picked_object(ev), open_map)
    c1, c2 = st.columns([5, 1.3], vertical_alignment="bottom")
    by = c1.pills("Rank by", ["Most delayed", "Lowest % on time", "Most bunching", "Busiest"], default="Most delayed", key="st_rank") or "Most delayed"
    top = c2.selectbox("Show", [25, 50, 100, 200], key="st_top")
    col, asc = {"Most delayed": ("mean_d", False), "Lowest % on time": ("otp", True), "Most bunching": ("bunched", False), "Busiest": ("n", False)}[by]
    v = s.sort_values(col, ascending=asc).head(top).reset_index(drop=True)
    rows(v, "stop", STOP_SPEC, lambda r: stop_dialog(r.stop))


def page_routes():
    a = q(f"SELECT route_short_name Route, COUNT(DISTINCT stop) Stops, {D_AGG} FROM {DEL} WHERE {WS} GROUP BY 1 HAVING SUM(n_valid) >= {MIN_N}", PS)
    head("Routes", "Every route in the selection. Click a row or a road on the map to open its full breakdown.")
    if a.empty:
        st.info("No routes match these filters.")
        return
    b = q(f"SELECT route_short_name Route, {H_AGG} FROM {HW} WHERE {WS} GROUP BY 1", PS)
    sp = q(f"SELECT route_short_name Route, {S_AGG} FROM gold_speed_hour WHERE {WN} GROUP BY 1", PN)
    r = a.merge(b[["Route", "bunched", "ewt"]], on="Route", how="left").merge(sp[["Route", "kmh"]], on="Route", how="left")
    r = r.sort_values("Route", key=lambda c: c.map(nat)).reset_index(drop=True)
    with box("Route map", "Broad lines are the roads buses use, coloured by how much they slow buses down. Pick routes to isolate them."):
        c1, c2 = st.columns([3, 2])
        how = c1.pills("Colour roads by", ["Slowdown", "Speed", "Bus-hours lost"], default="Slowdown", key="rt_how") or "Slowdown"
        focus = c2.multiselect("Focus on routes", list(r.Route), key="rt_focus", placeholder="All routes")
        seg = road_frame()
        lyr, lg = road_layers(seg, how, tuple(focus))
        ev = deck(net_layer() + lyr, "routes_map", 440)
        if lg:
            legend({"Slowdown": "Slowdown against free flow", "Speed": "Average speed", "Bus-hours lost": "Bus-hours lost"}[how], lg[0], lg[1])
    fire("routes_map", picked_object(ev), open_map)
    rows(r, "route", ROUTE_SPEC, lambda x: route_dialog(x.Route))


def page_maps():
    head("Maps", "Roads and stops together, with the trouble spots lit up. Click any road or dot for details.")
    c = st.columns([1.5, 1.4, 1.7, 1.4, 1.5], vertical_alignment="bottom")
    roads = c[0].selectbox("Roads", ["Slowdown (travel time index)", "Speed (km/h)", "Bus-hours lost", "Off"])
    pts = c[1].selectbox("Stops", ["Mean delay", "% on time", "% bunched", "Off"])
    heat_how = c[2].selectbox("Heatmap", ["Off", "Bus-hours lost (roads)", "Delay (stops)", "Bunching (stops)"])
    hot = c[3].toggle("Highlight worst 10", value=True)
    st.session_state["base"] = c[4].selectbox("Background", BASES)
    seg = road_frame()
    s = stop_table()
    layers = net_layer()
    lyr, rl = road_layers(seg, roads)
    if heat_how.startswith("Bus-hours"):
        layers += road_heat(seg)
    elif heat_how.startswith("Delay"):
        layers += stop_heat(s, "mean_d")
    elif heat_how.startswith("Bunching"):
        layers += stop_heat(s, "bunched")
    layers += lyr
    if pts != "Off":
        layers += stop_layers(s, pts, ring=hot)
    elif hot and not seg.empty:
        worst = seg.nlargest(10, "lost_h")
        layers.append(pdk.Layer("PathLayer", id="hot_roads", data=worst[["path"]], get_path="path", get_color=[228, 71, 75, 255], get_width=12, width_units="pixels",
                                cap_rounded=True, joint_rounded=True))
    ev = deck(layers, "big_map", 620)
    fire("big_map", picked_object(ev), open_map)
    lc = st.columns(2)
    with lc[0]:
        if rl:
            legend(roads, rl[0], rl[1])
    with lc[1]:
        if pts != "Off":
            legend(f"Stops: {pts}", STOP_METRICS[pts][1], STOP_METRICS[pts][2])
    l, r = st.columns(2)
    with l:
        st.markdown('<div class="bt" style="margin:18px 0 4px;font-size:1.3rem">Worst stops</div>', unsafe_allow_html=True)
        w = s.nlargest(8, "mean_d").reset_index(drop=True)
        rows(w, "hot_stops", [STOP_SPEC[0], STOP_SPEC[2], STOP_SPEC[3]], lambda x: stop_dialog(x.stop))
    with r:
        st.markdown('<div class="bt" style="margin:18px 0 4px;font-size:1.3rem">Costliest roads</div>', unsafe_allow_html=True)
        if not seg.empty:
            w = seg.nlargest(8, "lost_h").reset_index(drop=True)
            rows(w, "hot_roads", [
                ("Road", "minmax(0,2.4fr)", lambda x: f'<span class="n">{esc(x.from_stop_name)} to {esc(x.to_stop_name)}</span><span class="s">Routes {esc(x.routes)}</span>'),
                ("Hours lost", "minmax(0,1fr)", lambda x: f'<span class="mv">{x.lost_h:,.0f} h</span>'),
                ("Slowdown", "minmax(0,1fr)", lambda x: f'<span class="mv">{"–" if isna(x.tti) else f"{x.tti:.2f}x"}</span>')],
                 lambda x: route_dialog(x.routes_key))


def page_heatmaps():
    head("Heatmaps", "Where and when it goes wrong. Greener cells run well, redder cells run badly.")
    c1, c2 = st.columns([3, 2])
    met = c1.pills("Measure", ["Mean delay", "% on time", "% bunched"], default="Mean delay", key="hm_m") or "Mean delay"
    dim = c2.pills("Rows", ["Routes", "Stops", "Weekday"], default="Routes", key="hm_d") or "Routes"

    def one(dim_):
        g = {"Routes": "route_short_name", "Stops": "stop", "Weekday": "ISODOW(CAST(service_date AS DATE)) - 1"}[dim_]
        if met == "% bunched":
            df = q(f"SELECT {g} y, local_hour, {H_AGG} FROM {HW} WHERE {WS} GROUP BY 1, 2", PS).rename(columns={"nh": "n"})
            df["z"], scale, lo, hi = df.bunched, HEAT_BAD, 0, None
        elif met == "% on time":
            df = q(f"SELECT {g} y, local_hour, {D_AGG} FROM {DEL} WHERE {WS} GROUP BY 1, 2", PS)
            df["z"], scale, lo, hi = df.otp, HEAT_GOOD, 50, 100
        else:
            df = q(f"SELECT {g} y, local_hour, {D_AGG} FROM {DEL} WHERE {WS} GROUP BY 1, 2", PS)
            df["z"], scale, lo, hi = df.mean_d / 60, HEAT_BAD, 0, None
        df = df[df.n >= 5]
        if df.empty:
            st.caption("Not enough data for this selection.")
            return
        if dim_ == "Weekday":
            df["y"] = df.y.map(lambda x: DAYS[int(x)])
            order = DAYS
        else:
            keep = df.groupby("y").n.sum().nlargest(30).index
            df = df[df.y.isin(keep)]
            order = sorted(keep, key=nat) if dim_ == "Routes" else list(df.groupby("y").n.sum().sort_values(ascending=False).index)
        pv = df.pivot_table(index="y", columns="local_hour", values="z", aggfunc="first")
        pv = pv.reindex([o for o in order if o in pv.index])
        heat(pv, scale, lo, hi if hi is not None else max(float(df.z.quantile(.97)), .5), ".0f" if met == "% on time" else ".1f")

    with box(f"{met} by hour", {"Routes": "The 30 busiest routes", "Stops": "The 30 busiest stops", "Weekday": "Every weekday"}[dim]):
        one(dim)
    if dim != "Weekday":
        with box(f"{met} by weekday and hour"):
            one("Weekday")


def page_trips():
    Wt, Pt = W(stop=False, h="start_hour")
    tb = f"FROM gold_trip_metrics WHERE n_stops >= 2 AND {Wt}"
    head("Trips", "Whole trips from start to finish: how much delay builds up on the way, and how often buses bunch.")
    t = q(f"SELECT COUNT(*) trips, AVG((n_bunched > 0)::INT) * 100 bunched, MEDIAN(delay_gained_s) gained, AVG(delay_gained_s) mean_gained {tb}", Pt).iloc[0]
    if isna(t.trips) or not t.trips:
        st.info("No trips match these filters.")
        return
    grid([tile("Trips", num(t.trips), "Complete trips in the selection", cls="sm"),
          tile("Trips with bunching", pct(t.bunched), "At least one bunched pair on the way", tn=grade(t.bunched, 20, 40), cls="sm"),
          tile("Median delay gained", "–" if isna(t.gained) else f"{t.gained / 60:+.1f} min", "Positive means delay builds up along the trip", cls="sm"),
          tile("Average delay gained", "–" if isna(t.mean_gained) else f"{t.mean_gained / 60:+.1f} min", "Mean across all trips", cls="sm")], 4)
    hr = q(f"SELECT start_hour h, COUNT(*) trips, AVG((n_bunched > 0)::INT) * 100 bunched {tb} GROUP BY 1 ORDER BY 1", Pt)
    dist = q(f"SELECT LEAST(GREATEST(FLOOR(delay_gained_s / 60.0), -10), 15) m, COUNT(*) c {tb} AND delay_gained_s IS NOT NULL GROUP BY 1 ORDER BY 1", Pt)
    l, r = st.columns(2)
    with l:
        with box("Trips by start hour", "Bars are trips, the line is the share with bunching."):
            f = go.Figure()
            f.add_bar(x=hr.h, y=hr.trips, marker_color="#D9E1E9", name="Trips", hovertemplate="%{x}:00, %{y:,} trips<extra></extra>")
            f.add_scatter(x=hr.h, y=hr.bunched, yaxis="y2", name="% with bunching", mode="lines+markers", line=dict(color=INFO, width=3, shape="spline", smoothing=.5),
                          marker=dict(size=7, color="#fff", line=dict(color=INFO, width=2)), hovertemplate="%{x}:00, %{y:.0f}% bunched<extra></extra>")
            f.update_layout(xaxis=dict(dtick=2, title=""), yaxis=dict(title="Trips"), bargap=.25, legend=dict(orientation="h", y=1.14, x=0),
                            yaxis2=dict(overlaying="y", side="right", showgrid=False, ticksuffix="%", rangemode="tozero"))
            show(f)
    with r:
        with box("How much delay trips gain", "Minutes gained between first and last stop. Left of zero, buses recover time."):
            f = go.Figure(go.Bar(x=dist.m, y=dist.c, marker_color=[GOOD if m < 0 else BAD for m in dist.m], hovertemplate="%{x:.0f} min: %{y:,} trips<extra></extra>"))
            f.update_layout(xaxis=dict(title="Minutes gained (clipped at -10 and +15)"), yaxis=dict(title="Trips"), bargap=.15)
            show(f)
    rt = q(f"SELECT route_short_name Route, COUNT(*) trips, AVG((n_bunched > 0)::INT) * 100 bunched, MEDIAN(delay_gained_s) gained {tb} GROUP BY 1 HAVING COUNT(*) >= 5", Pt)
    if rt.empty:
        return
    top = st.selectbox("Show", [25, 50, 100], key="tr_top")
    rt = rt.sort_values("gained", ascending=False).head(top).reset_index(drop=True)
    st.markdown('<div class="bt" style="margin:14px 0 2px;font-size:1.3rem">Routes where delay builds up most</div>', unsafe_allow_html=True)
    rows(rt, "trip", [("Route", "minmax(0,1.2fr)", BADGE),
                      ("Trips", "minmax(0,1fr)", lambda r: f'<span class="mv">{int(r.trips):,}</span>'),
                      ("Trips with bunching", "minmax(0,1.6fr)", lambda r: meter(r.bunched, BUNCH_STOPS, pct(r.bunched), hi=60)),
                      ("Median delay gained", "minmax(0,1.4fr)", lambda r: f'<span class="mv">{"–" if isna(r.gained) else f"{r.gained / 60:+.1f} min"}</span>')],
         lambda r: route_dialog(r.Route))


def page_data():
    head("Data", "The gold tables behind every number on these pages.")
    tabs = [p.stem for p in sorted(GOLD.glob("*.parquet"))]
    t = st.selectbox("Table", tabs, key="dt_tbl")
    total = int(q(f"SELECT COUNT(*) c FROM {t}").c.iloc[0])
    cols = q(f"DESCRIBE {t}")
    grid([tile("Rows", f"{total:,}", "In the full table", cls="sm"), tile("Columns", f"{len(cols)}", "Fields per row", cls="sm")], 4)
    with box("Columns"):
        st.dataframe(cols[["column_name", "column_type"]].rename(columns={"column_name": "Column", "column_type": "Type"}), hide_index=True, width="stretch")
    prev = q(f"SELECT * FROM {t} LIMIT 500")
    with box("Preview", "The first 500 rows"):
        st.dataframe(prev, hide_index=True, width="stretch", height=420)
        st.download_button("Download preview as CSV", prev.to_csv(index=False).encode(), file_name=f"{t}_preview.csv", mime="text/csv")


{"Overview": page_overview, "Stops": page_stops, "Routes": page_routes, "Maps": page_maps, "Heatmaps": page_heatmaps,
 "Trips": page_trips, "Data": page_data}[page]()