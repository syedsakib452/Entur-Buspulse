"""Usage: python patch_buspulse.py app_v3.py   ->   writes app_v4.py next to it.
Every edit is anchored on exact text from app_v3.py; if one is missing the script stops and tells you which."""
import pathlib, sys

src = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "app_v3.py")
s = src.read_text()


import re
missing = []


def R(old, new):
    global s
    if old not in s:
        missing.append(old[:90])
        return
    s = s.replace(old, new, 1)


def RX(pattern, new, must=True):
    """Same idea, but whitespace-tolerant (regex). must=False makes it a cosmetic, skippable edit."""
    global s
    s, n = re.subn(pattern, (lambda m: new(m) if callable(new) else new), s, count=1)
    if not n:
        if must:
            missing.append(pattern)
        else:
            print("Skipped optional edit:", pattern)


# ---------------------------------------------------------------- palette: slate bg, white cards, blue + teal
R('INK, MUTED, FAINT, TILE, LINE = "#13283B", "#5F7185", "#9AA9B8", "#F2F5F8", "#E3E9EF"',
  'INK, MUTED, FAINT, TILE, LINE = "#172B4D", "#5B6B82", "#97A4B6", "#F3F6FA", "#DCE3EB"\nBG, CARD, TEAL = "#F3F6FA", "#FFFFFF", "#55A6A0"')
R('"#1F9D6C", "#F0A12B", "#E4474B", "#2F6FB5"', '"#1F9D6C", "#F0A12B", "#E4474B", "#3478C8"')
R("colorway=[INK, BAD, INFO, WARN, GOOD]", "colorway=[INFO, TEAL, INK, WARN, BAD]")
R("--tile:{TILE};--line", "--tile:{TILE};--bg:{BG};--line")
R('fill="#13283B"', 'fill="#172B4D"')

# ---------------------------------------------------------------- CSS: bento cards + full-width layout
R('[data-testid="stMain"]{background:#fff}', '[data-testid="stMain"]{background:var(--bg)}')
R(".block-container{max-width:1360px;", ".block-container{max-width:100%!important;")
R('div[data-baseweb="select"]>div{background:var(--tile)!important;border:none!important;border-radius:14px!important;',
  'div[data-baseweb="select"]>div{background:#fff!important;border:1px solid var(--line)!important;border-radius:12px!important;')
R('button[data-testid^="stBaseButton-pills"]{border:none!important;border-radius:99px!important;background:var(--tile)!important;',
  'button[data-testid^="stBaseButton-pills"]{border:1px solid var(--line)!important;border-radius:99px!important;background:#fff!important;')
R('.block-container button[data-testid="stBaseButton-secondary"]{border:none;background:var(--tile);',
  '.block-container button[data-testid="stBaseButton-secondary"]{border:1px solid var(--line);background:#fff;')
R(".fc{display:inline-block;background:var(--tile);", ".fc{display:inline-block;background:#fff;border:1px solid var(--line);")
R(".tile{background:var(--tile);border-radius:20px;padding:18px 20px 14px;",
  ".tile{background:#fff;border:1px solid var(--line);box-shadow:0 1px 2px rgba(23,43,77,.05);border-radius:14px;padding:16px 18px 12px;")
R('[class*="st-key-box_"]{background:var(--tile);border-radius:16px;padding:18px 20px 12px;',
  '[class*="st-key-box_"]{background:#fff;border:1px solid var(--line);box-shadow:0 1px 2px rgba(23,43,77,.05);border-radius:14px;padding:16px 18px 10px;')
R(".chip{font-size:.72rem;font-weight:600;padding:3px 9px;border-radius:99px;background:#fff;",
  ".chip{font-size:.72rem;font-weight:600;padding:3px 9px;border-radius:99px;background:var(--tile);")
R('[class*="st-key-row_"]:hover{background:var(--tile)}', '[class*="st-key-row_"]:hover{background:#fff;box-shadow:0 0 0 1px var(--line)}')
R('div[role="dialog"]{border-radius:24px!important}',
  'div[role="dialog"]{border-radius:24px!important}\n'
  'section[data-testid="stSidebar"] [data-testid="stButtonGroup"]{justify-content:flex-start;flex-wrap:wrap}\n'
  '.tile.bx{min-height:150px;padding:14px 16px 10px}.bx .tv{font-size:1.9rem;margin-top:8px}\n'
  '.hero{display:flex;align-items:center;gap:12px;margin:6px 0 2px}\n'
  '.hero .big{font:800 4rem/1 var(--head);letter-spacing:-.05em;color:var(--ink)}\n'
  '.sec{font:700 1.15rem var(--head);letter-spacing:-.02em;color:var(--ink);margin:18px 0 6px}')

# ---------------------------------------------------------------- speed: hold the gold tables in memory, lighter map geometry
R("CREATE VIEW {p.stem} AS SELECT * FROM read_parquet", "CREATE TABLE {p.stem} AS SELECT * FROM read_parquet")
R('g["path"] = g["path"].map(lambda a: [list(x) for x in a])',
  'g["path"] = g["path"].map(lambda a: [[round(float(x[0]), 5), round(float(x[1]), 5)] for x in a])')

# ---------------------------------------------------------------- filters: native multi-select pills, drawer fully hidden when closed
R('''def option_list(field, opts):
    cur = flt[field]
    for v, label in opts.items():
        st.button(label, key=f"{field}_{v}", type="primary" if v in cur else "secondary", use_container_width=True, on_click=toggle, args=(field, v))''',
  '''def option_list(field, opts):
    key = f"w_{field}_{st.session_state.hrs_v}"  # new key after Clear, so the pills reset

    def sync():
        st.session_state.flt[field] = list(st.session_state[key] or [])

    st.pills(field, list(opts), selection_mode="multi", default=[v for v in flt[field] if v in opts], format_func=opts.get,
             key=key, on_change=sync, label_visibility="collapsed")''')
R('st.session_state.setdefault("open", True)',
  'st.session_state.setdefault("open", True)\n'
  'if not st.session_state.open:  # closed drawer takes no space, so the page uses the full width\n'
  '    st.markdown("<style>section[data-testid=\'stSidebar\']{display:none!important}</style>", unsafe_allow_html=True)')

# ---------------------------------------------------------------- maps: no grey mesh, Street / Dark only
R('BASES = ["Bus roads only (offline)", "Street map (online)"]', 'BASES = ["Street", "Dark"]')
R('base = st.session_state.get("base", BASES[0])', 'base = st.session_state.get("base") or BASES[0]')
R('map_style=None if base == BASES[0] else "light"', 'map_style="dark" if base == "Dark" else "road"')
RX(r"def\s+net_layer\s*\(\s*\)\s*:", "def net_layer():\n    return []  # grey road mesh removed: it covered the data and slowed loading\n\n\ndef _net_layer_unused():")
RX(r"(\w+),\s*c_chips\s*=\s*st\.columns\(\s*\[[^\]]*\]", lambda m: f"{m.group(1)}, c_chips, c_base = st.columns([5, 5, 2.4]")
R("chip_slot = c_chips.empty()", 'chip_slot = c_chips.empty()\nc_base.segmented_control("Map style", BASES, default=BASES[0], key="base", label_visibility="collapsed")')
R('c = st.columns([1.5, 1.4, 1.7, 1.4, 1.5], vertical_alignment="bottom")', 'c = st.columns([1.5, 1.4, 1.7, 1.4], vertical_alignment="bottom")')
RX(r'[ \t]*st\.session_state\["base"\]\s*=\s*c\[4\]\.selectbox\([^\n]*\)\n', "")

# ---------------------------------------------------------------- chart colours (blue = normal, red only for real delay)
RX(r'color=BAD,\s*width=3,\s*shape="spline"', 'color=INFO, width=3, shape="spline"', must=False)
RX(r'line=dict\(color=BAD,\s*width=2\)', 'line=dict(color=INFO, width=2)', must=False)
RX(r'marker_color="#D9E1E9",\s*name="Visits"', 'marker_color="#DCE3EB", name="Visits"', must=False)

# ---------------------------------------------------------------- Overview: bento layout, one explanatory line per chart
NEW_OV = r'''    head("Overview", "How the network is running in your selection: what is served, how late it is, and where buses bunch up.")
    n_types = lines_df[lines_df.route_short_name.isin(names)].route_type_name.nunique()
    gd, gb, go_ = grade(k.mean_d, 60, 180), grade(h.bunched, 8, 15), grade(k.otp, 85, 70, False)
    hq = hr[hr.n >= MIN_N]

    c_hero, c_kpi = st.columns([5, 7])
    with c_hero:
        with box("On-time performance", "Each bar is the share of stop visits on time in that hour; red hours let riders down."):
            st.markdown(f'<div class="hero"><span class="big">{pct(k.otp)}</span><span class="chip {go_}">{pct(k.late)} late</span></div>'
                        f'<div class="ts">of {num(k.n)} stop visits arrived on time</div>', unsafe_allow_html=True)
            f = go.Figure(go.Bar(x=hq.local_hour, y=hq.otp, marker_color=[tone(v, OTP_STOPS) for v in hq.otp],
                                 hovertemplate="%{x}:00, %{y:.0f}% on time<extra></extra>"))
            f.update_layout(xaxis=dict(dtick=2, title=""), yaxis=dict(title="% on time", range=[0, 100]), bargap=.2)
            show(f, 250)
    with c_kpi:
        grid([
            tile("Stops", num(k.stops), f"{num(k.plats)} platforms", sp=spark(hr.stops, INFO), cap="Active by hour", cls="bx"),
            tile("Trips", num(tr.trips), f"{pct(tr.bunched_trips)} with bunching", sp=spark(trh.trips, INFO), cap="By start hour", cls="bx"),
            tile("Routes", num(k.routes), f"{n_types} bus type{'s' if n_types != 1 else ''}", sp=spark(hr.routes, INFO), cap="Running by hour", cls="bx"),
            tile("Mean delay", "–" if isna(k.mean_d) else unit(f"{k.mean_d / 60:.1f}", "min"), "Average lateness", tn=gd, chip=pct(k.late) + " late",
                 sp=spark(hr.mean_d, TONE[gd]), cap="By hour", cls="bx"),
            tile("Bunched", pct(h.bunched), f"{pct(h.gap)} large gaps", tn=gb, sp=spark(bh.bunched, TONE[gb]), cap="By hour", cls="bx"),
            tile("Extra wait", mins(h.ewt), "Beyond the timetable", cls="bx"),
            tile("Average speed", kmh(sp.kmh), f"{num(sp.runs)} bus runs", sp=spark(sph.kmh, TEAL), cap="By hour", cls="bx"),
            tile("Bus-hours lost", "–" if isna(sp.lost_h) else f"{sp.lost_h:,.0f} h", "Versus free-flowing traffic", cls="bx"),
        ], 4)

    ins = []
    if not hq.empty:
        w, b = hq.loc[hq.mean_d.idxmax()], hq.loc[hq.otp.idxmax()]
        ins.append(("Worst hour", hh(w.local_hour), f"Buses average {mins(w.mean_d)} late. Timetables hold best at {hh(b.local_hour)} ({pct(b.otp)} on time)."))
    if len(wd) > 1:
        w = wd.loc[wd.mean_d.idxmax()]
        ins.append(("Hardest day", DAYS[int(w.weekday)], f"{mins(w.mean_d)} mean delay and {pct(w.otp)} on time."))
    if not rr.empty:
        w, b = rr.loc[rr.otp.idxmin()], rr.loc[rr.otp.idxmax()]
        ins.append(("Route gap", f"{b.otp - w.otp:.0f} points", f"Route {b.r} is the most punctual ({pct(b.otp)}), route {w.r} the least ({pct(w.otp)})."))
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
        st.markdown('<div class="sec">What stands out</div>', unsafe_allow_html=True)
        grid([tile(a, b, c, cls="ins") for a, b, c in ins], 3)

    l, r = st.columns([3, 2])
    with l:
        with box("Delay through the day", "The line is how late buses ran each hour; the bars show how many stop visits it is based on."):
            hour_chart(hq, 260)
    with r:
        with box("Average speed by hour", "Lower speed means buses were stuck in traffic at that time of day."):
            f = go.Figure(go.Scatter(x=sph.local_hour, y=sph.kmh, mode="lines", fill="tozeroy", fillcolor="rgba(85,166,160,.12)",
                                     line=dict(color=TEAL, width=3, shape="spline", smoothing=.5), hovertemplate="%{x}:00, %{y:.1f} km/h<extra></extra>"))
            f.update_layout(xaxis=dict(dtick=2, title=""), yaxis=dict(title="km/h", rangemode="tozero"))
            show(f, 260)
    c1, c2, c3 = st.columns(3)
    with c1:
        with box("Bunching through the day", "Bunched means buses arrive together; a large gap means a long wait."):
            bq = bh[bh.nh >= MIN_N]
            f = go.Figure()
            f.add_scatter(x=bq.local_hour, y=bq.bunched, name="Bunched", mode="lines", fill="tozeroy", fillcolor="rgba(52,120,200,.12)",
                          line=dict(color=INFO, width=3, shape="spline", smoothing=.5), hovertemplate="%{x}:00, %{y:.1f}% bunched<extra></extra>")
            f.add_scatter(x=bq.local_hour, y=bq.gap, name="Large gap", mode="lines", line=dict(color=WARN, width=3, shape="spline", smoothing=.5),
                          hovertemplate="%{x}:00, %{y:.1f}% large gaps<extra></extra>")
            f.update_layout(xaxis=dict(dtick=2, title=""), yaxis=dict(title="% of bus pairs"), legend=dict(orientation="h", y=1.14, x=0))
            show(f, 260)
    with c2:
        with box("Delay by weekday", "Shows which day of the week buses run latest, in average minutes."):
            if len(wd) > 1:
                f = go.Figure(go.Bar(x=[DAYS[int(x)][:3] for x in wd.weekday], y=wd.mean_d / 60, marker_color=[tone(v, DELAY_STOPS) for v in wd.mean_d],
                                     text=[f"{v / 60:.1f}" for v in wd.mean_d], textposition="outside", cliponaxis=False, hovertemplate="%{x}: %{y:.1f} min<extra></extra>"))
                f.update_layout(yaxis=dict(title="Minutes late", rangemode="tozero"), bargap=.35)
                show(f, 260)
            else:
                st.caption("Select more than one day to compare weekdays.")
    with c3:
        with box("On time by bus type", "Shows which kind of bus keeps to the timetable best."):
            bt = rr.merge(lines_df.drop_duplicates("route_short_name").rename(columns={"route_short_name": "r"}), on="r").assign(w=lambda x: x.otp * x.n)
            bt = bt.groupby("route_type_name").agg(w=("w", "sum"), n=("n", "sum")).reset_index()
            bt["otp"] = bt.w / bt.n
            hbar(bt, "route_type_name", "otp", INFO, fmt=".0f", suffix="%")
    l, r = st.columns(2)
    with l:
        with box("Most delayed routes", "The routes that run latest on average, so delays pile up here."):
            t = rr.nlargest(8, "mean_d").assign(min=lambda x: x.mean_d / 60)
            t["r"] = "Route " + t.r.astype(str)
            hbar(t, "r", "min", BAD)
    with r:
        with box("Most delayed stops", "The stops where buses are latest, which affects every route serving them."):
            hbar(ss.assign(min=ss.mean_d / 60), "stop", "min", BAD)


'''
if "def page_stops" not in s or '    head("Overview"' not in s:
    missing.append("Overview block")
if missing:
    sys.exit("Nothing written. These anchors were not found in your file:\n - " + "\n - ".join(missing))
a, b = s.index('    head("Overview"'), s.index("def page_stops")
s = s[:a] + NEW_OV + s[b:]

out = src.with_name("app_v4.py")
out.write_text(s)
print(f"Wrote {out}")