# BusPulse dashboard

Streamlit app on the gold layer: punctuality, delays, speeds and regularity of Kolumbus buses, with
road-level heatmaps on the Stavanger bus network.

```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py -- --gold tables/parquet_2026-10-24/gold-layer-parquet
```

This starts the app at http://localhost:8501. `--gold` is the folder `analysis/gold/build_gold.ipynb`
writes to (that path is also the default). `--map` defaults to `dashboard/map`.

## Pages

| Page | What it shows |
|---|---|
| Overview | Stop visits, % on time / late, mean, median and 90th-percentile delay, trips, average speed vs timetable, bus-hours lost, % of trips not late at their last stop; delay and % on time by hour; delay distribution |
| Road map | Every stop-to-stop road segment coloured by **speed**, **delay gained per bus** or **number of buses**; stops coloured by mean delay or % on time, or a heatmap of late visits; table of the slowest segments |
| Routes | Per route: visits, trips, % on time, mean and 90th-percentile delay, speed, bus-hours lost, % not late at the end, delay gained per trip; route × hour heatmap; how delay builds up along one route |
| Stops | Most delayed and busiest stops; stop × hour heatmap of the 25 busiest stops |
| Time patterns | Date × hour heatmaps, 15-minute delay profile, weekday × hour (when the data covers more than one weekday) |
| Reliability | Bunching, large gaps and excess waiting time, by hour and by route |
| Data | Visits per service date, the gold build checks and metadata |

The sidebar filters everything: service dates, hours, route types, routes, direction, the on-time window and
the minimum number of observations for a cell or segment to be shown.

## The road map: a static file built from GTFS

Map tile services need internet and often an API key, and they load slowly. The bus network itself is already
in our data: `gtfs_shapes` holds the exact path of every bus trip, a point roughly every 10 m, with the distance
driven so far. `build_map.py` cuts those paths at the stops, giving one piece of road per pair of consecutive
stops. That is the same key the gold table `gold_segment_hour` uses, so every segment can be coloured by its
own measurements.

```bash
python dashboard/build_map.py --gtfs <folder with the bronze gtfs_*.parquet files> --out dashboard/map
```

- Takes about 3 s and writes `road_segments.parquet` (1.2 MB, ~6,000 segments), `stops.parquet`, and
  `bus_roads.geojson` (the same roads, for QGIS or any other GIS tool).
- Ferries are left out.
- On the 30 Oct GTFS, the drawn length agrees with the GTFS distance to within 0.4 % (median 0.996).
- Rebuild it when the GTFS changes. Segments missing from the map are simply not drawn.

The default background is **Bus roads only (offline)**: deck.gl draws our own layers and nothing is fetched from
the internet. **Street map (online)** adds a CARTO basemap underneath; your browser loads it, so it needs internet.

## Definitions

- **Visit**: an observed bus stop call with an actual time (gold `valid`).
- **On time**: from 60 s early to 5 min late (gold rule); adjustable in the sidebar. Note: the silver
  `on_time` flag uses −60 to +180 s.
- **Speed**: GTFS road length divided by the actual stop-to-stop time, including time at the stop. Kolumbus
  reports a single time per stop (actual arrival = actual departure in 100 % of visits), so driving and dwell
  cannot be separated.
- **Delay gained / bus-hours lost**: actual minus scheduled time over road segments. The timetable has
  one-minute resolution, so a single short segment can be off by up to a minute. Sums over a route or the
  network are reliable.
- **Bunched / large gap**: two consecutive buses of a route at a stop arrive less than 0.5× or more than 1.5×
  the scheduled gap apart. Only scheduled gaps of 2–60 minutes are counted.
- **Excess waiting time**: `sum(actual headway²)/(2·sum(actual)) − sum(scheduled²)/(2·sum(scheduled))`. This is
  how many seconds longer a passenger who arrives at a random time waits, because the buses are irregular.
- Weekdays come from the service date. A Friday trip after midnight still counts as Friday.

## Performance

The app opens the gold parquet files with DuckDB (no copy, no database server) and caches every query result.
On a full service day (153k visits) the first page renders in about 3 s and filter changes are near-instant.
The road geometry is read once per session.
