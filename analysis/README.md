# BusPulse analysis pipeline

The production data flow is deliberately simple and local after Bronze:

```text
OpenStack Swift (buspulse-raw)
    │
    ▼
Bronze: analysis/bronze-layer/build_bronze_tables.py
    │  tables/bronze-layer/YYYY-MM-DD/<table>.parquet
    ▼
Silver: Spark cleaning + deduplication + stop/GPS analysis
    │  tables/silver-layer/YYYY-MM-DD/<table>.parquet
    ▼
Gold: DuckDB aggregation and analytics
       tables/gold-layer/<gold-table>.parquet + gold.duckdb
```

Swift is used only by Bronze to download the raw collector objects. Silver reads the local
Bronze folders and Gold reads the local Silver folders. There is no Silver or Gold upload back
to Swift in the active pipeline.

## Run one layer independently

Run commands from the repository root (the directory containing `analysis/` and `tables/`).
Create the shared output folders once:

```bash
mkdir -p tables/bronze-layer tables/silver-layer tables/gold-layer
```

Put OpenStack credentials in `analysis/.env` (copy `analysis/.env.example`) or export them in your shell;
do not commit secrets. The Compose file forwards standard `OS_*` variables to Bronze. For an
OpenStack shell file with `export` statements, run `set -a; source openstack.env; set +a` first.

```bash
# Build only a selected image
docker compose -f analysis/compose.yaml build bronze
docker compose -f analysis/compose.yaml build silver
docker compose -f analysis/compose.yaml build gold

# Run only the selected stage (example dates)
docker compose --env-file analysis/.env -f analysis/compose.yaml run --rm bronze --date 2026-11-05 --strict
docker compose --env-file analysis/.env -f analysis/compose.yaml run --rm bronze --date 2026-11-06 --strict
docker compose --env-file analysis/.env -f analysis/compose.yaml run --rm silver --start 2026-11-05 --end 2026-11-05
docker compose --env-file analysis/.env -f analysis/compose.yaml run --rm gold --start 2026-11-05 --end 2026-11-05

# Optional: process multiple Bronze dates with bounded parallelism and audit CSVs
docker compose --env-file analysis/.env -f analysis/compose.yaml run --rm --entrypoint python bronze \
  /app/run_bronze_range.py --start 2026-10-20 --end 2026-11-06 --workers 2 \
  --log-dir /data/tables/bronze-layer/_audit/range-runs
```

Each service is placed behind its own Compose profile, so a plain `docker compose up` does not
start all three stages by accident. There are no `depends_on` links: you decide when each
stage runs and can rerun any stage on its own. A later stage does not implicitly start an
earlier stage. Docker builds use `analysis/` as the context, and `.dockerignore` excludes the
archive, notebooks and caches; the potentially multi-GB `tables/` data folder is not sent to builds.

## Layer behaviour

**Bronze** downloads objects from `RAW_CONTAINER` (default `buspulse-raw`), parses them into
Parquet, and writes date folders under `tables/bronze-layer`. It also writes per-file CSV audit
records under `tables/bronze-layer/_audit/<date>/`. Use `--strict` to make failed raw files
return exit code 1.

**Silver** reads `stop_visits` for service date D and D+1 because late trips can receive final
times after midnight. Therefore Bronze must contain both days before Silver can finish D.
It uses GTFS snapshots from the nearest available Bronze date, removes ferry services, dedupes
snapshots, calculates visit/delay fields, compares against scheduled calls, and performs the
stop/GPS analysis. Completed outputs and `_done_clean` / `_done_stop_analysis` markers are
written under `tables/silver-layer/<date>/`. The runner fingerprints the Silver code and pinned
requirements; a code/dependency change automatically rebuilds the affected dates instead of
trusting stale completion markers.

**Gold** includes only Silver date folders that contain both completion markers. It merges each
Silver table across the selected dates, runs `build_gold.py`, and writes Gold Parquet tables plus
`gold.duckdb` under `tables/gold-layer/`. Re-running with unchanged Silver inputs and unchanged
Gold code is skipped; a Gold code/dependency change invalidates the saved build signature. Pass
`--redo` to force a rebuild.

## Transformations, formulas, and data quality

The rules below describe the active Python/Spark/DuckDB pipeline, not the retired pipeline in
`archive/`. Thresholds are project choices for this analysis; they are not external service
standards. The recorded data-quality results are snapshots and will change when new dates are
processed. See the [Bronze audit notebook](../tables/bronze-layer/_audit/audit.ipynb), its
[date-range audit CSV](../tables/bronze-layer/_audit/download_audit_2026-10-21_to_2026-11-13.csv),
the [most recent range-run summary](../tables/bronze-layer/_audit/range-runs/summary.csv), and
the generated `gold_data_quality` / `gold_meta` tables for the underlying checks.

### Bronze: parse and preserve

The [Bronze builder](bronze-layer/build_bronze_tables.py) downloads raw collector objects,
decompresses and parses them into tables with explicit Arrow schemas. Each row receives
`snapshot_time` from the timestamp in the source filename and `source_file` for traceability.
Feed timestamps and most source values are retained as provided; Bronze does not perform the
business cleaning used by Silver.

Each raw file is all-or-nothing: a parse or schema-conversion failure contributes no rows from
that file. Its per-file audit status is:

- `ok`: parsed and produced rows;
- `empty`: parsed successfully but produced no rows;
- `failed`: download, parse, or Arrow conversion failed (with stage and error).

The builder compares the Parquet row count with the total rows reported by non-failed file
parses and prints a verification mismatch if the counts differ. This checks for write/aggregation
mismatches, but does not prove that the source feed itself was complete or semantically correct.

The audit notebook covers 2026-10-21 through 2026-11-13 (24 days) and checks expected collection
cadence for two feeds. Its expected count is `86,400 seconds / feed interval` per day, using
60-second intervals for estimated timetables and 30-second intervals for vehicle positions.
Daily shortfall and excess are `max(0, expected - ok)` and `max(0, ok - expected)`, respectively.

| Feed | Expected objects | `ok` | Empty | Failed | Daily cadence shortfall | Daily excess |
|---|---:|---:|---:|---:|---:|---:|
| Estimated timetable | 34,560 | 34,391 (99.51%) | 0 | 0 | 169 | 0 |
| Vehicle positions | 69,120 | 68,973 (99.79%) | 5 | 0 | 157 | 10 |

The five vehicle-position files with no rows have status `empty`, not `failed`; the audit
notebook's `failed` column counts every non-`ok` status, so it reports these five as failures.
There were no non-empty error fields in this audit. The cadence figures are expected-file
comparisons, not a row-level completeness or timestamp-gap test. A separate range run for
2026-11-14 reported zero failed files for estimated timetables, vehicle positions, service
alerts, and static timetables.

### Silver: clean visits and derive operational measures

The [Silver cleaning job](silver-layer/spark_jobs/silver.py) processes service date `D` using
real-time stop visits from `D` and `D+1`, so late trips can receive final observations after
midnight. It selects the nearest available GTFS snapshot (within the configured seven-day search)
for the schedule and stop reference data.

Cleaning and matching rules:

1. Retain the requested `data_frame_ref`; remove records without the visit key
   (`data_frame_ref`, `journey_ref`, `stop_order`) or `stop_ref`.
2. Collapse snapshots to one row per visit. Non-time fields come from the newest snapshot;
   each actual arrival/departure time comes from the newest snapshot where that field is present.
3. Exclude ferries using the feed's `vehicle_mode` and GTFS route type. Keep bus route types
   `3` and `700-799`, and keep only visits whose stop matches GTFS.
4. Classify a visit as `cancelled`, `observed` (an actual arrival or departure exists), or
   `unconfirmed`. `clean_stop_visits` contains observed calls; scheduled calls and their match
   outcomes are retained separately in `stop_event_status`.
5. Parse event timestamps as UTC instants. Derive local date/hour, weekday, and period using
   `Europe/Oslo`; UTC timestamps remain available.

Core calculations:

| Measure | Formula / rule |
|---|---|
| Event time | Actual departure when present, otherwise actual arrival |
| Aimed time | Corresponding aimed departure/arrival |
| Delay | `delay_s = event_time - aimed_time`, in seconds |
| Delay outlier | `delay_s < -1,800` or `delay_s > 10,800` (more than 30 min early or 3 h late) |
| Delay class | Early if `< -60 s`; on time if `-60 <= delay_s < 300 s`; late if `>= 300 s` |
| Delay change | Current delay minus previous-stop delay, only for consecutive stop orders and non-outlier visits |
| Headway | Current actual visit time minus previous actual visit at the same line/direction/stop |
| Scheduled headway | Current aimed time minus previous aimed time |
| Headway ratio | `actual headway / scheduled headway`; bunching `< 0.25`, large gap `> 2.0` |
| Segment speed | Haversine distance between consecutive stops divided by elapsed seconds, multiplied by `3.6` for km/h |

Headways exclude outlier visits and require a positive scheduled interval. Silver marks segment
runs valid only when elapsed time is positive, no more than 3,600 seconds, and both endpoint
visits are not delay outliers.

The [stop/GPS analysis](silver-layer/spark_jobs/stop_analysis.py) matches observed calls to GPS
tracks and GTFS route geometry. It derives speeds from consecutive pings at least five seconds
and no more than 90 seconds apart. A vehicle's local cruise speed is the 75th percentile over
nearby step speeds (11 neighboring steps). Vehicle assignment requires at least three matched
stops and a median distance no greater than 100 m. The stop test examines pings within 60 seconds
of a recorded call and within 30 m of the stop: a ping is slow at no more than 2 m/s or 75% of
local cruise speed. Two or more slow pings mean `stopped_confirmed`, one means `likely_stopped`,
and nearby pings at 90% or more of local cruise speed indicate `likely_passed`; other nearby
evidence is `near_unclear`. Missing vehicle/GPS matches remain explicit statuses. Route evidence
uses the GTFS shape when available, otherwise a stop-to-stop line. Its route snapping and
off-route tolerances are configured in `silver-layer/spark_jobs/stop_geometry.py`.

The `p_excess` measure is the smoothed empirical upper-tail rank of a call's time loss among
stop-free intervals:
`(number of null intervals with loss >= call loss + 1) / (number of null intervals + 1)`.

Quality evidence is written per date: `removal_log` counts rows before/after each cleaning rule;
`stop_event_status` distinguishes observed, cancelled, unconfirmed, and unmatched scheduled
calls; GPS outputs retain route/GPS coverage statuses. A date is complete only after both
`_done_clean` and `_done_stop_analysis` markers exist. The current local Silver output has both
markers for 25 dates, 2026-10-20 through 2026-11-13.

### Gold: aggregate and reconcile

The [Gold builder](gold/build_gold.py) defines an analysis-valid call as
`is_actual AND NOT delay_outlier AND delay_s IS NOT NULL`. It classifies valid calls with the
same production delay thresholds as Silver (`early_s = -60`, `late_s = 300`) and aggregates
counts, delay sums, variance, histograms, route/stop/date cubes, headways, segments, and
completeness measures.

Additional Gold filters and formulas include:

- Headway metrics use scheduled headways from 120 through 3,600 seconds. The coefficient of
  variation is `standard deviation(actual headway) / mean(actual headway)`. Bunching and large
  gaps use ratios `< 0.25` and `> 2.0`.
- Gold road-segment analysis keeps runs no longer than 1,800 seconds, ignores segments shorter
  than 50 m and speeds above 90 km/h, and ranks roads only with at least 100 runs and 20 weekday
  peak runs. Free-flow speed is the 85th percentile.
- Route reliability is a weighted score:
  `0.40*pct_on_time + 0.25*norm_delay + 0.20*(1-pct_bunched) + 0.15*norm_speed`.
  Delay is inverted min-max normalized (lower delay scores higher), speed is min-max normalized,
  and the score is divided by the weights of components available for that route.
- Daily anomaly metrics use a trailing seven-day history with at least three history days.
  Moderate/high/critical z-score cutoffs are 2.0/2.5/3.5; unusually low speed is treated as an
  anomaly, while high delay, headway variability, and missed-visit percentage are flagged.

Gold's reconciliation checks verify that row-level facts and aggregate tables conserve call,
headway, and segment counts; delay buckets sum to valid calls; early/on-time/late counts sum to
valid calls; aggregate delay sums match row-level sums; and expected/observed trip and stop
counts agree. These checks detect internal inconsistencies, not missing or biased source data.

The recorded Gold build on 2026-11-15 covers service dates 2026-10-20 through 2026-11-13:

- 3,159,750 cleaned visits; 3,158,322 valid for Gold analysis (99.95%).
- Mean valid delay 200.3 seconds; 5.4% early, 42.3% on time, and 52.2% late using the
  `-60/300` second cutoffs.
- Delay arithmetic agreed with `actual_departure - aimed_departure` within two seconds for
  100% of rows in that check; the opposite sign matched 0.4%.
- All 23 recorded reconciliation checks passed (zero failures). The audit also reported
  4,552/4,552 visited stop IDs matched, no unknown local hours, and no Gold stop-map rows
  without coordinates. It counted 2,933,744 usable segment legs.

For interpretation, cleaning/outlier rules and threshold changes affect denominators and
comparability. The production value of `late_s` is 300 seconds in `gold/build_gold.py`; a
notebook edited for threshold experiments may have different source values or stale saved
outputs. Check `gold_meta` (build time, service dates, thresholds, row counts, and check totals)
when reporting a specific run.

## Rebuild only images affected by code changes

Docker does not automatically rebuild an image when source files change. The helper below
checks Git changes and builds only the affected layer image(s):

```bash
# Detect working-tree, staged, and untracked changes against HEAD
bash analysis/scripts/build-changed-images.sh

# Compare committed changes against a base branch/ref
bash analysis/scripts/build-changed-images.sh origin/main

# Explicitly rebuild all three images
bash analysis/scripts/build-changed-images.sh --all
```

Only production inputs are mapped to images: Bronze's Dockerfile, requirements and builder;
Silver's Dockerfile, requirements and runtime Spark modules; and Gold's Dockerfile, requirements
and two runtime Python scripts. Notebook and dashboard edits do not rebuild ETL images.

## Structure

- `bronze-layer/`: Swift raw download, Bronze Parquet builder and date-range audit runner.
- `silver-layer/spark_jobs/`: local-only Spark helpers, Silver cleaning and GPS analysis.
- `gold/`: DuckDB Gold builder, local date-folder runner, and dashboard source.
- `compose.yaml`: three separate one-shot Docker services with an `analysis/`-only build context.
- `.dockerignore`: keeps notebooks, archives and caches out of image build contexts.
- `scripts/`: changed-image build helper.
- `notebooks/`: research/reference notebooks, excluded from all Docker images.
- `archive/`: retired Swift-to-Swift Spark pipeline and old wrappers retained for reference only.

## Tests and dependencies

Silver tests live in `silver-layer/spark_jobs/tests/`. Run them from the repository root after
installing the Silver requirements and Java 17:

```bash
pip install -r analysis/silver-layer/requirements.txt pytest
PYTHONPATH=analysis/silver-layer pytest analysis/silver-layer/spark_jobs/tests -q
```

The historical notebooks and archived pipeline are not part of the runtime image builds.
