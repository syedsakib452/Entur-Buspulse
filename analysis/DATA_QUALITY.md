# BusPulse data quality, cleaning, and formulas

This report documents the active Bronze -> Silver -> Gold pipeline. Thresholds are project
analysis choices, not external service standards. The measured results below describe the
available audit/build snapshot and will change when more dates are processed.

Primary evidence:

- [Bronze audit notebook](../tables/bronze-layer/_audit/audit.ipynb)
- [Bronze daily audit CSV, 2026-10-21 to 2026-11-13](../tables/bronze-layer/_audit/download_audit_2026-10-21_to_2026-11-13.csv)
- [Bronze range-run summary](../tables/bronze-layer/_audit/range-runs/summary.csv)
- [Bronze failed-files list](../tables/bronze-layer/_audit/range-runs/failed_files.csv)
- Gold `gold_data_quality` and `gold_meta` tables under `tables/gold-layer/`

## At a glance

| Layer | What is checked | Recorded result |
|---|---|---|
| Bronze | Per-file parse status, error, row count and expected collection cadence | 2026-10-21 to 2026-11-13: 34,391/34,560 estimated-timetable files and 68,973/69,120 vehicle-position files were `ok`; five additional vehicle-position files were empty and none had status `failed`. |
| Silver | Per-rule row removal, trip/stop schedule matching, completion markers, and GPS coverage/status | 25 service dates, 2026-10-20 to 2026-11-13, have both Silver completion markers. Detailed removal and status counts are stored per date. |
| Gold | Arithmetic and row-count reconciliation across fact, cube, headway, segment, schedule, and stop outputs | Recorded 2026-11-15 run: 23 reconciliation checks passed, zero failed; 3,158,322 of 3,159,750 visits were valid for analysis. |

These are not equivalent quality scores. Bronze measures ingestion outcomes and cadence,
Silver records cleaning/matching decisions, and Gold mainly tests consistency among derived
tables. Passing downstream checks does not prove that the source feed was complete or unbiased.

### Captured dataset totals

The local Bronze snapshot contains 26 dates, 2026-10-20 through 2026-11-14. Its 318 Parquet
files occupy 5,012,718,163 bytes (4.67 GiB). Counting all Bronze Parquet rows gives
403,562,239 rows:

| Bronze table group | Rows |
|---|---:|
| Stop visits | 367,587,856 |
| Vehicle positions | 11,301,865 |
| Service alerts | 20,404 |
| GTFS tables | 24,652,114 |
| **Total** | **403,562,239** |

The GTFS count includes 24 daily copies of each GTFS table; it is not a count of distinct
feed entities. The Gold snapshot covers 25 completed service dates, 2026-10-20 through
2026-11-13. It has 3,159,750 visit rows, 275 route names, 4,552 stop IDs, and 106,601
distinct `(service_date, journey_ref)` trips.

## Bronze: ingestion and raw-table quality

The [Bronze builder](bronze-layer/build_bronze_tables.py) decompresses and parses raw JSON/XML
and GTFS files into tables with explicit Arrow schemas. Rows receive `snapshot_time` from the
source filename and `source_file` for traceability. Most feed values, including timestamp text,
are preserved; business cleaning happens in Silver.

Each raw file is all-or-nothing. The audit status is:

- `ok`: parsed and produced rows;
- `empty`: parsed successfully but produced no rows;
- `failed`: download, parse, or Arrow conversion failed; the audit records the stage and error.

The builder compares the written Parquet row count with the total parsed rows from non-failed
files and prints a mismatch if the counts differ. This is a write-integrity check, not a check
that every source snapshot exists or is semantically correct.

### Expected-cadence audit

The audit notebook covers 24 dates, 2026-10-21 through 2026-11-13. It checks two feeds and
assumes a full UTC day at a fixed cadence:

```text
expected files per day = 86,400 / interval_seconds
daily shortfall        = max(0, expected - ok)
daily excess           = max(0, ok - expected)
```

| Feed | Assumed interval | Expected | `ok` | Empty | Actual `failed` status | Daily shortfall | Daily excess |
|---|---:|---:|---:|---:|---:|---:|---:|
| Estimated timetable | 60 s | 34,560 | 34,391 (99.51%) | 0 | 0 | 169 | 0 |
| Vehicle positions | 30 s | 69,120 | 68,973 (99.79%) | 5 | 0 | 157 | 10 |

The audit notebook's `failed` column is calculated as `attempts - ok`; consequently, its total
of five non-`ok` vehicle-position files represents the five `empty` files, not parse/download
failures. The per-file audit status is the authoritative distinction. No error fields were
non-empty in this 24-day audit. The cadence comparison does not locate timestamp gaps or
validate content within an `ok` file.

The separate 2026-11-14 range run reports zero failed files for estimated timetables, vehicle
positions, service alerts, and static timetables. Its `failed_files.csv` currently contains only
the header row (no recorded failures). The range summary lists 1,440 estimated-timetable files,
2,880 vehicle-position files, 15 service-alert files, and one static-timetable archive expanded
to 10 members as processed.

## Silver: cleaning, schedule matching, and GPS evidence

The [Silver cleaning job](silver-layer/spark_jobs/silver.py) processes service date `D` using
stop-visit snapshots from `D` and `D+1`, allowing late trips to acquire final actual times after
midnight. It selects the nearest GTFS snapshot within the configured seven-day search window.

### Cleaning and matching rules

1. Keep the requested `data_frame_ref`; remove records missing any visit key
   (`data_frame_ref`, `journey_ref`, `stop_order`) or `stop_ref`.
2. Deduplicate to one row per visit. Non-time fields come from the newest snapshot; actual
   arrival/departure fields come from the newest snapshot where each actual value is present.
3. Exclude ferries by feed mode and GTFS route type. Keep bus route types `3` and `700-799`;
   require a stop match in GTFS.
4. Classify calls as `cancelled`, `observed` (actual arrival or departure exists), or
   `unconfirmed`. `clean_stop_visits` contains observed calls; scheduled calls and match outcomes
   are retained in `stop_event_status`.
5. Preserve timestamps as UTC instants and derive local calendar fields using `Europe/Oslo`.

### Silver formulas and thresholds

| Measure | Formula / rule |
|---|---|
| Event time | Actual departure if present, otherwise actual arrival |
| Aimed time | Corresponding aimed departure/arrival |
| Delay | `delay_s = event_time - aimed_time`, seconds |
| Delay outlier | `delay_s < -1,800` or `delay_s > 10,800` |
| Delay class | Early `< -60 s`; on time `-60 <= delay_s < 300 s`; late `>= 300 s` |
| Delay change | Current delay minus the previous stop's delay, only for consecutive stop orders and non-outliers |
| Actual headway | Current actual visit time minus previous actual visit at the same line/direction/stop |
| Scheduled headway | Current aimed time minus previous aimed time |
| Headway ratio | `actual / scheduled`; bunched `< 0.25`, large gap `> 2.0` |
| Segment speed | Haversine distance between consecutive stops / elapsed seconds * `3.6` (km/h) |

Headways exclude outlier visits, require distinct consecutive trips, and require a positive
scheduled gap. Silver marks a segment valid only for positive elapsed time no greater than
3,600 seconds, with neither endpoint marked as a delay outlier.

### GPS-derived quality evidence

The [stop/GPS analysis](silver-layer/spark_jobs/stop_analysis.py) matches calls to vehicles and
GTFS route geometry. Ping speeds use consecutive pings 5-90 seconds apart. Local cruise speed is
the 75th percentile of 11 neighboring step speeds. Vehicle-to-trip assignment requires at least
three matched stops and a median distance no greater than 100 m.

For stop evidence, pings within 60 seconds of a call and 30 m of the stop are considered near.
A near ping is slow at no more than 2 m/s or 75% of local cruise speed. Two or more slow pings
classify a stop as `stopped_confirmed`, one as `likely_stopped`; near pings at 90% or more of
cruise speed support `likely_passed`; other nearby evidence is `near_unclear`. Missing vehicle
or GPS coverage remains an explicit status. Route geometry uses a GTFS shape when available or
a stop-to-stop line otherwise.

The empirical upper-tail score for time lost at a stop is:

```text
p_excess = (count of stop-free intervals with time_loss >= call_time_loss + 1)
           / (count of stop-free intervals + 1)
```

Silver writes `removal_log` counts by rule, schedule comparisons in `stop_event_status`, and
coverage/classification statuses in GPS outputs for each date. There is no single Silver
cross-date removal percentage in the checked artifacts; use the per-date `removal_log` when
reporting how many rows each rule removed. A date is complete only when both `_done_clean` and
`_done_stop_analysis` markers exist. The current local outputs have both markers for 25 dates,
2026-10-20 through 2026-11-13.

#### Recorded Silver row-removal and event-status counts

The following sums cover all 25 completed service-date jobs:

| Removal rule | Rows removed |
|---|---:|
| Keep chosen service dates | 355,680,639 |
| Deduplicate snapshots (keep latest per visit) | 335,042,579 |
| Exclude ferry lines | 4,730 |
| Require stop to exist in GTFS | 20 |
| Drop null key / stop reference | 0 |
| Keep bus route types only (`3`, `700-799`) | 0 |
| **Sum of per-job removal operations** | **690,727,968** |

This is a sum of the per-date `removal_log`, not a count of globally unique source rows:
each service date uses snapshots from D and D+1, so a source record may participate in
multiple date jobs. Do not interpret the sum as a distinct-row total or a percentage of the
raw Bronze rows.

Gold's accumulated schedule-match statuses count 2,963,687 `observed`, 14,709 `cancelled`,
9,276 `unconfirmed`, 57,213 `trip_not_in_feed`, and 44 `stop_not_in_feed` events. These
are event-status records, not the Gold-valid visit denominator.

For GPS stop evidence, the local cruise-speed ratio and near-stop distance are:

```text
rho_i  = min(incoming_speed_i, outgoing_speed_i) / local_cruise_speed_i
         (defined when local_cruise_speed_i >= 3 m/s)
delta_j = Euclidean distance from ping j to the stop in the local projected grid
```

Only pings within 60 seconds of the call are considered; a ping is near when
`delta_j <= 30 m`. Slow evidence is `speed <= 2 m/s` or `rho <= 0.75`; two slow near
pings mean `stopped_confirmed`, one means `likely_stopped`, and near pings with
`rho >= 0.90` can support `likely_passed`. The full classification and caveats are in
[Silver and Gold formulae](formulae/README.md).

## Gold: analytical validity and reconciliation

The [Gold builder](gold/build_gold.py) defines an analysis-valid call as:

```text
valid = is_actual AND NOT delay_outlier AND delay_s IS NOT NULL
```

The production thresholds are `early_s = -60` and `late_s = 300`; therefore early calls are
below -60 seconds, on-time calls are from -60 up to (but not including) 300 seconds, and late
calls are at least 300 seconds late.

Other analytical formulas include:

- Headway metrics use scheduled gaps from 120 through 3,600 seconds. Headway coefficient of
  variation is `standard_deviation(actual_headway) / mean(actual_headway)`. Bunching and large
  gaps use ratios `< 0.25` and `> 2.0`.
- Road-segment analysis accepts runs no longer than 1,800 seconds, excludes segments shorter
  than 50 m and speeds above 90 km/h, and ranks roads only with at least 100 runs and 20 weekday
  peak runs. Free-flow speed is the 85th percentile.
- Route normalization across the comparison period is:
  `minmax(x) = (x - min(x)) / (max(x) - min(x))`,
  `N_d = 1 - minmax(avg_delay_s)` (lower delay is better),
  `N_v = minmax(avg_speed_kmh)`, and
  `N_b = 1 - clip(pct_bunched, 0, 1)`. `pct_on_time` and `pct_bunched` are fractions
  from 0 to 1. `I_b` and `I_v` are 1 when the route has the corresponding bunching/speed
  metric and 0 otherwise. Its score is
  `100*(0.40*pct_on_time + 0.25*N_d + 0.20*I_b*N_b + 0.15*I_v*N_v) /
  (0.65 + 0.20*I_b + 0.15*I_v)`. A zero range in the min-max inputs maps to 1.
- Daily anomaly metrics compare against a trailing seven-day history with at least three history
  days. Moderate/high/critical z-score cutoffs are 2.0/2.5/3.5. High delay, headway variability,
  and missed-visit percentage are flagged on positive z-scores; unusually low speed is flagged
  on negative z-scores.

Gold reconciliation checks verify count conservation between row-level facts and aggregates,
delay-bucket totals, early/on-time/late totals, delay-sum equality, headway and segment counts,
and schedule/stop matching. They check internal consistency; they cannot establish source-feed
completeness.

### Recorded Gold quality results

The saved build dated 2026-11-15 covers service dates 2026-10-20 through 2026-11-13:

- 3,159,750 cleaned visits; 3,158,322 valid for Gold analysis (99.95%). There are 275 routes,
  4,552 stops and 106,601 distinct service-date/trip pairs.
- Mean delay is 200.3 seconds, median 129.3 seconds, and P90 499.4 seconds over valid visits.
  At the `-60/300` second cutoffs, 171,274 (5.42%) were early, 2,276,100 (72.07%) on time,
  and 710,948 (22.51%) late.
- Delay arithmetic agreed with `actual_departure - aimed_departure` within two seconds for
  100% of rows in that check; the opposite sign matched 0.4%.
- All 23 recorded reconciliation checks passed; zero failed.
- 4,552/4,552 visited stop IDs matched; no unknown local hours; no Gold stop-map rows lacked
  coordinates; 2,933,744 usable segment legs.

#### Route OTP and reliability

These rankings use routes with at least 1,000 valid visits; OTP is valid on-time visits divided
by valid visits. Mean delay is in seconds.

| Highest OTP | OTP | Valid visits | Mean delay | Reliability |
|---|---:|---:|---:|---:|
| SK4918 | 94.41% | 1,020 | 55.1 s | 87.68 |
| SK0202 | 92.56% | 3,927 | 107.4 s | 88.81 |
| 207 | 91.83% | 12,707 | 78.9 s | 90.43 |
| SK3509 | 89.93% | 1,073 | 47.8 s | 82.97 |
| 1 | 88.48% | 145,612 | 56.8 s | 84.81 |

| Lowest OTP | OTP | Valid visits | Mean delay | Reliability |
|---|---:|---:|---:|---:|
| X39 | 31.99% | 8,502 | 544.9 s | 62.14 |
| SK0302 | 35.66% | 1,335 | 369.1 s | 53.21 |
| 57 | 46.86% | 2,774 | 338.9 s | 72.48 |
| SK4910 | 47.12% | 5,891 | 511.5 s | 64.46 |
| 58 | 47.85% | 3,515 | 380.5 s | 63.27 |

Reliability leaders by the same 1,000-valid-visit filter were 207 (90.43), 45 (89.44),
151 (88.99), SK0202 (88.81), and SK4918 (87.68). The lowest were SK0302 (53.21), X39
(62.14), 58 (63.27), SK4910 (64.46), and 62 (64.56).

#### Stops with highest mean delay

Sorted by mean delay, with at least 100 valid visits per stop:

| Stop | Valid visits | Mean delay | OTP |
|---|---:|---:|---:|
| Bjerkreim skule | 153 | 875.7 s | 50.98% |
| Fister | 126 | 765.7 s | 33.33% |
| Døvika | 107 | 727.8 s | 26.17% |
| Moi stasjon | 160 | 640.4 s | 33.75% |
| Emmausveien | 324 | 596.2 s | 1.54% |

#### OTP by local hour

Values use `Europe/Oslo` local hour and valid visits. `n` is the denominator; OTP uses the
same `-60/300` second rule.

| Hour | n | OTP | Hour | n | OTP |
|---:|---:|---:|---:|---:|---:|
| 00 | 35,774 | 79.14% | 12 | 175,499 | 77.03% |
| 01 | 17,825 | 76.37% | 13 | 186,463 | 73.16% |
| 02 | 16,499 | 77.27% | 14 | 235,048 | 64.51% |
| 03 | 12,825 | 78.55% | 15 | 237,150 | 55.06% |
| 04 | 9,380 | 85.97% | 16 | 224,630 | 57.45% |
| 05 | 24,527 | 91.85% | 17 | 192,217 | 67.21% |
| 06 | 144,238 | 84.49% | 18 | 155,527 | 69.00% |
| 07 | 229,894 | 71.73% | 19 | 137,940 | 75.33% |
| 08 | 209,714 | 64.49% | 20 | 115,992 | 79.26% |
| 09 | 165,613 | 77.96% | 21 | 102,656 | 82.09% |
| 10 | 165,816 | 80.24% | 22 | 100,873 | 83.51% |
| 11 | 168,053 | 80.33% | 23 | 94,169 | 82.68% |

#### OTP by day of week

| Day | Valid visits | OTP | Mean delay | P90 delay |
|---|---:|---:|---:|---:|
| Monday | 451,868 | 71.97% | 196.8 s | 501.4 s |
| Tuesday | 491,360 | 72.26% | 202.3 s | 504.5 s |
| Wednesday | 602,273 | 70.63% | 208.7 s | 530.3 s |
| Thursday | 601,340 | 70.35% | 212.1 s | 524.3 s |
| Friday | 597,435 | 70.94% | 211.3 s | 513.8 s |
| Saturday | 255,423 | 75.70% | 171.5 s | 425.3 s |
| Sunday | 158,623 | 82.12% | 131.9 s | 310.3 s |

#### Excess wait, tests, author, and dashboard

- Excess wait was **18.75 seconds** across 2,705,009 headways. It is the difference between
  `sum(actual_headway^2)/(2*sum(actual_headway))` and
  `sum(scheduled_headway^2)/(2*sum(scheduled_headway))`; accepted scheduled headways are
  120-3,600 seconds.
- The active analysis test files define 20 tests (8 GPS geometry, 5 Silver runner, and 7
  container/pipeline tests). The [pipeline cleanup report](REPORT.md) records the 8-test
  geometry suite as passed; the complete 20-test set has not been run for this snapshot.
- The repository's latest recorded commit author is **Irtaza0420BM**; the report contains no
  separate authorship metadata.
- No hosted dashboard URL is configured. Streamlit's default local address is
  `http://localhost:8501`, but the current [dashboard app](gold/dashboard_app.py) expects
  `gold_segments_hour`, whereas this Gold build exposes the legacy compatibility view as
  `gold_segment_hour`. The app therefore needs that schema mismatch resolved before this URL
  can be considered a working dashboard.

The saved `gold_heat_weekday_hour.parquet` is a retained legacy output and its on-time/early/late
bucket counts differ from `fact_stop_visits` / `gold_delay_hour`; the hourly and weekday metrics
above were computed directly from valid rows in `fact_stop_visits`. The source of truth for production
thresholds is `gold/build_gold.py`, not an exploratory notebook. Consult `gold_meta` for build
time, service dates, thresholds, row counts, and check totals before quoting results.
The source of truth for production thresholds is `gold/build_gold.py`, not an exploratory
notebook. Notebooks may contain experiments or saved outputs from an earlier run; consult
`gold_meta` for the build time, service dates, thresholds, row counts, and check totals before
quoting results.
