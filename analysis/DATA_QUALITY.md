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
- Route reliability is:
  `0.40*pct_on_time + 0.25*norm_delay + 0.20*(1-pct_bunched) + 0.15*norm_speed`.
  Delay is inverted min-max normalized; speed is min-max normalized. The score is divided by
  the weights of components available for that route.
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

- 3,159,750 cleaned visits; 3,158,322 valid for Gold analysis (99.95%).
- Mean valid delay: 200.3 seconds; 5.4% early, 42.3% on time, and 52.2% late at the `-60/300`
  second cutoffs.
- Delay arithmetic agreed with `actual_departure - aimed_departure` within two seconds for
  100% of rows in that check; the opposite sign matched 0.4%.
- All 23 recorded reconciliation checks passed; zero failed.
- 4,552/4,552 visited stop IDs matched; no unknown local hours; no Gold stop-map rows lacked
  coordinates; 2,933,744 usable segment legs.

The source of truth for production thresholds is `gold/build_gold.py`, not an exploratory
notebook. Notebooks may contain experiments or saved outputs from an earlier run; consult
`gold_meta` for the build time, service dates, thresholds, row counts, and check totals before
quoting results.
