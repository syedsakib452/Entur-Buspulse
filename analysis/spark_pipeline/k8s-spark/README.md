# spark-pipeline: raw -> bronze -> silver with PySpark

Replaces the `bronze-silver` CronJob (`tables/`, swiftclient + pyarrow). One pod, Spark in local mode,
reads and writes Swift through its S3 API.

| stage | input | output (Swift) | was |
|---|---|---|---|
| bronze | `buspulse-raw/<feed>/date=D/*` | `buspulse-bronze/<table>/date=D/` (same columns as before) | `tables/build_bronze_tables.py` |
| clean | bronze D and D+1, GTFS | `buspulse-silver/{clean_stop_visits, stop_event_status, expected_per_stop, removal_log}/service_date=D/` | notebook v3 |
| stop_analysis | clean D, vehicle positions D-1..D+1, GTFS | `buspulse-silver/{stop_gps_status, trip_vehicle, time_lost_null, per_stop_evidence}/service_date=D/` | notebook v6 |

Buses only: ferry lines are removed in `clean` (by GTFS route_type and by vehicle_mode).

## Why memory stays flat
- Bronze: raw files are parsed one at a time inside Spark workers; rows stream into Parquet.
- Dedup of the ~9.5 M snapshot rows per day is a Spark aggregation (`max_by`), it spills to `/work`, never a pandas frame.
- Only the needed days (partition pruning) and columns are read.
- The GPS geometry (v6) runs per vehicle and per line (`applyInPandas`), so one group is in memory at a time.
- Every day is written as its own partition; finished days get a marker in `buspulse-silver/_done/<stage>/<day>` and are skipped next time. A failed day is retried on the next run; other days still run.

## Dropped from the notebooks
Charts, the scipy KS test and the printed exploration cells. All numbers, thresholds and output columns are kept.

## Files
- `configmap.yaml`: endpoint, buckets, dates, Spark memory
- `cronjob.yaml`: 04:10 UTC daily, 2-3 GiB memory, 10 GiB spill disk
- `kustomization.yaml`: image name and tag
- `secret.example.yaml`: shape of the `s3-credentials` Secret (create it with kubectl, never commit keys)

The Docker build instructions and the layer-separated source layout are in
[`../README.md`](../README.md). Set the S3 endpoint and credentials before deployment;
the example manifest intentionally contains placeholders.

## Troubleshooting
| log / status | meaning |
|---|---|
| `cannot reach storage` | wrong `S3_ENDPOINT`, wrong keys, or a bucket missing |
| `OOMKilled` in `kubectl describe pod` | lower `SPARK_DRIVER_MEMORY` to `1200m` or raise the memory limit |
| `clean D: waiting for bronze D+1` | normal: a day is cleaned once the next day's raw files exist |
| `FAILED` for one day, exit 1 | that day is retried on the next run; the traceback is above the line |
