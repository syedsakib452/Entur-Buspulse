# Spark pipeline

The Spark pipeline converts raw Entur data to bronze Parquet tables and then builds the
silver datasets. Swift objects are staged on local disk through `python-swiftclient`;
Spark operates on those staged files, then writes completed Parquet outputs back to Swift.
Set `SWIFT_CACHE_DIR` to a disk with enough room for the input partitions and Spark spill.
Its code is kept with the corresponding layers:

- `../bronze-layer/spark_jobs/`: bronze conversion and feed parsers
- `../silver-layer/spark_jobs/`: shared storage helpers, silver jobs, pipeline runner, and tests
- `k8s-spark/`: Kubernetes CronJob and configuration

The `spark_jobs` package is a Python namespace package shared across both layer directories.

## Run locally

Install Java 17, then install the pipeline dependencies:

```sh
python3 -m pip install -r analysis/silver-layer/spark_jobs/requirements.txt pytest
export PYTHONPATH="analysis/bronze-layer:analysis/silver-layer"
```

Run the unit tests:

```sh
python3 -m pytest analysis/silver-layer/spark_jobs/tests -v
```

To run against local folders, set `DATA_ROOT` to a directory containing the raw bucket
folder; the bronze and silver bucket folders are created under it:

```sh
export DATA_ROOT="$PWD/data"
python3 -m spark_jobs.run_pipeline --start YYYY-MM-DD --end YYYY-MM-DD
```

For Swift, unset `DATA_ROOT` and activate the environment containing your OpenStack
`OS_*` credentials (including `OS_AUTH_URL`). The job uses the same credentials as
`python-swiftclient`; no S3 endpoint or Java Swift connector is required. Bucket names
default to `buspulse-raw`, `buspulse-bronze`, and `buspulse-silver`.

To run Spark bronze for a range with up to four dates in parallel and write per-date
logs plus failed-file audit CSVs:

```sh
python analysis/bronze-layer/run_bronze_range.py \
  --start 2026-10-22 --end 2026-11-05 --workers 4 --spark
```

This Spark job writes the table/date partition layout used by the Spark silver readers.
The standalone bronze builder writes a different, date-first layout.

## Build the container

Build from the repository root so the Dockerfile can copy both layer directories:

```sh
docker build -f analysis/spark_pipeline/Dockerfile -t entur-buspulse-spark:latest .
```

The container entrypoint runs `spark_jobs.run_pipeline`. Configure the date range,
stages, storage endpoint, and credentials through environment variables.

## Kubernetes

Update the Keystone auth URL and date range in `k8s-spark/configmap.yaml`, build and publish the
image, and configure `swift-credentials` as a Kubernetes Secret (do not commit credentials).
The sample image is `ghcr.io/syedsakib452/entur-buspulse-spark:latest`; change the tag in
`k8s-spark/kustomization.yaml` when publishing a versioned image. See
[`k8s-spark/README.md`](./k8s-spark/README.md) for deployment notes.
