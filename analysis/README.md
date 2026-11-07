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
