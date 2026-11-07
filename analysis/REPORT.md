# Cleanup report

## What changed

- Standardised the filesystem contract to `tables/bronze-layer/<date>`, `tables/silver-layer/<date>`, and `tables/gold-layer/`.
- Replaced the Spark/Swift Bronze Docker image with the actual `build_bronze_tables.py` program.
- Fixed Silver's stale `bronze` / `silver` path names and made its local completion markers agree with its output folders.
- Replaced the Swift-bucket Gold runner with a local Silver-folder consolidator and Gold-folder publisher.
- Removed Swift client dependencies from Silver and Gold runtime images; only Bronze needs Swift credentials.
- Added three separately buildable and runnable Compose services and a helper that rebuilds only images affected by production-code changes.
- Added per-layer source fingerprints so rebuilt Silver/Gold images do not silently skip outputs created by older transformation code.
- Moved notebooks out of runtime folders and moved the competing historical Swift/Spark implementation into `archive/`.

## Active file inventory (one-line purpose per file)

| File | Purpose |
|---|---|
| `README.md` | Documents the canonical data flow, commands, dependencies and operating rules. |
| `REPORT.md` | Summarises the cleanup and inventories the active files. |
| `compose.yaml` | Defines independent Bronze, Silver and Gold one-shot Docker services with a local-only build context. |
| `.dockerignore` | Excludes notebooks, archives, caches and reports from Docker build contexts. |
| `.env.example` | Safe template for OpenStack credentials and per-layer runtime settings. |
| `.gitignore` | Prevents local credentials, logs and Python cache files from being committed. |
| `bronze-layer/Dockerfile` | Builds the Swift-download/Arrow Bronze image without Spark. |
| `bronze-layer/requirements.txt` | Pins Bronze's Arrow, Swift, Keystone and JSON-parser dependencies. |
| `bronze-layer/build_bronze_tables.py` | Downloads raw Swift files, parses each feed, writes daily Bronze Parquet and per-file audit CSVs. |
| `bronze-layer/run_bronze_range.py` | Runs the standalone Bronze builder across a date range and writes per-date logs plus aggregate audit CSVs. |
| `silver-layer/Dockerfile` | Builds the Java/Spark Silver image from only Silver runtime modules. |
| `silver-layer/requirements.txt` | Pins PySpark, pandas, PyArrow and NumPy for Silver transformations. |
| `silver-layer/spark_jobs/__init__.py` | Marks the Silver Spark jobs as a Python package. |
| `silver-layer/spark_jobs/common.py` | Provides local path mapping, Spark settings, completion markers, shared transforms and streaming Parquet export. |
| `silver-layer/spark_jobs/run_silver_tables.py` | Discovers Bronze dates, checks D+1/GTFS readiness and fingerprints Silver code to invalidate stale results. |
| `silver-layer/spark_jobs/silver.py` | Cleans stop visits, deduplicates snapshots, excludes ferries and builds timetable/stop performance tables. |
| `silver-layer/spark_jobs/stop_analysis.py` | Matches observed calls to GPS pings and produces stop/GPS evidence tables. |
| `silver-layer/spark_jobs/stop_geometry.py` | Contains the per-vehicle/per-route geometry and GPS classification calculations. |
| `silver-layer/spark_jobs/tests/__init__.py` | Marks the Silver test helpers as an importable Python package. |
| `silver-layer/spark_jobs/tests/test_silver_tables.py` | Tests date discovery and the canonical Bronze/Silver path contract. |
| `silver-layer/spark_jobs/tests/sim.py` | Generates deterministic synthetic bus routes and GPS tracks for geometry tests. |
| `silver-layer/spark_jobs/tests/test_stop_geometry.py` | Tests the GPS/geometry helper calculations. |
| `gold/Dockerfile` | Builds the lightweight DuckDB Gold image without Spark or Swift. |
| `gold/requirements.txt` | Pins DuckDB for Gold consolidation and analytics. |
| `gold/run_gold.py` | Selects complete Silver days, fingerprints Gold code/inputs, consolidates tables and publishes outputs locally. |
| `gold/build_gold.py` | Builds Gold dimensions, cubes, heatmap tables, fact tables, metadata and reconciliation checks. |
| `gold/dashboard_app.py` | Serves the Streamlit dashboard from the generated `gold.duckdb`. |
| `gold/config.toml` | Configures the Streamlit dashboard theme and server behaviour. |
| `scripts/build-changed-images.sh` | Detects changed production inputs in Git and builds only the affected image(s). |
| `archive/README.md` | Explains which legacy files were moved out of the production path. |

## Moved out of the runtime path

- `notebooks/bronze/research/`: exploratory key/field notebook for the raw Bronze feeds.
- `notebooks/silver/research/`: Silver Parquet analysis notebook used during development.
- `notebooks/silver/reference/`: notebook reference for the current Silver transformation.
- `notebooks/silver/archive/`: explicitly superseded Silver v1/v5 and per-file exploration notebooks.
- `notebooks/gold/reference/`: Gold build, heatmap and analysis notebooks retained as references.
- `archive/legacy-swift-spark-pipeline/`: retired Spark Bronze implementation, all-in-one Swift pipeline, Kubernetes manifests and legacy tests.
- `archive/legacy-silver-operations/`: old Compose loop, tmux wrapper and entrypoint superseded by the root Compose file.
- `archive/legacy-docker-runner/`: old generic day runner superseded by independent layer services.

## Reference and archived file inventory (one-line purpose per file)

### Retained notebooks (not copied into Docker images)

| File | Purpose |
|---|---|
| `notebooks/bronze/research/finding-keys.ipynb` | Explores source feed fields and candidate keys for Bronze data. |
| `notebooks/silver/research/analyzing_parquet_files_v6.ipynb` | Researches Silver input fields, data quality and transformation decisions. |
| `notebooks/silver/reference/building_silver_layer_v2.ipynb` | Keeps the notebook reference for the newer Silver transformation. |
| `notebooks/silver/archive/building_silver_layer_v1.ipynb` | Preserves the superseded first Silver notebook implementation. |
| `notebooks/silver/archive/analyzing_parquet_files_v5.ipynb` | Preserves an earlier Silver Parquet inspection notebook. |
| `notebooks/silver/archive/analyzing_each_parquet_file.ipynb` | Preserves per-file exploration used during Silver development. |
| `notebooks/gold/reference/build_gold.ipynb` | Keeps the notebook version and development notes for Gold construction. |
| `notebooks/gold/reference/analyze_silver_layer.ipynb` | Explores Silver inputs for Gold modelling. |
| `notebooks/gold/reference/analyze_gold_layer.ipynb` | Inspects Gold outputs and analytical tables. |
| `notebooks/gold/reference/gold_heatmaps.ipynb` | Experiments with heatmap-ready Gold outputs and visualisations. |

### Retired Docker and operations files (archive only)

| File | Purpose |
|---|---|
| `archive/legacy-docker-runner/docker/Dockerfile` | Old shared image for the generic date-range runner. |
| `archive/legacy-docker-runner/docker/requirements.txt` | Dependencies for that retired generic runner. |
| `archive/legacy-docker-runner/docker/run_days.py` | Old Python wrapper for running processing over multiple dates. |
| `archive/legacy-docker-runner/docker/run_days.sh` | Old shell wrapper for the generic date runner. |
| `archive/legacy-silver-operations/compose.yaml` | Previous Silver-only Docker Compose definition. |
| `archive/legacy-silver-operations/entrypoint.sh` | Previous Silver container entrypoint and mode selector. |
| `archive/legacy-silver-operations/run_silver_loop.sh` | Previous repeated Silver-processing loop. |
| `archive/legacy-silver-operations/run_silver_tmux.sh` | Previous tmux launcher for the Silver loop. |
| `archive/legacy-swift-spark-pipeline/run_pipeline.py` | Previous all-in-one Swift/Spark layer orchestrator. |
| `archive/legacy-swift-spark-pipeline/bronze-spark-jobs/bronze.py` | Retired Spark-based Bronze processing implementation. |
| `archive/legacy-swift-spark-pipeline/bronze-spark-jobs/parsers.py` | Parsers used by the retired Spark Bronze job. |
| `archive/legacy-swift-spark-pipeline/spark_pipeline/Dockerfile` | Previous combined Spark pipeline image. |
| `archive/legacy-swift-spark-pipeline/spark_pipeline/README.md` | Notes for the former combined Spark pipeline. |
| `archive/legacy-swift-spark-pipeline/spark_pipeline/k8s-spark/README.md` | Deployment notes for the old Kubernetes Spark jobs. |
| `archive/legacy-swift-spark-pipeline/spark_pipeline/k8s-spark/configmap.yaml` | Runtime settings for the former Kubernetes Spark deployment. |
| `archive/legacy-swift-spark-pipeline/spark_pipeline/k8s-spark/cronjob.yaml` | Old scheduled Kubernetes Spark job definition. |
| `archive/legacy-swift-spark-pipeline/spark_pipeline/k8s-spark/kustomization.yaml` | Kustomize entry point for the retired Spark manifests. |
| `archive/legacy-swift-spark-pipeline/spark_pipeline/k8s-spark/secret.example.yaml` | Example Kubernetes secret format for the old Swift integration. |
| `archive/legacy-swift-spark-pipeline/previous-versions/previous-bronze-Dockerfile` | Snapshot of the previous Bronze image definition. |
| `archive/legacy-swift-spark-pipeline/previous-versions/previous-bronze-requirements.txt` | Snapshot of dependencies for the previous Bronze image. |
| `archive/legacy-swift-spark-pipeline/previous-versions/previous-silver-Dockerfile` | Snapshot of the previous Swift-enabled Silver image. |
| `archive/legacy-swift-spark-pipeline/previous-versions/previous-silver-requirements.txt` | Snapshot of dependencies for the previous Silver image. |
| `archive/legacy-swift-spark-pipeline/previous-versions/previous-silver-common.py` | Snapshot of the former Swift-aware shared storage helpers. |
| `archive/legacy-swift-spark-pipeline/previous-versions/previous-gold-Dockerfile` | Snapshot of the previous Swift-enabled Gold image. |
| `archive/legacy-swift-spark-pipeline/previous-versions/previous-gold-requirements.txt` | Snapshot of dependencies for the previous Gold image. |
| `archive/legacy-swift-spark-pipeline/previous-versions/previous-swift-gold-runner.py` | Snapshot of the old Swift-to-Swift Gold publisher. |

### Retired tests (archive only)

| File | Purpose |
|---|---|
| `archive/legacy-swift-spark-pipeline/tests/fixtures.py` | Fixtures for the retired Spark pipeline tests. |
| `archive/legacy-swift-spark-pipeline/tests/sim.py` | Synthetic data generator used by the old pipeline tests. |
| `archive/legacy-swift-spark-pipeline/tests/test_spark_jobs.py` | Tests for the former combined Spark jobs. |
| `archive/legacy-swift-spark-pipeline/tests/test_swift_storage.py` | Tests for the former Swift storage helpers. |

## Validation notes

- Python syntax, Compose YAML, Dockerfile source paths and changed-image routing were checked.
- The pure GPS/geometry test suite passed: 8 tests.
- The Docker CLI was not available here, so the three images were not built or launched.
- Full ETL was not executed: the inspection environment lacks the container runtime dependencies (PySpark/PyArrow/DuckDB) and your OpenStack Swift credentials.
- Bronze writes local Parquet by design; there are no active Silver/Gold Swift writes.
