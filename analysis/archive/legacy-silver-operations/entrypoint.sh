#!/usr/bin/env bash
set -euo pipefail

if [ -n "${TABLES_DIR:-}" ]; then
  export DATA_ROOT="${DATA_ROOT:-/work/runtime}"
  export SILVER_LOCAL_DIR="${SILVER_LOCAL_DIR:-$TABLES_DIR/silver}"
  exec bash /app/run_silver_loop.sh "$@"
fi

exec python -m spark_jobs.run_pipeline --stages clean stop_analysis "$@"
