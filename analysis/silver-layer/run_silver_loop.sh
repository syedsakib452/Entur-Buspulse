#!/usr/bin/env bash
# Runs silver over the requested date range, retrying dates whose bronze files have not arrived.
set -u
EVERY_MIN="${CHECK_EVERY_MINUTES:-60}"
echo "silver loop: checking every ${EVERY_MIN} min; tables root: ${TABLES_DIR:-tables}"
while true; do
  echo "===== silver pass $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if python -m spark_jobs.run_silver_tables "$@"; then
    status=0
  else
    status=$?
    echo "pass failed (exit ${status}); retrying next pass"
  fi
  if [ "${RUN_ONCE:-}" = "1" ]; then exit "$status"; fi
  sleep $(( EVERY_MIN * 60 ))
done
