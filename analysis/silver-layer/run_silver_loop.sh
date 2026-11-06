#!/usr/bin/env bash
# Runs the silver layer over and over (inside the silver Docker container).
# Each pass builds only the days whose bronze (of the next day) finished SILVER_DELAY_HOURS ago
# and are not built yet, so every day's silver starts about 12 hours after its bronze.
# Each pass is a fresh Python/Spark process, so memory is given back between passes.
set -u
EVERY_MIN="${CHECK_EVERY_MINUTES:-60}"
echo "silver loop: a pass every ${EVERY_MIN} min, silver delay ${SILVER_DELAY_HOURS:-0} h after bronze"
while true; do
  echo "===== silver pass $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  python -m spark_jobs.run_pipeline --stages clean stop_analysis || echo "pass failed (exit $?), retried next pass"
  if [ "${RUN_ONCE:-}" = "1" ]; then exit 0; fi
  sleep $(( EVERY_MIN * 60 ))
done
