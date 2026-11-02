#!/bin/bash
# Build bronze + silver for every finished day that has no silver yet.
#   bash tables/run_days.sh                         from 2026-10-20 to yesterday (UTC)
#   bash tables/run_days.sh 2026-10-20 2026-10-20   only that one day
set -u
cd "$HOME/buspulse"
set -a; . "$HOME/openstack.env"; set +a

PY="$HOME/venvs/bronze/bin/python -u"
SWIFT="$HOME/venvs/bronze/bin/swift"
START="${1:-2026-10-20}"
END="${2:-$(date -u -d yesterday +%F)}"

d="$START"
while [[ ! "$d" > "$END" ]]; do
  if $SWIFT list buspulse-silver --prefix "stop_visits/date=$d/" 2>/dev/null | grep -q .; then
    echo "=== $d: already in buspulse-silver, skipping"
  else
    echo "=== $d: bronze ($(date -u +%H:%M) UTC)"
    if $PY tables/build_bronze_tables.py --date "$d" && \
       echo "=== $d: silver ($(date -u +%H:%M) UTC)" && \
       $PY tables/build_silver_tables.py --date "$d"; then
      echo "=== $d: done ($(date -u +%H:%M) UTC)"
    else
      echo "=== $d: FAILED, continuing with the next day"
    fi
  fi
  d=$(date -u -d "$d + 1 day" +%F)
done
echo "=== all days processed"
