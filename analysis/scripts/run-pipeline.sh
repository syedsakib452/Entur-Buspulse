#!/usr/bin/env bash
# Daily pipeline on the VM, using the images GitHub Actions built:
#   1. bronze: download every missing day from SINCE up to yesterday (UTC)
#   2. silver: every bronze day that is ready (silver for day D needs bronze D and D+1)
#   3. gold:   rebuilt from every completed silver day
# Silver and gold keep their own "done" markers under tables/, so nothing is redone.
#
# Usage:   bash analysis/scripts/run-pipeline.sh
# Optional env vars:
#   OWNER (default irtaza0420bm)  SINCE (default 2026-10-20)  TABLES (default <repo>/tables)
#   ENV_FILE (OpenStack login; default: <repo>/openstack.env, then analysis/.env, then ~/openstack.env)
#   SPARK_CORES (default min(nproc,8))  SPARK_MEM (default 12g)  BRONZE_PROCS (4)  BRONZE_DL_THREADS (3)
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OWNER="${OWNER:-syedsakib452}"; OWNER="${OWNER,,}"          # GHCR names must be lowercase
SINCE="${SINCE:-2026-10-20}"
TABLES="${TABLES:-$REPO/tables}"
CORES="$(nproc)"
SPARK_CORES="${SPARK_CORES:-$(( CORES > 8 ? 8 : CORES ))}"
SPARK_MEM="${SPARK_MEM:-12g}"
BRONZE_PROCS="${BRONZE_PROCS:-4}"
BRONZE_DL_THREADS="${BRONZE_DL_THREADS:-3}"
REG="ghcr.io/${OWNER}/entur-buspulse"
ENV_FILE="/home/ubuntu/.config/buspulse/openstack.env"
if [[ -z "${ENV_FILE:-}" ]]; then
  for f in "$REPO/openstack.env" "$REPO/analysis/.env" "$HOME/openstack.env"; do
    [[ -f "$f" ]] && ENV_FILE="$f" && break
  done
fi
[[ -n "${ENV_FILE:-}" && -f "$ENV_FILE" ]] || { echo "No OpenStack env file found. Set ENV_FILE=/path/to/openstack.env"; exit 1; }
set -a; source "$ENV_FILE"; set +a
[[ -n "${OS_AUTH_URL:-}" ]] || { echo "OS_AUTH_URL is empty after reading $ENV_FILE"; exit 1; }
os_args=()
for v in $(compgen -e | grep -E '^OS_[A-Z_]+$|^RAW_CONTAINER$'); do os_args+=(-e "$v"); done

mkdir -p "$TABLES/bronze-layer" "$TABLES/silver-layer" "$TABLES/gold-layer"

# never run two pipelines at once (a long backfill must not overlap the next cron run)
exec 9>"$TABLES/.pipeline.lock"
flock -n 9 || { echo "$(date -u) previous run still going, exiting"; exit 0; }

echo "=== $(date -u) pipeline start | repo=$REPO tables=$TABLES since=$SINCE env=$ENV_FILE ==="
for layer in bronze silver gold; do docker pull -q "$REG-$layer:latest" >/dev/null; done

# containers run as uid 1000: make sure they can write to tables/
docker run --rm -v "$TABLES:/tables" --entrypoint sh "$REG-gold:latest" \
  -c 'touch /tables/.write_test && rm /tables/.write_test' \
  || { echo "Containers cannot write to $TABLES. Fix once with: sudo chown -R 1000:1000 $TABLES"; exit 1; }

rc=0

# ---- 1. BRONZE
echo "--- bronze ---"
docker run --rm --name buspulse-bronze "${os_args[@]}" \
  -e OUT_DIR=/tables/bronze-layer -e PROCS="$BRONZE_PROCS" -e DL_THREADS="$BRONZE_DL_THREADS" \
  -v "$TABLES:/tables" \
  --entrypoint tini "$REG-bronze:latest" \
  -- python /app/run_bronze_range.py \
     --catch-up --since "$SINCE" --workers 1 \
     --log-dir /tables/bronze-layer/_audit/range-runs \
  || { echo "bronze reported failures (days that failed are retried next run)"; rc=1; }

# ---- 2. SILVER (skips finished days; waits for bronze D+1 on its own)
echo "--- silver ---"
docker run --rm --name buspulse-silver \
  --memory=20g --memory-swap=20g \
  -e TABLES_DIR=/tables -e SILVER_LOCAL_DIR=/tables/silver-layer \
  -e DATA_ROOT=/work/runtime -e SPARK_LOCAL_DIR=/work/spark \
  -e SPARK_MASTER="local[$SPARK_CORES]" -e SPARK_DRIVER_MEMORY="$SPARK_MEM" \
  -e SPARK_SHUFFLE_PARTITIONS=64 \
  -v "$TABLES:/tables" -v buspulse-silver-work:/work \
  "$REG-silver:latest" --start "$SINCE" \
  || { echo "silver reported failed days"; rc=1; }

# ---- 3. GOLD (uses only completed silver days; skips if nothing changed)
echo "--- gold ---"

if [ -z "${TABLES:-}" ]; then
    echo "ERROR: TABLES is not set"
    rc=1
elif [ -z "${REG:-}" ]; then
    echo "ERROR: REG is not set"
    rc=1
elif [ -z "${SINCE:-}" ]; then
    echo "ERROR: SINCE is not set"
    rc=1
else
    docker run --rm --name buspulse-gold \
        -e SILVER_DIR=/tables/silver-layer \
        -e GOLD_DIR=/tables/gold-layer \
        -e GOLD_WORK_DIR=/work/gold \
        -v "$TABLES:/tables" \
        -v buspulse-gold-work:/work \
        "$REG-gold:latest" \
        --start "$SINCE" \
        || {
            echo "ERROR: Gold processing failed"
            rc=1
        }
fi

echo "=== $(date -u) pipeline end (exit $rc) ==="
exit "$rc"
