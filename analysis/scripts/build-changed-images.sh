#!/usr/bin/env bash
# Build only the Docker image(s) affected by tracked/untracked source changes.
# Usage: bash analysis/scripts/build-changed-images.sh [BASE_REF|--all]
set -euo pipefail
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
compose=(docker compose -f analysis/compose.yaml)

if [[ "${1:-}" == "--all" ]]; then
  changed="analysis/bronze-layer/Dockerfile
analysis/bronze-layer/requirements.txt
analysis/bronze-layer/build_bronze_tables.py
analysis/bronze-layer/run_bronze_range.py
analysis/silver-layer/Dockerfile
analysis/silver-layer/requirements.txt
analysis/silver-layer/spark_jobs/common.py
analysis/silver-layer/spark_jobs/run_silver_tables.py
analysis/silver-layer/spark_jobs/silver.py
analysis/silver-layer/spark_jobs/stop_analysis.py
analysis/silver-layer/spark_jobs/stop_geometry.py
analysis/gold/Dockerfile
analysis/gold/requirements.txt
analysis/gold/build_gold.py
analysis/gold/run_gold.py"
elif [[ -n "${1:-}" ]]; then
  base="$1"
  changed="$(git diff --name-only "$base"...HEAD)"
  changed+=$'\n'"$(git diff --name-only --cached)"
  changed+=$'\n'"$(git diff --name-only)"
  changed+=$'\n'"$(git ls-files --others --exclude-standard)"
else
  changed="$(git diff --name-only HEAD)"
  changed+=$'\n'"$(git diff --name-only --cached)"
  changed+=$'\n'"$(git ls-files --others --exclude-standard)"
fi

build=()
while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  case "$file" in
    analysis/bronze-layer/Dockerfile|analysis/bronze-layer/requirements.txt|analysis/bronze-layer/build_bronze_tables.py|analysis/bronze-layer/run_bronze_range.py)
      build+=(bronze) ;;
    analysis/silver-layer/Dockerfile|analysis/silver-layer/requirements.txt|analysis/silver-layer/spark_jobs/common.py|analysis/silver-layer/spark_jobs/run_silver_tables.py|analysis/silver-layer/spark_jobs/silver.py|analysis/silver-layer/spark_jobs/stop_analysis.py|analysis/silver-layer/spark_jobs/stop_geometry.py)
      build+=(silver) ;;
    analysis/gold/Dockerfile|analysis/gold/requirements.txt|analysis/gold/build_gold.py|analysis/gold/run_gold.py)
      build+=(gold) ;;
  esac
done <<< "$changed"

if ((${#build[@]} == 0)); then
  echo "No production Docker inputs changed; no images need rebuilding."
  exit 0
fi
mapfile -t unique_builds < <(printf '%s\n' "${build[@]}" | sort -u)
printf 'Building changed layer images only: %s\n' "${unique_builds[*]}"
for service in "${unique_builds[@]}"; do
  "${compose[@]}" build "$service"
done
