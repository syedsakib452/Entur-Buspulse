#!/usr/bin/env bash
# Starts the local-tables silver container in a tmux session.
#
#   START_DATE=2026-10-30 END_DATE=2026-11-02 ./analysis/silver-layer/run_silver_tmux.sh start
#   ./analysis/silver-layer/run_silver_tmux.sh attach    # watch it (leave again: Ctrl-b then d)
#   ./analysis/silver-layer/run_silver_tmux.sh status    # container state + last log lines
#   START_DATE=2026-10-30 END_DATE=2026-11-02 ./analysis/silver-layer/run_silver_tmux.sh once
#   ./analysis/silver-layer/run_silver_tmux.sh stop      # stop container and session
#
# Silver tables appear in <repo>/tables/silver/<date>/<table>.parquet.
set -euo pipefail
SESSION="${SESSION:-buspulse-silver}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE=(docker compose -f "$REPO/analysis/silver-layer/compose.yaml")

need() { command -v "$1" >/dev/null || { echo "missing: $1 (sudo apt install $2)"; exit 1; }; }

start() {
  local mode="${1:-loop}"
  need tmux tmux; need docker docker.io
  if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "session $SESSION is already running: $0 attach"; exit 0
  fi
  mkdir -p "$REPO/tables/silver" "$REPO/logs"
  # loop: always-on container (restarts itself); once: a single pass in a throw-away container
  local run="${COMPOSE[*]} up --build silver"
  [ "$mode" = "once" ] && run="${COMPOSE[*]} build silver && RUN_ONCE=1 ${COMPOSE[*]} run --rm -e RUN_ONCE=1 silver"
  # date-range and loop settings from the caller are handed to the tmux window
  local pass=""
  for v in START_DATE END_DATE REDO CHECK_EVERY_MINUTES; do
    [ -n "${!v:-}" ] && pass+="export $v='${!v}'; "
  done
  # Keep a log file and leave the shell open when the container stops.
  tmux new-session -d -s "$SESSION" -c "$REPO" \
    "$pass \
     ( $run ) 2>&1 | tee -a '$REPO/logs/silver.log'; \
     echo; echo 'silver stopped. Press Enter to close.'; read"
  echo "started in tmux session '$SESSION'.  watch: $0 attach   log: $REPO/logs/silver.log"
}

case "${1:-}" in
  start)  start loop ;;
  once)   start once ;;
  attach) tmux attach -t "$SESSION" ;;
  status) "${COMPOSE[@]}" ps; tail -n 20 "$REPO/logs/silver.log" 2>/dev/null || true ;;
  stop)   "${COMPOSE[@]}" down || true; tmux kill-session -t "$SESSION" 2>/dev/null || true; echo stopped ;;
  *) sed -n '2,10p' "$0"; exit 1 ;;
esac
