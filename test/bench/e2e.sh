#!/usr/bin/env bash
# End-to-end throughput and latency of golb against a direct-to-backend
# baseline, at several connection counts and response sizes, with golb's CPU
# and memory use. Complements scripts/bench.sh, which covers the policy,
# access-log, reload, ReverseProxy and slow-backend scenarios at one
# connection count.
#
#   test/bench/e2e.sh OUT_DIR
#
# Writes OUT_DIR/results.md (Markdown tables) and one JSON report per
# scenario in OUT_DIR/raw. Everything listens on 127.0.0.1 and only the
# processes started here are stopped, by PID.
#
# Environment (defaults in brackets):
#   CONNS_LIST ["16 64 256"]  BODIES ["1024 10240"]  FIXED_RATE_CONNS [64]
#   DURATION [15s]  WARMUP [3s]  RUNS [3]
#   PROXY_PORT [18080]  ADMIN_PORT [19901]  BACKEND_PORT [19001, and the next 3]
set -euo pipefail

OUT=${1:?usage: test/bench/e2e.sh OUT_DIR}
CONNS_LIST=${CONNS_LIST:-"16 64 256"}
BODIES=${BODIES:-"1024 10240"}
FIXED_RATE_CONNS=${FIXED_RATE_CONNS:-64}
DURATION=${DURATION:-15s}
WARMUP=${WARMUP:-3s}
RUNS=${RUNS:-3}
PROXY_PORT=${PROXY_PORT:-18080}
ADMIN_PORT=${ADMIN_PORT:-19901}
BACKEND_PORT=${BACKEND_PORT:-19001}

START_TIMEOUT=10   # seconds to wait for a process to listen
SAMPLE_EVERY=2     # seconds between memory samples

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
HOST=127.0.0.1
PROXY=$HOST:$PROXY_PORT
BACKENDS=("$HOST:$BACKEND_PORT" "$HOST:$((BACKEND_PORT + 1))" "$HOST:$((BACKEND_PORT + 2))" "$HOST:$((BACKEND_PORT + 3))")
PIDS=()

mkdir -p "$OUT/raw" "$OUT/bin"
BIN=$OUT/bin
RESULTS=$OUT/results.md

log() { echo "e2e: $*" >&2; }

cleanup() {
  local pid
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

listening() { (exec 3<>"/dev/tcp/$HOST/$1") 2>/dev/null; }

start() {
  local name=$1 port=$2 waited=0
  shift 2
  "$@" >"$OUT/raw/$name.out" 2>"$OUT/raw/$name.err" &
  LAST_PID=$!
  PIDS+=("$LAST_PID")
  until listening "$port"; do
    if ! kill -0 "$LAST_PID" 2>/dev/null || [ "$waited" -ge $((START_TIMEOUT * 10)) ]; then
      log "$name did not start listening on port $port"
      exit 1
    fi
    sleep 0.1
    waited=$((waited + 1))
  done
}

stop() {
  kill "$1" 2>/dev/null || true
  wait "$1" 2>/dev/null || true
}

# cpu_seconds PID  prints the CPU time the process has used so far.
cpu_seconds() {
  ps -o cputime= -p "$1" | awk -F: '{ s = 0; for (i = 1; i <= NF; i++) s = s * 60 + $i; printf "%.2f", s }'
}

# sample_rss PID FILE  records the largest resident set size seen, in KiB.
sample_rss() {
  local peak=0 now
  while kill -0 "$1" 2>/dev/null; do
    now=$(ps -o rss= -p "$1" 2>/dev/null | tr -d ' ' || true)
    if [ -n "$now" ] && [ "$now" -gt "$peak" ]; then
      peak=$now
      echo "$peak" >"$2"
    fi
    sleep "$SAMPLE_EVERY"
  done
}

golb_config() {
  {
    echo "admin:"
    echo "  addr: $HOST:$ADMIN_PORT"
    echo "access_log:"
    echo "  enabled: false"
    echo "listeners:"
    echo "  - name: bench"
    echo "    addr: $PROXY"
    echo "    protocol: http"
    echo "    routes:"
    echo "      - path_prefix: /"
    echo "        pool: echo"
    echo "pools:"
    echo "  - name: echo"
    echo "    policy: round_robin"
    echo "    backends:"
    local backend
    for backend in "${BACKENDS[@]}"; do
      echo "      - addr: $backend"
    done
  } >"$OUT/raw/golb.yaml"
}

# load NAME URL CONNS [flags...]  prints one table row and saves the JSON report.
load() {
  local name=$1 url=$2 conns=$3
  shift 3
  "$BIN/golb-bench" load -url "$url" -c "$conns" -d "$DURATION" -warmup "$WARMUP" -runs "$RUNS" \
    -format markdown -label "$name" -save "$OUT/raw/$name.json" "$@"
}

median_rps() { sed -n 's/.*"median_rps":\([0-9.]*\).*/\1/p' "$1"; }

# through_golb NAME CONNS [flags...]  as load, with golb's CPU and memory
# appended to the row. golb is started fresh for every scenario.
through_golb() {
  local name=$1 conns=$2
  shift 2
  start "golb-$name" "$PROXY_PORT" "$BIN/golb" -config "$OUT/raw/golb.yaml"
  local pid=$LAST_PID
  sample_rss "$pid" "$OUT/raw/$name.rss" &
  local sampler=$!
  PIDS+=("$sampler")
  local cpu_before wall_before row cpu_after wall_after
  cpu_before=$(cpu_seconds "$pid")
  wall_before=$(date +%s)
  row=$(load "$name" "http://$PROXY/" "$conns" "$@")
  cpu_after=$(cpu_seconds "$pid")
  wall_after=$(date +%s)
  local cores rss
  cores=$(awk -v a="$cpu_before" -v b="$cpu_after" -v w="$((wall_after - wall_before))" 'BEGIN { printf "%.2f", (b - a) / w }')
  rss=$(awk '{ printf "%.0f", $1 / 1024 }' "$OUT/raw/$name.rss" 2>/dev/null || echo "?")
  stop "$pid"
  stop "$sampler"
  echo "$row $cores | $rss |"
}

header() {
  {
    echo
    echo "### $1"
    echo
    echo "| scenario | offered req/s | ok req/s (median) | ok req/s (min - max) | errors | non-2xx | p50 | p90 | p99 | p99.9 | max | golb CPU (cores) | golb peak RSS (MiB) |"
    echo "|---|---|---|---|---|---|---|---|---|---|---|---|---|"
  } >>"$RESULTS"
}

# ---------------------------------------------------------------------------

for port in "$PROXY_PORT" "$ADMIN_PORT" "$BACKEND_PORT" $((BACKEND_PORT + 1)) $((BACKEND_PORT + 2)) $((BACKEND_PORT + 3)); do
  if listening "$port"; then
    log "port $port is already in use; set PROXY_PORT, ADMIN_PORT or BACKEND_PORT"
    exit 1
  fi
done

log "building into $BIN"
(cd "$ROOT" && go build -o "$BIN/golb-bench" ./cmd/golb-bench && go build -o "$BIN/golb" ./cmd/golb)
golb_config

{
  echo "## End-to-end run $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo "- $(uname -sm), $(getconf _NPROCESSORS_ONLN) logical CPUs, $(go version), GOMAXPROCS unset"
  echo "- commit $(cd "$ROOT" && git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "- $RUNS runs of $DURATION after $WARMUP warm-up per scenario; the median run by req/s is shown"
  echo "- load average at start: $(uptime | sed 's/.*load averages*: //')"
} >"$RESULTS"

backend_list=$(IFS=,; echo "${BACKENDS[*]}")
for body in $BODIES; do
  start "echo-$body" "$BACKEND_PORT" "$BIN/golb-bench" echo -addr "$backend_list" -size "$body"
  echo_pid=$LAST_PID

  header "Closed loop, $body byte responses"
  for conns in $CONNS_LIST; do
    log "body $body, $conns connections: direct"
    echo "$(load "direct-b$body-c$conns" "http://${BACKENDS[0]}/" "$conns") - | - |" >>"$RESULTS"
    log "body $body, $conns connections: golb"
    through_golb "golb-b$body-c$conns" "$conns" >>"$RESULTS"
  done

  echo "- load average after closed loop, $body bytes: $(uptime | sed 's/.*load averages*: //')" >>"$RESULTS"
  rate=$(median_rps "$OUT/raw/golb-b$body-c$FIXED_RATE_CONNS.json" | awk '{ printf "%d", $1 / 2 }')
  if [ -n "$rate" ] && [ "$rate" -gt 0 ]; then
    header "Fixed rate $rate req/s (50% of golb's closed-loop median), $FIXED_RATE_CONNS connections, $body byte responses"
    log "body $body, fixed rate $rate: direct"
    echo "$(load "direct-b$body-rate" "http://${BACKENDS[0]}/" "$FIXED_RATE_CONNS" -rate "$rate") - | - |" >>"$RESULTS"
    log "body $body, fixed rate $rate: golb"
    through_golb "golb-b$body-rate" "$FIXED_RATE_CONNS" -rate "$rate" >>"$RESULTS"
  fi

  stop "$echo_pid"
done

echo "- load average at end: $(uptime | sed 's/.*load averages*: //')" >>"$RESULTS"
log "done: $RESULTS"
