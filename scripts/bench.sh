#!/usr/bin/env bash
# Runs the golb benchmark matrix and prints a Markdown report on stdout, the
# input for docs/BENCHMARKS.md. Progress goes to stderr.
#
#   scripts/bench.sh > bench-report.md
#
# Everything is started on 127.0.0.1 and stopped again on exit. Proxies run
# one at a time on the same port against the same four echo backends.
#
# Environment (defaults in brackets):
#   CONNS [64] DURATION [15s] WARMUP [3s] RUNS [3] BODY [1024]
#   PROXY_PORT [18080] ADMIN_PORT [19901] BACKEND_PORT [19001, and the next 4]
#   BASELINE_STOCK [0]  1 also runs ReverseProxy with net/http's stock idle
#                       pool of 2 per host. At 64 connections it reconnects on
#                       almost every request, which exhausts the ephemeral
#                       port range within seconds on macOS; expect errors.
#   COMPETITORS [1]     0 skips nginx, haproxy, caddy, traefik and envoy.
#                       Their configs below were written without the binaries
#                       at hand (none is installed on the development
#                       machine), so treat a first run of each as a test of
#                       the config as well.
set -euo pipefail

CONNS=${CONNS:-64}
DURATION=${DURATION:-15s}
WARMUP=${WARMUP:-3s}
RUNS=${RUNS:-3}
BODY=${BODY:-1024}
PROXY_PORT=${PROXY_PORT:-18080}
ADMIN_PORT=${ADMIN_PORT:-19901}
BACKEND_PORT=${BACKEND_PORT:-19001}
BASELINE_STOCK=${BASELINE_STOCK:-0}
COMPETITORS=${COMPETITORS:-1}

SLOW_DELAY=200ms          # the slow backend of the tail-latency scenario
RELOAD_EVERY=1            # seconds between SIGHUPs in the reload scenario
START_TIMEOUT=10          # seconds to wait for a process to listen
DRAIN_TIMEOUT=35          # seconds to wait for closed connections to expire
DRAIN_SLACK=300           # TIME_WAIT sockets tolerated above the starting count

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d)
BIN=$WORK/bin
PIDS=()

HOST=127.0.0.1
PROXY=$HOST:$PROXY_PORT
FAST=("$HOST:$BACKEND_PORT" "$HOST:$((BACKEND_PORT + 1))" "$HOST:$((BACKEND_PORT + 2))" "$HOST:$((BACKEND_PORT + 3))")
SLOW=$HOST:$((BACKEND_PORT + 4))

log() { echo "bench: $*" >&2; }

cleanup() {
  local pid
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

listening() { (exec 3<>"/dev/tcp/$HOST/$1") 2>/dev/null; }

require_free() {
  local port
  for port in "$@"; do
    if listening "$port"; then
      log "port $port is already in use; set PROXY_PORT, ADMIN_PORT or BACKEND_PORT"
      exit 1
    fi
  done
}

# start NAME PORT COMMAND...  runs COMMAND in the background and waits until
# PORT accepts connections. The pid is left in LAST_PID.
start() {
  local name=$1 port=$2 waited=0
  shift 2
  "$@" >"$WORK/$name.out" 2>"$WORK/$name.err" &
  LAST_PID=$!
  PIDS+=("$LAST_PID")
  until listening "$port"; do
    if ! kill -0 "$LAST_PID" 2>/dev/null || [ "$waited" -ge $((START_TIMEOUT * 10)) ]; then
      log "$name did not start listening on port $port:"
      tail -n 5 "$WORK/$name.err" >&2 || true
      return 1
    fi
    sleep 0.1
    waited=$((waited + 1))
  done
}

stop() {
  kill "$1" 2>/dev/null || true
  wait "$1" 2>/dev/null || true
}

time_wait() { netstat -an 2>/dev/null | grep -c TIME_WAIT || true; }

# Connections closed by one scenario stay in TIME_WAIT and hold ephemeral
# ports; wait for them to expire so the next scenario starts clean. The
# count before the first scenario is the machine's own background level.
drain() {
  command -v netstat >/dev/null 2>&1 || return 0
  local waited=0
  while [ "$(time_wait)" -gt $((TIME_WAIT_AT_START + DRAIN_SLACK)) ] && [ "$waited" -lt "$DRAIN_TIMEOUT" ]; do
    sleep 1
    waited=$((waited + 1))
  done
}

# measure LABEL [load flags...]  prints one table row.
measure() {
  local label=$1
  shift
  drain
  log "measuring $label"
  "$BIN/golb-bench" load -url "http://$PROXY/" -c "$CONNS" -d "$DURATION" -warmup "$WARMUP" -runs "$RUNS" \
    -format markdown -label "$label" "$@"
}

not_measured() { echo "| $1 | not measured ($2) | | | | | | | | | |"; }

median_rps() { sed -n 's/.*"median_rps":\([0-9.]*\).*/\1/p' "$1"; }

# half_of FILE  prints half the median successful req/s saved in FILE, or
# nothing when the file is missing or the rate would be zero.
half_of() {
  [ -s "$1" ] || return 0
  median_rps "$1" | awk '{ rate = int($1 / 2); if (rate > 0) print rate }'
}

golb_config() {
  local policy=$1 access_log=$2
  shift 2
  {
    echo "admin:"
    echo "  addr: $HOST:$ADMIN_PORT"
    echo "access_log:"
    echo "  enabled: $access_log"
    echo "listeners:"
    echo "  - name: bench"
    echo "    addr: $PROXY"
    echo "    protocol: http"
    echo "    routes:"
    echo "      - path_prefix: /"
    echo "        pool: echo"
    echo "pools:"
    echo "  - name: echo"
    echo "    policy: $policy"
    echo "    backends:"
    local backend
    for backend in "$@"; do
      echo "      - addr: $backend"
    done
  } >"$WORK/golb.yaml"
}

# golb_scenario LABEL POLICY ACCESS_LOG RELOAD BACKEND... [-- load flags...]
golb_scenario() {
  local label=$1 policy=$2 access_log=$3 reload=$4
  shift 4
  local backends=()
  while [ $# -gt 0 ] && [ "$1" != "--" ]; do
    backends+=("$1")
    shift
  done
  [ $# -gt 0 ] && shift
  if [ ! -x "$BIN/golb" ]; then
    not_measured "$label" "golb did not build"
    return 0
  fi
  golb_config "$policy" "$access_log" "${backends[@]}"
  if ! start golb "$PROXY_PORT" "$BIN/golb" -config "$WORK/golb.yaml"; then
    not_measured "$label" "golb did not start"
    return 0
  fi
  local golb_pid=$LAST_PID reloader=
  if [ "$reload" = reload ]; then
    (while sleep "$RELOAD_EVERY"; do kill -HUP "$golb_pid" 2>/dev/null || exit 0; done) &
    reloader=$!
    PIDS+=("$reloader")
  fi
  measure "$label" "$@" || not_measured "$label" "load generator failed"
  [ -n "$reloader" ] && stop "$reloader"
  stop "$golb_pid"
}

# proxy_scenario LABEL NAME COMMAND... [-- load flags...]  for every proxy
# other than golb.
proxy_scenario() {
  local label=$1 name=$2
  shift 2
  local command=()
  while [ $# -gt 0 ] && [ "$1" != "--" ]; do
    command+=("$1")
    shift
  done
  [ $# -gt 0 ] && shift
  if ! start "$name" "$PROXY_PORT" "${command[@]}"; then
    not_measured "$label" "$name did not start"
    return 0
  fi
  local pid=$LAST_PID
  measure "$label" "$@" || not_measured "$label" "load generator failed"
  stop "$pid"
}

competitor() {
  local name=$1
  shift
  if [ "$COMPETITORS" != 1 ] || ! command -v "$name" >/dev/null 2>&1; then
    not_measured "$name (round robin)" "$name is not installed"
    return 0
  fi
  "write_$name"
  proxy_scenario "$name (round robin)" "$name" "$@"
}

write_nginx() {
  {
    echo "daemon off;"
    echo "worker_processes auto;"
    echo "pid $WORK/nginx.pid;"
    echo "error_log stderr error;"
    echo "events { worker_connections 4096; }"
    echo "http {"
    echo "  access_log off;"
    echo "  client_body_temp_path $WORK/nginx-body;"
    echo "  proxy_temp_path $WORK/nginx-proxy;"
    echo "  fastcgi_temp_path $WORK/nginx-fastcgi;"
    echo "  uwsgi_temp_path $WORK/nginx-uwsgi;"
    echo "  scgi_temp_path $WORK/nginx-scgi;"
    echo "  upstream echo {"
    local backend
    for backend in "${FAST[@]}"; do echo "    server $backend;"; done
    echo "    keepalive $CONNS;"
    echo "  }"
    echo "  server {"
    echo "    listen $PROXY;"
    echo "    location / {"
    echo "      proxy_pass http://echo;"
    echo "      proxy_http_version 1.1;"
    echo "      proxy_set_header Connection \"\";"
    echo "    }"
    echo "  }"
    echo "}"
  } >"$WORK/nginx.conf"
}

write_haproxy() {
  {
    echo "global"
    echo "  maxconn 4096"
    echo "defaults"
    echo "  mode http"
    echo "  timeout connect 2s"
    echo "  timeout client 60s"
    echo "  timeout server 60s"
    echo "  http-reuse always"
    echo "frontend bench"
    echo "  bind $PROXY"
    echo "  default_backend echo"
    echo "backend echo"
    echo "  balance roundrobin"
    local i=0 backend
    for backend in "${FAST[@]}"; do
      i=$((i + 1))
      echo "  server s$i $backend"
    done
  } >"$WORK/haproxy.cfg"
}

write_caddy() {
  {
    echo "{"
    echo "  admin off"
    echo "  auto_https off"
    echo "}"
    echo "http://$PROXY {"
    echo "  bind $HOST"
    echo "  reverse_proxy ${FAST[*]} {"
    echo "    lb_policy round_robin"
    echo "  }"
    echo "}"
  } >"$WORK/Caddyfile"
}

write_traefik() {
  {
    echo "http:"
    echo "  routers:"
    echo "    bench:"
    echo "      rule: \"PathPrefix(\`/\`)\""
    echo "      entryPoints: [web]"
    echo "      service: echo"
    echo "  services:"
    echo "    echo:"
    echo "      loadBalancer:"
    echo "        servers:"
    local backend
    for backend in "${FAST[@]}"; do echo "          - url: \"http://$backend\""; done
  } >"$WORK/traefik-dynamic.yaml"
}

write_envoy() {
  {
    echo "static_resources:"
    echo "  listeners:"
    echo "    - address: {socket_address: {address: $HOST, port_value: $PROXY_PORT}}"
    echo "      filter_chains:"
    echo "        - filters:"
    echo "            - name: envoy.filters.network.http_connection_manager"
    echo "              typed_config:"
    echo "                \"@type\": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager"
    echo "                stat_prefix: bench"
    echo "                route_config:"
    echo "                  virtual_hosts:"
    echo "                    - name: all"
    echo "                      domains: [\"*\"]"
    echo "                      routes:"
    echo "                        - match: {prefix: \"/\"}"
    echo "                          route: {cluster: echo}"
    echo "                http_filters:"
    echo "                  - name: envoy.filters.http.router"
    echo "                    typed_config:"
    echo "                      \"@type\": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"
    echo "  clusters:"
    echo "    - name: echo"
    echo "      connect_timeout: 2s"
    echo "      type: STATIC"
    echo "      lb_policy: ROUND_ROBIN"
    echo "      load_assignment:"
    echo "        cluster_name: echo"
    echo "        endpoints:"
    echo "          - lb_endpoints:"
    local backend
    for backend in "${FAST[@]}"; do
      echo "              - endpoint: {address: {socket_address: {address: ${backend%:*}, port_value: ${backend##*:}}}}"
    done
  } >"$WORK/envoy.yaml"
}

table_header() {
  echo
  echo "### $1"
  echo
  echo "| scenario | offered req/s | ok req/s (median) | ok req/s (min - max) | errors | non-2xx | p50 | p90 | p99 | p99.9 | max |"
  echo "|---|---|---|---|---|---|---|---|---|---|---|"
}

# ---------------------------------------------------------------------------

mkdir -p "$BIN"
log "building into $BIN"
(cd "$ROOT" && go build -o "$BIN/golb-bench" ./cmd/golb-bench)
if ! (cd "$ROOT" && go build -o "$BIN/golb" ./cmd/golb) 2>"$WORK/golb-build.err"; then
  log "golb did not build; its scenarios will be reported as not measured"
  tail -n 5 "$WORK/golb-build.err" >&2 || true
fi

TIME_WAIT_AT_START=$(time_wait)
require_free "$PROXY_PORT" "$ADMIN_PORT" "$BACKEND_PORT" $((BACKEND_PORT + 1)) $((BACKEND_PORT + 2)) $((BACKEND_PORT + 3)) $((BACKEND_PORT + 4))

fast_list=$(IFS=,; echo "${FAST[*]}")
start echo "$BACKEND_PORT" "$BIN/golb-bench" echo -addr "$fast_list" -size "$BODY"
start echo-slow "$((BACKEND_PORT + 4))" "$BIN/golb-bench" echo -addr "$SLOW" -size "$BODY" -delay "$SLOW_DELAY"

commit=$(cd "$ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "not a git checkout")
echo "## Benchmark run $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo
echo "- machine: $(uname -sm), $(getconf _NPROCESSORS_ONLN) logical CPUs; load generator, proxy and backends share it over loopback"
echo "- $(go version), GOMAXPROCS unset"
echo "- commit: $commit"
echo "- $CONNS keep-alive connections, $BODY byte responses, $RUNS runs of $DURATION after $WARMUP warm-up; the median run by req/s is shown"
echo "- \"ok req/s\" counts responses with a status below 400; latencies cover every request, failed ones included, and in fixed-rate rows run from the intended send time"
echo "- latencies are bucketed with at most 0.8% rounding, upwards"
echo "- command: \`CONNS=$CONNS DURATION=$DURATION WARMUP=$WARMUP RUNS=$RUNS BODY=$BODY scripts/bench.sh\`"

table_header "Throughput, closed loop"
PROXY=${FAST[0]} measure "direct to one backend (no proxy)" -save "$WORK/direct.json"
golb_scenario "golb round_robin" round_robin false none "${FAST[@]}" -- -save "$WORK/golb.json"
golb_scenario "golb p2c_ewma" p2c_ewma false none "${FAST[@]}"
golb_scenario "golb least_conn" least_conn false none "${FAST[@]}"
golb_scenario "golb ring_hash" ring_hash false none "${FAST[@]}"
golb_scenario "golb round_robin, access log on" round_robin true none "${FAST[@]}"
golb_scenario "golb round_robin, reload every ${RELOAD_EVERY}s" round_robin false reload "${FAST[@]}"
proxy_scenario "ReverseProxy, idle pool sized to $CONNS per host" baseline \
  "$BIN/golb-bench" baseline -addr "$PROXY" -backends "$fast_list" -idle-per-host "$CONNS"
if [ "$BASELINE_STOCK" = 1 ]; then
  proxy_scenario "ReverseProxy, stock transport (2 idle per host)" baseline-stock \
    "$BIN/golb-bench" baseline -addr "$PROXY" -backends "$fast_list"
else
  not_measured "ReverseProxy, stock transport (2 idle per host)" "set BASELINE_STOCK=1; exhausts ephemeral ports"
fi
competitor nginx nginx -c "$WORK/nginx.conf" -p "$WORK"
competitor haproxy haproxy -f "$WORK/haproxy.cfg"
competitor caddy caddy run --config "$WORK/Caddyfile" --adapter caddyfile
competitor traefik traefik --entrypoints.web.address="$PROXY" --providers.file.filename="$WORK/traefik-dynamic.yaml" --log.level=ERROR
competitor envoy envoy -c "$WORK/envoy.yaml" -l error

rate=$(half_of "$WORK/golb.json")
table_header "Latency at a fixed rate (50% of golb round_robin throughput)"
if [ -z "$rate" ]; then
  not_measured "fixed rate" "no golb round_robin throughput to derive the rate from"
else
  echo "<!-- rate: $rate req/s. Overhead = golb row minus direct row. -->"
  PROXY=${FAST[0]} measure "direct to one backend, $rate req/s" -rate "$rate"
  golb_scenario "golb round_robin, $rate req/s" round_robin false none "${FAST[@]}" -- -rate "$rate"
  golb_scenario "golb p2c_ewma, $rate req/s" p2c_ewma false none "${FAST[@]}" -- -rate "$rate"
  proxy_scenario "ReverseProxy sized pool, $rate req/s" baseline \
    "$BIN/golb-bench" baseline -addr "$PROXY" -backends "$fast_list" -idle-per-host "$CONNS" -- -rate "$rate"
fi

# The two policies are compared at the same offered load. Closed loop would
# not do that: every connection that lands on the slow backend stalls, so
# round robin would be offered a fraction of what p2c_ewma is. The rate is
# half of what round robin itself sustains with the slow backend in the pool.
mixed=("${FAST[0]}" "${FAST[1]}" "${FAST[2]}" "$SLOW")
table_header "Tail latency with one slow backend (+$SLOW_DELAY on 1 of 4)"
golb_scenario "golb round_robin, one slow backend, capacity" round_robin false none "${mixed[@]}" -- -save "$WORK/slow.json"
slow_rate=$(half_of "$WORK/slow.json")
if [ -z "$slow_rate" ]; then
  not_measured "one slow backend at a fixed rate" "no round_robin capacity to derive the rate from"
else
  echo "<!-- rate: $slow_rate req/s for both policies -->"
  golb_scenario "golb round_robin, one slow backend, $slow_rate req/s" round_robin false none "${mixed[@]}" -- -rate "$slow_rate"
  golb_scenario "golb p2c_ewma, one slow backend, $slow_rate req/s" p2c_ewma false none "${mixed[@]}" -- -rate "$slow_rate"
fi

log "done"
