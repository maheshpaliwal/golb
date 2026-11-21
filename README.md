# golb

golb is a load balancer in a single static binary: an HTTP reverse proxy
(HTTP/1.1 and HTTP/2 from clients, HTTP/1.1 to backends) and a TCP proxy,
configured by one YAML file. It is built on Go's `net/http` and has one
third-party dependency (the YAML parser).

Status: v0.1, new and not yet run in production. The specification is in
[docs/PRD.md](docs/PRD.md).

## What it does

- Routes by host and path prefix; balances with `p2c_ewma` (default),
  `round_robin` (weighted), `least_conn` or `ring_hash`.
- Active health checks (HTTP or TCP) and passive outlier ejection.
- Retries on another backend when that is safe, limited by a retry budget.
- TLS termination with several certificates per listener (SNI), TLS 1.2 minimum.
- WebSocket and other `Upgrade` connections, streaming bodies, trailers.
- Hot reload on `SIGHUP`, on file change, or `POST /reload`; graceful drain on
  `SIGTERM`/`SIGINT`.
- Prometheus metrics and an admin endpoint on `127.0.0.1:9901`.
- Sampled JSON access log that never blocks requests.
- Per-route rate limit.

Not in v0.1: TLS or HTTP/2 to backends (so no gRPC), HTTP/3, service
discovery, ACME, per-client rate limits. See the PRD for the full list.

## Quick start

Requires Go 1.25.

```sh
make build                                  # writes bin/golb
bin/golb -check -config configs/golb.example.yaml
bin/golb -config configs/golb.example.yaml
```

The example listens on `127.0.0.1:8080` (HTTP) and `127.0.0.1:9000` (TCP) and
expects backends on `127.0.0.1:9001` and `127.0.0.1:9002`. Two throwaway
backends for trying it out:

```sh
go run ./cmd/golb-bench echo -addr 127.0.0.1:9001,127.0.0.1:9002
curl -i http://127.0.0.1:8080/
curl http://127.0.0.1:9901/metrics
curl http://127.0.0.1:9901/backends
```

Operating it:

| Action | How |
|---|---|
| Validate a config | `golb -check -config FILE` (exit code 0 when valid) |
| Reload | edit the file (picked up within about two seconds), `kill -HUP PID`, or `curl -X POST http://127.0.0.1:9901/reload` |
| Drain and stop | `kill -TERM PID`; a second signal exits immediately |

Process logs go to stderr as JSON; the access log goes to stdout.

## Configuration

A minimal file:

```yaml
listeners:
  - name: web
    addr: 127.0.0.1:8080
    protocol: http
    routes:
      - path_prefix: /
        pool: app
pools:
  - name: app
    backends:
      - addr: 127.0.0.1:9001
      - addr: 127.0.0.1:9002
```

Parsing is strict: an unknown key is an error, and all validation errors are
reported together. Durations are written like `500ms`, `5s`, `2m`.

### Top level

| Key | Default | Meaning |
|---|---|---|
| `admin.addr` | `127.0.0.1:9901` | Admin listener; `off` disables it |
| `admin.pprof` | `false` | Serve `/debug/pprof/` on the admin listener |
| `admin.allow_non_loopback` | `false` | Required to put the admin listener on a non-loopback address |
| `access_log.enabled` | `false` | Write one JSON line per request to stdout |
| `access_log.sample_rate` | `1` | Fraction of requests logged, 0 to 1 |
| `access_log.buffer_size` | `8192` | Queued records; when full, records are dropped and counted |
| `shutdown_grace` | `30s` | How long a drain waits for requests and connections |

### Listeners

| Key | Default | Meaning |
|---|---|---|
| `name` | required | Unique; used in metrics and logs |
| `addr` | required | `host:port`. The host is mandatory: `:8080` is rejected. Binding every interface requires writing `0.0.0.0` or `[::]` |
| `protocol` | required | `http` or `tcp` |
| `routes` | | HTTP only, see below |
| `pool` | | TCP only: the pool to forward to |
| `tls.certificates` | | HTTP only: list of `cert_file` / `key_file`; chosen by SNI, first one is the fallback |
| `tls.min_version` | `"1.2"` | `"1.2"` or `"1.3"` |
| `h2c` | `false` | Accept cleartext HTTP/2 (HTTP/2 over TLS needs no setting) |
| `timeouts.read_header` | `5s` | Time allowed to send request headers |
| `timeouts.idle` | `60s` | Keep-alive idle time; for TCP, time without bytes in either direction |
| `timeouts.request` | `60s` | Time the pool has to produce response headers, across all tries. Time the client spends uploading the body is not counted; `0` disables |
| `timeouts.body_idle` | `60s` | Longest a request body, a response body or an upgraded connection may go without a single byte in either direction; `0` disables |
| `timeouts.max_header_bytes` | `65536` | Request header size limit |

Routes:

| Key | Default | Meaning |
|---|---|---|
| `host` | any | Exact host, or `*.suffix` |
| `path_prefix` | `/` | Matches whole path segments: `/api` covers `/api` and `/api/v1`, not `/apiary` |
| `pool` | required | Pool name |
| `rate_limit.requests_per_second`, `rate_limit.burst` | none | Token bucket for the route; excess requests get 429 |

An exact host beats a wildcard (longest suffix first), which beats a route
without a host; within a host the longest prefix wins. Paths are matched after
cleaning (`//`, `/./`, `/../`) and forwarded unmodified.

### Pools

| Key | Default | Meaning |
|---|---|---|
| `name` | required | Unique |
| `policy` | `p2c_ewma` | `round_robin`, `least_conn`, `p2c_ewma`, `ring_hash` |
| `backends[].addr`, `backends[].weight` | weight `1` | `host:port`, weight 1 to 256 |
| `hash_header` | client IP | `ring_hash` only: request header to hash |
| `health_check.type` | none | `http` or `tcp` |
| `health_check.path`, `interval`, `timeout`, `healthy_threshold`, `unhealthy_threshold` | `/`, `2s`, `1s`, `2`, `3` | HTTP status 200 to 399 is healthy |
| `outlier.disabled`, `consecutive_failures`, `base_ejection`, `max_ejection`, `max_ejected_percent` | `false`, `5`, `10s`, `5m`, `50` | Passive ejection |
| `retry.max_attempts` | `3` | Total tries; `1` disables retries |
| `retry.per_try_timeout` | `10s` | Time one backend has to return response headers |
| `retry.budget_percent` | `20` | Retries allowed as a share of the last 10 s of requests, with a floor of 3 per second |
| `retry.retry_on` | `[502, 503, 504]` | Statuses that are retried |
| `transport.dial_timeout` | `2s` | Connect timeout |
| `transport.max_idle_conns_per_host` | `256` | Idle keep-alive connections kept per backend |
| `transport.idle_conn_timeout` | `90s` | How long an idle backend connection is kept |

## Behaviour worth knowing

**Retries.** A failed connect is retried on another backend for any method. A
reset, a timeout or a `retry_on` status is retried only for GET, HEAD, OPTIONS,
PUT and DELETE requests without a body, and never for an upgrade request. A
request is never retried once response bytes have gone to the client, never on
the same backend twice, and never because the client went away. Request bodies
are streamed, not buffered, so a request with a body is retried only when the
connect failed.

**Timeouts.** `request` limits the wait for a backend; `body_idle` limits
silence. A request body that stops arriving is answered with 408 after the
shorter of the two. Once the response is flowing only `body_idle` applies, so
a large download, a server-sent event stream or a WebSocket lasts as long as
bytes keep moving and is closed when nothing has moved for a whole interval
(a stall is noticed between one and two intervals after it begins). An event
stream that can be quiet for longer needs heartbeats or a larger `body_idle`.
A stalled client or a quiet stream is never counted as a backend failure.

**Headers.** Hop-by-hop headers, including those named in `Connection`, are
removed in both directions. From the client, `Forwarded`, every `X-Forwarded-*`
header, `X-Real-Ip` and `True-Client-Ip` are discarded; golb then sets
`X-Forwarded-For`, `X-Forwarded-Host` and `X-Forwarded-Proto` from the
connection. The `Host` header is passed through; the backend address always
comes from the pool, never from the request. Request and response trailers
are forwarded.

**Request framing.** Requests are parsed by `net/http` only. Conflicting or
malformed `Content-Length`, unsupported `Transfer-Encoding` and oversized
headers are rejected before any backend is contacted. Two cases are handled
rather than refused, because of how `net/http` works:

- Both `Content-Length` and `Transfer-Encoding: chunked`: `net/http` drops the
  length before golb sees the request and does not say so. golb forwards one
  unambiguous chunked request and closes the client connection after the
  response (RFC 9112 section 6.3). Since the case cannot be told apart, every
  chunked HTTP/1 request closes its connection; clients that upload with
  chunked encoding do not get keep-alive.
- Broken chunk framing in the body: bodies are streamed, so the request has
  already been opened on a backend. The client gets 400, the backend receives
  an aborted request, never a complete one, and is not charged with a failure.

**Reload.** The new file is parsed, validated and built before anything
changes; on any error the running configuration stays and
`golb_reload_total{result="error"}` increases. Requests in flight finish on the
configuration they started with. Routes, pools, retry and transport settings,
rate limits, `timeouts.request`, `timeouts.body_idle`, certificates and the
minimum TLS version reload. The watcher also follows the certificate and key
files the config names, so a rotated certificate is loaded without a signal.
Metric series of removed backends and pools disappear. Listener names, addresses and protocols, turning TLS or h2c on or off,
`read_header`, `idle` (HTTP), `max_header_bytes`, the admin settings and the
access log buffer size need a restart; a reload that changes them is refused.

**Shutdown.** Listeners stop accepting, in-flight requests and TCP connections
get `shutdown_grace` to finish, the rest are closed, the access log is flushed
and the process exits 0. Upgraded connections (WebSocket) get the same grace
period and are then closed.

**Proxy-generated responses.** 502 (connect failed or connection reset), 503
(no backend available), 504 (upstream timeout), 429 (rate limited), 404 (no
route), 400 (request body could not be read), 408 (request body stalled),
each with a one-line plain-text body.

## Admin endpoints

| Endpoint | Purpose |
|---|---|
| `GET /metrics` | Prometheus text format |
| `GET /healthz` | Process is alive |
| `GET /readyz` | 200 while serving, 503 while draining |
| `GET /backends` | JSON: pool, address, weight, health, ejection, in-flight, latency |
| `POST /reload` | Reload the config file |
| `/debug/pprof/*` | Only with `admin.pprof: true` |

The admin listener refuses requests a browser could be tricked into sending:
the `Host` header must be an IP address, `localhost` or the configured admin
host (not checked when `allow_non_loopback` is set), and a request with a
cross-site `Origin` or `Sec-Fetch-Site` gets 403.

Metrics cover requests by listener, route and status class, request and
upstream latency histograms, per-backend results, in-flight and health,
retries and retry-budget exhaustion, rate-limited requests, TCP connections
and bytes, reloads and dropped access-log records. All names start with
`golb_`.

## Performance

No performance claim is made here. Measured results, with the method, machine,
Go version and commands needed to reproduce them, are in
`docs/BENCHMARKS.md`. The targets in the PRD are goals, not results.

To run the micro-benchmarks yourself:

```sh
make bench
```

### Memory and the garbage collector

A proxy keeps little memory alive and allocates a lot per second, so Go's
stock collector setting (collect when the heap has doubled) ran about 120
collections per second under load. Unless `GOGC` is set, golb therefore lets
the heap grow by about 16 MiB between collections while less than that is
alive, and uses the stock setting above it. Measured on the development
machine at 64 connections (`docs/BENCHMARKS.md`, section 6): CPU time per
request down about 15 %, resident memory up from about 27 MiB to about
46 MiB. At 256 connections the saving was about 4 % for about 40 MiB more.

- Set `GOGC` to control the collector yourself; golb then leaves it alone.
  `GOGC=100` restores the stock behaviour. `GOGC=400` saved more CPU at 256
  connections in one trial (about 12 %) for about 55 MiB more memory.
- `GOMEMLIMIT` is honoured as usual and is the way to cap memory in a
  container.
- `GOMAXPROCS` made no consistent difference to throughput in the same
  runs, so there is no recommendation for it.

## Development

```sh
make test     # go test -race -count=1 ./...
make vet
make fmt      # fails if any file needs gofmt
```

Tests use in-process backends on `127.0.0.1` only and generate throwaway TLS
certificates at run time; no keys are stored in the repository.
