# golb — Product Requirements (v0.1)

Module `github.com/maheshpaliwal/golb` · Go 1.25 · single static binary · L7 (HTTP/1.1, HTTP/2) + L4 (TCP) load balancer.

Status of every number in this document: **target, not result**. Nothing here has been measured yet. Results are reported exactly as measured (section 3); no claim is made beyond the data.

---

## 1. Positioning

### 1.1 What "better" means

"Better" is the acceptance table below, nothing else. A row that fails is reported as failed.

| # | Criterion | How it is measured | Pass |
|---|---|---|---|
| B1 | Zero-dependency deploy | `go build ./cmd/golb`; `go list -m all` | one binary, at most 1 third-party module |
| B2 | Reload drops nothing | load test at steady rate while reloading 20 times (SIGHUP + file change) | 0 failed requests, 0 reset connections |
| B3 | Drain drops nothing | SIGTERM under load with `shutdown_grace` longer than the slowest request | 0 failed in-flight requests |
| B4 | Tail latency under a slow backend | 4 backends, one with +200 ms latency; `p2c_ewma` vs `round_robin` | p99 with `p2c_ewma` at most 0.5x the `round_robin` p99 |
| B5 | Failure containment | kill 1 of 4 backends under load, outlier ejection + retries on | client error rate under 0.1 % for idempotent requests; dead backend gets no traffic within 1 s |
| B6 | Retry storm bounded | all backends return 503 | upstream requests at most (1 + budget) x client requests |
| B7 | Proxy overhead | section 3 | targets in section 3.2 |
| B8 | Safe by default | fresh config with only listeners, routes, backends | timeouts set, admin on loopback, header size capped, no listener without an explicit host |
| B9 | Observable by default | `curl 127.0.0.1:9901/metrics` | per-route and per-backend counters, latency histograms, health state, no extra exporter |

### 1.2 Where golb can win, and where it will not

Established facts are stated plainly; anything marked *(hypothesis)* must be proven by section 3 before it appears in the README.

| Versus | Realistic advantage | Will not win |
|---|---|---|
| Traefik, Caddy | Both are Go and build on `net/http`, as golb does. A narrower feature set and an allocation-free pick/metrics path should give higher req/s and lower p99 *(hypothesis)*. | Ecosystem: ACME, service discovery, plugins, Kubernetes ingress. |
| nginx (open source) | Active health checks are not in open-source nginx; golb has them, plus latency-aware balancing, outlier ejection and built-in Prometheus metrics. Reload swaps one pointer instead of starting new worker processes. | Raw req/s and memory per connection. A C event loop without a garbage collector is faster. |
| HAProxy | Simpler config, latency-aware P2C, retry budgets *(verify HAProxy's current feature list before publishing a comparison)*. | Raw throughput, tail latency at saturation, L4 performance, maturity. HAProxy is expected to be faster. |
| Envoy | Single binary, static YAML a person can read, no control plane needed. Envoy already has P2C least-request, outlier detection and retry budgets, so algorithms are parity, not a win. | Throughput, HTTP/2 and gRPC depth, xDS, filters. |

Honest summary: golb targets the best throughput and tail latency **among Go load balancers**, and operational simplicity plus modern balancing defaults against all of them. It does not target beating HAProxy or nginx on raw requests per second.

---

## 2. Scope

### 2.1 v0.1 (ships this session)

1. HTTP reverse proxy: HTTP/1.1 and HTTP/2 from clients (HTTP/2 over TLS via ALPN; cleartext h2c opt-in per listener), WebSocket / `Upgrade` pass-through, streaming bodies, trailers.
2. Upstream protocol: HTTP/1.1 keep-alive over plain TCP, pooled.
3. TCP (L4) proxy: bidirectional copy, half-close, connect retry on another backend, idle timeout.
4. Balancing policies: `round_robin` (weighted when weights differ — one policy covers plain and weighted RR), `least_conn`, `p2c_ewma` (default), `ring_hash` (key from a header, falling back to client IP).
5. Active health checks (HTTP, TCP) with rise/fall thresholds; passive outlier ejection (consecutive failures, exponential ejection time, max ejected percent).
6. Retries: per-try timeout, max attempts, retry budget, never the same backend twice, only when safe (section 4.4).
7. TLS termination: multiple certificates per listener (SNI), min TLS 1.2, certificates re-read on reload.
8. YAML config, strict parsing (unknown keys are errors), validation with all errors reported, `golb -check`.
9. Hot reload on SIGHUP and on file change (polling, no dependency); invalid config is rejected and the running config stays.
10. Graceful shutdown and drain on SIGTERM/SIGINT.
11. Prometheus text metrics and admin endpoint, default `127.0.0.1:9901`.
12. Structured JSON access log, asynchronous, sampled, drops on overflow (counted) instead of blocking.
13. Per-route rate limit (token bucket, one bucket per route). **First item to cut if time runs out.**

Changes from the suggested scope, with reasons:

- **Ring hash instead of Maglev.** Ring with virtual nodes handles weights and skips unhealthy backends by walking forward, with no table rebuild on health change. Maglev moves to v0.2.
- **Upstream is HTTP/1.1 plain only.** Upstream TLS and upstream HTTP/2 double the transport test matrix. Consequence: gRPC proxying is not supported in v0.1.
- **Reload cannot add or remove listeners.** A reload that changes the listener set is rejected with a clear error. Routes, pools, timeouts, limits and certificates all reload.
- **Rate limit is per route, not per client.** Per-client needs a bounded keyed store with eviction.

### 2.2 v0.2

Upstream TLS and HTTP/2 (gRPC), Maglev and bounded-load hashing, panic threshold (fail open when most backends are down), listener add/remove on reload, zero-downtime binary upgrade (socket handoff), per-client rate limits, PROXY protocol, mTLS, slow start, DNS re-resolution, circuit breaking (max connections / pending), `trust_forwarded` for client IP, OpenTelemetry traces, request mirroring, Linux `SO_REUSEPORT` and splice tuning.

### 2.3 Non-goals

HTTP/3 / QUIC, UDP, ACME, WAF, caching, compression, response rewriting, scripting or plugins, Kubernetes ingress controller, service discovery, a control-plane API, a web UI, a hand-written HTTP parser.

---

## 3. Performance

### 3.1 Method

Machine: the development machine (darwin/arm64, 11 logical CPUs). Load generator, proxy and backend share the machine over loopback, so absolute numbers are lower than on separate hosts and are only comparable to each other.

- Tools are in-repo (`cmd/golb-bench`, Dev A): `echo` is a fixed-response upstream with optional delay and failure rate; `load` is a keep-alive HTTP/1.1 load generator with closed-loop (`-c`) and fixed-rate (`-rate`) modes, reporting req/s, errors and p50/p90/p99/p99.9. No external load tool is required.
- Every scenario runs 3 times for 15 s after a 3 s warm-up; report the median run and the spread.
- Latency overhead is measured in fixed-rate mode at 50 % of the proxy's measured maximum, as (through proxy) minus (direct to backend).
- Record Go version, commit, `GOMAXPROCS`, and config with every result in `docs/BENCHMARKS.md`.

Competitors: none of nginx, HAProxy, Envoy, Traefik or Caddy is installed on this machine at the time of writing (checked). `scripts/bench.sh` runs each one only if its binary is on `PATH`, with an equivalent minimal config (same listener, same 4 backends, round robin, access log off, keep-alive on), and skips the rest. The always-available baseline is a bare `httputil.ReverseProxy` with a default transport; it is a stand-in for "unoptimised Go proxy", not for any product. **If a competitor was not run, the report says "not measured" and makes no comparison.**

### 3.2 Targets

Micro (`go test -bench . -benchmem`, also with `-cpu 1,4,11`):

| Benchmark | Target |
|---|---|
| `Pick`+`Done`, `round_robin`, `least_conn`, 16 backends | at most 25 ns/op, 0 allocs/op |
| `Pick`+`Done`, `p2c_ewma`, 16 backends | at most 40 ns/op, 0 allocs/op |
| `Pick`+`Done`, `ring_hash`, 100 backends | at most 80 ns/op, 0 allocs/op |
| Any policy under `b.RunParallel` at 11 procs | per-op time at most 3x the single-proc time |
| `metrics.Counter.Inc`, `Histogram.Observe` | 0 allocs/op |
| HTTP handler with a stub transport, vs bare `httputil.ReverseProxy` in the same harness | at most +5 allocs/op and +15 % ns/op, access log off |

End to end (4 echo backends, 1 KiB response, 64 keep-alive connections, access log off):

| Metric | Target |
|---|---|
| Throughput through golb | at least 50 % of direct-to-backend req/s, and at least 40,000 req/s |
| Added latency at 50 % load | p50 at most 150 µs, p99 at most 1 ms |
| Throughput vs bare `httputil.ReverseProxy` baseline | at least equal |
| Access log on (sample 1.0) | throughput loss at most 10 % |
| Reload every second during the run | p99 within 10 % of the no-reload run, 0 errors |

A missed target is a finding to report and explain with a profile (`-cpuprofile`, `-memprofile`), not something to hide by changing the benchmark.

---

## 4. Architecture

### 4.1 Decision: stdlib `net/http`, not a custom parser

Use `net/http` server, one tuned `http.Transport` per pool, and `httputil.ReverseProxy` (with `Rewrite`) as the forwarding core. Reasons: correct HTTP/1.1 framing and request-smuggling defences, HTTP/2, upgrades, trailers, 1xx handling and hop-by-hop header removal are already there and heavily tested. A custom parser could be faster but cannot be made correct and reviewed in this timeframe, and it is where proxies get security bugs. The cost is roughly tens of allocations per request inside the stdlib that golb does not control; golb's job is to add almost nothing on top.

### 4.2 Request path

```
listener -> http.Server -> handler: load snapshot (atomic.Pointer) -> match route
  -> rate limit -> ReverseProxy.Rewrite (set X-Forwarded-*, Host policy)
  -> retry RoundTripper: Pool.Pick -> set URL host -> http.Transport.RoundTrip (per-try timeout)
       on retryable failure: Pool.Done(failed) -> budget check -> Pick excluding tried -> again
  -> response streamed to client -> on body close: Pool.Done, metrics, access log enqueue
```

TCP: accept -> load snapshot -> `Pool.Pick` -> dial with timeout (retry on another backend) -> copy both ways -> `Pool.Done` on close.

### 4.3 Hot-path rules

- All runtime state (routes, pools, limits) lives in one immutable snapshot behind `atomic.Pointer`. A request loads it once and uses it to the end; reload builds a new snapshot and swaps.
- `Pick` and `Done` take no locks and allocate nothing. Round robin is an atomic counter over a precomputed weighted schedule; P2C uses `math/rand/v2` top-level functions (per-thread state, no lock); ring lookup is a binary search over a sorted slice built at construction.
- Backend state (in-flight, health, ejection, latency) is atomics on a `*Backend` that survives reloads.
- Metric handles are resolved when the snapshot is built. The request path does only atomic adds. If profiling shows contention on a shared counter, stripe it across cache-line-padded cells; do not do this before measuring.
- `ReverseProxy.BufferPool` backed by `sync.Pool` of 32 KiB buffers. No `fmt`, no string concatenation, no map writes, no `time.Now()` more than twice per request on the path.
- Access log: the request goroutine fills a pooled record and does a non-blocking channel send; one writer goroutine encodes and writes through a `bufio.Writer`. Full channel means drop and count.
- Transport tuning from config: `MaxIdleConnsPerHost` 256, `IdleConnTimeout` 90 s, `DisableCompression` true, response header timeout from the per-try timeout, dial timeout 2 s, keep-alive 30 s.

### 4.4 Retry rules

Retry only when the request cannot have been processed or is safe to repeat: dial/connect errors for any method; reset, timeout or configured statuses (default 502, 503, 504) only for idempotent methods (GET, HEAD, OPTIONS, PUT, DELETE) with no body or a body that was not yet read. Never retry after response bytes reached the client. Never retry an upgrade request after the upstream connected. Budget: retries may not exceed `budget_percent` (default 20) of requests in the last 10 s, with a floor of 3 retries per second so low-traffic routes still retry. Client cancellation is not a failure and is never retried.

### 4.5 Reload and shutdown

Reload: read file -> strict parse -> defaults -> validate -> build pools (`upstream.New` with the previous pool so backend state carries over) -> swap snapshot -> `Close` old pools (stops their health checks only; in-flight requests still call `Done` on them) -> close idle connections of transports no longer used. Any failure before the swap leaves the running config untouched and increments `golb_reload_total{result="error"}`. The file watcher polls size and mtime every second and reloads when the content hash changes; a burst of writes is debounced.

Shutdown: stop accepting, `http.Server.Shutdown` with `shutdown_grace`, TCP connections get the same grace then are closed, flush the access log, exit 0. A second signal exits immediately.

### 4.6 Packages

| Path | Purpose | Owner |
|---|---|---|
| `internal/core` | contract: backend, pool and balancer interfaces, pool config | A |
| `internal/balancer` | the four policies | A |
| `internal/health` | active checker, outlier detector | A |
| `internal/upstream` | `core.Pool` implementation tying the above together | A |
| `internal/metrics` | counters, gauges, histograms, Prometheus text output | A |
| `cmd/golb-bench`, `scripts/bench.sh` | echo upstream, load generator, benchmark runner | A |
| `internal/config` | file structs, strict load, defaults, validation | B |
| `internal/proxy` | router, HTTP handler, retry transport, retry budget | B |
| `internal/tcpproxy` | L4 proxy | B |
| `internal/ratelimit` | token bucket | B |
| `internal/accesslog` | async sampled JSON log | B |
| `internal/admin` | admin HTTP endpoints | B |
| `internal/server` | listeners, TLS, snapshot, reload, watcher, drain | B |
| `cmd/golb/main.go` | flags, signals, wiring | B |
| `Makefile`, `README.md`, `configs/golb.example.yaml`, `.gitignore` | | B |
| `go.mod`, `go.sum` | created by A (module + go directive only), then modified only by B | A then B |
| `test/` | end-to-end and failure-injection tests | QA |
| `docs/BENCHMARKS.md` | measured results | QA |

### 4.7 Dependencies

| Module | Why |
|---|---|
| `go.yaml.in/yaml/v3` | YAML parsing with strict unknown-field checking and duration strings; the stdlib has no YAML. If it cannot be fetched, use `gopkg.in/yaml.v3` (same API). |

Nothing else. Logging is `log/slog`, metrics are hand-written text exposition, file watching is polling, tests use the stdlib `testing` package only (no assertion library), TLS test certificates are generated in memory at test time.

---

## 5. Workstreams and contract

Two developers work at the same time in the same directory and cannot see each other's code. The rules:

1. Touch only files you own (table in 4.6). Never create, edit, format or delete a file in the other developer's directories. Do not run `gofmt -w`, `go fix` or `go mod tidy` on the whole tree; scope them to your own packages. Only B changes `go.mod`/`go.sum` after A creates it.
2. The exported signatures in 5.2 and 5.3 are frozen. If one is wrong, stop and report it; do not work around it silently.
3. **Dev A's first action**, before anything else: create `go.mod` (`module github.com/maheshpaliwal/golb`, `go 1.25`) and `internal/core/core.go` exactly as in 5.2, and confirm `go build ./internal/core` passes. Second: `internal/metrics` (5.3), since B's hot path needs it. Then balancer, health, upstream, benchmarks, bench tool.
4. **Dev B** starts with `internal/config`, `internal/ratelimit` and `internal/accesslog`, which need nothing from A beyond `internal/core`. If `go.mod` or `internal/core` does not exist yet, wait and re-check; if still absent after 3 minutes, create both verbatim from this document (the content is identical, so a collision is harmless).
5. B's packages depend on `core.Pool` (an interface), never on A's concrete types. Only `internal/server` calls `upstream.New`. B's unit tests use a fake `core.Pool`, so B is not blocked on A.
6. Neither developer commits. Each finishes with `go build ./...`, `go vet` and `go test -race` passing for their own packages and a short list of anything left undone.

### 5.2 `internal/core/core.go` (verbatim; A owns)

A may add unexported fields and improve method bodies. Exported names, signatures, yaml tags and default values are frozen.

```go
// Package core is the contract between upstream selection (balancing, health)
// and the data path (config, proxying, admin).
package core

import (
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

type Policy string

const (
	RoundRobin Policy = "round_robin" // weighted when weights differ
	LeastConn  Policy = "least_conn"
	P2CEWMA    Policy = "p2c_ewma"
	RingHash   Policy = "ring_hash"
)

type CheckType string

const (
	CheckNone CheckType = ""
	CheckHTTP CheckType = "http"
	CheckTCP  CheckType = "tcp"
)

const MaxWeight = 256

// Backend is one upstream endpoint. Addr and Weight never change after
// creation; all other state is atomic and safe for concurrent use.
type Backend struct {
	Addr   string // host:port, unique within a pool
	Weight uint32 // 1..MaxWeight

	inflight     atomic.Int64
	unhealthy    atomic.Bool  // active health-check verdict
	ejectedUntil atomic.Int64 // unix nanoseconds; passive ejection
	ewmaNanos    atomic.Int64 // smoothed latency
}

func NewBackend(addr string, weight uint32) *Backend {
	return &Backend{Addr: addr, Weight: weight}
}

func (b *Backend) Inflight() int64        { return b.inflight.Load() }
func (b *Backend) Healthy() bool          { return !b.unhealthy.Load() }
func (b *Backend) Ejected(now int64) bool { return b.ejectedUntil.Load() > now }
func (b *Backend) Latency() time.Duration { return time.Duration(b.ewmaNanos.Load()) }

// Available reports whether the backend may receive traffic at now (unix nanoseconds).
func (b *Backend) Available(now int64) bool {
	return !b.unhealthy.Load() && b.ejectedUntil.Load() <= now
}

// The mutators below are for Dev A's packages only. The data path changes
// backend state exclusively through Pool.Pick and Pool.Done.

func (b *Backend) Acquire() { b.inflight.Add(1) }
func (b *Backend) Release() { b.inflight.Add(-1) }

// SetHealthy records the active-check verdict and reports whether it changed.
func (b *Backend) SetHealthy(ok bool) (changed bool) { return b.unhealthy.Swap(!ok) == ok }

// Eject removes the backend from rotation until the given unix nanoseconds; 0 clears.
func (b *Backend) Eject(until int64) { b.ejectedUntil.Store(until) }

func (b *Backend) ObserveLatency(d time.Duration) {
	const smoothing = 8
	old := b.ewmaNanos.Load()
	if old == 0 {
		b.ewmaNanos.Store(int64(d))
		return
	}
	b.ewmaNanos.Store(old + (int64(d)-old)/smoothing)
}

// PickInfo carries per-request input to selection. It is passed by value and
// must not cause an allocation.
type PickInfo struct {
	Key     uint64     // affinity hash from Hash; read by RingHash only
	Exclude []*Backend // already tried for this request; nil on the first attempt
}

// Result is the outcome of one attempt against one backend.
type Result struct {
	Latency time.Duration // time to response headers (HTTP) or to connect (TCP); <= 0 means do not sample
	Failed  bool          // connect error, timeout, reset, or HTTP status >= 500; never client cancellation
}

// Balancer selects among a fixed backend set. Pick does no bookkeeping,
// takes no locks, allocates nothing, and returns nil when nothing is available.
type Balancer interface {
	Pick(info PickInfo) *Backend
}

// Pool is what the data path uses. Safe for concurrent use.
type Pool interface {
	// Pick returns nil when no backend is available. A non-nil result has had
	// its in-flight count incremented and must be passed to Done exactly once.
	Pick(info PickInfo) *Backend
	// Done releases the in-flight slot and feeds latency and outlier detection.
	// HTTP: call when the response body is closed or the attempt failed.
	// TCP and upgraded connections: call when the connection closes.
	// Done stays valid after Close.
	Done(b *Backend, res Result)
	// Backends returns the pool's backends in config order. Callers must not modify the slice.
	Backends() []*Backend
	// Close stops health checking. Idempotent.
	Close()
}

type EventKind uint8

const (
	BackendUp EventKind = iota + 1
	BackendDown
	BackendEjected
)

type Event struct {
	Pool    string
	Backend *Backend
	Kind    EventKind
}

// Observer receives state changes. It is called from health-check and request
// goroutines, so it must be fast and must not block.
type Observer func(Event)

type BackendSpec struct {
	Addr   string `yaml:"addr"`
	Weight uint32 `yaml:"weight"`
}

type HealthCheck struct {
	Type               CheckType     `yaml:"type"`
	Path               string        `yaml:"path"` // HTTP only; healthy means status 200..399
	Interval           time.Duration `yaml:"interval"`
	Timeout            time.Duration `yaml:"timeout"`
	HealthyThreshold   int           `yaml:"healthy_threshold"`
	UnhealthyThreshold int           `yaml:"unhealthy_threshold"`
}

type Outlier struct {
	Disabled            bool          `yaml:"disabled"`
	ConsecutiveFailures int           `yaml:"consecutive_failures"`
	BaseEjection        time.Duration `yaml:"base_ejection"` // multiplied by the number of consecutive ejections
	MaxEjection         time.Duration `yaml:"max_ejection"`
	MaxEjectedPercent   int           `yaml:"max_ejected_percent"`
}

type PoolSpec struct {
	Name     string        `yaml:"name"`
	Policy   Policy        `yaml:"policy"`
	Backends []BackendSpec `yaml:"backends"`
	Health   HealthCheck   `yaml:"health_check"`
	Outlier  Outlier       `yaml:"outlier"`
}

const (
	defaultCheckInterval      = 2 * time.Second
	defaultCheckTimeout       = time.Second
	defaultHealthyThreshold   = 2
	defaultUnhealthyThreshold = 3
	defaultConsecutive        = 5
	defaultBaseEjection       = 10 * time.Second
	defaultMaxEjection        = 5 * time.Minute
	defaultMaxEjectedPercent  = 50
)

// ApplyDefaults fills zero values. Config loading calls it before Validate.
func (s *PoolSpec) ApplyDefaults() {
	s.Policy = orDefault(s.Policy, P2CEWMA)
	for i := range s.Backends {
		s.Backends[i].Weight = orDefault(s.Backends[i].Weight, 1)
	}
	if s.Health.Type != CheckNone {
		s.Health.Path = orDefault(s.Health.Path, "/")
		s.Health.Interval = orDefault(s.Health.Interval, defaultCheckInterval)
		s.Health.Timeout = orDefault(s.Health.Timeout, defaultCheckTimeout)
		s.Health.HealthyThreshold = orDefault(s.Health.HealthyThreshold, defaultHealthyThreshold)
		s.Health.UnhealthyThreshold = orDefault(s.Health.UnhealthyThreshold, defaultUnhealthyThreshold)
	}
	if s.Outlier.Disabled {
		return
	}
	s.Outlier.ConsecutiveFailures = orDefault(s.Outlier.ConsecutiveFailures, defaultConsecutive)
	s.Outlier.BaseEjection = orDefault(s.Outlier.BaseEjection, defaultBaseEjection)
	s.Outlier.MaxEjection = orDefault(s.Outlier.MaxEjection, defaultMaxEjection)
	s.Outlier.MaxEjectedPercent = orDefault(s.Outlier.MaxEjectedPercent, defaultMaxEjectedPercent)
}

// Validate reports every problem in the spec, joined into one error.
func (s *PoolSpec) Validate() error {
	var errs []error
	if s.Name == "" {
		errs = append(errs, errors.New("pool name is empty"))
	}
	switch s.Policy {
	case RoundRobin, LeastConn, P2CEWMA, RingHash:
	default:
		errs = append(errs, fmt.Errorf("pool %q: unknown policy %q", s.Name, s.Policy))
	}
	if len(s.Backends) == 0 {
		errs = append(errs, fmt.Errorf("pool %q: no backends", s.Name))
	}
	seen := make(map[string]bool, len(s.Backends))
	for _, b := range s.Backends {
		host, port, err := net.SplitHostPort(b.Addr)
		if err != nil || host == "" || port == "" {
			errs = append(errs, fmt.Errorf("pool %q: backend %q is not host:port", s.Name, b.Addr))
		}
		if seen[b.Addr] {
			errs = append(errs, fmt.Errorf("pool %q: duplicate backend %q", s.Name, b.Addr))
		}
		seen[b.Addr] = true
		if b.Weight < 1 || b.Weight > MaxWeight {
			errs = append(errs, fmt.Errorf("pool %q: backend %q weight %d outside 1..%d", s.Name, b.Addr, b.Weight, MaxWeight))
		}
	}
	errs = append(errs, s.Health.validate(s.Name), s.Outlier.validate(s.Name))
	return errors.Join(errs...)
}

func (h HealthCheck) validate(pool string) error {
	switch h.Type {
	case CheckNone:
		return nil
	case CheckHTTP, CheckTCP:
	default:
		return fmt.Errorf("pool %q: unknown health check type %q", pool, h.Type)
	}
	if h.Interval <= 0 || h.Timeout <= 0 || h.Timeout > h.Interval {
		return fmt.Errorf("pool %q: health check needs 0 < timeout <= interval", pool)
	}
	if h.HealthyThreshold < 1 || h.UnhealthyThreshold < 1 {
		return fmt.Errorf("pool %q: health check thresholds must be at least 1", pool)
	}
	return nil
}

func (o Outlier) validate(pool string) error {
	if o.Disabled {
		return nil
	}
	if o.ConsecutiveFailures < 1 || o.BaseEjection <= 0 || o.MaxEjection < o.BaseEjection {
		return fmt.Errorf("pool %q: outlier needs consecutive_failures >= 1 and 0 < base_ejection <= max_ejection", pool)
	}
	if o.MaxEjectedPercent < 0 || o.MaxEjectedPercent > 100 {
		return fmt.Errorf("pool %q: outlier max_ejected_percent outside 0..100", pool)
	}
	return nil
}

const (
	fnvOffset uint64 = 14695981039346656037
	fnvPrime  uint64 = 1099511628211
)

// Hash is FNV-1a 64. It is stable across processes and restarts, so several
// golb instances map the same key to the same backend. It does not allocate.
func Hash(s string) uint64 {
	h := fnvOffset
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime
	}
	return h
}

func orDefault[T comparable](v, fallback T) T {
	var zero T
	if v == zero {
		return fallback
	}
	return v
}
```

### 5.3 Other signatures A provides to B (frozen)

```go
// package upstream
// New builds a pool and starts its health checks. spec must already have
// defaults applied and be valid. prev may be nil; when given, a backend whose
// Addr and Weight are unchanged reuses the *core.Backend from prev, so health,
// in-flight and latency state survive a reload. obs may be nil.
func New(spec core.PoolSpec, prev core.Pool, obs core.Observer) (core.Pool, error)

// package balancer
func New(policy core.Policy, backends []*core.Backend) (core.Balancer, error)

// package metrics
type Registry struct{ /* unexported */ }
type Counter struct{ /* unexported */ }
type Gauge struct{ /* unexported */ }
type Histogram struct{ /* unexported */ }

func NewRegistry() *Registry

// labels are alternating key, value. Registering the same name and labels
// again returns the existing metric, so values survive reloads. Registration
// may lock and allocate; it happens at snapshot build, never per request.
func (r *Registry) Counter(name, help string, labels ...string) *Counter
func (r *Registry) Gauge(name, help string, labels ...string) *Gauge
func (r *Registry) GaugeFunc(name, help string, fn func() float64, labels ...string)
func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *Histogram
func (r *Registry) WriteTo(w io.Writer) (int64, error) // Prometheus text format 0.0.4

func (c *Counter) Inc()
func (c *Counter) Add(n uint64)
func (c *Counter) Value() uint64
func (g *Gauge) Set(v int64)
func (g *Gauge) Add(delta int64)
func (h *Histogram) Observe(seconds float64)

var LatencyBuckets []float64 // seconds: 0.0005 .. 10
```

Behaviour A must implement behind these:

- `round_robin`: atomic counter over a schedule precomputed by smooth weighted interleaving (weights divided by their GCD); skip unavailable and excluded backends.
- `least_conn`: lowest `Inflight()/Weight`; scan from a rotating start so ties spread. O(n) is accepted for v0.1.
- `p2c_ewma`: two distinct random candidates, choose the lower `(Latency()+1) * (Inflight()+1) / Weight`; a backend with no latency sample yet is preferred so new backends get probed; fall back to a scan if both candidates are unavailable.
- `ring_hash`: 160 virtual nodes per unit of weight, sorted `uint64` slice, binary search on `Key`, walk forward to the first available, non-excluded backend.
- Every policy returns nil only when no available, non-excluded backend exists.
- Active checker: one goroutine per backend, jittered start, state changes only after `healthy_threshold` / `unhealthy_threshold` consecutive results, backends start healthy, `Close` stops and waits for all goroutines. HTTP checks use their own client with the check timeout and no keep-alive reuse across pools.
- Outlier: per-backend consecutive-failure count reset on success; on reaching the threshold eject for `base_ejection * ejections` capped at `max_ejection`; never eject beyond `max_ejected_percent` of the pool; the ejection count decays after a healthy period.
- Emit `core.Event` on every up, down and ejection.

### 5.4 Config schema (B owns `internal/config`)

```go
type Config struct {
	Admin         Admin         `yaml:"admin"`
	AccessLog     AccessLog     `yaml:"access_log"`
	ShutdownGrace time.Duration `yaml:"shutdown_grace"` // default 30s
	Listeners     []Listener    `yaml:"listeners"`
	Pools         []Pool        `yaml:"pools"`
}

type Admin struct {
	Addr             string `yaml:"addr"` // default 127.0.0.1:9901; "off" disables
	Pprof            bool   `yaml:"pprof"`
	AllowNonLoopback bool   `yaml:"allow_non_loopback"`
}

type AccessLog struct {
	Enabled    bool    `yaml:"enabled"`
	SampleRate float64 `yaml:"sample_rate"` // 0..1, default 1
	BufferSize int     `yaml:"buffer_size"` // records, default 8192
}

type Listener struct {
	Name     string   `yaml:"name"`
	Addr     string   `yaml:"addr"`     // host is mandatory; ":8080" is rejected
	Protocol string   `yaml:"protocol"` // "http" | "tcp"
	TLS      *TLS     `yaml:"tls"`
	H2C      bool     `yaml:"h2c"`
	Timeouts Timeouts `yaml:"timeouts"`
	Routes   []Route  `yaml:"routes"` // http only
	Pool     string   `yaml:"pool"`   // tcp only
}

type TLS struct {
	Certificates []Certificate `yaml:"certificates"`
	MinVersion   string        `yaml:"min_version"` // "1.2" (default) | "1.3"
}

type Certificate struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type Timeouts struct {
	ReadHeader     time.Duration `yaml:"read_header"`      // default 5s
	Idle           time.Duration `yaml:"idle"`             // default 60s
	Request        time.Duration `yaml:"request"`          // whole request incl. retries, default 60s; 0 = none
	MaxHeaderBytes int           `yaml:"max_header_bytes"` // default 65536
}

type Route struct {
	Host       string     `yaml:"host"`        // exact, or "*.suffix"; empty matches any
	PathPrefix string     `yaml:"path_prefix"` // default "/"; longest prefix wins
	Pool       string     `yaml:"pool"`
	RateLimit  *RateLimit `yaml:"rate_limit"`
}

type RateLimit struct {
	RequestsPerSecond float64 `yaml:"requests_per_second"`
	Burst             int     `yaml:"burst"`
}

type Pool struct {
	core.PoolSpec `yaml:",inline"`
	HashHeader    string    `yaml:"hash_header"` // ring_hash key; empty or absent header falls back to client IP
	Retry         Retry     `yaml:"retry"`
	Transport     Transport `yaml:"transport"`
}

type Retry struct {
	MaxAttempts   int           `yaml:"max_attempts"`   // total tries, default 3; 1 disables
	PerTryTimeout time.Duration `yaml:"per_try_timeout"` // default 10s
	BudgetPercent int           `yaml:"budget_percent"` // default 20
	RetryOn       []int         `yaml:"retry_on"`       // default [502, 503, 504]
}

type Transport struct {
	DialTimeout         time.Duration `yaml:"dial_timeout"`            // default 2s
	MaxIdleConnsPerHost int           `yaml:"max_idle_conns_per_host"` // default 256
	IdleConnTimeout     time.Duration `yaml:"idle_conn_timeout"`       // default 90s
}
```

Example (`configs/golb.example.yaml`):

```yaml
admin:
  addr: 127.0.0.1:9901
listeners:
  - name: web
    addr: 127.0.0.1:8080
    protocol: http
    routes:
      - path_prefix: /
        pool: app
  - name: raw
    addr: 127.0.0.1:9000
    protocol: tcp
    pool: app
pools:
  - name: app
    policy: p2c_ewma
    backends:
      - addr: 127.0.0.1:9001
      - addr: 127.0.0.1:9002
        weight: 2
    health_check:
      type: http
      path: /healthz
```

Validation beyond the struct tags: every listener address has an explicit host; binding all interfaces is only possible by writing `0.0.0.0` or `[::]` explicitly, and no default, example or test does. Admin on a non-loopback address is rejected unless `allow_non_loopback: true`. Route and listener pool names must exist. Listener names and addresses are unique. Certificate files must load. All errors are reported together, each naming the offending listener, route or pool.

### 5.5 Admin and metrics (B)

Endpoints: `GET /metrics`, `GET /healthz` (process alive), `GET /readyz` (config loaded, not draining), `GET /backends` (JSON: pool, addr, weight, healthy, ejected, in-flight, latency), `POST /reload`, `/debug/pprof/*` only with `pprof: true`. The admin server has its own timeouts and never shares a listener with traffic.

Metric names: `golb_requests_total{listener,route,code_class}`, `golb_request_duration_seconds{listener,route}`, `golb_upstream_requests_total{pool,backend,result}`, `golb_upstream_duration_seconds{pool}`, `golb_backend_inflight{pool,backend}`, `golb_backend_healthy{pool,backend}`, `golb_backend_ejections_total{pool,backend}`, `golb_retries_total{pool}`, `golb_retry_budget_exhausted_total{pool}`, `golb_ratelimited_total{route}`, `golb_tcp_connections_total{listener}`, `golb_tcp_bytes_total{listener,direction}`, `golb_reload_total{result}`, `golb_accesslog_dropped_total`, `golb_config_last_reload_success_timestamp_seconds`.

Proxy-generated statuses: 502 upstream connect/reset, 503 no backend available, 504 upstream timeout, 429 rate limited, each with a short plain-text body and no internal detail.

### 5.6 Code conventions (both developers)

- Unexported by default; export only what another package uses. No `Get` prefix on accessors. No package named `util`, `helper` or `common`. Names do not repeat the package or receiver.
- One `err` variable per function. Handle the error or empty case first and return; avoid `else` and nesting. Never discard an error: return it, or log it once where it happens if the caller cannot act.
- Wrap errors with the action attempted (`"load certificate: %w"`), no "failed to". Callers distinguish errors with `errors.Is`/`errors.As`, never by string.
- No `panic` or `log.Fatal` outside process start-up. Every long-lived goroutine has an owner, a stop signal and a recover that logs; nothing is started that cannot be stopped.
- Every timeout, limit and threshold comes from config with a named default constant. No magic numbers or strings. Durations are `time.Duration`, closed sets are typed constants.
- Pass `context.Context` down from the caller; no detached or `TODO` contexts. Many parameters become a struct.
- Small interfaces, defined where they are consumed. Mutexes are value fields on structs passed by pointer. Channel parameters carry a direction.
- No comments that narrate the code. A comment states a constraint or a reason the code cannot show.
- Tests sit beside the code, are table-driven with named fields, assert on observable results (not just "no error"), use `t.Helper` and `t.Cleanup` in helpers, never call `t.Fatal` from another goroutine, never call real external services, and wait by polling with a deadline rather than a fixed sleep.
- Delete unused code before handing over. `gofmt` and `go vet` clean.

---

## 6. Test plan (QA)

All listeners in tests bind `127.0.0.1:0`. Backends are in-process (`httptest.Server`, `net.Listen`). No external network.

**Unit** (owners write these; QA checks coverage of the cases):
- Balancers: distribution within 5 % of weights over 100k picks; unavailable and excluded backends never returned; nil when none; ring key stability (removing 1 of 10 backends remaps about 1/10 of keys, never keys of unaffected backends); P2C prefers the faster backend.
- Health: rise/fall thresholds, flapping, timeout, `Close` leaves no goroutine. Outlier: ejection, backoff growth, cap, max-percent guard, recovery.
- Config: every validation rule, unknown key rejected, defaults, duration parsing, example file loads.
- Metrics: text format parses, histogram bucket math, concurrent increments sum exactly.
- Retry budget, token bucket, router precedence, access log drop-on-full.

**Race**: `go test -race -count=1 ./...` clean. Add concurrent tests: 64 goroutines doing `Pick`/`Done` while health state flips and while the snapshot is swapped.

**Integration** (`test/`):
- HTTP/1.1 and HTTP/2 (TLS) round trip; large and chunked bodies both directions; trailers; streaming response flushes promptly; WebSocket echo through the proxy; TCP echo and half-close.
- Headers: hop-by-hop headers and headers named in `Connection` are removed; client-supplied `X-Forwarded-For` does not survive unmodified; `Host` policy.
- Malformed input: both `Content-Length` and `Transfer-Encoding`, duplicate `Content-Length`, oversized headers, bad chunk framing. Each is rejected by golb and never reaches a backend.
- SNI picks the right certificate; TLS 1.1 refused.

**Failure injection**:
- Backend closed mid-response: client sees an error or truncated body, not a hang; the proxy does not retry.
- Backend refuses connections: idempotent requests succeed via retry; backend ejected; returns after it recovers.
- Slow backend: per-try timeout fires, 504 or successful retry; total time bounded by the request timeout.
- All backends down: fast 503, upstream attempts bounded by the retry budget.
- Slow client sending headers byte by byte: connection closed at the read-header timeout.
- Client disconnects mid-request: upstream request cancelled, `Done` called, in-flight returns to 0.
- Reload under load: 20 reloads (alternating two valid configs) at steady load, 0 errors. Reload with an invalid file: rejected, traffic unaffected, error metric incremented.
- SIGTERM under load: in-flight requests complete, new connections refused, exit code 0 within the grace period.
- Leak check after every scenario: goroutine count and backend in-flight return to baseline.

**Commands**:
```
go build ./... && go vet ./...
go test -race -count=1 ./...
go test -run '^$' -bench . -benchmem -cpu 1,4,11 ./internal/balancer ./internal/metrics ./internal/proxy
go run ./cmd/golb -check -config configs/golb.example.yaml
go run ./cmd/golb-bench echo -addr 127.0.0.1:9001      # repeat for 9002..9004
go run ./cmd/golb-bench load -url http://127.0.0.1:8080/ -c 64 -d 15s
go run ./cmd/golb-bench load -url http://127.0.0.1:8080/ -rate 20000 -d 15s
scripts/bench.sh                                        # full matrix, writes docs/BENCHMARKS.md input
```

**Shippable when**: build, vet and race tests pass three times in a row; every integration and failure-injection case passes; criteria B1–B3, B5, B6, B8, B9 pass; B4 and B7 are measured and reported whether or not they meet target; no open high-severity review finding; README claims match `docs/BENCHMARKS.md` exactly.

---

## 7. Review checklist

**Concurrency**
- The snapshot is loaded once per request and never mutated after publication; no code path reads it twice and assumes the same value.
- Every non-nil `Pick` reaches exactly one `Done` on all paths: success, error, retry, client cancel, panic in a handler, upgrade, body never read.
- No lock, map write or allocation in `Pick`/`Done`; no data race on backend state; counters cannot go negative.
- Reload racing with reload, and reload racing with shutdown, are serialised.

**Leaks**
- Health-check, watcher, access-log and TCP copy goroutines all exit on `Close`/shutdown.
- Response bodies are always closed; the per-try timeout's cancel function runs when the body is closed, not when headers arrive (cancelling early truncates the body; never cancelling leaks a timer).
- Old transports release idle connections after reload. TCP proxy closes both sockets when either direction ends and honours half-close.

**HTTP correctness and smuggling surface**
- No hand-rolled parsing of request lines, headers or chunking anywhere. `Content-Length` and `Transfer-Encoding` are never set or forwarded by golb's own code.
- Hop-by-hop headers removed in both directions, including those listed in `Connection`. Upgrade is forwarded only for real upgrade requests.
- Inbound `X-Forwarded-*` and `Forwarded` are replaced, not appended to, unless trust is configured (not in v0.1).
- The outbound URL host comes only from the picked backend, never from the request (no open proxy, no SSRF via absolute-form URLs or `Host`).
- Retries never replay a consumed body and never happen after bytes were sent to the client.
- Paths are matched on the cleaned path but forwarded unmodified; check `//`, `/../` and encoded-slash routing bypasses.

**Timeouts and resource limits**
- Server: read-header and idle timeouts, max header bytes set on every listener including admin. Upstream: dial, per-try, total request, idle-connection timeouts. Health checks: own timeout. No `http.DefaultClient` or `http.DefaultTransport` anywhere.
- Slowloris: default read-header timeout is 5 s and cannot be disabled by omission. HTTP/2 concurrent streams are capped.
- Access-log channel, retry budget and rate limiter are bounded; nothing grows with request count or with attacker-chosen keys (metric labels never contain request-derived values such as path or host header).

**Exposure**
- Admin binds loopback by default; non-loopback needs an explicit flag; pprof is off by default; `/backends` and errors reveal no file contents or key material.
- No default, example, test or doc binds a wildcard address. No secrets, real hostnames or organisation names in the repo. TLS minimum 1.2.
- Error responses to clients carry no backend addresses or internal error text.

**Error handling and style**
- No discarded errors, no panics outside start-up, errors logged once. Invalid reload leaves the old config fully in force.
- Conventions in 5.6 are followed; exported surface is minimal; no dead code; ownership table in 4.6 was respected and the contract in 5.2/5.3 is unchanged.
- Benchmarks measure what they claim (no dead-code elimination, `b.ReportAllocs`, realistic backend counts), and no published number lacks a reproducible command.
