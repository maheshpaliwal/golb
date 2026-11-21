package proxy

import (
	"net"
	"net/http"
	"time"

	"github.com/maheshpaliwal/golb/internal/config"
	"github.com/maheshpaliwal/golb/internal/core"
	"github.com/maheshpaliwal/golb/internal/metrics"
)

const (
	dialKeepAlive         = 30 * time.Second
	expectContinueTimeout = time.Second

	resultOK    = "ok"
	resultError = "error"
)

// Upstream is one pool as the HTTP data path sees it: selection, a tuned
// transport, the retry policy and metric handles resolved ahead of requests.
type Upstream struct {
	pool      core.Pool
	transport *http.Transport
	tuning    transportTuning

	maxAttempts int
	retryOn     []int
	perTry      time.Duration
	dialTimeout time.Duration
	budget      *budget

	ringHash   bool
	hashHeader string

	backends        map[*core.Backend]backendCounters
	duration        *metrics.Histogram
	retries         *metrics.Counter
	budgetExhausted *metrics.Counter
}

type backendCounters struct {
	ok     *metrics.Counter
	failed *metrics.Counter
}

// transportTuning is everything a transport is built from; an unchanged
// value lets a reload keep the transport and its warm connections.
type transportTuning struct {
	config.Transport
	perTry time.Duration
}

type UpstreamOptions struct {
	Config   config.Pool
	Pool     core.Pool
	Registry *metrics.Registry
	// Prev is the upstream of the same name from the running configuration,
	// or nil. Its retry budget and, when the tuning is unchanged, its
	// transport carry over.
	Prev *Upstream
}

func NewUpstream(opts UpstreamOptions) *Upstream {
	cfg := opts.Config
	reg := opts.Registry
	u := &Upstream{
		pool:        opts.Pool,
		tuning:      transportTuning{Transport: cfg.Transport, perTry: cfg.Retry.PerTryTimeout},
		maxAttempts: cfg.Retry.MaxAttempts,
		retryOn:     cfg.Retry.RetryOn,
		perTry:      cfg.Retry.PerTryTimeout,
		dialTimeout: cfg.Transport.DialTimeout,
		budget:      newBudget(cfg.Retry.BudgetPercent),
		ringHash:    cfg.Policy == core.RingHash,
		hashHeader:  http.CanonicalHeaderKey(cfg.HashHeader),
		backends:    make(map[*core.Backend]backendCounters, len(cfg.Backends)),
		duration: reg.Histogram("golb_upstream_duration_seconds",
			"Time from sending a request upstream to its response headers.", metrics.LatencyBuckets, "pool", cfg.Name),
		retries: reg.Counter("golb_retries_total", "Requests retried on another backend.", "pool", cfg.Name),
		budgetExhausted: reg.Counter("golb_retry_budget_exhausted_total",
			"Retries skipped because the retry budget was spent.", "pool", cfg.Name),
	}
	const help = "Upstream attempts by backend and result."
	for _, b := range opts.Pool.Backends() {
		u.backends[b] = backendCounters{
			ok:     reg.Counter("golb_upstream_requests_total", help, "pool", cfg.Name, "backend", b.Addr, "result", resultOK),
			failed: reg.Counter("golb_upstream_requests_total", help, "pool", cfg.Name, "backend", b.Addr, "result", resultError),
		}
	}

	prev := opts.Prev
	if prev != nil && prev.budget.percent == u.budget.percent {
		u.budget = prev.budget
	}
	if prev != nil && prev.tuning == u.tuning {
		u.transport = prev.transport
		return u
	}
	u.transport = newTransport(u.tuning)
	return u
}

// Retire releases the idle connections of a transport that next (the
// upstream replacing this one, or nil) did not take over. In-flight requests
// keep their connections.
func (u *Upstream) Retire(next *Upstream) {
	if next != nil && next.transport == u.transport {
		return
	}
	u.transport.CloseIdleConnections()
}

func newTransport(tuning transportTuning) *http.Transport {
	dialer := &net.Dialer{Timeout: tuning.DialTimeout, KeepAlive: dialKeepAlive}
	return &http.Transport{
		DialContext:           dialer.DialContext,
		MaxIdleConnsPerHost:   tuning.MaxIdleConnsPerHost,
		IdleConnTimeout:       tuning.IdleConnTimeout,
		ResponseHeaderTimeout: tuning.perTry,
		ExpectContinueTimeout: expectContinueTimeout,
		// The proxy forwards bytes as they are; asking for gzip on the
		// client's behalf would change the response.
		DisableCompression: true,
	}
}

// done reports one finished attempt to the pool and the metrics.
func (u *Upstream) done(b *core.Backend, latency time.Duration, failed bool) {
	u.pool.Done(b, core.Result{Latency: latency, Failed: failed})
	u.duration.Observe(latency.Seconds())
	counters := u.backends[b]
	if failed {
		counters.failed.Inc()
		return
	}
	counters.ok.Inc()
}

// abandon returns a picked backend that produced no outcome worth sampling,
// such as an attempt the client cancelled.
func (u *Upstream) abandon(b *core.Backend) {
	u.pool.Done(b, core.Result{})
}
