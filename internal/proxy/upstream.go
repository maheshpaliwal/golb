package proxy

import (
	"net"
	"net/http"
	"sync/atomic"
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

	metricUpstreamRequests = "golb_upstream_requests_total"
	metricUpstreamDuration = "golb_upstream_duration_seconds"
	metricRetries          = "golb_retries_total"
	metricBudgetExhausted  = "golb_retry_budget_exhausted_total"
)

// Upstream is one pool as the HTTP data path sees it: selection, a tuned
// transport, the retry policy and metric handles resolved ahead of requests.
type Upstream struct {
	name     string
	registry *metrics.Registry
	pool     core.Pool
	conns    *connections
	tuning   transportTuning

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

// connections is a transport and whether it has been replaced. Upstreams of
// successive configurations share one value while the tuning is unchanged.
type connections struct {
	transport *http.Transport
	// retired is set once no configuration uses the transport any more.
	// Requests that started earlier still finish on it; each one then
	// closes the connection it would otherwise park in an idle pool that
	// nothing drains.
	retired atomic.Bool
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
		name:        cfg.Name,
		registry:    reg,
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
		duration: reg.Histogram(metricUpstreamDuration,
			"Time from sending a request upstream to its response headers.", metrics.LatencyBuckets, "pool", cfg.Name),
		retries: reg.Counter(metricRetries, "Requests retried on another backend.", "pool", cfg.Name),
		budgetExhausted: reg.Counter(metricBudgetExhausted,
			"Retries skipped because the retry budget was spent.", "pool", cfg.Name),
	}
	const help = "Upstream attempts by backend and result."
	for _, b := range opts.Pool.Backends() {
		u.backends[b] = backendCounters{
			ok:     reg.Counter(metricUpstreamRequests, help, "pool", cfg.Name, "backend", b.Addr, "result", resultOK),
			failed: reg.Counter(metricUpstreamRequests, help, "pool", cfg.Name, "backend", b.Addr, "result", resultError),
		}
	}

	prev := opts.Prev
	if prev != nil && prev.budget.percent == u.budget.percent {
		u.budget = prev.budget
	}
	if prev != nil && prev.tuning == u.tuning {
		u.conns = prev.conns
		return u
	}
	u.conns = &connections{transport: newTransport(u.tuning)}
	return u
}

// Retire releases the idle connections of a transport that next (the
// upstream replacing this one, or nil) did not take over. In-flight requests
// keep their connections.
func (u *Upstream) Retire(next *Upstream) {
	if next != nil && next.conns == u.conns {
		return
	}
	u.conns.retired.Store(true)
	u.conns.transport.CloseIdleConnections()
}

// Forget removes the metric series of backends that are not in next, the
// upstream replacing this one, and of the whole pool when next is nil. A
// request still finishing on a removed backend counts into a series that is
// no longer exported.
func (u *Upstream) Forget(next *Upstream) {
	kept := make(map[string]bool)
	if next != nil {
		for b := range next.backends {
			kept[b.Addr] = true
		}
	}
	for b := range u.backends {
		if kept[b.Addr] {
			continue
		}
		u.registry.Delete(metricUpstreamRequests, "pool", u.name, "backend", b.Addr, "result", resultOK)
		u.registry.Delete(metricUpstreamRequests, "pool", u.name, "backend", b.Addr, "result", resultError)
	}
	if next != nil {
		return
	}
	u.registry.Delete(metricUpstreamDuration, "pool", u.name)
	u.registry.Delete(metricRetries, "pool", u.name)
	u.registry.Delete(metricBudgetExhausted, "pool", u.name)
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
	} else {
		counters.ok.Inc()
	}
	if u.conns.retired.Load() {
		// The connection of the attempt that just ended is idle by now.
		u.conns.transport.CloseIdleConnections()
	}
}

// abandon returns a picked backend that produced no outcome worth sampling,
// such as an attempt the client cancelled.
func (u *Upstream) abandon(b *core.Backend) {
	u.pool.Done(b, core.Result{})
}
