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
