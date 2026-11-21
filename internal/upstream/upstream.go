// Package upstream implements core.Pool: a balancer over a backend set with
// active health checks and passive outlier ejection.
package upstream

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maheshpaliwal/golb/internal/balancer"
	"github.com/maheshpaliwal/golb/internal/core"
	"github.com/maheshpaliwal/golb/internal/health"
)

// failurePenalty is the least latency recorded for a failed attempt. A
// refused connection fails in microseconds; sampled as measured, or not at
// all, it would make a broken backend the cheapest choice for p2c_ewma.
const failurePenalty = time.Second

type pool struct {
	name     string
	backends []*core.Backend
	balancer core.Balancer
	outlier  *health.Outlier // nil when disabled
	checker  *health.Checker // nil without a health check
	observer core.Observer
	// resetHealth and resetEjection say that this pool has nothing that
	// would ever undo an unhealthy verdict or an ejection deadline carried
	// over on a reused backend.
	resetHealth   bool
	resetEjection bool
	// successor is the pool built with this one as prev.
	successor atomic.Pointer[pool]

	closeOnce sync.Once
}

// New builds a pool and starts its health checks. spec must already have
// defaults applied and be valid. prev may be nil; when given, a backend whose
// Addr and Weight are unchanged reuses the *core.Backend from prev, so health,
// in-flight and latency state survive a reload. obs may be nil.
func New(spec core.PoolSpec, prev core.Pool, obs core.Observer) (core.Pool, error) {
	// The caller has validated the spec already; checking again costs
	// microseconds per reload and keeps a zero interval or weight from
	// surfacing as a panic inside a health-check goroutine.
	err := spec.Validate()
	if err != nil {
		return nil, fmt.Errorf("build pool: %w", err)
	}
	if obs == nil {
		obs = func(core.Event) {}
	}
	p := &pool{
		name:          spec.Name,
		backends:      carryOver(spec.Backends, prev),
		observer:      obs,
		resetHealth:   spec.Health.Type == core.CheckNone,
		resetEjection: spec.Outlier.Disabled,
	}
	p.balancer, err = balancer.New(spec.Policy, p.backends)
	if err != nil {
		return nil, fmt.Errorf("build pool %q: %w", spec.Name, err)
	}
	old, _ := prev.(*pool)
	if !spec.Outlier.Disabled {
		cfg := health.OutlierConfig{Pool: spec.Name, Spec: spec.Outlier, Backends: p.backends, Observer: obs}
		if old != nil {
			cfg.Prev = old.outlier
		}
		p.outlier = health.NewOutlier(cfg)
	}
	if spec.Health.Type != core.CheckNone {
		p.checker = health.StartChecker(health.CheckerConfig{Pool: spec.Name, Spec: spec.Health, Backends: p.backends, Observer: obs})
	}
	// The reset runs now so the new pool is usable at once, and again when
	// the previous pool is closed: until then its checker is still running
	// and can mark a shared backend down after this reset, with nothing in
	// this pool to bring it back.
	p.resetCarriedState()
	if old != nil {
		old.successor.Store(p)
	}
	return p, nil
}

func (p *pool) Pick(info core.PickInfo) *core.Backend {
	b := p.balancer.Pick(info)
	if b != nil {
		b.Acquire()
	}
	return b
}

// Done samples a failed attempt at no less than failurePenalty, even when
// the caller gave no latency for it, and reports it to the outlier detector.
func (p *pool) Done(b *core.Backend, res core.Result) {
	if b == nil {
		return
	}
	b.Release()
	switch {
	case res.Failed:
		b.ObserveLatency(max(res.Latency, failurePenalty))
	case res.Latency > 0:
		b.ObserveLatency(res.Latency)
	}
	if p.outlier != nil {
		p.outlier.Record(b, res.Failed)
	}
}

func (p *pool) Backends() []*core.Backend { return p.backends }

func (p *pool) Close() {
	p.closeOnce.Do(func() {
		if p.checker != nil {
			p.checker.Close()
		}
		// This pool's checker can no longer write, so the successor's
		// reset now sticks.
		if next := p.successor.Load(); next != nil {
			next.resetCarriedState()
		}
	})
}

// resetCarriedState clears state inherited on reused backends that this
// pool's configuration could never undo.
func (p *pool) resetCarriedState() {
	for _, b := range p.backends {
		if p.resetEjection {
			b.Eject(0)
		}
		if p.resetHealth && b.SetHealthy(true) {
			p.observer(core.Event{Pool: p.name, Backend: b, Kind: core.BackendUp})
		}
	}
}

// carryOver returns the backends for specs in config order, taking the
// existing value from prev where address and weight are unchanged.
func carryOver(specs []core.BackendSpec, prev core.Pool) []*core.Backend {
	existing := make(map[string]*core.Backend)
	if prev != nil {
		for _, b := range prev.Backends() {
			existing[b.Addr] = b
		}
	}
	backends := make([]*core.Backend, len(specs))
	for i, s := range specs {
		if b := existing[s.Addr]; b != nil && b.Weight == s.Weight {
			backends[i] = b
			continue
		}
		backends[i] = core.NewBackend(s.Addr, s.Weight)
	}
	return backends
}
