// Package upstream implements core.Pool: a balancer over a backend set with
// active health checks and passive outlier ejection.
package upstream

import (
	"fmt"
	"sync"

	"github.com/maheshpaliwal/golb/internal/balancer"
	"github.com/maheshpaliwal/golb/internal/core"
	"github.com/maheshpaliwal/golb/internal/health"
)

type pool struct {
	backends []*core.Backend
	balancer core.Balancer
	outlier  *health.Outlier // nil when disabled
	checker  *health.Checker // nil without a health check

	closeOnce sync.Once
}

// New builds a pool and starts its health checks. spec must already have
// defaults applied and be valid. prev may be nil; when given, a backend whose
// Addr and Weight are unchanged reuses the *core.Backend from prev, so health,
// in-flight and latency state survive a reload. obs may be nil.
func New(spec core.PoolSpec, prev core.Pool, obs core.Observer) (core.Pool, error) {
	err := spec.Validate()
	if err != nil {
		return nil, fmt.Errorf("build pool: %w", err)
	}
	if obs == nil {
		obs = func(core.Event) {}
	}
	p := &pool{backends: carryOver(spec.Backends, prev)}
	p.balancer, err = balancer.New(spec.Policy, p.backends)
	if err != nil {
		return nil, fmt.Errorf("build pool %q: %w", spec.Name, err)
	}

	p.startOutlier(spec, prev, obs)
	p.startChecker(spec, obs)
	return p, nil
}

// startOutlier and startChecker also reset state carried over from the
// previous pool that the new spec could never undo: with the outlier
// detector off nothing would clear an ejection deadline, and with no health
// check nothing would mark a backend healthy again.
func (p *pool) startOutlier(spec core.PoolSpec, prev core.Pool, obs core.Observer) {
	if spec.Outlier.Disabled {
		for _, b := range p.backends {
			b.Eject(0)
		}
		return
	}
	p.outlier = health.NewOutlier(health.OutlierConfig{
		Pool:     spec.Name,
		Spec:     spec.Outlier,
		Backends: p.backends,
		Observer: obs,
		Prev:     previousOutlier(prev),
	})
}

func (p *pool) startChecker(spec core.PoolSpec, obs core.Observer) {
	if spec.Health.Type == core.CheckNone {
		for _, b := range p.backends {
			if b.SetHealthy(true) {
				obs(core.Event{Pool: spec.Name, Backend: b, Kind: core.BackendUp})
			}
		}
		return
	}
	p.checker = health.StartChecker(health.CheckerConfig{
		Pool:     spec.Name,
		Spec:     spec.Health,
		Backends: p.backends,
		Observer: obs,
	})
}

func (p *pool) Pick(info core.PickInfo) *core.Backend {
	b := p.balancer.Pick(info)
	if b != nil {
		b.Acquire()
	}
	return b
}

// Done does not sample the latency of a failed attempt: a connection refused
// in microseconds would make a broken backend look like the fastest one to
// p2c_ewma. Failures reach the outlier detector instead.
func (p *pool) Done(b *core.Backend, res core.Result) {
	if b == nil {
		return
	}
	b.Release()
	if res.Latency > 0 && !res.Failed {
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
	})
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

// previousOutlier returns the detector to inherit failure streaks from, or
// nil when prev is absent, is another Pool implementation, or had none.
func previousOutlier(prev core.Pool) *health.Outlier {
	old, ok := prev.(*pool)
	if !ok {
		return nil
	}
	return old.outlier
}
