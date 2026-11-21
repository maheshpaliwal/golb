// Package health decides which backends may receive traffic: Checker probes
// them actively, Outlier ejects them on consecutive request failures.
package health

import (
	"sync/atomic"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

const percent = 100

type OutlierConfig struct {
	Pool     string
	Spec     core.Outlier // enabled, defaults applied, valid
	Backends []*core.Backend
	Observer core.Observer
	// Prev is the detector of the pool being replaced by a reload. Backends
	// present in both keep their failure streak and ejection count.
	Prev *Outlier
}

// Outlier ejects a backend after consecutive failures, for a time that grows
// with each ejection. Record is safe for concurrent use and never blocks.
type Outlier struct {
	pool       string
	threshold  int32
	base       time.Duration
	max        time.Duration
	maxEjected int
	backends   []*core.Backend
	states     map[*core.Backend]*outlierState // never written after NewOutlier
	// busy is a try-lock around ejection and recovery decisions, which read
	// and write several backends. A caller that finds it taken skips the
	// decision instead of waiting; the next result retries. It is shared with
	// Prev so two pool generations cannot both eject up to the limit.
	busy     *atomic.Bool
	observer core.Observer
	now      func() int64
}

type outlierState struct {
	fails     atomic.Int32 // consecutive failures since the last success or decision
	ejections atomic.Int32 // multiplier for the next ejection time
	// healthySince is the unix-nanosecond start of the failure-free period
	// that decays ejections: the end of the last ejection, or a later failure.
	healthySince atomic.Int64
}

func NewOutlier(cfg OutlierConfig) *Outlier {
	o := &Outlier{
		pool:       cfg.Pool,
		threshold:  int32(cfg.Spec.ConsecutiveFailures),
		base:       cfg.Spec.BaseEjection,
		max:        cfg.Spec.MaxEjection,
		maxEjected: len(cfg.Backends) * cfg.Spec.MaxEjectedPercent / percent,
		backends:   cfg.Backends,
		states:     make(map[*core.Backend]*outlierState, len(cfg.Backends)),
		busy:       new(atomic.Bool),
		observer:   cfg.Observer,
		now:        func() int64 { return time.Now().UnixNano() },
	}
	if o.observer == nil {
		o.observer = func(core.Event) {}
	}
	if cfg.Prev != nil {
		o.busy = cfg.Prev.busy
	}
	for _, b := range cfg.Backends {
		if cfg.Prev != nil && cfg.Prev.states[b] != nil {
			o.states[b] = cfg.Prev.states[b]
			continue
		}
		o.states[b] = new(outlierState)
	}
	return o
}

// Record feeds one request outcome. A success on a backend with no failure
// history costs a map read and three atomic loads, and never reads the clock.
func (o *Outlier) Record(b *core.Backend, failed bool) {
	st := o.states[b]
	if st == nil {
		return
	}
	if failed {
		o.fail(b, st)
		return
	}
	if st.fails.Load() != 0 {
		st.fails.Store(0)
	}
	if st.ejections.Load() == 0 && b.Available(0) {
		return
	}
	o.settle(b, st)
}

func (o *Outlier) fail(b *core.Backend, st *outlierState) {
	fails := st.fails.Add(1)
	now := o.now()
	if st.ejections.Load() > 0 && now > st.healthySince.Load() {
		st.healthySince.Store(now)
	}
	if fails < o.threshold || !o.busy.CompareAndSwap(false, true) {
		return
	}
	ejected := o.eject(b, st, now)
	o.busy.Store(false)
	if ejected {
		o.observer(core.Event{Pool: o.pool, Backend: b, Kind: core.BackendEjected})
	}
}

// eject runs with busy held. The streak restarts whether or not the backend
// is ejected, so a pool at its limit re-evaluates once per threshold
// failures rather than on every failure.
func (o *Outlier) eject(b *core.Backend, st *outlierState, now int64) bool {
	st.fails.Store(0)
	if b.Ejected(now) || o.ejected(now) >= o.maxEjected {
		return false
	}
	count := st.ejections.Load() + 1
	duration := o.base * time.Duration(count)
	if duration > o.max {
		// The multiplier stops growing at the cap so it cannot overflow and
		// decays back in bounded time.
		duration = o.max
		count--
	}
	st.ejections.Store(count)
	until := now + int64(duration)
	st.healthySince.Store(until)
	b.Eject(until)
	return true
}

func (o *Outlier) ejected(now int64) int {
	n := 0
	for _, b := range o.backends {
		if b.Ejected(now) {
			n++
		}
	}
	return n
}

// settle handles a success on a backend that has ejection history. It
// clears an expired ejection deadline, which lets balancers test
// availability without reading the clock again, and lowers the ejection
// multiplier by one for every base_ejection that passed without a failure.
func (o *Outlier) settle(b *core.Backend, st *outlierState) {
	now := o.now()
	if b.Ejected(now) {
		return
	}
	expired := b.Ejected(0)
	steps := o.decaySteps(st, now)
	if !expired && steps == 0 {
		return
	}
	if !o.busy.CompareAndSwap(false, true) {
		return
	}
	if expired && !b.Ejected(now) {
		b.Eject(0)
	}
	if steps > 0 {
		st.ejections.Store(max(0, st.ejections.Load()-steps))
		st.healthySince.Store(now)
	}
	o.busy.Store(false)
}

func (o *Outlier) decaySteps(st *outlierState, now int64) int32 {
	count := st.ejections.Load()
	if count == 0 {
		return 0
	}
	periods := (now - st.healthySince.Load()) / int64(o.base)
	if periods <= 0 {
		return 0
	}
	return int32(min(periods, int64(count)))
}
