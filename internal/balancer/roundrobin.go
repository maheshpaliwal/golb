package balancer

import (
	"sync/atomic"

	"github.com/maheshpaliwal/golb/internal/core"
)

// cacheLine is the largest line size among supported CPUs (Apple arm64).
const cacheLine = 128

// roundRobin steps an atomic counter through a precomputed schedule in which
// each backend appears weight/GCD times, interleaved smoothly.
type roundRobin struct {
	backends []*core.Backend
	schedule []*core.Backend
	// The counter is written on every pick; the padding keeps those writes
	// from invalidating the line that holds the read-only slice headers.
	_       [cacheLine]byte
	next    atomic.Uint64
	retries atomic.Uint64
	_       [cacheLine]byte
}

func newRoundRobin(backends []*core.Backend) *roundRobin {
	return &roundRobin{backends: backends, schedule: smoothSchedule(backends)}
}

func (r *roundRobin) Pick(info core.PickInfo) *core.Backend {
	if len(info.Exclude) != 0 {
		return r.pickRetry(info.Exclude)
	}
	b := r.schedule[(r.next.Add(1)-1)%uint64(len(r.schedule))]
	if available(b) {
		return b
	}
	return r.redraw()
}

// redraw takes further slots from the counter until one holds an available
// backend. Every slot of an unavailable backend is passed over this way, so
// the available ones are picked in the ratio of their own slots; handing a
// skipped slot to the next backend in order would give that neighbour the
// whole share of the one that is down.
func (r *roundRobin) redraw() *core.Backend {
	if !anyAvailable(r.backends) {
		return nil
	}
	slots := uint64(len(r.schedule))
	for range slots {
		b := r.schedule[(r.next.Add(1)-1)%slots]
		if available(b) {
			return b
		}
	}
	// A full cycle of draws passes every slot unless concurrent picks took
	// some of them; a direct scan settles that case.
	return firstUsable(r.backends, 0, nil)
}

// pickRetry serves a retry without advancing the main counter. If retries
// consumed its slots, a pool of two with one failing backend that is not yet
// ejected would start every request on the failing one: first attempt and
// retry together take two slots, which brings the counter back to it.
// Retries step their own counter so that a burst of them spreads out.
func (r *roundRobin) pickRetry(exclude []*core.Backend) *core.Backend {
	slots := uint64(len(r.schedule))
	slot := r.next.Load() + r.retries.Add(1)
	for step := range uint64(len(r.backends)) {
		b := r.schedule[(slot+step)%slots]
		if usable(b, exclude) {
			return b
		}
	}
	return firstUsable(r.backends, int(slot%uint64(len(r.backends))), exclude)
}

// smoothSchedule is nginx's smooth weighted round robin run for one full
// cycle: every step each backend gains its weight, the highest total is
// chosen and pays back the sum of weights. Equal weights need no schedule.
func smoothSchedule(backends []*core.Backend) []*core.Backend {
	units, total := weightUnits(backends)
	if total == len(backends) {
		return backends
	}
	current := make([]int, len(backends))
	schedule := make([]*core.Backend, 0, total)
	for range total {
		best := 0
		for i, unit := range units {
			current[i] += int(unit)
			if current[i] > current[best] {
				best = i
			}
		}
		current[best] -= total
		schedule = append(schedule, backends[best])
	}
	return schedule
}
