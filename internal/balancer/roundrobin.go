package balancer

import (
	"sync/atomic"

	"github.com/maheshpaliwal/golb/internal/core"
)

// roundRobin steps an atomic counter through a precomputed schedule in which
// each backend appears weight/GCD times, interleaved smoothly.
type roundRobin struct {
	backends []*core.Backend
	schedule []*core.Backend
	next     atomic.Uint64
}

func newRoundRobin(backends []*core.Backend) *roundRobin {
	return &roundRobin{backends: backends, schedule: smoothSchedule(backends)}
}

func (r *roundRobin) Pick(info core.PickInfo) *core.Backend {
	slot := r.next.Add(1) - 1
	b := r.schedule[slot%uint64(len(r.schedule))]
	if usable(b, info.Exclude) {
		return b
	}
	return r.pickSkipping(slot, info.Exclude)
}

// pickSkipping walks the schedule past unusable backends, which keeps the
// weight ratio among the rest. The walk is bounded by the backend count, not
// the schedule length, so a pool with one heavy dead backend does not scan
// hundreds of slots; a direct scan then guarantees nil only when nothing is
// usable.
func (r *roundRobin) pickSkipping(slot uint64, exclude []*core.Backend) *core.Backend {
	slots := uint64(len(r.schedule))
	for step := uint64(1); step <= uint64(len(r.backends)); step++ {
		b := r.schedule[(slot+step)%slots]
		if usable(b, exclude) {
			return b
		}
	}
	start := int(slot % uint64(len(r.backends)))
	for i := range r.backends {
		b := r.backends[(start+i)%len(r.backends)]
		if usable(b, exclude) {
			return b
		}
	}
	return nil
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
