package balancer

import (
	"sync/atomic"

	"github.com/maheshpaliwal/golb/internal/core"
)

// leastConn picks the lowest in-flight count per unit of weight. Backends
// tied for lowest, which is all of them when the pool is idle, share the
// picks in proportion to their weights.
type leastConn struct {
	backends []*core.Backend
	schedule []*core.Backend // the round-robin schedule; orders idle picks
	_        [cacheLine]byte
	next     atomic.Uint64 // position in schedule
	ties     atomic.Uint64 // position among tied backends
	_        [cacheLine]byte
}

func newLeastConn(backends []*core.Backend) *leastConn {
	return &leastConn{backends: backends, schedule: smoothSchedule(backends)}
}

func (l *leastConn) Pick(info core.PickInfo) *core.Backend {
	// Nothing is less loaded than an idle backend, so when the next one in
	// weighted rotation is idle the scan is skipped. An idle pool is thereby
	// served exactly like weighted round robin.
	b := l.schedule[l.next.Add(1)%uint64(len(l.schedule))]
	if b.Inflight() <= 0 && usable(b, info.Exclude) {
		return b
	}
	return l.scan(info.Exclude)
}

func (l *leastConn) scan(exclude []*core.Backend) *core.Backend {
	var best *core.Backend
	var bestLoad int64
	var tiedWeight uint64
	for _, b := range l.backends {
		if !usable(b, exclude) {
			continue
		}
		load := b.Inflight()
		switch order := compareLoad(load, b, bestLoad, best); {
		case order < 0:
			best, bestLoad, tiedWeight = b, load, uint64(b.Weight)
		case order == 0:
			tiedWeight += uint64(b.Weight)
		}
	}
	if best == nil || tiedWeight == uint64(best.Weight) {
		return best
	}
	// Several backends are equally loaded. A counter of their own walks
	// their combined weight, so each is chosen weight times per cycle and
	// an unusable backend's share goes to all of them, not to a neighbour.
	// The schedule position cannot be reused for this: it is correlated
	// with which backend sent the pick here.
	target := l.ties.Add(1) % tiedWeight
	for _, b := range l.backends {
		if !usable(b, exclude) || compareLoad(b.Inflight(), b, bestLoad, best) != 0 {
			continue
		}
		if target < uint64(b.Weight) {
			return b
		}
		target -= uint64(b.Weight)
	}
	// Loads moved between the two passes and the tie came out lighter than
	// counted; best was a minimum a moment ago.
	return best
}

// compareLoad orders load/b.Weight against otherLoad/other.Weight by
// cross-multiplying, in integers. A nil other is greater than everything.
func compareLoad(load int64, b *core.Backend, otherLoad int64, other *core.Backend) int {
	if other == nil {
		return -1
	}
	left, right := load*int64(other.Weight), otherLoad*int64(b.Weight)
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	}
	return 0
}
