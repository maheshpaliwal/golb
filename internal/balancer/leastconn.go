package balancer

import (
	"sync/atomic"

	"github.com/maheshpaliwal/golb/internal/core"
)

// leastConn picks the lowest in-flight count per unit of weight. The scan
// starts at a rotating position so equally loaded backends share traffic.
type leastConn struct {
	backends []*core.Backend
	next     atomic.Uint64
}

func (l *leastConn) Pick(info core.PickInfo) *core.Backend {
	n := len(l.backends)
	start := int(l.next.Add(1) % uint64(n))
	var best *core.Backend
	var bestLoad int64
	for step := range n {
		i := start + step
		if i >= n {
			i -= n
		}
		b := l.backends[i]
		if !usable(b, info.Exclude) {
			continue
		}
		load := b.Inflight()
		if load <= 0 {
			// Nothing can be lower than idle, so the rest of the scan is skipped.
			return b
		}
		// load/weight < bestLoad/bestWeight, cross-multiplied to stay in integers.
		if best == nil || load*int64(best.Weight) < bestLoad*int64(b.Weight) {
			best, bestLoad = b, load
		}
	}
	return best
}
