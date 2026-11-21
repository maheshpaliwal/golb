package proxy

import "sync/atomic"

const (
	budgetWindowSeconds  = 10
	budgetFloorPerSecond = 3
	percentScale         = 100
	countBits            = 32
	countMask            = 1<<countBits - 1
)

// budget caps retries at a percentage of the requests seen in the last
// budgetWindowSeconds, plus a small per-second floor so quiet routes can
// still retry. Each cell packs the second it belongs to with a count, which
// makes expiry and increment one compare-and-swap with no lock.
type budget struct {
	percent  uint64
	requests [budgetWindowSeconds]atomic.Uint64
	retries  [budgetWindowSeconds]atomic.Uint64
}

func newBudget(percent int) *budget {
	return &budget{percent: uint64(percent)}
}

// recordRequest counts one client request at sec (unix seconds).
func (b *budget) recordRequest(sec uint64) {
	cell := &b.requests[sec%budgetWindowSeconds]
	for {
		old := cell.Load()
		if cell.CompareAndSwap(old, bumped(old, sec)) {
			return
		}
	}
}

// allowRetry reserves one retry at sec and reports whether the budget had room.
func (b *budget) allowRetry(sec uint64) bool {
	cell := &b.retries[sec%budgetWindowSeconds]
	for {
		old := cell.Load()
		thisSecond := uint64(0)
		if old>>countBits == sec {
			thisSecond = old & countMask
		}
		withinPercent := (windowSum(&b.retries, sec)+1)*percentScale <= windowSum(&b.requests, sec)*b.percent
		if !withinPercent && thisSecond >= budgetFloorPerSecond {
			return false
		}
		if cell.CompareAndSwap(old, bumped(old, sec)) {
			return true
		}
	}
}

func bumped(old, sec uint64) uint64 {
	if old>>countBits == sec {
		return old + 1
	}
	return sec<<countBits | 1
}

func windowSum(cells *[budgetWindowSeconds]atomic.Uint64, sec uint64) uint64 {
	var total uint64
	for i := range cells {
		v := cells[i].Load()
		// Unsigned subtraction also discards cells stamped after sec.
		if sec-v>>countBits < budgetWindowSeconds {
			total += v & countMask
		}
	}
	return total
}
