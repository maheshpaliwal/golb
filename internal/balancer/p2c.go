package balancer

import (
	"math/rand/v2"

	"github.com/maheshpaliwal/golb/internal/core"
)

const halfWord = 32

// p2cEWMA compares two random backends and takes the cheaper one. A backend
// without a latency sample costs almost nothing, so it is probed first.
type p2cEWMA struct {
	backends []*core.Backend
}

func (p *p2cEWMA) Pick(info core.PickInfo) *core.Backend {
	n := uint64(len(p.backends))
	if n == 1 {
		if usable(p.backends[0], info.Exclude) {
			return p.backends[0]
		}
		return nil
	}
	// One random word yields both indexes: each half is scaled into its range
	// by multiply-shift, and the second skips over the first so they differ.
	word := rand.Uint64()
	i := (word >> halfWord) * n >> halfWord
	j := (word & (1<<halfWord - 1)) * (n - 1) >> halfWord
	if j >= i {
		j++
	}
	first, second := p.backends[i], p.backends[j]
	firstOK, secondOK := usable(first, info.Exclude), usable(second, info.Exclude)
	switch {
	case firstOK && secondOK:
		if cost(second) < cost(first) {
			return second
		}
		return first
	case firstOK:
		return first
	case secondOK:
		return second
	}
	return p.cheapest(int(i), info.Exclude)
}

// cheapest is the fallback when both candidates were unusable.
func (p *p2cEWMA) cheapest(start int, exclude []*core.Backend) *core.Backend {
	var best *core.Backend
	var bestCost float64
	for step := range p.backends {
		b := p.backends[(start+step)%len(p.backends)]
		if !usable(b, exclude) {
			continue
		}
		c := cost(b)
		if best == nil || c < bestCost {
			best, bestCost = b, c
		}
	}
	return best
}

func cost(b *core.Backend) float64 {
	return float64(b.Latency()+1) * float64(b.Inflight()+1) / float64(b.Weight)
}
