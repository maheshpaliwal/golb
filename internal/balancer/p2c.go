package balancer

import (
	"math/rand/v2"

	"github.com/maheshpaliwal/golb/internal/core"
)

const (
	// One random word is cut into two index fields and a probe field.
	indexBits  = 24
	indexMask  = 1<<indexBits - 1
	firstShift = 64 - indexBits
	nextShift  = firstShift - indexBits
	// probeMask selects one pick in sixteen to consider a probe; only
	// those picks read the clock.
	probeMask = 1<<4 - 1
)

// p2cEWMA compares two random backends and takes the cheaper one.
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
	// Each index field is scaled into its range by multiply-shift, and the
	// second skips over the first so the two differ.
	word := rand.Uint64()
	i := (word >> firstShift) * n >> indexBits
	j := (word >> nextShift & indexMask) * (n - 1) >> indexBits
	if j >= i {
		j++
	}
	first, second := p.backends[i], p.backends[j]
	firstOK, secondOK := usable(first, info.Exclude), usable(second, info.Exclude)
	switch {
	case firstOK && secondOK:
		return choose(first, second, word)
	case firstOK:
		return first
	case secondOK:
		return second
	}
	return p.cheapest(info.Exclude)
}

// choose returns the cheaper of two usable backends, with two corrections.
//
// A backend without a latency sample is compared as if it had the other's
// latency, so load and weight decide. Treating "unknown" as "fastest" would
// send a new backend half of all traffic until its first response returns.
//
// The costlier backend wins when it is due a probe. Cost only changes when
// a sample arrives, and a backend that looks slow gets no requests and
// therefore no samples; without probes one slow response would exclude it
// for good. core.Backend grants one probe per second without samples, so a
// backend that really is slow receives about one request a second.
func choose(first, second *core.Backend, word uint64) *core.Backend {
	firstLatency, secondLatency := int64(first.Latency()), int64(second.Latency())
	if firstLatency == 0 {
		firstLatency = secondLatency
	}
	if secondLatency == 0 {
		secondLatency = firstLatency
	}
	cheap, costly := first, second
	if cost(second, secondLatency) < cost(first, firstLatency) {
		cheap, costly = second, first
	}
	if word&probeMask == 0 && costly.TryProbe(core.Now()) {
		return costly
	}
	return cheap
}

// cheapest is the fallback when both candidates were unusable. Unsampled
// backends are costed at the mean latency of the sampled usable ones, and
// equally cheap backends are chosen between at random: taking the first in
// slice order would favour whichever follows the unusable ones.
func (p *p2cEWMA) cheapest(exclude []*core.Backend) *core.Backend {
	var sum, sampled int64
	for _, b := range p.backends {
		if latency := int64(b.Latency()); latency != 0 && usable(b, exclude) {
			sum += latency
			sampled++
		}
	}
	var mean int64
	if sampled != 0 {
		mean = sum / sampled
	}
	var best *core.Backend
	var bestCost float64
	var tied uint64
	for _, b := range p.backends {
		if !usable(b, exclude) {
			continue
		}
		latency := int64(b.Latency())
		if latency == 0 {
			latency = mean
		}
		c := cost(b, latency)
		switch {
		case best == nil || c < bestCost:
			best, bestCost, tied = b, c, 1
		case c == bestCost:
			tied++
			if rand.Uint64N(tied) == 0 {
				best = b
			}
		}
	}
	return best
}

func cost(b *core.Backend, latencyNanos int64) float64 {
	return float64(latencyNanos+1) * float64(b.Inflight()+1) / float64(b.Weight)
}
