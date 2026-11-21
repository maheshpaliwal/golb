// Package ratelimit is a lock-free token bucket.
package ratelimit

import (
	"sync/atomic"
	"time"
)

// Bucket admits rate requests per second with bursts of up to burst. It is
// the generic cell rate algorithm: one atomic word holds the time at which the
// bucket would be full again, so Allow is a single compare-and-swap.
type Bucket struct {
	interval int64 // nanoseconds per token
	slack    int64 // how far the fill time may run ahead of now
	fullAt   atomic.Int64
}

// New returns a full bucket. rate must be positive and burst at least 1.
func New(rate float64, burst int) *Bucket {
	interval := max(int64(float64(time.Second)/rate), 1)
	return &Bucket{
		interval: interval,
		slack:    interval * int64(burst-1),
	}
}

// Allow takes one token at now (unix nanoseconds) and reports whether one was available.
func (b *Bucket) Allow(now int64) bool {
	for {
		old := b.fullAt.Load()
		base := max(old, now)
		if base-now > b.slack {
			return false
		}
		if b.fullAt.CompareAndSwap(old, base+b.interval) {
			return true
		}
	}
}
