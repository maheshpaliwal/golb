package main

import (
	"math"
	"math/bits"
	"time"
)

const (
	// Values below linearLimit nanoseconds get one bucket each. Above it
	// every power of two is split into subBuckets, so a bucket is at most
	// 1/128 = 0.8% wide relative to its value.
	subBucketBits = 7
	subBuckets    = 1 << subBucketBits
	linearLimit   = 2 * subBuckets
	wordBits      = 64
	bucketCount   = linearLimit + (wordBits-subBucketBits-1)*subBuckets
)

// histogram records durations without allocating, so the load generator's
// own garbage does not disturb what it measures.
type histogram struct {
	counts [bucketCount]uint64
	total  uint64
	sum    float64 // nanoseconds
	max    time.Duration
}

func bucketOf(nanos uint64) int {
	if nanos < linearLimit {
		return int(nanos)
	}
	shift := bits.Len64(nanos) - (subBucketBits + 1)
	return linearLimit + (shift-1)*subBuckets + int(nanos>>shift) - subBuckets
}

// bucketTop returns the largest value that falls into bucket i.
func bucketTop(i int) uint64 {
	if i < linearLimit {
		return uint64(i)
	}
	shift := (i-linearLimit)/subBuckets + 1
	low := uint64((i-linearLimit)%subBuckets+subBuckets) << shift
	return low + (1 << shift) - 1
}

func (h *histogram) record(d time.Duration) {
	d = max(d, 0)
	h.counts[bucketOf(uint64(d))]++
	h.total++
	h.sum += float64(d)
	h.max = max(h.max, d)
}

func (h *histogram) merge(other *histogram) {
	for i, n := range other.counts {
		h.counts[i] += n
	}
	h.total += other.total
	h.sum += other.sum
	h.max = max(h.max, other.max)
}

// quantile returns the value below which fraction q of the samples fall,
// rounded up to its bucket's top and never above the recorded maximum.
func (h *histogram) quantile(q float64) time.Duration {
	if h.total == 0 {
		return 0
	}
	rank := max(uint64(math.Ceil(q*float64(h.total))), 1)
	var seen uint64
	for i, n := range h.counts {
		seen += n
		if seen >= rank {
			return min(time.Duration(bucketTop(i)), h.max)
		}
	}
	return h.max
}

func (h *histogram) mean() time.Duration {
	if h.total == 0 {
		return 0
	}
	return time.Duration(h.sum / float64(h.total))
}
