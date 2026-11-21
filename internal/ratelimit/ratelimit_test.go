package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBucketAllow(t *testing.T) {
	const start = int64(1_000_000_000_000)
	type step struct {
		at   time.Duration
		want bool
	}
	tests := []struct {
		name  string
		rate  float64
		burst int
		steps []step
	}{
		{
			name:  "burst then reject",
			rate:  10,
			burst: 3,
			steps: []step{{at: 0, want: true}, {at: 0, want: true}, {at: 0, want: true}, {at: 0, want: false}},
		},
		{
			name:  "refills one token per interval",
			rate:  10,
			burst: 1,
			steps: []step{
				{at: 0, want: true},
				{at: 50 * time.Millisecond, want: false},
				{at: 100 * time.Millisecond, want: true},
				{at: 150 * time.Millisecond, want: false},
			},
		},
		{
			name:  "idle time does not bank more than burst",
			rate:  10,
			burst: 2,
			steps: []step{
				{at: 0, want: true},
				{at: time.Hour, want: true},
				{at: time.Hour, want: true},
				{at: time.Hour, want: false},
			},
		},
		{
			name:  "fractional rate",
			rate:  0.5,
			burst: 1,
			steps: []step{{at: 0, want: true}, {at: time.Second, want: false}, {at: 2 * time.Second, want: true}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bucket := New(tc.rate, tc.burst)
			for i, s := range tc.steps {
				got := bucket.Allow(start + int64(s.at))
				if got != s.want {
					t.Errorf("step %d at %v: Allow = %v, want %v", i, s.at, got, s.want)
				}
			}
		})
	}
}

func TestBucketConcurrentNeverExceedsBurst(t *testing.T) {
	const (
		burst      = 100
		goroutines = 32
		perRoutine = 200
		now        = int64(5_000_000_000)
	)
	bucket := New(1, burst)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perRoutine {
				if bucket.Allow(now) {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != burst {
		t.Errorf("allowed %d requests at one instant, want exactly the burst %d", got, burst)
	}
}

func BenchmarkBucketAllow(b *testing.B) {
	bucket := New(1e9, 1000)
	now := time.Now().UnixNano()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bucket.Allow(now)
		}
	})
}
