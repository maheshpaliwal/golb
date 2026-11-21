package proxy

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestBudget(t *testing.T) {
	const now = uint64(1_700_000_000)
	type step struct {
		sec      uint64
		requests int
		retries  int // attempted
		allowed  int // expected to pass
	}
	tests := []struct {
		name    string
		percent int
		steps   []step
	}{
		{
			name:    "floor lets a quiet route retry",
			percent: 20,
			steps:   []step{{sec: now, requests: 1, retries: 10, allowed: budgetFloorPerSecond}},
		},
		{
			name:    "percent of recent requests",
			percent: 20,
			steps:   []step{{sec: now, requests: 100, retries: 50, allowed: 20}},
		},
		{
			name:    "floor applies when percent is zero",
			percent: 0,
			steps:   []step{{sec: now, requests: 1000, retries: 10, allowed: budgetFloorPerSecond}},
		},
		{
			name:    "floor renews each second",
			percent: 0,
			steps: []step{
				{sec: now, retries: 5, allowed: budgetFloorPerSecond},
				{sec: now + 1, retries: 5, allowed: budgetFloorPerSecond},
			},
		},
		{
			name:    "requests inside the window still count",
			percent: 10,
			steps: []step{
				{sec: now, requests: 100},
				{sec: now + budgetWindowSeconds - 1, retries: 30, allowed: 10},
			},
		},
		{
			name:    "requests older than the window expire",
			percent: 10,
			steps: []step{
				{sec: now, requests: 100},
				{sec: now + budgetWindowSeconds, retries: 30, allowed: budgetFloorPerSecond},
			},
		},
		{
			name:    "retries spent earlier in the window count against the percent",
			percent: 10,
			steps: []step{
				{sec: now, requests: 100, retries: 10, allowed: 10},
				{sec: now + 1, retries: 10, allowed: budgetFloorPerSecond},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newBudget(tc.percent)
			for i, s := range tc.steps {
				for range s.requests {
					b.recordRequest(s.sec)
				}
				allowed := 0
				for range s.retries {
					if b.allowRetry(s.sec) {
						allowed++
					}
				}
				if allowed != s.allowed {
					t.Errorf("step %d: %d of %d retries allowed, want %d", i, allowed, s.retries, s.allowed)
				}
			}
		})
	}
}

func TestBudgetConcurrentNeverOverspends(t *testing.T) {
	const (
		now        = uint64(1_700_000_000)
		requests   = 1000
		percent    = 10
		goroutines = 16
		tries      = 100
	)
	b := newBudget(percent)
	for range requests {
		b.recordRequest(now)
	}
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range tries {
				if b.allowRetry(now) {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	const want = requests * percent / percentScale
	if got := allowed.Load(); got != want {
		t.Errorf("allowed %d retries, want exactly %d", got, want)
	}
}
