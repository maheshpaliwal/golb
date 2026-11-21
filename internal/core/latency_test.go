package core

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestObserveLatencyAt(t *testing.T) {
	const start = int64(time.Hour)
	type sample struct {
		after   time.Duration // time since the previous sample
		latency time.Duration
	}
	tests := []struct {
		name    string
		samples []sample
		want    time.Duration
	}{
		{name: "first sample is taken as is", samples: []sample{{latency: 8 * time.Millisecond}}, want: 8 * time.Millisecond},
		{name: "a close sample moves the average by an eighth", samples: []sample{{latency: 8 * time.Millisecond}, {after: time.Millisecond, latency: 16 * time.Millisecond}}, want: 9 * time.Millisecond},
		{name: "one spike among close samples decays slowly", samples: []sample{{latency: time.Millisecond}, {after: time.Millisecond, latency: 201 * time.Millisecond}, {after: time.Millisecond, latency: time.Millisecond}}, want: 22875 * time.Microsecond},
		{name: "a sample after a quiet second replaces the spike", samples: []sample{{latency: time.Millisecond}, {after: time.Millisecond, latency: 201 * time.Millisecond}, {after: latencyStaleAfter, latency: time.Millisecond}}, want: time.Millisecond},
		{name: "just under a second still smooths", samples: []sample{{latency: 80 * time.Millisecond}, {after: latencyStaleAfter - 1, latency: 0}}, want: 70*time.Millisecond + 1},
		{name: "a zero sample does not read as unsampled", samples: []sample{{latency: 0}}, want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBackend("127.0.0.1:9001", 1)
			now := start
			for _, s := range tc.samples {
				now += int64(s.after)
				b.ObserveLatencyAt(s.latency, now)
			}
			if got := b.Latency(); got != tc.want {
				t.Errorf("latency: got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTryProbe(t *testing.T) {
	const at = int64(time.Hour)
	b := NewBackend("127.0.0.1:9001", 1)
	if b.TryProbe(at + int64(time.Hour)) {
		t.Error("probe granted for a backend without a sample; it has no average to distrust")
	}
	b.ObserveLatencyAt(time.Millisecond, at)
	steps := []struct {
		name  string
		after time.Duration // since the sample
		want  bool
	}{
		{name: "right after a sample", after: 0, want: false},
		{name: "just before the average goes stale", after: latencyStaleAfter - 1, want: false},
		{name: "once it is stale", after: latencyStaleAfter, want: true},
		{name: "second caller in the same period", after: latencyStaleAfter, want: false},
		{name: "while the probe is still in flight", after: 2*latencyStaleAfter - 1, want: false},
		{name: "a period after the last probe", after: 2 * latencyStaleAfter, want: true},
	}
	for _, step := range steps {
		if got := b.TryProbe(at + int64(step.after)); got != step.want {
			t.Errorf("%s: got %v, want %v", step.name, got, step.want)
		}
	}
	b.ObserveLatencyAt(time.Millisecond, at+int64(3*latencyStaleAfter))
	if b.TryProbe(at + int64(3*latencyStaleAfter) + 1) {
		t.Error("probe granted right after a fresh sample")
	}
}

func TestTryProbeGrantsOneOfManyConcurrentCallers(t *testing.T) {
	b := NewBackend("127.0.0.1:9001", 1)
	const at = int64(time.Hour)
	b.ObserveLatencyAt(200*time.Millisecond, at)
	var granted atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			if b.TryProbe(at + int64(latencyStaleAfter)) {
				granted.Add(1)
			}
		})
	}
	wg.Wait()
	if got := granted.Load(); got != 1 {
		t.Errorf("got %d probes granted to 64 concurrent callers, want 1", got)
	}
}

func TestObserveLatencyUsesNow(t *testing.T) {
	b := NewBackend("127.0.0.1:9001", 1)
	b.ObserveLatency(5 * time.Millisecond)
	if b.Latency() != 5*time.Millisecond || b.TryProbe(Now()) {
		t.Error("right after ObserveLatency: want 5ms and no probe")
	}
	if !b.TryProbe(Now() + int64(latencyStaleAfter)) {
		t.Error("no probe one second after the sample")
	}
}

func TestNowIsMonotonicAndNearWallClock(t *testing.T) {
	prev := Now()
	for range 10_000 {
		now := Now()
		if now < prev {
			t.Fatalf("Now went backwards: %d after %d", now, prev)
		}
		prev = now
	}
	if diff := time.Duration(time.Now().UnixNano() - Now()); diff < -time.Second || diff > time.Second {
		t.Errorf("Now is %v away from the wall clock", diff)
	}
}

func TestLatencyPathDoesNotAllocate(t *testing.T) {
	b := NewBackend("127.0.0.1:9001", 1)
	allocs := testing.AllocsPerRun(1000, func() {
		b.ObserveLatency(time.Millisecond)
		_ = b.TryProbe(Now())
	})
	if allocs != 0 {
		t.Errorf("got %v allocs, want 0", allocs)
	}
}

func BenchmarkNow(b *testing.B) {
	b.ReportAllocs()
	var sink int64
	for b.Loop() {
		sink += Now()
	}
	if sink == 0 {
		b.Fatal("clock returned zero")
	}
}

func BenchmarkObserveLatency(b *testing.B) {
	backend := NewBackend("127.0.0.1:9001", 1)
	b.ReportAllocs()
	for b.Loop() {
		backend.ObserveLatency(time.Millisecond)
	}
}
