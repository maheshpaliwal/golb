package health

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

const (
	testBase = 10 * time.Second
	testMax  = 35 * time.Second
)

// fakeClock is the detector's time source; tests advance it by hand.
type fakeClock struct{ nanos atomic.Int64 }

func (c *fakeClock) now() int64              { return c.nanos.Load() }
func (c *fakeClock) advance(d time.Duration) { c.nanos.Add(int64(d)) }

type eventLog struct {
	mu     sync.Mutex
	events []core.Event
}

func (l *eventLog) observe(e core.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) snapshot() []core.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]core.Event(nil), l.events...)
}

func (l *eventLog) count(kind core.EventKind, b *core.Backend) int {
	n := 0
	for _, e := range l.snapshot() {
		if e.Kind == kind && e.Backend == b {
			n++
		}
	}
	return n
}

func ejected(backends []*core.Backend, now int64) int {
	n := 0
	for _, b := range backends {
		if b.Ejected(now) {
			n++
		}
	}
	return n
}

func discard(core.Event) {}

func testBackends(n int) []*core.Backend {
	backends := make([]*core.Backend, n)
	for i := range backends {
		backends[i] = core.NewBackend(fmt.Sprintf("127.0.0.1:%d", 9000+i), 1)
	}
	return backends
}

type outlierFixture struct {
	detector *Outlier
	backends []*core.Backend
	clock    *fakeClock
	events   *eventLog
}

func newOutlierFixture(backends int, spec core.Outlier) outlierFixture {
	f := outlierFixture{backends: testBackends(backends), clock: new(fakeClock), events: new(eventLog)}
	f.clock.advance(time.Hour)
	f.detector = NewOutlier(OutlierConfig{Pool: "app", Spec: spec, Backends: f.backends, Observer: f.events.observe})
	f.detector.now = f.clock.now
	return f
}

func (f outlierFixture) fail(b *core.Backend, n int) {
	for range n {
		f.detector.Record(b, true)
	}
}

func defaultOutlier() core.Outlier {
	return core.Outlier{ConsecutiveFailures: 3, BaseEjection: testBase, MaxEjection: testMax, MaxEjectedPercent: 50}
}

func TestOutlierEjectsOnConsecutiveFailures(t *testing.T) {
	tests := []struct {
		name string
		// results are fed in order: true is a failure.
		results     []bool
		wantEjected bool
	}{
		{name: "below threshold", results: []bool{true, true}, wantEjected: false},
		{name: "at threshold", results: []bool{true, true, true}, wantEjected: true},
		{name: "success resets the streak", results: []bool{true, true, false, true, true}, wantEjected: false},
		{name: "streak after a reset", results: []bool{true, false, true, true, true}, wantEjected: true},
		{name: "only successes", results: []bool{false, false, false, false}, wantEjected: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newOutlierFixture(4, defaultOutlier())
			target := f.backends[1]
			for _, failed := range tc.results {
				f.detector.Record(target, failed)
			}
			if got := target.Ejected(f.clock.now()); got != tc.wantEjected {
				t.Errorf("ejected: got %v, want %v", got, tc.wantEjected)
			}
			wantEvents := 0
			if tc.wantEjected {
				wantEvents = 1
			}
			events := f.events.snapshot()
			if len(events) != wantEvents {
				t.Fatalf("got %d events, want %d", len(events), wantEvents)
			}
			if wantEvents == 1 && events[0] != (core.Event{Pool: "app", Backend: target, Kind: core.BackendEjected}) {
				t.Errorf("got event %+v, want ejection of %s in pool app", events[0], target.Addr)
			}
			for _, other := range []*core.Backend{f.backends[0], f.backends[2], f.backends[3]} {
				if other.Ejected(f.clock.now()) {
					t.Errorf("backend %s ejected without failures", other.Addr)
				}
			}
		})
	}
}

func TestOutlierEjectionTimeGrowsAndIsCapped(t *testing.T) {
	f := newOutlierFixture(4, defaultOutlier())
	target := f.backends[0]
	// base 10s, cap 35s: 10s, 20s, 30s, then the cap.
	for i, want := range []time.Duration{10 * time.Second, 20 * time.Second, 30 * time.Second, testMax, testMax} {
		f.fail(target, 3)
		if !target.Ejected(f.clock.now() + int64(want) - 1) {
			t.Fatalf("ejection %d: ended before %v", i+1, want)
		}
		if target.Ejected(f.clock.now() + int64(want)) {
			t.Fatalf("ejection %d: lasts longer than %v", i+1, want)
		}
		f.clock.advance(want)
	}
	if got := f.events.count(core.BackendEjected, target); got != 5 {
		t.Errorf("got %d ejection events, want 5", got)
	}
}

func TestOutlierFailuresWhileEjectedDoNotExtend(t *testing.T) {
	f := newOutlierFixture(4, defaultOutlier())
	target := f.backends[0]
	f.fail(target, 3)
	f.clock.advance(testBase / 2)
	f.fail(target, 30)
	if target.Ejected(f.clock.now() + int64(testBase/2)) {
		t.Error("late failures of in-flight requests extended the ejection")
	}
	if got := f.events.count(core.BackendEjected, target); got != 1 {
		t.Errorf("got %d ejection events, want 1", got)
	}
}

func TestOutlierMaxEjectedPercent(t *testing.T) {
	tests := []struct {
		name     string
		backends int
		percent  int
		// unhealthy is how many backends the active check has marked down
		// before the failures start.
		unhealthy int
		want      int // backends ejected when all the healthy ones fail
	}{
		{name: "half of four", backends: 4, percent: 50, want: 2},
		{name: "rounds down", backends: 3, percent: 50, want: 1},
		{name: "single backend is never ejected", backends: 1, percent: 50, want: 0},
		{name: "zero percent disables ejection", backends: 4, percent: 0, want: 0},
		{name: "everything but the last available backend", backends: 3, percent: 100, want: 2},
		{name: "unhealthy backends use up the limit", backends: 4, percent: 50, unhealthy: 2, want: 0},
		{name: "one unhealthy leaves room for one ejection", backends: 4, percent: 50, unhealthy: 1, want: 1},
		{name: "last available backend is kept at 100 percent", backends: 4, percent: 100, unhealthy: 3, want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := defaultOutlier()
			spec.MaxEjectedPercent = tc.percent
			f := newOutlierFixture(tc.backends, spec)
			for _, b := range f.backends[:tc.unhealthy] {
				b.SetHealthy(false)
			}
			for range 5 {
				for _, b := range f.backends[tc.unhealthy:] {
					f.fail(b, 3)
				}
			}
			if got := ejected(f.backends, f.clock.now()); got != tc.want {
				t.Errorf("got %d ejected, want %d", got, tc.want)
			}
			available := 0
			for _, b := range f.backends {
				if b.Available(f.clock.now()) {
					available++
				}
			}
			if tc.backends > tc.unhealthy && available == 0 {
				t.Error("ejection left no backend available")
			}
			if got := len(f.events.snapshot()); got != tc.want {
				t.Errorf("got %d ejection events, want %d", got, tc.want)
			}
		})
	}
}

func TestOutlierLimitFreesWhenEjectionExpires(t *testing.T) {
	f := newOutlierFixture(4, defaultOutlier())
	f.fail(f.backends[0], 3)
	f.fail(f.backends[1], 3)
	f.fail(f.backends[2], 3)
	if f.backends[2].Ejected(f.clock.now()) {
		t.Fatal("third backend ejected beyond the 50% limit")
	}
	f.clock.advance(testBase)
	f.fail(f.backends[2], 3)
	if !f.backends[2].Ejected(f.clock.now()) {
		t.Error("third backend not ejected after the earlier ejections expired")
	}
}

func TestOutlierSuccessClearsExpiredEjection(t *testing.T) {
	f := newOutlierFixture(4, defaultOutlier())
	target := f.backends[0]
	f.fail(target, 3)

	f.detector.Record(target, false)
	if !target.Ejected(f.clock.now()) {
		t.Fatal("a success during the ejection ended it")
	}
	f.clock.advance(testBase)
	if target.Available(0) {
		t.Fatal("deadline cleared before any success after expiry")
	}
	f.detector.Record(target, false)
	if !target.Available(0) {
		t.Error("expired ejection deadline was not cleared by a success")
	}
}

func TestOutlierEjectionCountDecays(t *testing.T) {
	f := newOutlierFixture(4, defaultOutlier())
	target := f.backends[0]
	eject := func(want time.Duration) {
		t.Helper()
		f.fail(target, 3)
		if !target.Ejected(f.clock.now()+int64(want)-1) || target.Ejected(f.clock.now()+int64(want)) {
			t.Fatalf("ejection does not last exactly %v", want)
		}
		f.clock.advance(want)
	}
	eject(10 * time.Second)
	eject(20 * time.Second)
	eject(30 * time.Second)

	// Less than one failure-free base_ejection: no decay, next is the cap.
	f.clock.advance(testBase - time.Second)
	f.detector.Record(target, false)
	eject(testMax)

	// A failure restarts the healthy period, so 9s + 9s decays nothing.
	f.clock.advance(testBase - time.Second)
	f.detector.Record(target, true)
	f.clock.advance(testBase - time.Second)
	f.detector.Record(target, false)
	eject(testMax)

	// Two full periods without a failure take the multiplier from 3 to 1.
	f.clock.advance(2 * testBase)
	f.detector.Record(target, false)
	eject(20 * time.Second)

	// A long quiet time decays to zero and no further.
	f.clock.advance(100 * testBase)
	f.detector.Record(target, false)
	eject(10 * time.Second)
}

func TestOutlierStateSurvivesReload(t *testing.T) {
	f := newOutlierFixture(4, defaultOutlier())
	kept, dropped := f.backends[0], f.backends[3]
	f.fail(kept, 3)
	f.clock.advance(testBase)
	f.fail(kept, 2)
	f.fail(dropped, 2)

	added := core.NewBackend("127.0.0.1:9100", 1)
	replaced := core.NewBackend(dropped.Addr, 2)
	next := NewOutlier(OutlierConfig{
		Pool:     "app",
		Spec:     defaultOutlier(),
		Backends: []*core.Backend{kept, f.backends[1], added, replaced},
		Observer: f.events.observe,
		Prev:     f.detector,
	})
	next.now = f.clock.now

	next.Record(kept, true)
	if !kept.Ejected(f.clock.now() + int64(2*testBase) - 1) {
		t.Error("kept backend: streak of 2 and ejection count of 1 did not carry over to a 20s ejection")
	}
	next.Record(replaced, true)
	if replaced.Ejected(f.clock.now()) {
		t.Error("replaced backend inherited the failure streak of the backend it replaced")
	}
	next.Record(dropped, true)
	next.Record(dropped, true)
	if dropped.Ejected(f.clock.now()) {
		t.Error("backend outside the new pool was ejected by it")
	}
}

func TestOutlierConcurrentRecordRespectsLimit(t *testing.T) {
	const goroutines, perGoroutine = 64, 2000
	spec := core.Outlier{ConsecutiveFailures: 2, BaseEjection: time.Hour, MaxEjection: time.Hour, MaxEjectedPercent: 25}
	backends := testBackends(8)
	var ejections atomic.Int64
	detector := NewOutlier(OutlierConfig{Pool: "app", Spec: spec, Backends: backends, Observer: func(core.Event) { ejections.Add(1) }})

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range perGoroutine {
				detector.Record(backends[(g+i)%len(backends)], i%5 != 0)
			}
		})
	}
	wg.Wait()

	if got := ejected(backends, core.Now()); got != 2 {
		t.Errorf("got %d ejected of 8 under sustained failures, want exactly the 25%% limit of 2", got)
	}
	if got := ejections.Load(); got != 2 {
		t.Errorf("got %d ejection events, want 2", got)
	}
}

func TestOutlierRecordDoesNotAllocate(t *testing.T) {
	f := newOutlierFixture(8, defaultOutlier())
	i := 0
	allocs := testing.AllocsPerRun(2000, func() {
		i++
		f.detector.Record(f.backends[i%len(f.backends)], i%4 == 0)
	})
	if allocs != 0 {
		t.Errorf("got %v allocs per record, want 0", allocs)
	}
}

func BenchmarkOutlierRecordSuccess(b *testing.B) {
	backends := testBackends(16)
	detector := NewOutlier(OutlierConfig{Pool: "app", Spec: defaultOutlier(), Backends: backends, Observer: discard})
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		detector.Record(backends[i&15], false)
		i++
	}
}

func BenchmarkOutlierRecordSuccessParallel(b *testing.B) {
	backends := testBackends(16)
	detector := NewOutlier(OutlierConfig{Pool: "app", Spec: defaultOutlier(), Backends: backends, Observer: discard})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			detector.Record(backends[i&15], false)
			i++
		}
	})
}

func BenchmarkOutlierRecordMixed(b *testing.B) {
	backends := testBackends(16)
	detector := NewOutlier(OutlierConfig{Pool: "app", Spec: defaultOutlier(), Backends: backends, Observer: discard})
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		detector.Record(backends[i&15], i&63 == 0)
		i++
	}
}
