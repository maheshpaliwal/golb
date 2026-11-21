package upstream

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

const (
	pollDeadline = 5 * time.Second
	pollStep     = time.Millisecond
	// Validation requires timeout <= interval, so the interval is as long as
	// a loaded machine may need to answer a loopback probe.
	checkInterval = 25 * time.Millisecond
)

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(pollDeadline)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(pollStep)
	}
}

type eventLog struct {
	mu     sync.Mutex
	events []core.Event
}

func (l *eventLog) observe(e core.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) count(kind core.EventKind, addr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.events {
		if e.Kind == kind && e.Backend.Addr == addr {
			n++
		}
	}
	return n
}

func addr(i int) string { return fmt.Sprintf("127.0.0.1:%d", 9000+i) }

// spec returns a valid pool spec with n backends, no health check, and an
// outlier detector that ejects on the second consecutive failure.
func spec(policy core.Policy, n int) core.PoolSpec {
	s := core.PoolSpec{
		Name:    "app",
		Policy:  policy,
		Outlier: core.Outlier{ConsecutiveFailures: 2, BaseEjection: time.Hour, MaxEjection: time.Hour},
	}
	for i := range n {
		s.Backends = append(s.Backends, core.BackendSpec{Addr: addr(i)})
	}
	s.ApplyDefaults()
	return s
}

func mustNew(t testing.TB, s core.PoolSpec, prev core.Pool, obs core.Observer) core.Pool {
	t.Helper()
	p, err := New(s, prev, obs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func byAddr(p core.Pool) map[string]*core.Backend {
	backends := make(map[string]*core.Backend)
	for _, b := range p.Backends() {
		backends[b.Addr] = b
	}
	return backends
}

func TestNewRejectsInvalidSpec(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(s *core.PoolSpec)
	}{
		{name: "no backends", mutate: func(s *core.PoolSpec) { s.Backends = nil }},
		{name: "unknown policy", mutate: func(s *core.PoolSpec) { s.Policy = "maglev" }},
		{name: "defaults not applied", mutate: func(s *core.PoolSpec) { s.Backends[0].Weight = 0 }},
		{name: "duplicate backend", mutate: func(s *core.PoolSpec) { s.Backends[1].Addr = s.Backends[0].Addr }},
		{name: "health check without interval", mutate: func(s *core.PoolSpec) { s.Health = core.HealthCheck{Type: core.CheckTCP} }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := spec(core.RoundRobin, 2)
			tc.mutate(&s)
			before := runtime.NumGoroutine()
			p, err := New(s, nil, nil)
			if err == nil || p != nil {
				t.Fatalf("got pool %v and error %v, want an error and no pool", p, err)
			}
			if got := runtime.NumGoroutine(); got != before {
				t.Errorf("a rejected spec left %d goroutines running", got-before)
			}
		})
	}
}

func TestPickAndDoneTrackInflightAndLatency(t *testing.T) {
	for _, policy := range []core.Policy{core.RoundRobin, core.LeastConn, core.P2CEWMA, core.RingHash} {
		t.Run(string(policy), func(t *testing.T) {
			p := mustNew(t, spec(policy, 4), nil, nil)
			if got := len(p.Backends()); got != 4 {
				t.Fatalf("got %d backends, want 4", got)
			}
			for i, b := range p.Backends() {
				if b.Addr != addr(i) {
					t.Errorf("backend %d: got %s, want config order %s", i, b.Addr, addr(i))
				}
			}

			var held []*core.Backend
			for i := range 8 {
				b := p.Pick(core.PickInfo{Key: uint64(i)})
				if b == nil {
					t.Fatal("got nil with every backend available")
				}
				held = append(held, b)
			}
			var inflight int64
			for _, b := range p.Backends() {
				inflight += b.Inflight()
			}
			if inflight != 8 {
				t.Errorf("in-flight total after 8 picks: got %d, want 8", inflight)
			}

			for _, b := range held {
				p.Done(b, core.Result{Latency: 4 * time.Millisecond})
			}
			for _, b := range p.Backends() {
				if b.Inflight() != 0 {
					t.Errorf("backend %s in-flight after Done: got %d, want 0", b.Addr, b.Inflight())
				}
			}
			for _, b := range held {
				if b.Latency() != 4*time.Millisecond {
					t.Errorf("backend %s latency: got %v, want 4ms", b.Addr, b.Latency())
				}
			}
		})
	}
}

func TestDoneLatencySampling(t *testing.T) {
	tests := []struct {
		name   string
		result core.Result
		want   time.Duration
	}{
		{name: "success is sampled", result: core.Result{Latency: 8 * time.Millisecond}, want: 8 * time.Millisecond},
		{name: "zero is not sampled", result: core.Result{}, want: 0},
		{name: "negative is not sampled", result: core.Result{Latency: -time.Second}, want: 0},
		{name: "failure is not sampled", result: core.Result{Latency: time.Microsecond, Failed: true}, want: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := mustNew(t, spec(core.RoundRobin, 1), nil, nil)
			b := p.Pick(core.PickInfo{})
			p.Done(b, tc.result)
			if got := b.Latency(); got != tc.want {
				t.Errorf("latency: got %v, want %v", got, tc.want)
			}
			if b.Inflight() != 0 {
				t.Errorf("in-flight: got %d, want 0", b.Inflight())
			}
		})
	}
}

func TestDoneFailuresEjectAndEmit(t *testing.T) {
	events := new(eventLog)
	s := spec(core.RoundRobin, 4)
	p := mustNew(t, s, nil, events.observe)
	target := p.Backends()[2]

	for range 2 {
		target.Acquire()
		p.Done(target, core.Result{Failed: true})
	}
	if !target.Ejected(time.Now().UnixNano()) {
		t.Fatal("backend not ejected after consecutive_failures failed results")
	}
	if got := events.count(core.BackendEjected, target.Addr); got != 1 {
		t.Errorf("got %d ejection events, want 1", got)
	}
	for i := range 200 {
		b := p.Pick(core.PickInfo{Key: uint64(i)})
		if b == target {
			t.Fatal("ejected backend was picked")
		}
		p.Done(b, core.Result{Latency: time.Millisecond})
	}
}

func TestOutlierDisabled(t *testing.T) {
	s := spec(core.RoundRobin, 2)
	s.Outlier = core.Outlier{Disabled: true}
	p := mustNew(t, s, nil, nil)
	b := p.Backends()[0]
	for range 50 {
		b.Acquire()
		p.Done(b, core.Result{Failed: true})
	}
	if b.Ejected(time.Now().UnixNano()) {
		t.Error("backend ejected although outlier detection is disabled")
	}
}

func TestPickSkipsExcludedAndReturnsNilWhenNoneLeft(t *testing.T) {
	p := mustNew(t, spec(core.P2CEWMA, 3), nil, nil)
	var tried []*core.Backend
	for range 3 {
		b := p.Pick(core.PickInfo{Exclude: tried})
		if b == nil {
			t.Fatalf("got nil with %d untried backends", 3-len(tried))
		}
		for _, prev := range tried {
			if prev == b {
				t.Fatalf("backend %s picked twice", b.Addr)
			}
		}
		tried = append(tried, b)
	}
	if b := p.Pick(core.PickInfo{Exclude: tried}); b != nil {
		t.Errorf("got %s with every backend tried, want nil", b.Addr)
	}
	var inflight int64
	for _, b := range p.Backends() {
		inflight += b.Inflight()
	}
	if inflight != 3 {
		t.Errorf("in-flight: got %d, want 3 (the nil pick must not count)", inflight)
	}
	p.Done(nil, core.Result{})
}

func TestNewReusesBackendState(t *testing.T) {
	events := new(eventLog)
	old := mustNew(t, spec(core.P2CEWMA, 4), nil, events.observe)
	olds := byAddr(old)

	kept := olds[addr(0)]
	kept.Acquire()
	kept.ObserveLatency(7 * time.Millisecond)
	down := olds[addr(1)]
	down.SetHealthy(false)
	failing := olds[addr(2)]
	failing.Acquire()
	old.Done(failing, core.Result{Failed: true})

	next := spec(core.RoundRobin, 4)
	next.Backends[3].Weight = 5
	next.Backends = append(next.Backends, core.BackendSpec{Addr: addr(9), Weight: 1})
	next.Health = core.HealthCheck{Type: core.CheckTCP, Interval: time.Hour, Timeout: time.Hour, HealthyThreshold: 1, UnhealthyThreshold: 1}
	reloaded := mustNew(t, next, old, events.observe)
	news := byAddr(reloaded)

	for _, a := range []string{addr(0), addr(1), addr(2)} {
		if news[a] != olds[a] {
			t.Errorf("backend %s with unchanged weight was not reused", a)
		}
	}
	if news[addr(3)] == olds[addr(3)] {
		t.Error("backend with a changed weight was reused")
	}
	if got := news[addr(3)].Weight; got != 5 {
		t.Errorf("new weight: got %d, want 5", got)
	}
	if news[addr(9)] == nil || news[addr(9)].Inflight() != 0 || !news[addr(9)].Healthy() {
		t.Error("added backend is missing or does not start idle and healthy")
	}
	for i, b := range reloaded.Backends() {
		if b.Addr != next.Backends[i].Addr {
			t.Errorf("backend %d: got %s, want config order %s", i, b.Addr, next.Backends[i].Addr)
		}
	}

	if got := news[addr(0)].Inflight(); got != 1 {
		t.Errorf("in-flight did not survive the reload: got %d, want 1", got)
	}
	if got := news[addr(0)].Latency(); got != 7*time.Millisecond {
		t.Errorf("latency did not survive the reload: got %v, want 7ms", got)
	}
	if news[addr(1)].Healthy() {
		t.Error("unhealthy verdict did not survive a reload that keeps health checks")
	}

	// One failure was recorded by the old pool; one more through the new
	// pool completes the streak of two.
	failing.Acquire()
	reloaded.Done(failing, core.Result{Failed: true})
	if !failing.Ejected(time.Now().UnixNano()) {
		t.Error("failure streak did not survive the reload")
	}

	// Requests picked before the reload finish on the old pool after Close.
	old.Close()
	old.Done(kept, core.Result{Latency: 7 * time.Millisecond})
	if got := news[addr(0)].Inflight(); got != 0 {
		t.Errorf("Done on the closed old pool: in-flight %d, want 0", got)
	}
}

func TestNewResetsStateTheNewSpecCannotUndo(t *testing.T) {
	events := new(eventLog)
	old := mustNew(t, spec(core.RoundRobin, 3), nil, events.observe)
	down, ejected := old.Backends()[0], old.Backends()[1]
	down.SetHealthy(false)
	for range 2 {
		ejected.Acquire()
		old.Done(ejected, core.Result{Failed: true})
	}
	if !ejected.Ejected(time.Now().UnixNano()) {
		t.Fatal("setup: backend not ejected")
	}

	next := spec(core.RoundRobin, 3)
	next.Outlier = core.Outlier{Disabled: true}
	reloaded := mustNew(t, next, old, events.observe)

	if !down.Healthy() {
		t.Error("backend stayed unhealthy in a pool without health checks")
	}
	if got := events.count(core.BackendUp, down.Addr); got != 1 {
		t.Errorf("got %d up events for the reset backend, want 1", got)
	}
	if !ejected.Available(0) {
		t.Error("backend stayed ejected in a pool without outlier detection")
	}
	seen := make(map[*core.Backend]bool)
	for range 30 {
		b := reloaded.Pick(core.PickInfo{})
		seen[b] = true
		reloaded.Done(b, core.Result{})
	}
	if len(seen) != 3 {
		t.Errorf("got traffic on %d backends after the reset, want 3", len(seen))
	}
}

// fakePool stands for another core.Pool implementation passed as prev.
type fakePool struct{ backends []*core.Backend }

func (f fakePool) Pick(core.PickInfo) *core.Backend { return nil }
func (f fakePool) Done(*core.Backend, core.Result)  {}
func (f fakePool) Backends() []*core.Backend        { return f.backends }
func (f fakePool) Close()                           {}

func TestNewReusesBackendsOfForeignPool(t *testing.T) {
	existing := core.NewBackend(addr(0), 1)
	existing.Acquire()
	p := mustNew(t, spec(core.RoundRobin, 2), fakePool{backends: []*core.Backend{existing}}, nil)
	if p.Backends()[0] != existing {
		t.Error("backend of a foreign previous pool was not reused")
	}
	if p.Backends()[1] == nil || p.Backends()[1].Addr != addr(1) {
		t.Error("backend absent from the previous pool was not created")
	}
}

func TestHealthCheckLifecycle(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusOK)
	var probes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		w.WriteHeader(int(status.Load()))
	}))
	t.Cleanup(server.Close)
	server.CloseClientConnections()
	before := runtime.NumGoroutine()

	events := new(eventLog)
	backend := server.Listener.Addr().String()
	s := core.PoolSpec{
		Name:     "app",
		Policy:   core.RoundRobin,
		Backends: []core.BackendSpec{{Addr: backend, Weight: 1}, {Addr: addr(0), Weight: 1}},
		Health: core.HealthCheck{
			Type: core.CheckHTTP, Path: "/healthz", Interval: checkInterval, Timeout: checkInterval,
			HealthyThreshold: 1, UnhealthyThreshold: 2,
		},
		Outlier: core.Outlier{Disabled: true},
	}
	p, err := New(s, nil, events.observe)
	if err != nil {
		t.Fatal(err)
	}
	live := byAddr(p)[backend]

	// 127.0.0.1:9000 has no listener, so its probes are refused.
	eventually(t, "unreachable backend to go down", func() bool { return !byAddr(p)[addr(0)].Healthy() })
	for range 20 {
		b := p.Pick(core.PickInfo{})
		if b != live {
			t.Fatalf("got %v, want only the live backend", b)
		}
		p.Done(b, core.Result{})
	}

	status.Store(http.StatusServiceUnavailable)
	eventually(t, "live backend to go down", func() bool { return !live.Healthy() })
	if b := p.Pick(core.PickInfo{}); b != nil {
		t.Errorf("got %s with every backend unhealthy, want nil", b.Addr)
	}
	status.Store(http.StatusOK)
	eventually(t, "live backend to come back", func() bool { return live.Healthy() })
	eventually(t, "up event", func() bool { return events.count(core.BackendUp, backend) == 1 })
	if got := events.count(core.BackendDown, backend); got != 1 {
		t.Errorf("got %d down events, want 1", got)
	}

	held := p.Pick(core.PickInfo{})
	p.Close()
	p.Close()
	atClose := probes.Load()
	eventually(t, "goroutines to return to the baseline", func() bool { return runtime.NumGoroutine() <= before })
	if got := probes.Load(); got != atClose {
		t.Errorf("got %d probes after Close, want none", got-atClose)
	}
	p.Done(held, core.Result{Latency: time.Millisecond})
	if live.Inflight() != 0 {
		t.Errorf("Done after Close: in-flight %d, want 0", live.Inflight())
	}
	if b := p.Pick(core.PickInfo{}); b != live {
		t.Errorf("Pick after Close: got %v, want the live backend", b)
	}
}

// 64 goroutines pick and finish requests while health flips and the pool is
// rebuilt over the same backends, as a reload under load does.
func TestConcurrentPickDoneDuringReloads(t *testing.T) {
	const goroutines, perGoroutine = 64, 2000
	s := spec(core.P2CEWMA, 8)
	s.Outlier = core.Outlier{ConsecutiveFailures: 3, BaseEjection: time.Millisecond, MaxEjection: 10 * time.Millisecond, MaxEjectedPercent: 50}

	var current atomic.Pointer[core.Pool]
	first := mustNew(t, s, nil, nil)
	current.Store(&first)

	var stop atomic.Bool
	var background sync.WaitGroup
	background.Go(func() {
		policies := []core.Policy{core.RoundRobin, core.LeastConn, core.P2CEWMA, core.RingHash}
		for i := 0; !stop.Load(); i++ {
			next := s
			next.Policy = policies[i%len(policies)]
			old := *current.Load()
			p, err := New(next, old, nil)
			if err != nil {
				t.Errorf("rebuild pool: %v", err)
				return
			}
			current.Store(&p)
			old.Close()
		}
	})
	background.Go(func() {
		for i := 0; !stop.Load(); i++ {
			backends := (*current.Load()).Backends()
			// The last backend is never marked down, so a pick can only
			// fail while it is ejected.
			backends[i%(len(backends)-1)].SetHealthy(i%2 == 0)
		}
	})

	var picked atomic.Int64
	var workers sync.WaitGroup
	for g := range goroutines {
		workers.Go(func() {
			for i := range perGoroutine {
				p := *current.Load()
				b := p.Pick(core.PickInfo{Key: uint64(g*perGoroutine + i)})
				if b == nil {
					continue
				}
				picked.Add(1)
				p.Done(b, core.Result{Latency: time.Duration(i%7+1) * time.Millisecond, Failed: i%11 == 0})
			}
		})
	}
	workers.Wait()
	stop.Store(true)
	background.Wait()
	(*current.Load()).Close()

	if picked.Load() == 0 {
		t.Fatal("no pick succeeded")
	}
	for _, b := range (*current.Load()).Backends() {
		if b.Inflight() != 0 {
			t.Errorf("backend %s in-flight after every Done: got %d, want 0", b.Addr, b.Inflight())
		}
	}
}

func TestPickDoneDoesNotAllocate(t *testing.T) {
	for _, policy := range []core.Policy{core.RoundRobin, core.LeastConn, core.P2CEWMA, core.RingHash} {
		t.Run(string(policy), func(t *testing.T) {
			p := mustNew(t, spec(policy, 8), nil, nil)
			i := 0
			allocs := testing.AllocsPerRun(2000, func() {
				i++
				b := p.Pick(core.PickInfo{Key: uint64(i)})
				p.Done(b, core.Result{Latency: time.Millisecond, Failed: i%16 == 0})
			})
			if allocs != 0 {
				t.Errorf("got %v allocs per Pick+Done, want 0", allocs)
			}
		})
	}
}

func benchSpec(policy core.Policy) core.PoolSpec {
	n := 16
	if policy == core.RingHash {
		n = 100
	}
	s := spec(policy, n)
	s.Outlier = core.Outlier{}
	s.ApplyDefaults()
	return s
}

var benchPolicies = []core.Policy{core.RoundRobin, core.LeastConn, core.P2CEWMA, core.RingHash}

// The full pool path: selection, in-flight accounting, latency sample and
// the outlier detector with default settings.
func BenchmarkPoolPickDone(b *testing.B) {
	for _, policy := range benchPolicies {
		b.Run(string(policy), func(b *testing.B) {
			p := mustNew(b, benchSpec(policy), nil, nil)
			res := core.Result{Latency: time.Millisecond}
			b.ReportAllocs()
			var i uint64
			for b.Loop() {
				backend := p.Pick(core.PickInfo{Key: i * 0x9e3779b97f4a7c15})
				p.Done(backend, res)
				i++
			}
		})
	}
}

func BenchmarkPoolPickDoneParallel(b *testing.B) {
	for _, policy := range benchPolicies {
		b.Run(string(policy), func(b *testing.B) {
			p := mustNew(b, benchSpec(policy), nil, nil)
			res := core.Result{Latency: time.Millisecond}
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				var i uint64
				for pb.Next() {
					backend := p.Pick(core.PickInfo{Key: i * 0x9e3779b97f4a7c15})
					p.Done(backend, res)
					i++
				}
			})
		})
	}
}
