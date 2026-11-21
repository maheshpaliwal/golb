package health

import (
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

const (
	testInterval = 5 * time.Millisecond
	// Generous, so a loaded machine does not turn a passing probe into a
	// timeout; the timeout test sets its own.
	testTimeout  = time.Second
	shortTimeout = 5 * time.Millisecond
	// quietIntervals is how many probe intervals must pass without a probe
	// to conclude that probing has stopped.
	quietIntervals = 6
	pollDeadline   = 5 * time.Second
	pollStep       = time.Millisecond
)

// eventually polls cond until it holds or the deadline passes.
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

// fakeHTTPBackend answers health probes on 127.0.0.1 with a status the test
// controls, and counts the probes it has answered.
type fakeHTTPBackend struct {
	server *httptest.Server
	status atomic.Int32
	// flap alternates 200 and 503 on successive probes when set.
	flap    atomic.Bool
	probes  atomic.Int64
	block   chan struct{} // closed to release handlers held by hang
	hang    atomic.Bool
	paths   atomic.Value // last request path
	agents  atomic.Value // last User-Agent
	backend *core.Backend
}

func newFakeHTTPBackend(t *testing.T) *fakeHTTPBackend {
	t.Helper()
	f := &fakeHTTPBackend{block: make(chan struct{})}
	f.status.Store(http.StatusOK)
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Recorded before the count moves, so a test that waits for the
		// count always finds them.
		f.paths.Store(r.URL.RequestURI())
		f.agents.Store(r.UserAgent())
		n := f.probes.Add(1)
		if f.hang.Load() {
			select {
			case <-f.block:
			case <-r.Context().Done():
			}
			return
		}
		status := int(f.status.Load())
		if f.flap.Load() && n%2 == 0 {
			status = http.StatusServiceUnavailable
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(func() {
		close(f.block)
		f.server.Close()
	})
	f.backend = core.NewBackend(f.server.Listener.Addr().String(), 1)
	return f
}

func httpSpec() core.HealthCheck {
	return core.HealthCheck{
		Type:               core.CheckHTTP,
		Path:               "/healthz",
		Interval:           testInterval,
		Timeout:            testTimeout,
		HealthyThreshold:   2,
		UnhealthyThreshold: 3,
	}
}

func startChecker(t *testing.T, spec core.HealthCheck, events *eventLog, backends ...*core.Backend) *Checker {
	t.Helper()
	checker := StartChecker(CheckerConfig{Pool: "app", Spec: spec, Backends: backends, Observer: events.observe})
	t.Cleanup(checker.Close)
	return checker
}

func TestStreakThresholds(t *testing.T) {
	spec := core.HealthCheck{HealthyThreshold: 2, UnhealthyThreshold: 3}
	tests := []struct {
		name    string
		healthy bool   // verdict before the first result
		results string // one probe per byte: '+' ok, '-' failed
		want    string // verdict after each result: 'H' healthy, 'U' unhealthy
	}{
		{name: "stays healthy on successes", healthy: true, results: "++++", want: "HHHH"},
		{name: "falls on the third consecutive failure", healthy: true, results: "---", want: "HHU"},
		{name: "a success restarts the failure streak", healthy: true, results: "--+--+---", want: "HHHHHHHHU"},
		{name: "rises on the second consecutive success", healthy: false, results: "++", want: "UH"},
		{name: "a failure restarts the success streak", healthy: false, results: "+-+-++", want: "UUUUUH"},
		{name: "flapping never changes a healthy backend", healthy: true, results: "-+-+-+-+", want: "HHHHHHHH"},
		{name: "down then up", healthy: true, results: "---+-++", want: "HHUUUUH"},
		{name: "old successes do not count towards recovery", healthy: true, results: "+++---+", want: "HHHHHUU"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var results streak
			healthy := tc.healthy
			got := make([]byte, 0, len(tc.results))
			for _, r := range []byte(tc.results) {
				healthy = results.next(r == '+', healthy, spec)
				if healthy {
					got = append(got, 'H')
					continue
				}
				got = append(got, 'U')
			}
			if string(got) != tc.want {
				t.Errorf("verdicts: got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestHTTPCheckTransitions(t *testing.T) {
	fake := newFakeHTTPBackend(t)
	events := new(eventLog)
	spec := httpSpec()
	startChecker(t, spec, events, fake.backend)

	if !fake.backend.Healthy() {
		t.Fatal("backend does not start healthy")
	}
	eventually(t, "first probes", func() bool { return fake.probes.Load() >= 3 })
	if !fake.backend.Healthy() || len(events.snapshot()) != 0 {
		t.Fatalf("passing probes changed state: healthy=%v events=%d", fake.backend.Healthy(), len(events.snapshot()))
	}
	if got := fake.paths.Load(); got != "/healthz" {
		t.Errorf("probe path: got %v, want /healthz", got)
	}
	if got := fake.agents.Load(); got != userAgent {
		t.Errorf("probe user agent: got %v, want %s", got, userAgent)
	}

	failingFrom := fake.probes.Load()
	fake.status.Store(http.StatusInternalServerError)
	eventually(t, "backend to go down", func() bool { return !fake.backend.Healthy() })
	if failed := fake.probes.Load() - failingFrom; failed < int64(spec.UnhealthyThreshold)-1 {
		// The probe in flight when the status flipped may have been answered
		// with either status, hence the one-probe slack.
		t.Errorf("went down after %d failing probes, want at least %d", failed, spec.UnhealthyThreshold-1)
	}
	// The verdict is stored before the event is emitted, so the event is
	// waited for rather than read at once.
	eventually(t, "down event", func() bool { return events.count(core.BackendDown, fake.backend) == 1 })

	fake.status.Store(http.StatusNoContent)
	eventually(t, "backend to come back", func() bool { return fake.backend.Healthy() })
	eventually(t, "up event", func() bool { return events.count(core.BackendUp, fake.backend) == 1 })
	for _, e := range events.snapshot() {
		if e.Pool != "app" {
			t.Errorf("event pool: got %q, want app", e.Pool)
		}
	}
}

func TestHTTPCheckStatusRange(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		wantHealthy bool
	}{
		{name: "200", status: http.StatusOK, wantHealthy: true},
		{name: "redirect is healthy and not followed", status: http.StatusFound, wantHealthy: true},
		{name: "399", status: 399, wantHealthy: true},
		{name: "400", status: http.StatusBadRequest, wantHealthy: false},
		{name: "503", status: http.StatusServiceUnavailable, wantHealthy: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeHTTPBackend(t)
			fake.status.Store(int32(tc.status))
			// The backend starts in the opposite state, so reaching the
			// wanted one proves the probes were classified correctly.
			fake.backend.SetHealthy(!tc.wantHealthy)
			startChecker(t, httpSpec(), new(eventLog), fake.backend)
			eventually(t, "verdict", func() bool { return fake.backend.Healthy() == tc.wantHealthy })
		})
	}
}

func TestHTTPCheckFlappingKeepsState(t *testing.T) {
	fake := newFakeHTTPBackend(t)
	fake.flap.Store(true)
	events := new(eventLog)
	startChecker(t, httpSpec(), events, fake.backend)

	eventually(t, "20 alternating probes", func() bool { return fake.probes.Load() >= 20 })
	if !fake.backend.Healthy() {
		t.Error("alternating results took a healthy backend down")
	}
	if got := len(events.snapshot()); got != 0 {
		t.Errorf("got %d events from alternating results, want 0", got)
	}
}

func TestHTTPCheckTimeoutCountsAsFailure(t *testing.T) {
	fake := newFakeHTTPBackend(t)
	fake.hang.Store(true)
	events := new(eventLog)
	spec := httpSpec()
	spec.Timeout = shortTimeout
	startChecker(t, spec, events, fake.backend)

	eventually(t, "backend to go down on timeouts", func() bool { return !fake.backend.Healthy() })
	eventually(t, "down event", func() bool { return events.count(core.BackendDown, fake.backend) == 1 })
}

func TestHTTPCheckPathWithoutLeadingSlash(t *testing.T) {
	fake := newFakeHTTPBackend(t)
	spec := httpSpec()
	spec.Path = "ready?deep=1"
	startChecker(t, spec, new(eventLog), fake.backend)
	eventually(t, "a probe", func() bool { return fake.probes.Load() >= 1 })
	if got := fake.paths.Load(); got != "/ready?deep=1" {
		t.Errorf("probe path: got %v, want /ready?deep=1", got)
	}
}

func TestTCPCheckTransitions(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	backend := core.NewBackend(addr, 1)
	events := new(eventLog)
	spec := core.HealthCheck{Type: core.CheckTCP, Interval: testInterval, Timeout: testTimeout, HealthyThreshold: 2, UnhealthyThreshold: 2}
	startChecker(t, spec, events, backend)

	err = listener.Close()
	if err != nil {
		t.Fatalf("close listener: %v", err)
	}
	eventually(t, "backend to go down after the listener closed", func() bool { return !backend.Healthy() })

	// The port was just released by this process; retry in case the kernel
	// has not finished tearing it down.
	eventually(t, "listening on the same port again", func() bool {
		listener, err = net.Listen("tcp", addr)
		return err == nil
	})
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	})
	eventually(t, "backend to come back", func() bool { return backend.Healthy() })
	eventually(t, "down and up events", func() bool {
		return events.count(core.BackendDown, backend) == 1 && events.count(core.BackendUp, backend) == 1
	})
}

func TestCheckerProbesEveryBackendIndependently(t *testing.T) {
	good, bad := newFakeHTTPBackend(t), newFakeHTTPBackend(t)
	bad.status.Store(http.StatusServiceUnavailable)
	events := new(eventLog)
	startChecker(t, httpSpec(), events, good.backend, bad.backend)

	eventually(t, "failing backend to go down", func() bool { return !bad.backend.Healthy() })
	eventually(t, "probes of the passing backend", func() bool { return good.probes.Load() >= 5 })
	if !good.backend.Healthy() {
		t.Error("passing backend went down with its neighbour")
	}
	if got := events.count(core.BackendDown, good.backend); got != 0 {
		t.Errorf("got %d down events for the passing backend, want 0", got)
	}
}

func TestCheckerCloseLeavesNoGoroutines(t *testing.T) {
	tests := []struct {
		name string
		spec func(fake *fakeHTTPBackend) core.HealthCheck
	}{
		{name: "http", spec: func(*fakeHTTPBackend) core.HealthCheck { return httpSpec() }},
		{name: "http closed during a hanging probe", spec: func(fake *fakeHTTPBackend) core.HealthCheck {
			fake.hang.Store(true)
			spec := httpSpec()
			spec.Interval, spec.Timeout = time.Minute, time.Minute
			return spec
		}},
		{name: "tcp", spec: func(*fakeHTTPBackend) core.HealthCheck {
			return core.HealthCheck{Type: core.CheckTCP, Interval: testInterval, Timeout: testTimeout, HealthyThreshold: 1, UnhealthyThreshold: 1}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakes := []*fakeHTTPBackend{newFakeHTTPBackend(t), newFakeHTTPBackend(t), newFakeHTTPBackend(t)}
			spec := tc.spec(fakes[0])
			backends := make([]*core.Backend, len(fakes))
			for i, fake := range fakes {
				backends[i] = fake.backend
			}
			for _, fake := range fakes {
				fake.server.CloseClientConnections()
			}
			before := runtime.NumGoroutine()

			checker := StartChecker(CheckerConfig{Pool: "app", Spec: spec, Backends: backends, Observer: discard})
			if spec.Type == core.CheckHTTP {
				// With a one-minute interval the start jitter can be long, so
				// only wait for a probe when probes are frequent.
				if spec.Interval == testInterval {
					eventually(t, "probes to run", func() bool { return fakes[1].probes.Load() >= 2 })
				}
			}
			closed := make(chan struct{})
			go func() {
				checker.Close()
				checker.Close()
				close(closed)
			}()
			select {
			case <-closed:
			case <-time.After(pollDeadline):
				t.Fatal("Close did not return")
			}

			// The checker's goroutines are gone when Close returns, but a
			// request it had already written may still be on its way through
			// the server. The count is therefore taken once the server's
			// connection goroutines have ended too, and must then stay put
			// for several probe intervals.
			eventually(t, "goroutines to return to the baseline", func() bool { return runtime.NumGoroutine() <= before })
			settled := fakes[1].probes.Load()
			time.Sleep(quietIntervals * testInterval)
			if got := fakes[1].probes.Load(); got != settled {
				t.Errorf("got %d probes after Close and after the server went quiet, want none", got-settled)
			}
		})
	}
}

func TestCheckerWithoutCheckTypeStartsNothing(t *testing.T) {
	backend := core.NewBackend("127.0.0.1:1", 1)
	before := runtime.NumGoroutine()
	checker := StartChecker(CheckerConfig{Pool: "app", Spec: core.HealthCheck{}, Backends: []*core.Backend{backend}, Observer: discard})
	if got := runtime.NumGoroutine(); got != before {
		t.Errorf("got %d goroutines started without a check type, want 0", got-before)
	}
	checker.Close()
	if !backend.Healthy() {
		t.Error("backend marked unhealthy without any check")
	}
}
