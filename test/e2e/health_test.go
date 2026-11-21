package e2e

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The probe timeout is as long as the interval allows, so a busy machine
// does not turn a passing probe into a failure.
const fastCheck = "      interval: 100ms\n      timeout: 100ms\n      healthy_threshold: 2\n      unhealthy_threshold: 2\n"

func TestHealthCheckRemovesAndRestoresBackend(t *testing.T) {
	tests := []struct {
		name  string
		check string
		kill  func(b *backend)
		heal  func(b *backend)
	}{
		{
			name:  "http check answering 500",
			check: "    health_check:\n      type: http\n      path: /healthz\n" + fastCheck,
			kill:  func(b *backend) { b.sick.Store(true) },
			heal:  func(b *backend) { b.sick.Store(false) },
		},
		{
			name:  "tcp check against a closed port",
			check: "    health_check:\n      type: tcp\n" + fastCheck,
			kill:  func(b *backend) { b.stop() },
			heal:  func(b *backend) { b.restart() },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backends := newBackends(t, 2)
			steady, victim := backends[0], backends[1]
			in := start(t, webConfig(poolOf("round_robin", backends...).with(tc.check+outlierOff), ""))
			client := newClient(t)

			for range 10 {
				get(t, client, in.url("/"))
			}
			if victim.hits.Load() != 5 {
				t.Fatalf("victim served %d of 10 requests while healthy, want 5", victim.hits.Load())
			}

			tc.kill(victim)
			if !eventually(waitTimeout, func() bool { return !in.backend(victim.addr).Healthy }) {
				t.Fatalf("backend was not marked unhealthy")
			}
			if got := in.metrics().sum("golb_backend_healthy", "backend", victim.addr); got != 0 {
				t.Errorf("golb_backend_healthy for the dead backend is %v, want 0", got)
			}
			before := victim.hits.Load()
			for range 30 {
				r := get(t, client, in.url("/"))
				if r.status != http.StatusOK || r.body != steady.name {
					t.Fatalf("with one backend down: status %d from %q, want 200 from %q", r.status, r.body, steady.name)
				}
			}
			if got := victim.hits.Load(); got != before {
				t.Errorf("unhealthy backend received %d requests", got-before)
			}

			tc.heal(victim)
			if !eventually(waitTimeout, func() bool { return in.backend(victim.addr).Healthy }) {
				t.Fatalf("backend was not marked healthy again")
			}
			before = victim.hits.Load()
			for range 10 {
				get(t, client, in.url("/"))
			}
			if got := victim.hits.Load() - before; got != 5 {
				t.Errorf("recovered backend served %d of 10 requests, want 5", got)
			}
		})
	}
}

func TestAllBackendsUnhealthyAnswers503Fast(t *testing.T) {
	backends := newBackends(t, 2)
	check := "    health_check:\n      type: http\n      path: /healthz\n" + fastCheck
	in := start(t, webConfig(poolOf("round_robin", backends...).with(check), ""))
	client := newClient(t)
	for _, b := range backends {
		b.sick.Store(true)
	}
	ok := eventually(waitTimeout, func() bool {
		for _, row := range in.backends() {
			if row.Healthy {
				return false
			}
		}
		return true
	})
	if !ok {
		t.Fatalf("backends were not marked unhealthy")
	}
	r := get(t, client, in.url("/"))
	if r.status != http.StatusServiceUnavailable || r.body != "Service Unavailable\n" {
		t.Errorf("status %d body %q, want 503 with the plain status text", r.status, r.body)
	}
	if r.elapsed > 500*time.Millisecond {
		t.Errorf("503 took %v, want it at once", r.elapsed)
	}
	for _, b := range backends {
		if b.hits.Load() != 0 {
			t.Errorf("unhealthy backend %s received %d requests", b.name, b.hits.Load())
		}
	}
}

const quickOutlier = "    outlier:\n      consecutive_failures: 3\n      base_ejection: 1500ms\n      max_ejection: 3s\n      max_ejected_percent: 50\n"

func TestOutlierEjectsOn5xxAndRecovers(t *testing.T) {
	var broken atomic.Bool
	broken.Store(true)
	good := newBackend(t, "good", nil)
	bad := newBackend(t, "bad", func(w http.ResponseWriter, r *http.Request) {
		if broken.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		writeString(w, "bad")
	})
	in := start(t, webConfig(poolOf("round_robin", good, bad).with(quickOutlier+"    retry:\n      max_attempts: 1\n"), ""))
	client := newClient(t)

	// Three consecutive 500s from the bad backend eject it.
	for range 6 {
		get(t, client, in.url("/"))
	}
	if !in.backend(bad.addr).Ejected {
		t.Fatalf("backend not ejected after %d consecutive 500s", bad.hits.Load())
	}
	if got := bad.hits.Load(); got != 3 {
		t.Errorf("bad backend served %d requests before ejection, want 3", got)
	}
	if got := in.metrics().sum("golb_backend_ejections_total", "backend", bad.addr); got != 1 {
		t.Errorf("golb_backend_ejections_total = %v, want 1", got)
	}
	for range 20 {
		r := get(t, client, in.url("/"))
		if r.status != http.StatusOK || r.body != "good" {
			t.Fatalf("while ejected: status %d from %q, want 200 from good", r.status, r.body)
		}
	}
	if got := bad.hits.Load(); got != 3 {
		t.Errorf("ejected backend received %d more requests", got-3)
	}

	// After base_ejection it is tried again and, now healthy, stays.
	broken.Store(false)
	back := eventually(waitTimeout, func() bool {
		return get(t, client, in.url("/")).body == "bad"
	})
	if !back {
		t.Fatalf("backend did not return after its ejection time")
	}
	before := bad.hits.Load()
	for range 10 {
		get(t, client, in.url("/"))
	}
	if got := bad.hits.Load() - before; got != 5 {
		t.Errorf("recovered backend served %d of 10 requests, want 5", got)
	}
	if in.backend(bad.addr).Ejected {
		t.Errorf("/backends still reports the recovered backend as ejected")
	}
}

func TestOutlierNeverEjectsBeyondMaxPercent(t *testing.T) {
	failing := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) }
	a := newBackend(t, "a", failing)
	b := newBackend(t, "b", failing)
	in := start(t, webConfig(poolOf("round_robin", a, b).with(quickOutlier+"    retry:\n      max_attempts: 1\n"), ""))
	client := newClient(t)
	for range 40 {
		r := get(t, client, in.url("/"))
		if r.status != http.StatusInternalServerError {
			t.Fatalf("status %d, want the backend's 500 (never 503: one backend must stay in rotation)", r.status)
		}
		ejected := 0
		for _, row := range in.backends() {
			if row.Ejected {
				ejected++
			}
		}
		if ejected > 1 {
			t.Fatalf("%d of 2 backends ejected, max_ejected_percent 50 allows 1", ejected)
		}
	}
}

// PRD B5: kill 1 of 4 backends under load with outlier ejection and retries
// on; under 0.1 % of idempotent requests may fail and the dead backend gets
// no traffic within 1 s.
func TestKillBackendUnderLoad(t *testing.T) {
	const workers = 16
	backends := newBackends(t, 4)
	victim := backends[3]
	in := start(t, webConfig(poolOf("round_robin", backends...), ""))
	client := newClient(t)

	var total, failed atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				r, err := trySend(client, http.MethodGet, in.url("/"), "")
				total.Add(1)
				if err != nil || r.status != http.StatusOK {
					failed.Add(1)
				}
			}
		})
	}
	attempts := func() float64 { return in.metrics().sum("golb_upstream_requests_total", "backend", victim.addr) }

	if !eventually(waitTimeout, func() bool { return total.Load() > 500 }) {
		t.Fatalf("load did not start")
	}
	victim.stop()
	time.Sleep(time.Second) // the PRD's bound: no traffic to the dead backend after 1 s
	afterOneSecond := attempts()
	sentAtOneSecond := total.Load()
	if !eventually(waitTimeout, func() bool { return total.Load() > sentAtOneSecond+2000 }) {
		t.Fatalf("load stalled after the backend died")
	}
	atEnd := attempts()
	close(stop)
	wg.Wait()

	if atEnd != afterOneSecond {
		t.Errorf("dead backend was tried %v more times later than 1 s after it died", atEnd-afterOneSecond)
	}
	if !in.backend(victim.addr).Ejected {
		t.Errorf("dead backend is not ejected")
	}
	rate := float64(failed.Load()) / float64(total.Load())
	t.Logf("%d requests, %d failed (%.4f %%)", total.Load(), failed.Load(), 100*rate)
	if rate >= 0.001 {
		t.Errorf("error rate %.4f %% with one of four backends dead, want under 0.1 %%", 100*rate)
	}
}
