package e2e

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A backend that was slow for a while must win its share back once it is
// fast again: p2c_ewma only learns a backend's latency from requests it
// sends there, so without probes one slow spell would exclude it for good.
func TestP2CEWMARecoversBackendAfterLatencySpike(t *testing.T) {
	const (
		spike   = 150 * time.Millisecond
		workers = 4
		batch   = 400
	)
	var slow atomic.Bool
	steady := newBackends(t, 2)
	spiky := newBackend(t, "spiky", func(w http.ResponseWriter, r *http.Request) {
		if slow.Load() {
			time.Sleep(spike)
		}
		writeString(w, "spiky")
	})
	in := start(t, webConfig(poolOf("p2c_ewma", append(steady, spiky)...), ""))
	client := newClient(t)

	// share sends a batch and returns the fraction the spiky backend served.
	share := func() float64 {
		t.Helper()
		before := spiky.hits.Load()
		var wg sync.WaitGroup
		var failed atomic.Int64
		for range workers {
			wg.Go(func() {
				for range batch / workers {
					r, err := trySend(client, http.MethodGet, in.url("/"), "")
					if err != nil || r.status != http.StatusOK {
						failed.Add(1)
					}
				}
			})
		}
		wg.Wait()
		if failed.Load() != 0 {
			t.Fatalf("%d requests failed", failed.Load())
		}
		return float64(spiky.hits.Load()-before) / batch
	}

	if got := share(); got < 0.2 {
		t.Fatalf("before the spike the backend served %.0f %% of requests, want about a third", 100*got)
	}
	slow.Store(true)
	share() // lets the balancer learn the high latency
	if got := share(); got > 0.15 {
		t.Errorf("during the spike the slow backend served %.0f %% of requests, want it mostly avoided", 100*got)
	}
	slow.Store(false)
	recovered := eventually(15*time.Second, func() bool { return share() >= 0.2 })
	if !recovered {
		t.Errorf("15 s after the spike ended the backend serves %.0f %% of requests, want about a third again (latency in /backends: %.1f ms)",
			100*share(), in.backend(spiky.addr).LatencyMs)
	}
}

// With one of four backends gone, round robin must split traffic evenly
// over the three that are left; skipping to "the next one" would give the
// dead backend's neighbour a double share.
func TestRoundRobinSurvivorsShareEquallyAfterBackendDies(t *testing.T) {
	tests := []struct {
		name  string
		extra string
		kill  func(b *backend)
	}{
		{
			name:  "marked down by the health check",
			extra: "    health_check:\n      type: http\n      path: /healthz\n" + fastCheck + outlierOff,
			kill:  func(b *backend) { b.sick.Store(true) },
		},
		{
			name: "refusing connections, ejected by outlier detection",
			kill: func(b *backend) { b.stop() },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const requests = 600
			backends := newBackends(t, 4)
			victim := backends[1]
			in := start(t, webConfig(poolOf("round_robin", backends...).with(tc.extra), ""))
			client := newClient(t)

			tc.kill(victim)
			gone := eventually(waitTimeout, func() bool {
				// Traffic is what feeds outlier detection; a request or two
				// may fail before the backend is taken out.
				get(t, client, in.url("/"))
				row := in.backend(victim.addr)
				return !row.Healthy || row.Ejected
			})
			if !gone {
				t.Fatalf("dead backend was neither marked down nor ejected")
			}
			before := make([]int64, len(backends))
			for i, b := range backends {
				before[i] = b.hits.Load()
			}
			for range requests {
				if r := get(t, client, in.url("/")); r.status != http.StatusOK {
					t.Fatalf("status %d with three of four backends up", r.status)
				}
			}
			for i, b := range backends {
				got := b.hits.Load() - before[i]
				want := int64(requests / 3)
				if b == victim {
					want = 0
				}
				if !within(got, want, requests, distributionTolerance) {
					t.Errorf("backend %d served %d of %d requests, want %d ± 5%%", i, got, requests, want)
				}
			}
		})
	}
}

// PRD B6 for the timeout path: when every backend hangs past the per-try
// timeout, retries stay within the budget just as they do for 503s.
func TestRetryBudgetHoldsWhenBackendsHang(t *testing.T) {
	const (
		workers       = 20
		perWorker     = 10
		clients       = workers * perWorker
		budgetPercent = 20
		floorPerSec   = 3
	)
	var backends []*backend
	for i := range 3 {
		backends = append(backends, newBackend(t, fmt.Sprintf("hang%d", i), slowHandler(5*time.Second)))
	}
	retry := "    retry:\n      max_attempts: 3\n      per_try_timeout: 100ms\n"
	in := start(t, webConfig(poolOf("round_robin", backends...).with(outlierOff+retry), "    timeouts:\n      request: 10s\n"))
	client := newClient(t)

	began := time.Now()
	var wg sync.WaitGroup
	statuses := make(chan int, clients)
	for range workers {
		wg.Go(func() {
			for range perWorker {
				r, err := trySend(client, http.MethodGet, in.url("/"), "")
				if err != nil {
					statuses <- 0
					continue
				}
				statuses <- r.status
			}
		})
	}
	wg.Wait()
	close(statuses)
	seconds := int(time.Since(began).Seconds()) + 2

	seen := make(map[int]int)
	for status := range statuses {
		seen[status]++
	}
	if seen[http.StatusGatewayTimeout] != clients {
		t.Errorf("statuses %v, want %d x 504", seen, clients)
	}
	var upstream int64
	for _, b := range backends {
		upstream += b.hits.Load()
	}
	limit := int64(clients*(100+budgetPercent)/100 + floorPerSec*seconds)
	t.Logf("%d client requests caused %d upstream requests in %v (limit %d)", clients, upstream, time.Since(began).Round(time.Millisecond), limit)
	if upstream > limit {
		t.Errorf("retry storm on timeouts: %d upstream requests for %d client requests, limit %d", upstream, clients, limit)
	}
	if got := in.metrics().sum("golb_retries_total", "pool", "app"); int64(got) != upstream-clients {
		t.Errorf("golb_retries_total = %v, backends saw %d retries", got, upstream-clients)
	}
}

// Bytes that follow a chunked body which also carried Content-Length must
// never be answered as a second request: golb closes the connection after
// the first response (RFC 9112 6.3), so nothing smuggled behind it is read.
func TestBytesAfterAmbiguouslyFramedRequestAreNotServed(t *testing.T) {
	rec := newRecorder(t, nil)
	in := start(t, webConfig(poolOf("round_robin", rec.backend), ""))
	// Read by Content-Length the body would be "3\r\na"; read as chunked it
	// is "abc" and the GET that follows is a second request.
	request := "POST /first HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"3\r\nabc\r\n0\r\n\r\n" +
		"GET /smuggled HTTP/1.1\r\nHost: x\r\n\r\n"
	response := exchange(t, in.addr(web), request, time.Second)
	if n := strings.Count(response, "HTTP/1.1 "); n != 1 {
		t.Errorf("%d responses on the connection, want 1: the bytes after the first request were served\n%s", n, response)
	}
	if got := rec.hits.Load(); got > 1 {
		t.Errorf("backend saw %d requests, want at most the first", got)
	}
	if _, _, body, _ := rec.last(); rec.hits.Load() == 1 && body != "abc" {
		t.Errorf("backend read body %q, want %q", body, "abc")
	}
}
