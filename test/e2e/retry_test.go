package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const fullBudget = "    retry:\n      budget_percent: 100\n"

func TestRetryOnConnectFailureReachesAnotherBackend(t *testing.T) {
	live := newBackend(t, "live", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		writeString(w, r.Method+":"+string(body))
	})
	dead := deadAddr(t)
	p := pool{policy: "round_robin", addrs: []string{dead, live.addr}, extra: outlierOff + fullBudget}
	in := start(t, webConfig(p, ""))
	client := newClient(t)

	tests := []struct {
		name   string
		method string
		body   string
	}{
		{name: "GET", method: http.MethodGet},
		{name: "POST with a body, nothing was sent yet", method: http.MethodPost, body: "payload"},
		{name: "DELETE", method: http.MethodDelete},
	}
	const each = 10
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := in.metrics().sum("golb_retries_total", "pool", "app")
			for range each {
				r := send(t, client, tc.method, in.url("/"), tc.body)
				if r.status != http.StatusOK || r.body != tc.method+":"+tc.body {
					t.Fatalf("status %d body %q, want 200 %q via the live backend", r.status, r.body, tc.method+":"+tc.body)
				}
			}
			// At least every second request starts on the dead address.
			// (Sequential traffic over two backends starts all of them
			// there: the retry's pick advances the rotation too.)
			retried := in.metrics().sum("golb_retries_total", "pool", "app") - before
			if retried < each/2 || retried > each {
				t.Errorf("golb_retries_total grew by %v, want %d..%d", retried, each/2, each)
			}
		})
	}
	m := in.metrics()
	if got, retries := m.sum("golb_upstream_requests_total", "backend", dead, "result", "error"), m.sum("golb_retries_total", "pool", "app"); got != retries {
		t.Errorf("failed attempts against the dead backend: %v, want one per retry (%v)", got, retries)
	}
	if got := m.sum("golb_upstream_requests_total", "backend", live.addr, "result", "ok"); got != 3*each {
		t.Errorf("successful attempts against the live backend: %v, want %d", got, 3*each)
	}
}

func TestAllBackendsRefusingAnswers502WithoutDetail(t *testing.T) {
	p := pool{policy: "round_robin", addrs: []string{deadAddr(t), deadAddr(t), deadAddr(t)}, extra: outlierOff}
	in := start(t, webConfig(p, ""))
	client := newClient(t)
	r := get(t, client, in.url("/"))
	if r.status != http.StatusBadGateway || r.body != "Bad Gateway\n" {
		t.Errorf("status %d body %q, want 502 with the plain status text and no internal detail", r.status, r.body)
	}
	if r.elapsed > time.Second {
		t.Errorf("502 took %v with every backend refusing connections, want it fast", r.elapsed)
	}
	if got := in.metrics().sum("golb_upstream_requests_total", "result", "error"); got != 3 {
		t.Errorf("upstream attempts: %v, want 3 (max_attempts, each backend once)", got)
	}
}

// PRD B6: with every backend answering 503, upstream requests stay within
// (1 + budget) x client requests. The budget's documented floor of 3
// retries per second (PRD 4.4) is allowed on top.
func TestRetryBudgetBoundsRetryStorm(t *testing.T) {
	const (
		workers       = 10
		perWorker     = 80
		clients       = workers * perWorker
		budgetPercent = 20
		floorPerSec   = 3
	)
	var mu sync.Mutex
	seen := make(map[string]int) // "request id @ backend" -> attempts
	var backends []*backend
	for i := range 3 {
		name := fmt.Sprintf("b%d", i)
		backends = append(backends, newBackend(t, name, func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			seen[r.Header.Get("X-Request-Id")+"@"+name]++
			mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
	}
	in := start(t, webConfig(poolOf("round_robin", backends...).with(outlierOff), ""))
	client := newClient(t)

	began := time.Now()
	var wg sync.WaitGroup
	var wrong atomic.Int64
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				r, err := trySend(client, http.MethodGet, in.url("/"), "", "X-Request-Id", fmt.Sprintf("%d-%d", w, i))
				if err != nil || r.status != http.StatusServiceUnavailable {
					wrong.Add(1)
				}
			}
		})
	}
	wg.Wait()
	seconds := int(time.Since(began).Seconds()) + 2 // seconds touched, rounded up at both ends

	if wrong.Load() != 0 {
		t.Errorf("%d of %d clients did not get the backend's 503", wrong.Load(), clients)
	}
	var upstream int64
	for _, b := range backends {
		upstream += b.hits.Load()
	}
	limit := int64(clients*(100+budgetPercent)/100 + floorPerSec*seconds)
	t.Logf("%d client requests caused %d upstream requests (limit %d)", clients, upstream, limit)
	if upstream > limit {
		t.Errorf("retry storm: %d upstream requests for %d client requests, limit %d", upstream, clients, limit)
	}
	if upstream <= clients {
		t.Errorf("no retries at all (%d upstream for %d clients): the budget should allow about %d %%", upstream, clients, budgetPercent)
	}
	for key, n := range seen {
		if n > 1 {
			t.Errorf("a request was sent to the same backend %d times: %s", n, key)
		}
	}
	m := in.metrics()
	if got := m.sum("golb_retries_total", "pool", "app"); int64(got) != upstream-clients {
		t.Errorf("golb_retries_total = %v, backends saw %d retries", got, upstream-clients)
	}
	if m.sum("golb_retry_budget_exhausted_total", "pool", "app") == 0 {
		t.Errorf("golb_retry_budget_exhausted_total is 0 although retries were refused")
	}
}

// dropAfterReading reads the whole request and then closes the connection
// without answering: the request was processed, the response is lost.
func dropAfterReading(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Errorf("backend: read request body: %v", err)
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("backend: hijack: %v", err)
			return
		}
		closeConn(conn)
	}
}

func closeConn(c io.Closer) {
	// The peer usually closed first; the tests assert on what was read.
	_ = c.Close()
}

func TestNonIdempotentRequestIsNotReplayed(t *testing.T) {
	tests := []struct {
		name      string
		method    string
		body      string
		wantRetry bool
	}{
		{name: "POST with body", method: http.MethodPost, body: "charge the card"},
		{name: "POST without body", method: http.MethodPost},
		{name: "PATCH with body", method: http.MethodPatch, body: "x"},
		{name: "PUT with body (consumed, cannot be replayed)", method: http.MethodPut, body: "x"},
		{name: "GET is retried", method: http.MethodGet, wantRetry: true},
		{name: "DELETE is retried", method: http.MethodDelete, wantRetry: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lossy := newBackend(t, "lossy", nil)
			lossy.handler = dropAfterReading(t)
			good := newBackend(t, "good", nil)
			in := start(t, webConfig(poolOf("round_robin", lossy, good).with(outlierOff+fullBudget), ""))
			client := newClient(t)

			const requests = 8
			statuses := make(map[int]int)
			for range requests {
				r := send(t, client, tc.method, in.url("/"), tc.body)
				statuses[r.status]++
				if r.status != http.StatusOK && r.body != "Bad Gateway\n" {
					t.Errorf("status %d body %q, want 200 or a plain 502", r.status, r.body)
				}
			}
			if tc.wantRetry {
				if statuses[http.StatusOK] != requests {
					t.Errorf("statuses %v, want every request answered 200 through a retry", statuses)
				}
				if good.hits.Load() != requests {
					t.Errorf("good backend served %d requests, want %d", good.hits.Load(), requests)
				}
				return
			}
			// Round robin: half the requests reach the lossy backend. They
			// must end as 502 and must not show up on the good backend too.
			if statuses[http.StatusBadGateway] != requests/2 || statuses[http.StatusOK] != requests/2 {
				t.Errorf("statuses %v, want %d x 502 and %d x 200", statuses, requests/2, requests/2)
			}
			if got := lossy.hits.Load() + good.hits.Load(); got != requests {
				t.Errorf("backends saw %d requests for %d client requests: a non-idempotent request was replayed", got, requests)
			}
		})
	}
}

func TestRetryOnStatusOnlyForIdempotentRequests(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		body     string
		wantHits int64
	}{
		{name: "GET tries every backend", method: http.MethodGet, wantHits: 3},
		{name: "PUT without body tries every backend", method: http.MethodPut, wantHits: 3},
		{name: "POST is sent once", method: http.MethodPost, body: "x", wantHits: 1},
		{name: "GET with body is sent once", method: http.MethodGet, body: "x", wantHits: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var backends []*backend
			for i := range 3 {
				backends = append(backends, newBackend(t, fmt.Sprintf("b%d", i), func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusServiceUnavailable)
				}))
			}
			in := start(t, webConfig(poolOf("round_robin", backends...).with(outlierOff), ""))
			r := send(t, newClient(t), tc.method, in.url("/"), tc.body)
			if r.status != http.StatusServiceUnavailable {
				t.Errorf("status %d, want the backend's 503", r.status)
			}
			var hits int64
			for _, b := range backends {
				hits += b.hits.Load()
			}
			if hits != tc.wantHits {
				t.Errorf("backends saw %d requests, want %d", hits, tc.wantHits)
			}
		})
	}
}

// slowHandler answers after delay unless the proxy gives up first.
func slowHandler(delay time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			writeString(w, "slow")
		case <-r.Context().Done():
		}
	}
}

func TestPerTryTimeout(t *testing.T) {
	const perTry = 200 * time.Millisecond
	retry := func(attempts int) string {
		return fmt.Sprintf("    retry:\n      max_attempts: %d\n      per_try_timeout: %s\n      budget_percent: 100\n", attempts, perTry)
	}

	t.Run("idempotent request succeeds on a faster backend", func(t *testing.T) {
		slow := newBackend(t, "slow", slowHandler(5*time.Second))
		fast := newBackend(t, "fast", nil)
		in := start(t, webConfig(poolOf("round_robin", slow, fast).with(outlierOff+retry(3)), ""))
		client := newClient(t)
		for range 4 {
			r := get(t, client, in.url("/"))
			if r.status != http.StatusOK || r.body != "fast" {
				t.Errorf("status %d from %q, want 200 from fast", r.status, r.body)
			}
			if r.elapsed > perTry+time.Second {
				t.Errorf("took %v, want about %v for the timed-out try plus the retry", r.elapsed, perTry)
			}
		}
		tried := slow.hits.Load()
		if tried < 2 || tried > 4 {
			t.Errorf("slow backend was tried %d times in 4 requests, want 2..4", tried)
		}
		if got := in.metrics().sum("golb_retries_total", "pool", "app"); got != float64(tried) {
			t.Errorf("golb_retries_total = %v, want one per timed-out try (%d)", got, tried)
		}
	})

	t.Run("504 when nothing answers in time", func(t *testing.T) {
		slow := newBackend(t, "slow", slowHandler(5*time.Second))
		in := start(t, webConfig(poolOf("round_robin", slow).with(outlierOff+retry(1)), ""))
		r := get(t, newClient(t), in.url("/"))
		if r.status != http.StatusGatewayTimeout || r.body != "Gateway Timeout\n" {
			t.Errorf("status %d body %q, want 504 with the plain status text", r.status, r.body)
		}
		if r.elapsed < perTry-20*time.Millisecond || r.elapsed > perTry+time.Second {
			t.Errorf("504 after %v, want about %v", r.elapsed, perTry)
		}
	})

	t.Run("non-idempotent request is not retried after a timeout", func(t *testing.T) {
		slow := newBackend(t, "slow", slowHandler(5*time.Second))
		fast := newBackend(t, "fast", nil)
		in := start(t, webConfig(poolOf("round_robin", slow, fast).with(outlierOff+retry(3)), ""))
		r := send(t, newClient(t), http.MethodPost, in.url("/"), "payload")
		if r.status != http.StatusGatewayTimeout {
			t.Errorf("status %d, want 504", r.status)
		}
		if got := slow.hits.Load() + fast.hits.Load(); got != 1 {
			t.Errorf("backends saw the POST %d times, want 1", got)
		}
	})
}

func TestRequestTimeoutBoundsAllTries(t *testing.T) {
	const (
		perTry  = 500 * time.Millisecond
		request = 700 * time.Millisecond
	)
	var backends []*backend
	for i := range 3 {
		backends = append(backends, newBackend(t, fmt.Sprintf("slow%d", i), slowHandler(5*time.Second)))
	}
	retry := fmt.Sprintf("    retry:\n      max_attempts: 3\n      per_try_timeout: %s\n      budget_percent: 100\n", perTry)
	listener := fmt.Sprintf("    timeouts:\n      request: %s\n", request)
	in := start(t, webConfig(poolOf("round_robin", backends...).with(outlierOff+retry), listener))

	r := get(t, newClient(t), in.url("/"))
	if r.status != http.StatusGatewayTimeout {
		t.Errorf("status %d, want 504", r.status)
	}
	// Three unbounded tries would take 3 x 500 ms.
	if r.elapsed < request-50*time.Millisecond || r.elapsed > request+500*time.Millisecond {
		t.Errorf("504 after %v, want about the request timeout of %v", r.elapsed, request)
	}
}

func TestBackendKilledMidResponse(t *testing.T) {
	const (
		promised = 1 << 20
		sent     = 64 << 10
	)
	tests := []struct {
		name    string
		declare bool // send Content-Length; otherwise the response is chunked
	}{
		{name: "response with Content-Length", declare: true},
		{name: "chunked response"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dying := newBackend(t, "dying", func(w http.ResponseWriter, r *http.Request) {
				if tc.declare {
					w.Header().Set("Content-Length", fmt.Sprint(promised))
				}
				writeString(w, strings.Repeat("x", sent))
				w.(http.Flusher).Flush()
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Errorf("backend: hijack: %v", err)
					return
				}
				closeConn(conn)
			})
			good := newBackend(t, "good", nil)
			in := start(t, webConfig(poolOf("round_robin", dying, good).with(outlierOff+fullBudget), ""))
			client := newClient(t)

			const requests = 6
			truncated := 0
			for range requests {
				req, err := http.NewRequest(http.MethodGet, in.url("/"), nil)
				if err != nil {
					t.Fatalf("build request: %v", err)
				}
				r, err := do(client, req)
				if r.elapsed > 3*time.Second {
					t.Errorf("took %v, the client must not hang on a dead backend", r.elapsed)
				}
				if r.header.Get(backendName) != "dying" {
					if err != nil || r.body != "good" {
						t.Errorf("good backend: status %d body %q err %v", r.status, r.body, err)
					}
					continue
				}
				truncated++
				// The body must end in an error: a clean end would pass a
				// truncated response off as complete.
				if err == nil {
					t.Errorf("truncated response (%d of %d bytes) was delivered as a complete body", len(r.body), promised)
				}
				if len(r.body) > sent {
					t.Errorf("client received %d bytes, the backend sent %d", len(r.body), sent)
				}
			}
			if truncated != requests/2 {
				t.Errorf("%d requests reached the dying backend, want %d", truncated, requests/2)
			}
			// No retry once response bytes were on their way to the client.
			if got := dying.hits.Load() + good.hits.Load(); got != requests {
				t.Errorf("backends saw %d requests for %d client requests", got, requests)
			}
			if got := in.metrics().sum("golb_upstream_requests_total", "backend", dying.addr, "result", "error"); got != requests/2 {
				t.Errorf("broken responses counted as failed attempts: %v, want %d", got, requests/2)
			}
		})
	}
}

func TestClientDisconnectCancelsUpstream(t *testing.T) {
	arrived := make(chan struct{}, 1)
	cancelled := make(chan struct{}, 1)
	hang := newBackend(t, "hang", func(w http.ResponseWriter, r *http.Request) {
		notify(arrived)
		select {
		case <-r.Context().Done():
			notify(cancelled)
		case <-time.After(waitTimeout):
		}
	})
	in := start(t, webConfig(poolOf("round_robin", hang), ""))
	client := newClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, in.url("/"), nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	failed := make(chan error, 1)
	go func() {
		_, err := do(client, req)
		failed <- err
	}()

	select {
	case <-arrived:
	case <-time.After(waitTimeout):
		t.Fatalf("request did not reach the backend")
	}
	if got := in.backend(hang.addr).Inflight; got != 1 {
		t.Errorf("in-flight while the request is held: %d, want 1", got)
	}
	cancel()
	if err := <-failed; err == nil {
		t.Errorf("cancelled request returned without error")
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Errorf("upstream request was not cancelled after the client went away")
	}
	if !eventually(2*time.Second, func() bool { return in.backend(hang.addr).Inflight == 0 }) {
		t.Errorf("in-flight stayed at %d after the client went away", in.backend(hang.addr).Inflight)
	}
	// The client going away is not the backend's fault.
	if got := in.metrics().sum("golb_upstream_requests_total", "backend", hang.addr, "result", "error"); got != 0 {
		t.Errorf("client cancellation was counted as %v backend failures", got)
	}
}

// notify signals without blocking; one signal is enough for the waiter.
func notify(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
