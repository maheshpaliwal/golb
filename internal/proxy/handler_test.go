package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/golb/internal/accesslog"
	"github.com/maheshpaliwal/golb/internal/core"
	"github.com/maheshpaliwal/golb/internal/metrics"
)

func TestForwardedHeaders(t *testing.T) {
	seen := make(chan *http.Request, 1)
	addr := backend(t, func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Clone(context.Background())
		w.Header().Set("Connection", "X-Backend-Hop")
		w.Header().Set("X-Backend-Hop", "1")
		w.Header().Set("Keep-Alive", "timeout=5")
		w.Header().Set("X-Backend-Kept", "yes")
	})
	f := newFixture(t, fixtureOptions{backends: []string{addr}}).serve(t)

	req, err := http.NewRequest(http.MethodGet, f.server.URL+"/path?q=1", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Host = "client.example.test"
	req.Header.Set("X-Forwarded-For", "203.0.113.7")
	req.Header.Set("X-Forwarded-Host", "spoofed.example.test")
	req.Header.Set("X-Forwarded-Proto", "https")
	req.Header.Set("Forwarded", "for=203.0.113.7;proto=https")
	req.Header.Set("Connection", "X-Client-Hop")
	req.Header.Set("X-Client-Hop", "1")
	req.Header.Set("Proxy-Authorization", "Basic placeholder")
	req.Header.Set("Te", "gzip")
	req.Header.Set("X-Client-Kept", "yes")
	res, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	res.Body.Close()
	got := <-seen

	requestHeaders := []struct {
		name string
		want string
	}{
		{name: "X-Forwarded-For", want: "127.0.0.1"},
		{name: "X-Forwarded-Host", want: "client.example.test"},
		{name: "X-Forwarded-Proto", want: "http"},
		{name: "Forwarded", want: ""},
		{name: "X-Client-Hop", want: ""},
		{name: "Proxy-Authorization", want: ""},
		{name: "Te", want: ""},
		{name: "Upgrade", want: ""},
		{name: "X-Client-Kept", want: "yes"},
	}
	for _, h := range requestHeaders {
		if values := strings.Join(got.Header.Values(h.name), "|"); values != h.want {
			t.Errorf("backend saw %s = %q, want %q", h.name, values, h.want)
		}
	}
	if got.Host != "client.example.test" {
		t.Errorf("backend saw Host %q, want the client's Host", got.Host)
	}
	if got.URL.RequestURI() != "/path?q=1" {
		t.Errorf("backend saw %q, want /path?q=1", got.URL.RequestURI())
	}

	responseHeaders := []struct {
		name string
		want string
	}{
		{name: "X-Backend-Hop", want: ""},
		{name: "Keep-Alive", want: ""},
		{name: "X-Backend-Kept", want: "yes"},
	}
	for _, h := range responseHeaders {
		if value := res.Header.Get(h.name); value != h.want {
			t.Errorf("client saw %s = %q, want %q", h.name, value, h.want)
		}
	}
}

func TestPathForwardedUnmodified(t *testing.T) {
	seen := make(chan string, 1)
	addr := backend(t, func(w http.ResponseWriter, r *http.Request) { seen <- r.RequestURI })
	f := newFixture(t, fixtureOptions{backends: []string{addr}}).serve(t)

	const target = "/a//b/../c%2Fd?x=%20y"
	conn, err := net.Dial("tcp", f.server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", target)
	res, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	res.Body.Close()
	if got := <-seen; got != target {
		t.Errorf("backend saw %q, want the client's target %q", got, target)
	}
}

func TestProxyGeneratedStatuses(t *testing.T) {
	ok := backend(t, func(w http.ResponseWriter, r *http.Request) {})
	tests := []struct {
		name       string
		opts       fixtureOptions
		method     string
		path       string
		requests   int
		wantStatus int
		wantBody   string
		wantClass  string
		wantRoute  string
	}{
		{
			name:       "no backend available",
			opts:       fixtureOptions{backends: nil},
			path:       "/",
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "Service Unavailable\n",
			wantClass:  "5xx",
			wantRoute:  "/",
		},
		{
			name:       "connection refused",
			opts:       fixtureOptions{backends: []string{refusedAddr(t)}},
			path:       "/",
			wantStatus: http.StatusBadGateway,
			wantBody:   "Bad Gateway\n",
			wantClass:  "5xx",
			wantRoute:  "/",
		},
		{
			name:       "no route",
			opts:       fixtureOptions{backends: []string{ok}, routes: "      - path_prefix: /api\n        pool: app\n"},
			path:       "/elsewhere",
			wantStatus: http.StatusNotFound,
			wantBody:   "Not Found\n",
			wantClass:  "4xx",
			wantRoute:  unmatchedLabel,
		},
		{
			name: "rate limited",
			opts: fixtureOptions{
				backends: []string{ok},
				routes:   "      - pool: app\n        rate_limit:\n          requests_per_second: 0.001\n          burst: 1\n",
			},
			path:       "/",
			requests:   2,
			wantStatus: http.StatusTooManyRequests,
			wantBody:   "Too Many Requests\n",
			wantClass:  "4xx",
			wantRoute:  "/",
		},
		{
			name:       "CONNECT is not proxied",
			opts:       fixtureOptions{backends: []string{ok}},
			method:     http.MethodConnect,
			path:       "example.test:443",
			wantStatus: http.StatusBadRequest,
			wantBody:   "Bad Request\n",
			wantClass:  "4xx",
			wantRoute:  unmatchedLabel,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.opts.backends == nil {
				// The config needs a backend; the pool handed to the proxy has none.
				tc.opts.backends = []string{ok}
			}
			f := newFixture(t, tc.opts)
			if tc.name == "no backend available" {
				f.pool.backends = nil
			}
			f.serve(t)

			var res *http.Response
			for range max(tc.requests, 1) {
				conn, err := net.Dial("tcp", f.server.Listener.Addr().String())
				if err != nil {
					t.Fatalf("dial proxy: %v", err)
				}
				defer conn.Close()
				method := tc.method
				if method == "" {
					method = http.MethodGet
				}
				fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", method, tc.path)
				res, err = http.ReadResponse(bufio.NewReader(conn), nil)
				if err != nil {
					t.Fatalf("read response: %v", err)
				}
				defer res.Body.Close()
			}
			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if res.StatusCode != tc.wantStatus {
				t.Errorf("status = %d, want %d", res.StatusCode, tc.wantStatus)
			}
			if string(body) != tc.wantBody {
				t.Errorf("body = %q, want %q", body, tc.wantBody)
			}
			if got := f.counter("golb_requests_total", "listener", "web", "route", tc.wantRoute, "code_class", tc.wantClass); got != 1 {
				t.Errorf("golb_requests_total{route=%q,code_class=%q} = %d, want 1", tc.wantRoute, tc.wantClass, got)
			}
			f.pool.waitIdle(t)
		})
	}
}

func TestErrorBodyHidesBackend(t *testing.T) {
	dead := refusedAddr(t)
	f := newFixture(t, fixtureOptions{backends: []string{dead}}).serve(t)
	status, body := get(t, f.server.URL)
	if status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", status)
	}
	_, port, _ := net.SplitHostPort(dead)
	if strings.Contains(body, port) || strings.Contains(body, "refused") || strings.Contains(body, "dial") {
		t.Errorf("error body leaks upstream detail: %q", body)
	}
}

func TestRetry(t *testing.T) {
	type hits struct{ bad, good atomic.Int64 }
	failing := func(h *hits, status int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			h.bad.Add(1)
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(status)
			io.WriteString(w, "from the failing backend")
		}
	}
	healthy := func(h *hits) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			h.good.Add(1)
			body, _ := io.ReadAll(r.Body)
			fmt.Fprintf(w, "ok %s", body)
		}
	}
	tests := []struct {
		name        string
		firstStatus int    // 0 means the first backend refuses connections
		pool        string // extra pool YAML
		method      string
		body        string
		wantStatus  int
		wantBody    string
		wantBad     int64
		wantGood    int64
		wantRetries uint64
	}{
		{
			name: "idempotent request retried on a retriable status", firstStatus: 503, method: http.MethodGet,
			wantStatus: 200, wantBody: "ok ", wantBad: 1, wantGood: 1, wantRetries: 1,
		},
		{
			name: "status outside retry_on is passed through", firstStatus: 500, method: http.MethodGet,
			wantStatus: 500, wantBody: "from the failing backend", wantBad: 1,
		},
		{
			name: "POST is not retried on a status", firstStatus: 503, method: http.MethodPost, body: "payload",
			wantStatus: 503, wantBody: "from the failing backend", wantBad: 1,
		},
		{
			name: "PUT with a body is not retried on a status", firstStatus: 503, method: http.MethodPut, body: "payload",
			wantStatus: 503, wantBody: "from the failing backend", wantBad: 1,
		},
		{
			name: "DELETE without a body is retried", firstStatus: 502, method: http.MethodDelete,
			wantStatus: 200, wantBody: "ok ", wantBad: 1, wantGood: 1, wantRetries: 1,
		},
		{
			name: "max_attempts 1 disables retries", firstStatus: 503, method: http.MethodGet,
			pool:       "    retry:\n      max_attempts: 1\n",
			wantStatus: 503, wantBody: "from the failing backend", wantBad: 1,
		},
		{
			name: "connect failure retried for GET", method: http.MethodGet,
			wantStatus: 200, wantBody: "ok ", wantGood: 1, wantRetries: 1,
		},
		{
			name: "connect failure retried for POST with the body intact", method: http.MethodPost, body: "payload",
			wantStatus: 200, wantBody: "ok payload", wantGood: 1, wantRetries: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := new(hits)
			first := refusedAddr(t)
			if tc.firstStatus != 0 {
				first = backend(t, failing(h, tc.firstStatus))
			}
			second := backend(t, healthy(h))
			f := newFixture(t, fixtureOptions{backends: []string{first, second}, pool: tc.pool}).serve(t)

			req, err := http.NewRequest(tc.method, f.server.URL, strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			res, err := f.server.Client().Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}

			if res.StatusCode != tc.wantStatus || string(body) != tc.wantBody {
				t.Errorf("got %d %q, want %d %q", res.StatusCode, body, tc.wantStatus, tc.wantBody)
			}
			if h.bad.Load() != tc.wantBad || h.good.Load() != tc.wantGood {
				t.Errorf("backend hits: failing %d, healthy %d; want %d, %d", h.bad.Load(), h.good.Load(), tc.wantBad, tc.wantGood)
			}
			if got := f.counter("golb_retries_total", "pool", "app"); got != tc.wantRetries {
				t.Errorf("golb_retries_total = %d, want %d", got, tc.wantRetries)
			}
			f.pool.waitIdle(t)
			calls := f.pool.calls()
			if len(calls) != int(tc.wantRetries)+1 {
				t.Fatalf("Done called %d times, want %d: %+v", len(calls), tc.wantRetries+1, calls)
			}
			if !calls[0].result.Failed || calls[0].addr != first {
				t.Errorf("first Done = %+v, want a failure on %s", calls[0], first)
			}
			if tc.wantRetries == 1 && (calls[1].result.Failed || calls[1].addr != second || calls[1].result.Latency <= 0) {
				t.Errorf("second Done = %+v, want a sampled success on %s", calls[1], second)
			}
		})
	}
}

func TestRetryNeverPicksSameBackendTwice(t *testing.T) {
	var hits [3]atomic.Int64
	var addrs []string
	for i := range hits {
		addrs = append(addrs, backend(t, func(w http.ResponseWriter, r *http.Request) {
			hits[i].Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
	}
	f := newFixture(t, fixtureOptions{backends: addrs, pool: "    retry:\n      max_attempts: 5\n"}).serve(t)

	status, _ := get(t, f.server.URL)
	if status != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want the last backend's 503", status)
	}
	for i := range hits {
		if got := hits[i].Load(); got != 1 {
			t.Errorf("backend %d received %d requests, want exactly 1", i, got)
		}
	}
	f.pool.waitIdle(t)
}

func TestRetryBudgetBoundsUpstreamLoad(t *testing.T) {
	const (
		requests = 60
		// Requests may straddle a second boundary, so two seconds of floor apply.
		maxRetries = requests*20/percentScale + 2*budgetFloorPerSecond
	)
	var upstreamHits atomic.Int64
	failing := func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	f := newFixture(t, fixtureOptions{backends: []string{backend(t, failing), backend(t, failing), backend(t, failing)}}).serve(t)

	for range requests {
		status, _ := get(t, f.server.URL)
		if status != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", status)
		}
	}
	retries := upstreamHits.Load() - requests
	if retries < budgetFloorPerSecond || retries > maxRetries {
		t.Errorf("%d retries for %d requests, want between %d and %d", retries, requests, budgetFloorPerSecond, maxRetries)
	}
	if got := f.counter("golb_retries_total", "pool", "app"); int64(got) != retries {
		t.Errorf("golb_retries_total = %d, want %d", got, retries)
	}
	if got := f.counter("golb_retry_budget_exhausted_total", "pool", "app"); got == 0 {
		t.Error("golb_retry_budget_exhausted_total = 0, want the skipped retries counted")
	}
	f.pool.waitIdle(t)
}

func TestPerTryTimeout(t *testing.T) {
	const perTry = 150 * time.Millisecond
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	stalled := func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}
	fast := func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "fast") }

	tests := []struct {
		name       string
		backends   []http.HandlerFunc
		listener   string
		wantStatus int
		wantBody   string
		maxElapsed time.Duration
	}{
		{
			name:       "stalled backend is abandoned for the next one",
			backends:   []http.HandlerFunc{stalled, fast},
			wantStatus: http.StatusOK,
			wantBody:   "fast",
			maxElapsed: 10 * perTry,
		},
		{
			name:       "every backend stalled",
			backends:   []http.HandlerFunc{stalled, stalled},
			wantStatus: http.StatusGatewayTimeout,
			wantBody:   "Gateway Timeout\n",
			maxElapsed: 12 * perTry,
		},
		{
			name:       "request timeout cuts the last try short",
			backends:   []http.HandlerFunc{stalled, stalled, stalled},
			listener:   "    timeouts:\n      request: 200ms\n",
			wantStatus: http.StatusGatewayTimeout,
			wantBody:   "Gateway Timeout\n",
			// Three full tries would take 450ms; the bound is 200ms plus slack.
			maxElapsed: 400 * time.Millisecond,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var addrs []string
			for _, h := range tc.backends {
				addrs = append(addrs, backend(t, h))
			}
			f := newFixture(t, fixtureOptions{
				backends: addrs,
				listener: tc.listener,
				pool:     "    retry:\n      per_try_timeout: 150ms\n",
			}).serve(t)

			began := time.Now()
			status, body := get(t, f.server.URL)
			elapsed := time.Since(began)
			if status != tc.wantStatus || body != tc.wantBody {
				t.Errorf("got %d %q, want %d %q", status, body, tc.wantStatus, tc.wantBody)
			}
			if elapsed > tc.maxElapsed {
				t.Errorf("took %v, want under %v", elapsed, tc.maxElapsed)
			}
			f.pool.waitIdle(t)
			if first := f.pool.calls()[0]; !first.result.Failed {
				t.Errorf("timed-out try reported as %+v, want Failed", first)
			}
		})
	}
}

func TestBackendDiesMidResponse(t *testing.T) {
	var hits atomic.Int64
	dying := func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Length", "1000")
		io.WriteString(w, "partial")
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		conn.Close()
	}
	f := newFixture(t, fixtureOptions{backends: []string{backend(t, dying), backend(t, dying)}}).serve(t)

	// The client must see a broken response, either before the headers or
	// as a truncated body; it must never see a complete one or hang.
	client := &http.Client{Timeout: waitLimit}
	res, err := client.Get(f.server.URL)
	if err == nil {
		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr == nil {
			t.Errorf("read a complete body (%q) of a response the backend abandoned, want an error", body)
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Errorf("client hung until its timeout: %v", err)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("backends received %d requests, want 1: a response in progress must not be retried", got)
	}
	f.pool.waitIdle(t)
	calls := f.pool.calls()
	if len(calls) != 1 || !calls[0].result.Failed {
		t.Errorf("Done calls = %+v, want one failure", calls)
	}
}

func TestClientCancelIsNotAFailure(t *testing.T) {
	arrived := make(chan struct{}, 2)
	gone := make(chan struct{}, 2)
	waiting := func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		<-r.Context().Done()
		gone <- struct{}{}
	}
	f := newFixture(t, fixtureOptions{backends: []string{backend(t, waiting), backend(t, waiting)}}).serve(t)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.server.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	failed := make(chan error, 1)
	go func() {
		_, err := f.server.Client().Do(req)
		failed <- err
	}()
	<-arrived
	cancel()
	if err := <-failed; !errors.Is(err, context.Canceled) {
		t.Fatalf("client error = %v, want context.Canceled", err)
	}
	select {
	case <-gone:
	case <-time.After(waitLimit):
		t.Fatal("upstream request was not cancelled when the client left")
	}
	f.pool.waitIdle(t)
	calls := f.pool.calls()
	if len(calls) != 1 || calls[0].result.Failed || calls[0].result.Latency != 0 {
		t.Errorf("Done calls = %+v, want one unsampled non-failure and no retry", calls)
	}
}

func TestUpgradeEcho(t *testing.T) {
	echo := func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || !strings.EqualFold(r.Header.Get("Connection"), "upgrade") {
			http.Error(w, "expected an upgrade", http.StatusBadRequest)
			return
		}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		io.WriteString(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		rw.Flush()
		io.Copy(conn, rw)
	}
	f := newFixture(t, fixtureOptions{backends: []string{backend(t, echo)}}).serve(t)

	conn, err := net.Dial("tcp", f.server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(waitLimit))
	io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"+
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGVzdGluZy1vbmx5LWtleQ==\r\n\r\n")
	reader := bufio.NewReader(conn)
	res, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("read handshake: %v", err)
	}
	if res.StatusCode != http.StatusSwitchingProtocols || !strings.EqualFold(res.Header.Get("Upgrade"), "websocket") {
		t.Fatalf("handshake = %d Upgrade=%q, want 101 websocket", res.StatusCode, res.Header.Get("Upgrade"))
	}

	for _, message := range []string{"first frame", "second frame"} {
		io.WriteString(conn, message)
		got := make([]byte, len(message))
		_, err = io.ReadFull(reader, got)
		if err != nil {
			t.Fatalf("read echo: %v", err)
		}
		if string(got) != message {
			t.Errorf("echo = %q, want %q", got, message)
		}
	}
	if got := f.pool.inflight(); got != 1 {
		t.Errorf("in-flight during the upgraded connection = %d, want 1", got)
	}
	conn.Close()
	f.pool.waitIdle(t)
	calls := f.pool.calls()
	if len(calls) != 1 || calls[0].result.Failed {
		t.Errorf("Done calls = %+v, want one success after the connection closed", calls)
	}
	if got := f.counter("golb_requests_total", "listener", "web", "route", "/", "code_class", "1xx"); got != 1 {
		t.Errorf("golb_requests_total{code_class=1xx} = %d, want 1", got)
	}
}

func TestUpgradeRefusedByBackendIsNotRetried(t *testing.T) {
	var hits atomic.Int64
	dropping := func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		conn.Close()
	}
	f := newFixture(t, fixtureOptions{backends: []string{backend(t, dropping), backend(t, dropping)}}).serve(t)

	req, err := http.NewRequest(http.MethodGet, f.server.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	res, err := f.server.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", res.StatusCode)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("backends received %d upgrade requests, want 1", got)
	}
	f.pool.waitIdle(t)
}

func TestStreamingResponseIsFlushed(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
	}{
		{name: "server-sent events", contentType: "text/event-stream"},
		{name: "chunked without a length", contentType: "application/octet-stream"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			addr := backend(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				io.WriteString(w, "data: one\n\n")
				w.(http.Flusher).Flush()
				<-release
				io.WriteString(w, "data: two\n\n")
			})
			f := newFixture(t, fixtureOptions{backends: []string{addr}}).serve(t)

			client := &http.Client{Timeout: waitLimit}
			res, err := client.Get(f.server.URL)
			if err != nil {
				close(release)
				t.Fatalf("GET: %v", err)
			}
			defer res.Body.Close()
			reader := bufio.NewReader(res.Body)
			first, err := reader.ReadString('\n')
			close(release)
			if err != nil || first != "data: one\n" {
				t.Fatalf("first event = %q, %v; want it before the backend finishes", first, err)
			}
			rest, err := io.ReadAll(reader)
			if err != nil || string(rest) != "\ndata: two\n\n" {
				t.Errorf("rest = %q, %v", rest, err)
			}
		})
	}
}

func TestLargeBodiesBothDirections(t *testing.T) {
	const size = 4 << 20
	payload := make([]byte, size)
	_, err := rand.Read(payload)
	if err != nil {
		t.Fatalf("random payload: %v", err)
	}
	addr := backend(t, func(w http.ResponseWriter, r *http.Request) {
		// Read everything first: an HTTP/1 server stops reading the request
		// once it starts the response.
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Trailer", "X-Checksum")
		w.Write(body)
		w.Header().Set("X-Checksum", fmt.Sprint(len(body)))
	})
	f := newFixture(t, fixtureOptions{backends: []string{addr}}).serve(t)

	tests := []struct {
		name string
		body io.Reader
	}{
		{name: "known length", body: bytes.NewReader(payload)},
		{name: "chunked", body: io.MultiReader(bytes.NewReader(payload))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := http.Post(f.server.URL, "application/octet-stream", tc.body)
			if err != nil {
				t.Fatalf("POST: %v", err)
			}
			defer res.Body.Close()
			got, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if !bytes.Equal(got, payload) {
				t.Errorf("echoed body differs: %d bytes, want %d identical bytes", len(got), size)
			}
			if trailer := res.Trailer.Get("X-Checksum"); trailer != fmt.Sprint(size) {
				t.Errorf("trailer X-Checksum = %q, want %d", trailer, size)
			}
		})
	}
	f.pool.waitIdle(t)
}

func TestHTTP2Downstream(t *testing.T) {
	seen := make(chan *http.Request, 1)
	addr := backend(t, func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Clone(context.Background())
		io.WriteString(w, "over h2")
	})
	f := newFixture(t, fixtureOptions{backends: []string{addr}})
	server := httptest.NewUnstartedServer(f.handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	res, err := server.Client().Get(server.URL + "/h2")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if res.ProtoMajor != 2 {
		t.Errorf("client spoke HTTP/%d, want 2", res.ProtoMajor)
	}
	if string(body) != "over h2" {
		t.Errorf("body = %q", body)
	}
	got := <-seen
	if got.ProtoMajor != 1 {
		t.Errorf("backend saw HTTP/%d, want 1", got.ProtoMajor)
	}
	if proto := got.Header.Get("X-Forwarded-Proto"); proto != "https" {
		t.Errorf("X-Forwarded-Proto = %q, want https", proto)
	}
}

// Request framing is parsed by net/http alone. These cases pin what it does
// with ambiguous framing so a change in behaviour is noticed.
func TestAmbiguousFraming(t *testing.T) {
	tests := []struct {
		name        string
		request     string
		wantStatus  int
		wantBackend bool
		wantBody    string
	}{
		{
			name:       "conflicting Content-Length headers",
			request:    "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nContent-Length: 11\r\n\r\nabcdGET /x ",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid Content-Length",
			request:    "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: +4\r\n\r\nabcd",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "unsupported Transfer-Encoding",
			request:    "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked, identity\r\n\r\n4\r\nabcd\r\n0\r\n\r\n",
			wantStatus: http.StatusNotImplemented,
		},
		{
			name:       "space before the colon",
			request:    "POST / HTTP/1.1\r\nHost: x\r\nContent-Length : 4\r\n\r\nabcd",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "header block over the limit",
			request:    "GET / HTTP/1.1\r\nHost: x\r\nX-Big: " + strings.Repeat("a", 8192) + "\r\n\r\n",
			wantStatus: http.StatusRequestHeaderFieldsTooLarge,
		},
		{
			// net/http follows RFC 9112 6.3: chunked wins and Content-Length
			// is dropped before the handler runs, so the backend receives one
			// unambiguous message.
			name:        "Content-Length with chunked is reframed, not forwarded",
			request:     "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nabcd\r\n0\r\n\r\n",
			wantStatus:  http.StatusOK,
			wantBackend: true,
			wantBody:    "abcd",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var bodies []string
			var lengths []string
			addr := backend(t, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				defer mu.Unlock()
				bodies = append(bodies, string(body))
				lengths = append(lengths, r.Header.Get("Content-Length"))
			})
			f := newFixture(t, fixtureOptions{backends: []string{addr}})
			server := httptest.NewUnstartedServer(f.handler)
			server.Config.MaxHeaderBytes = 1024
			server.Start()
			t.Cleanup(server.Close)

			conn, err := net.Dial("tcp", server.Listener.Addr().String())
			if err != nil {
				t.Fatalf("dial proxy: %v", err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(waitLimit))
			io.WriteString(conn, tc.request)
			res, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			res.Body.Close()
			if res.StatusCode != tc.wantStatus {
				t.Errorf("status = %d, want %d", res.StatusCode, tc.wantStatus)
			}

			mu.Lock()
			defer mu.Unlock()
			if !tc.wantBackend {
				if len(bodies) != 0 {
					t.Errorf("backend received %d requests (%q), want none", len(bodies), bodies)
				}
				return
			}
			if len(bodies) != 1 || bodies[0] != tc.wantBody {
				t.Errorf("backend bodies = %q, want exactly [%q]", bodies, tc.wantBody)
			}
			if len(lengths) == 1 && lengths[0] != "" {
				t.Errorf("backend saw Content-Length %q next to chunked encoding, want it removed", lengths[0])
			}
		})
	}
}

func TestSwapRouterUnderLoad(t *testing.T) {
	const (
		workers = 16
		swaps   = 200
	)
	var hitsA, hitsB atomic.Int64
	addrA := backend(t, func(w http.ResponseWriter, r *http.Request) { hitsA.Add(1); io.WriteString(w, "a") })
	addrB := backend(t, func(w http.ResponseWriter, r *http.Request) { hitsB.Add(1); io.WriteString(w, "b") })
	fa := newFixture(t, fixtureOptions{backends: []string{addrA}})
	fb := newFixture(t, fixtureOptions{backends: []string{addrB}})

	var current atomic.Pointer[Router]
	current.Store(fa.router)
	handler := NewHandler(current.Load, quietLogger())
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	stop := make(chan struct{})
	var failures, served atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: waitLimit}
			defer client.CloseIdleConnections()
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := client.Get(server.URL)
				if err != nil {
					failures.Add(1)
					continue
				}
				body, err := io.ReadAll(res.Body)
				res.Body.Close()
				if err != nil || res.StatusCode != http.StatusOK || (string(body) != "a" && string(body) != "b") {
					failures.Add(1)
					continue
				}
				served.Add(1)
			}
		}()
	}
	for i := range swaps {
		next := fa.router
		if i%2 == 0 {
			next = fb.router
		}
		current.Store(next)
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if failures.Load() != 0 {
		t.Errorf("%d of %d requests failed while routers were swapped, want 0", failures.Load(), failures.Load()+served.Load())
	}
	if hitsA.Load() == 0 || hitsB.Load() == 0 {
		t.Errorf("backend hits a=%d b=%d, want traffic on both configurations", hitsA.Load(), hitsB.Load())
	}
	fa.pool.waitIdle(t)
	fb.pool.waitIdle(t)
}

func TestRingHashKey(t *testing.T) {
	addr := backend(t, func(w http.ResponseWriter, r *http.Request) {})
	tests := []struct {
		name        string
		pool        string
		header      string // X-User on the first request
		otherHeader string // X-User on the second request
		wantEqual   bool
	}{
		{name: "same header value, same key", pool: "    policy: ring_hash\n    hash_header: x-user\n", header: "alice", otherHeader: "alice", wantEqual: true},
		{name: "different header value, different key", pool: "    policy: ring_hash\n    hash_header: x-user\n", header: "alice", otherHeader: "bob", wantEqual: false},
		{name: "absent header falls back to the client IP", pool: "    policy: ring_hash\n    hash_header: x-user\n", wantEqual: true},
		{name: "no header configured uses the client IP", pool: "    policy: ring_hash\n", header: "alice", otherHeader: "bob", wantEqual: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, fixtureOptions{backends: []string{addr}, pool: tc.pool})
			keys := &keyRecorder{rotatingPool: f.pool}
			f.upstream.pool = keys
			f.serve(t)
			for _, value := range []string{tc.header, tc.otherHeader} {
				req, err := http.NewRequest(http.MethodGet, f.server.URL, nil)
				if err != nil {
					t.Fatalf("build request: %v", err)
				}
				if value != "" {
					req.Header.Set("X-User", value)
				}
				res, err := f.server.Client().Do(req)
				if err != nil {
					t.Fatalf("request: %v", err)
				}
				res.Body.Close()
			}
			seen := keys.keys()
			if len(seen) != 2 || seen[0] == 0 {
				t.Fatalf("keys = %v, want two non-zero keys", seen)
			}
			if equal := seen[0] == seen[1]; equal != tc.wantEqual {
				t.Errorf("keys %v equal = %v, want %v", seen, equal, tc.wantEqual)
			}
		})
	}
}

func TestAccessLogRecord(t *testing.T) {
	addr := backend(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "hello") })
	f := newFixture(t, fixtureOptions{backends: []string{addr}})
	out := new(bytes.Buffer)
	logger := accesslog.New(accesslog.Options{
		Out:        out,
		BufferSize: 8,
		SampleRate: 1,
		Dropped:    metrics.NewRegistry().Counter("dropped", ""),
		Logger:     quietLogger(),
	})
	f.router.log = logger
	f.serve(t)

	status, _ := get(t, f.server.URL+"/logged")
	if status != http.StatusOK {
		t.Fatalf("status = %d", status)
	}
	logger.Close()
	line := out.String()
	for _, want := range []string{
		`"listener":"web"`, `"route":"/"`, `"method":"GET"`, `"path":"/logged"`, `"status":200`,
		`"bytes":5`, `"backend":"` + addr + `"`, `"attempts":1`, `"proto":"HTTP/1.1"`,
	} {
		if !strings.Contains(line, want) {
			t.Errorf("access log line lacks %s:\n%s", want, line)
		}
	}
}

func TestUpstreamKeepsTransportAcrossReload(t *testing.T) {
	reg := metrics.NewRegistry()
	build := func(pool string, prev *Upstream) *Upstream {
		cfg := parseConfig(t, fixtureOptions{backends: []string{"127.0.0.1:1"}, pool: pool})
		return NewUpstream(UpstreamOptions{Config: cfg.Pools[0], Pool: newRotatingPool("127.0.0.1:1"), Registry: reg, Prev: prev})
	}
	first := build("", nil)
	same := build("    policy: round_robin\n", first)
	retuned := build("    transport:\n      dial_timeout: 1s\n", same)

	if same.transport != first.transport || same.budget != first.budget {
		t.Error("unchanged transport settings built a new transport or budget; warm connections would be lost on reload")
	}
	if retuned.transport == same.transport {
		t.Error("changed dial_timeout kept the old transport")
	}
	if retuned.budget != same.budget {
		t.Error("unchanged budget_percent reset the retry budget")
	}
}

// keyRecorder records the affinity key of every Pick.
type keyRecorder struct {
	*rotatingPool
	mu   sync.Mutex
	seen []uint64
}

func (k *keyRecorder) Pick(info core.PickInfo) *core.Backend {
	k.mu.Lock()
	k.seen = append(k.seen, info.Key)
	k.mu.Unlock()
	return k.rotatingPool.Pick(info)
}

func (k *keyRecorder) keys() []uint64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]uint64(nil), k.seen...)
}
