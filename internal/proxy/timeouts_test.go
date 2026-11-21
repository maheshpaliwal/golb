package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// rawExchange writes request to the proxy and returns everything the proxy
// sends until it closes the connection, plus how long that took. A proxy
// that keeps the connection open past limit fails the test.
func rawExchange(t *testing.T, addr, request string, limit time.Duration) (string, time.Duration) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	began := time.Now()
	conn.SetDeadline(began.Add(limit))
	io.WriteString(conn, request)
	reply, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("connection still open after %v (read so far %q): %v", limit, reply, err)
	}
	return string(reply), time.Since(began)
}

func statusLine(reply string) string {
	line, _, _ := strings.Cut(reply, "\r\n")
	return line
}

func TestStalledUploadIsEnded(t *testing.T) {
	tests := []struct {
		name     string
		listener string
		bound    time.Duration
	}{
		{name: "by the request timeout", listener: "    timeouts:\n      request: 300ms\n", bound: 300 * time.Millisecond},
		{name: "by body_idle", listener: "    timeouts:\n      request: 0s\n      body_idle: 150ms\n", bound: 300 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addr := backend(t, func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body) })
			f := newFixture(t, fixtureOptions{backends: []string{addr}, listener: tc.listener}).serve(t)

			// Half of the announced body, then silence.
			request := "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\n" + strings.Repeat("a", 500)
			reply, took := rawExchange(t, f.server.Listener.Addr().String(), request, waitLimit)
			if got := statusLine(reply); got != "HTTP/1.1 408 Request Timeout" {
				t.Errorf("status line %q, want 408", got)
			}
			if took > tc.bound+2*time.Second {
				t.Errorf("stalled upload held for %v, want about %v", took, tc.bound)
			}
			f.pool.waitIdle(t)
			for _, call := range f.pool.calls() {
				if call.result.Failed {
					t.Errorf("a stalled client was held against the backend: %+v", call)
				}
			}
		})
	}
}

func TestStalledResponseIsEnded(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	addr := backend(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "first bytes")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	f := newFixture(t, fixtureOptions{backends: []string{addr}, listener: "    timeouts:\n      body_idle: 150ms\n"}).serve(t)

	reply, took := rawExchange(t, f.server.Listener.Addr().String(), "GET / HTTP/1.1\r\nHost: x\r\n\r\n", waitLimit)
	if !strings.Contains(reply, "first bytes") {
		t.Errorf("reply %q lacks the bytes sent before the stall", reply)
	}
	if strings.HasSuffix(reply, "0\r\n\r\n") {
		t.Errorf("stalled response was completed as if it had ended normally: %q", reply)
	}
	if took > 2*time.Second {
		t.Errorf("stalled response held for %v, want between 150ms and 300ms", took)
	}
	f.pool.waitIdle(t)
	calls := f.pool.calls()
	if len(calls) != 1 || calls[0].result.Failed {
		t.Errorf("Done calls = %+v, want one that does not blame the backend for a quiet stream", calls)
	}
}

// A stream that keeps moving outlives both body_idle and the request timeout.
func TestSlowButMovingStreamsAreNotCut(t *testing.T) {
	const (
		idle   = 200 * time.Millisecond
		step   = 50 * time.Millisecond
		pieces = 12 // 600ms in total: three idle intervals
	)
	timeouts := "    timeouts:\n      body_idle: 200ms\n      request: 300ms\n"
	tests := []struct {
		name    string
		backend http.HandlerFunc
		request func(target string) (*http.Request, error)
		want    string
	}{
		{
			name: "response",
			backend: func(w http.ResponseWriter, r *http.Request) {
				for i := range pieces {
					fmt.Fprintf(w, "%x", i)
					w.(http.Flusher).Flush()
					time.Sleep(step)
				}
			},
			request: func(target string) (*http.Request, error) { return http.NewRequest(http.MethodGet, target, nil) },
			want:    "0123456789ab",
		},
		{
			name: "upload",
			backend: func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				w.Write(body)
			},
			request: func(target string) (*http.Request, error) {
				reader, writer := io.Pipe()
				go func() {
					for i := range pieces {
						fmt.Fprintf(writer, "%x", i)
						time.Sleep(step)
					}
					writer.Close()
				}()
				return http.NewRequest(http.MethodPost, target, reader)
			},
			want: "0123456789ab",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, fixtureOptions{backends: []string{backend(t, tc.backend)}, listener: timeouts}).serve(t)
			req, err := tc.request(f.server.URL)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			began := time.Now()
			res, err := f.server.Client().Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil || res.StatusCode != http.StatusOK || string(body) != tc.want {
				t.Errorf("got %d %q (%v) after %v, want 200 %q", res.StatusCode, body, err, time.Since(began), tc.want)
			}
			if took := time.Since(began); took < 2*idle {
				t.Fatalf("exchange took %v, too short to prove anything about a %v idle limit", took, idle)
			}
		})
	}
}

func TestIdleUpgradedConnectionIsClosed(t *testing.T) {
	echo := func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		io.WriteString(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		rw.Flush()
		io.Copy(conn, rw)
	}
	f := newFixture(t, fixtureOptions{backends: []string{backend(t, echo)}, listener: "    timeouts:\n      body_idle: 150ms\n"}).serve(t)

	conn, err := net.Dial("tcp", f.server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(waitLimit))
	io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	reader := bufio.NewReader(conn)
	res, err := http.ReadResponse(reader, nil)
	if err != nil || res.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake: %v, %v", res, err)
	}
	// Messages more often than the idle limit keep it open past the limit.
	for range 8 {
		time.Sleep(50 * time.Millisecond)
		io.WriteString(conn, "m")
		_, err = io.ReadFull(reader, make([]byte, 1))
		if err != nil {
			t.Fatalf("active upgraded connection was closed: %v", err)
		}
	}
	began := time.Now()
	_, err = reader.ReadByte()
	if err == nil {
		t.Fatal("read data from an idle echo connection")
	}
	if waited := time.Since(began); waited > 2*time.Second {
		t.Errorf("idle upgraded connection closed after %v (%v), want between 150ms and 300ms", waited, err)
	}
	f.pool.waitIdle(t)
	if calls := f.pool.calls(); len(calls) != 1 || calls[0].result.Failed {
		t.Errorf("Done calls = %+v, want one success", calls)
	}
}

// A backend that answers 503 and then stalls must not delay the retry.
func TestRetryDoesNotWaitForARejectedBody(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	stalling := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}
	good := func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }
	f := newFixture(t, fixtureOptions{backends: []string{backend(t, stalling), backend(t, good)}}).serve(t)

	client := &http.Client{Timeout: 2 * time.Second}
	began := time.Now()
	res, err := client.Get(f.server.URL)
	if err != nil {
		t.Fatalf("GET after %v: %v", time.Since(began), err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Errorf("got %d %q, want the second backend's 200 ok", res.StatusCode, body)
	}
	f.pool.waitIdle(t)
}

func TestRetryBudgetIsNotSpentWithoutAnotherBackend(t *testing.T) {
	const requests = 10
	failing := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }
	f := newFixture(t, fixtureOptions{backends: []string{backend(t, failing)}}).serve(t)
	for range requests {
		status, _ := get(t, f.server.URL)
		if status != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", status)
		}
	}
	if spent := windowSum(&f.upstream.budget.retries, uint64(time.Now().Unix())); spent != 0 {
		t.Errorf("%d retries charged to the budget with no second backend to retry on, want 0", spent)
	}
	if got := f.counter("golb_retry_budget_exhausted_total", "pool", "app"); got != 0 {
		t.Errorf("golb_retry_budget_exhausted_total = %d, want 0", got)
	}
}

func TestClientBodyErrorsAreNotBackendFailures(t *testing.T) {
	var completed atomic.Int64
	addr := backend(t, func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		if err == nil {
			completed.Add(1)
		}
	})
	f := newFixture(t, fixtureOptions{backends: []string{addr}}).serve(t)

	const malformed = "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nZZ\r\nabc\r\n0\r\n\r\n"
	for range 8 {
		reply, _ := rawExchange(t, f.server.Listener.Addr().String(), malformed, waitLimit)
		if got := statusLine(reply); got != "HTTP/1.1 400 Bad Request" {
			t.Errorf("status line %q, want 400", got)
		}
	}
	f.pool.waitIdle(t)
	for _, call := range f.pool.calls() {
		if call.result.Failed || call.result.Latency != 0 {
			t.Errorf("malformed client body reported to the pool as %+v, want an unsampled non-failure", call)
		}
	}
	if got := f.counter("golb_upstream_requests_total", "pool", "app", "backend", addr, "result", "error"); got != 0 {
		t.Errorf("%d backend errors counted for malformed client bodies, want 0", got)
	}
	if completed.Load() != 0 {
		t.Errorf("backend received %d complete requests from malformed input, want 0", completed.Load())
	}
}

// net/http drops Content-Length from a chunked request without telling the
// handler, so the connection of every chunked HTTP/1 request is closed, as
// RFC 9112 6.3 requires for the ambiguous case.
func TestChunkedRequestEndsTheClientConnection(t *testing.T) {
	addr := backend(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		fmt.Fprintf(w, "%s|%s|%s", body, r.Header.Get("Content-Length"), strings.Join(r.TransferEncoding, ","))
	})
	f := newFixture(t, fixtureOptions{backends: []string{addr}}).serve(t)
	tests := []struct {
		name      string
		request   string
		wantBody  string
		wantClose bool
	}{
		{
			name:      "Content-Length with chunked",
			request:   "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
			wantBody:  "abc||chunked",
			wantClose: true,
		},
		{
			name:      "chunked alone",
			request:   "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
			wantBody:  "abc||chunked",
			wantClose: true,
		},
		{
			name:     "Content-Length alone keeps the connection",
			request:  "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n\r\nabc",
			wantBody: "abc|3|",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.Dial("tcp", f.server.Listener.Addr().String())
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
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK || string(body) != tc.wantBody {
				t.Errorf("got %d %q, want 200 %q", res.StatusCode, body, tc.wantBody)
			}
			if res.Close != tc.wantClose {
				t.Errorf("Connection: close = %v, want %v", res.Close, tc.wantClose)
			}
		})
	}
}

func TestRequestTrailers(t *testing.T) {
	type seen struct {
		body    string
		trailer string
	}
	got := make(chan seen, 1)
	addr := backend(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- seen{body: string(body), trailer: r.Trailer.Get("X-Request-Sum")}
	})
	f := newFixture(t, fixtureOptions{backends: []string{addr}}).serve(t)

	req, err := http.NewRequest(http.MethodPost, f.server.URL, io.NopCloser(strings.NewReader("payload")))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Trailer = http.Header{"X-Request-Sum": {"abc123"}}
	res, err := f.server.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	res.Body.Close()
	want := seen{body: "payload", trailer: "abc123"}
	if received := <-got; received != want {
		t.Errorf("backend received %+v, want %+v", received, want)
	}
}

func TestDrainUpgradedConnections(t *testing.T) {
	echo := func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		io.WriteString(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		rw.Flush()
		io.Copy(conn, rw)
	}
	tests := []struct {
		name        string
		clientQuits bool
		wantErr     error
	}{
		{name: "waits for the connection to end", clientQuits: true},
		{name: "closes what is left at the deadline", wantErr: context.DeadlineExceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, fixtureOptions{backends: []string{backend(t, echo)}}).serve(t)
			if err := f.handler.Drain(context.Background()); err != nil {
				t.Fatalf("Drain with nothing open = %v, want nil", err)
			}
			conn, err := net.Dial("tcp", f.server.Listener.Addr().String())
			if err != nil {
				t.Fatalf("dial proxy: %v", err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(waitLimit))
			io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
			reader := bufio.NewReader(conn)
			res, err := http.ReadResponse(reader, nil)
			if err != nil || res.StatusCode != http.StatusSwitchingProtocols {
				t.Fatalf("handshake: %v, %v", res, err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			drained := make(chan error, 1)
			go func() { drained <- f.handler.Drain(ctx) }()

			// The connection keeps working while Drain waits.
			io.WriteString(conn, "m")
			_, err = io.ReadFull(reader, make([]byte, 1))
			if err != nil {
				t.Errorf("echo during drain: %v", err)
			}
			if tc.clientQuits {
				conn.Close()
			}
			select {
			case err = <-drained:
			case <-time.After(waitLimit):
				t.Fatal("Drain did not return")
			}
			if err != tc.wantErr {
				t.Errorf("Drain = %v, want %v", err, tc.wantErr)
			}
			if !tc.clientQuits {
				_, err = reader.ReadByte()
				if err == nil {
					t.Error("connection still open after Drain gave up waiting")
				}
			}
			f.pool.waitIdle(t)
		})
	}
}

// A request that finishes on a transport no configuration uses any more
// must not leave its connection in that transport's idle pool.
func TestRetiredTransportKeepsNoConnections(t *testing.T) {
	var open atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			open.Add(1)
		case http.StateClosed, http.StateHijacked:
			open.Add(-1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	f := newFixture(t, fixtureOptions{backends: []string{server.Listener.Addr().String()}}).serve(t)
	settle := func(want int64) {
		t.Helper()
		deadline := time.Now().Add(waitLimit)
		for open.Load() != want {
			if time.Now().After(deadline) {
				t.Fatalf("backend has %d connections open, want %d", open.Load(), want)
			}
			time.Sleep(time.Millisecond)
		}
	}

	get(t, f.server.URL)
	settle(1)
	f.upstream.Retire(nil)
	settle(0)
	// The handler still routes to the retired upstream, as a request does
	// that loaded the old configuration just before a reload.
	for range 3 {
		status, _ := get(t, f.server.URL)
		if status != http.StatusOK {
			t.Fatalf("request on a retired transport = %d, want 200", status)
		}
	}
	settle(0)
}
