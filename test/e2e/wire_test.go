package e2e

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// exchange writes payload to addr over a fresh TCP connection and returns
// everything the server sent until it closed the connection or went quiet
// for the given time.
func exchange(t *testing.T, addr, payload string, quiet time.Duration) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, waitTimeout)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer closeConn(conn)
	_, err = io.WriteString(conn, payload)
	if err != nil {
		t.Fatalf("write request: %v", err)
	}
	var received strings.Builder
	buf := make([]byte, 4096)
	for {
		err = conn.SetReadDeadline(time.Now().Add(quiet))
		if err != nil {
			t.Fatalf("set deadline: %v", err)
		}
		n, err := conn.Read(buf)
		received.Write(buf[:n])
		if err != nil {
			return received.String()
		}
	}
}

// statusLine returns the first line of a raw HTTP response.
func statusLine(response string) string {
	line, _, _ := strings.Cut(response, "\r\n")
	return line
}

// recorder is a backend that keeps the last request it received.
type recorder struct {
	*backend
	mu     sync.Mutex
	header http.Header
	host   string
	body   string
	// bodyErr is the error from reading the last request body, if any.
	bodyErr error
}

func newRecorder(t *testing.T, respond func(w http.ResponseWriter)) *recorder {
	t.Helper()
	rec := new(recorder)
	rec.backend = newBackend(t, "recorder", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.header = r.Header.Clone()
		rec.host = r.Host
		rec.body = string(body)
		rec.bodyErr = err
		rec.mu.Unlock()
		if respond != nil {
			respond(w)
		}
		writeString(w, "recorded")
	})
	return rec
}

func (r *recorder) last() (header http.Header, host, body string, bodyErr error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.header, r.host, r.body, r.bodyErr
}

func TestHopByHopHeadersAndForwardedFor(t *testing.T) {
	rec := newRecorder(t, func(w http.ResponseWriter) {
		w.Header().Set("Connection", "X-Response-Hop")
		w.Header().Set("X-Response-Hop", "1")
		w.Header().Set("Keep-Alive", "timeout=5")
		w.Header().Set("Proxy-Authenticate", "Basic")
		w.Header().Set("X-Response-Keep", "yes")
	})
	in := start(t, webConfig(poolOf("round_robin", rec.backend), ""))

	request := "GET /path?q=1 HTTP/1.1\r\n" +
		"Host: app.example.test\r\n" +
		"Connection: close, X-Request-Hop\r\n" +
		"X-Request-Hop: 1\r\n" +
		"Keep-Alive: timeout=5\r\n" +
		"Proxy-Authorization: Basic c2VjcmV0\r\n" +
		"Proxy-Connection: keep-alive\r\n" +
		"Te: gzip\r\n" +
		"X-Forwarded-For: 6.6.6.6\r\n" +
		"X-Forwarded-Host: evil.test\r\n" +
		"X-Forwarded-Proto: https\r\n" +
		"Forwarded: for=6.6.6.6;host=evil.test\r\n" +
		"X-Request-Keep: yes\r\n" +
		"\r\n"
	response := exchange(t, in.addr(web), request, time.Second)
	if statusLine(response) != "HTTP/1.1 200 OK" {
		t.Fatalf("status line %q, want 200\n%s", statusLine(response), response)
	}
	header, host, _, _ := rec.last()

	removed := []string{"X-Request-Hop", "Keep-Alive", "Proxy-Authorization", "Proxy-Connection", "Te", "Forwarded"}
	for _, name := range removed {
		if got := header.Values(name); len(got) != 0 {
			t.Errorf("request header %s reached the backend: %q", name, got)
		}
	}
	// "Connection: close" is the client's wish for its own connection; the
	// upstream connection is the proxy's and stays reusable.
	if got := header.Get("Connection"); got != "" {
		t.Errorf("Connection: %q reached the backend", got)
	}
	want := map[string]string{
		"X-Forwarded-For":   "127.0.0.1", // replaced, not appended to: the client's value is not trusted
		"X-Forwarded-Host":  "app.example.test",
		"X-Forwarded-Proto": "http",
		"X-Request-Keep":    "yes",
	}
	for name, value := range want {
		if got := header.Values(name); len(got) != 1 || got[0] != value {
			t.Errorf("request header %s at the backend: %q, want exactly %q", name, got, value)
		}
	}
	if host != "app.example.test" {
		t.Errorf("Host at the backend: %q, want the client's app.example.test", host)
	}

	res, err := http.ReadResponse(bufio.NewReader(strings.NewReader(response)), nil)
	if err != nil {
		t.Fatalf("parse response: %v", err)
	}
	defer res.Body.Close()
	for _, name := range []string{"X-Response-Hop", "Keep-Alive", "Proxy-Authenticate"} {
		if got := res.Header.Values(name); len(got) != 0 {
			t.Errorf("response header %s reached the client: %q", name, got)
		}
	}
	if got := res.Header.Get("X-Response-Keep"); got != "yes" {
		t.Errorf("response header X-Response-Keep: %q, want yes", got)
	}
}

// Requests a proxy must refuse outright. Forwarding any of them invites
// request smuggling, so none may reach a backend (PRD section 6).
func TestMalformedRequestsAreRejected(t *testing.T) {
	tests := []struct {
		name    string
		request string
	}{
		{
			name:    "duplicate Content-Length with different values",
			request: "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nContent-Length: 11\r\n\r\nabcdGET /x HTTP/1.1\r\n\r\n",
		},
		{
			name:    "Content-Length list with different values",
			request: "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4, 11\r\n\r\nabcd",
		},
		{
			name:    "negative Content-Length",
			request: "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: -1\r\n\r\n",
		},
		{
			name:    "unknown transfer coding",
			request: "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: xchunked\r\n\r\n0\r\n\r\n",
		},
		{
			name:    "chunked is not the final coding",
			request: "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked, identity\r\n\r\n0\r\n\r\n",
		},
		{
			name:    "space before the header colon",
			request: "POST / HTTP/1.1\r\nHost: x\r\nContent-Length : 4\r\n\r\nabcd",
		},
		{
			name:    "missing Host on HTTP/1.1",
			request: "GET / HTTP/1.1\r\n\r\n",
		},
		{
			name:    "two Host headers",
			request: "GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
		},
		{
			name:    "CONNECT",
			request: "CONNECT 127.0.0.1:22 HTTP/1.1\r\nHost: 127.0.0.1:22\r\n\r\n",
		},
		{
			name:    "oversized header block",
			request: "GET / HTTP/1.1\r\nHost: x\r\nX-Big: " + strings.Repeat("a", 32<<10) + "\r\n\r\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := newRecorder(t, nil)
			in := start(t, webConfig(poolOf("round_robin", rec.backend), "    timeouts:\n      max_header_bytes: 4096\n"))
			response := exchange(t, in.addr(web), tc.request, time.Second)
			line := statusLine(response)
			if !strings.HasPrefix(line, "HTTP/1.1 4") && !strings.HasPrefix(line, "HTTP/1.1 501") {
				t.Errorf("status line %q, want a 4xx (or 501) rejection", line)
			}
			if strings.Count(response, "HTTP/1.1 ") != 1 {
				t.Errorf("more than one response on the connection: a smuggled request was answered\n%s", response)
			}
			if got := rec.hits.Load(); got != 0 {
				header, _, body, _ := rec.last()
				t.Errorf("request reached the backend %d times: headers %v body %q", got, header, body)
			}
		})
	}
}

// A request carrying both Content-Length and Transfer-Encoding is the
// classic smuggling vector; PRD section 6 requires it to be rejected and
// never forwarded.
func TestContentLengthWithTransferEncodingIsRejected(t *testing.T) {
	knownBug(t, "BUG: a request with both Content-Length and Transfer-Encoding: chunked is accepted and forwarded "+
		"(as chunked, Content-Length dropped) with status 200, and the client connection is kept open for reuse; "+
		"PRD section 6 requires it to be rejected and never reach a backend, RFC 9112 6.1 requires at least closing the connection")
	rec := newRecorder(t, nil)
	in := start(t, webConfig(poolOf("round_robin", rec.backend), ""))
	request := "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"3\r\nabc\r\n0\r\n\r\n"
	conn, err := net.DialTimeout("tcp", in.addr(web), waitTimeout)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer closeConn(conn)
	_, err = io.WriteString(conn, request)
	if err != nil {
		t.Fatalf("write request: %v", err)
	}
	err = conn.SetReadDeadline(time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	response, err := io.ReadAll(conn)
	if line := statusLine(string(response)); !strings.HasPrefix(line, "HTTP/1.1 400") {
		t.Errorf("status line %q, want 400", line)
	}
	// RFC 9112 6.1: whatever the server does with such a request, it must
	// close the connection after responding.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Errorf("connection was left open for reuse after a request with both Content-Length and Transfer-Encoding")
	}
	if got := rec.hits.Load(); got != 0 {
		header, _, body, _ := rec.last()
		t.Errorf("request reached the backend: Content-Length %q Transfer-Encoding %q body %q",
			header.Get("Content-Length"), header.Get("Transfer-Encoding"), body)
	}
}

// What golb does today with Content-Length plus Transfer-Encoding, pinned
// so a change is noticed: the framing that reaches the backend must be
// unambiguous and the bytes after the chunked body must not be taken for a
// second request by anyone.
func TestContentLengthWithTransferEncodingIsNotSmuggled(t *testing.T) {
	rec := newRecorder(t, nil)
	in := start(t, webConfig(poolOf("round_robin", rec.backend), ""))
	request := "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n" +
		"3\r\nabc\r\n0\r\n\r\n"
	response := exchange(t, in.addr(web), request, time.Second)
	if strings.HasPrefix(statusLine(response), "HTTP/1.1 4") {
		if rec.hits.Load() != 0 {
			t.Errorf("request was rejected but still reached the backend")
		}
		return
	}
	header, _, body, bodyErr := rec.last()
	if rec.hits.Load() != 1 || bodyErr != nil || body != "abc" {
		t.Errorf("backend saw %d requests, body %q (err %v); want exactly one with the chunked body %q", rec.hits.Load(), body, bodyErr, "abc")
	}
	if header.Get("Content-Length") != "" && header.Get("Transfer-Encoding") != "" {
		t.Errorf("both framing headers were forwarded: Content-Length %q, Transfer-Encoding %q",
			header.Get("Content-Length"), header.Get("Transfer-Encoding"))
	}
}

// A folded header line is obsolete syntax that a proxy may either reject or
// replace with a space (RFC 9112 5.2); golb does the latter. What matters is
// that the backend receives one unfolded line.
func TestFoldedHeaderIsUnfoldedBeforeForwarding(t *testing.T) {
	rec := newRecorder(t, nil)
	in := start(t, webConfig(poolOf("round_robin", rec.backend), ""))
	response := exchange(t, in.addr(web), "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\nX-A: 1\r\n folded\r\n\r\n", time.Second)
	if strings.HasPrefix(statusLine(response), "HTTP/1.1 400") {
		return
	}
	header, _, _, _ := rec.last()
	if got := header.Values("X-A"); len(got) != 1 || got[0] != "1 folded" || rec.hits.Load() != 1 {
		t.Errorf("backend saw %d requests with X-A %q, want one request with the single value %q", rec.hits.Load(), got, "1 folded")
	}
}

func TestBadChunkFramingNeverCompletesUpstream(t *testing.T) {
	tests := []struct {
		name    string
		request string
	}{
		{
			name:    "chunk size is not hexadecimal",
			request: "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nZZ\r\nabc\r\n0\r\n\r\n",
		},
		{
			name:    "chunk data longer than its size",
			request: "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nabcdef\r\n0\r\n\r\n",
		},
		{
			name:    "chunk size overflows",
			request: "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nFFFFFFFFFFFFFFFFFF\r\nabc\r\n0\r\n\r\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := newRecorder(t, nil)
			in := start(t, webConfig(poolOf("round_robin", rec.backend).with(outlierOff), ""))
			response := exchange(t, in.addr(web), tc.request, time.Second)
			if strings.HasPrefix(statusLine(response), "HTTP/1.1 2") {
				t.Errorf("request with broken chunk framing was answered %q", statusLine(response))
			}
			if strings.Count(response, "HTTP/1.1 ") > 1 {
				t.Errorf("more than one response on the connection\n%s", response)
			}
			// The body is streamed, so the backend may see the request
			// start; it must never see it complete.
			ok := eventually(time.Second, func() bool {
				_, _, _, bodyErr := rec.last()
				return rec.hits.Load() == 0 || bodyErr != nil
			})
			if !ok {
				_, _, body, _ := rec.last()
				t.Errorf("backend received a complete request with body %q", body)
			}
		})
	}
}

// PRD section 6 words the bad-chunk case as "never reaches a backend".
func TestBadChunkFramingNeverReachesBackend(t *testing.T) {
	knownBug(t, "BUG (low): a request with a broken chunk header is opened on a backend before its body is validated; "+
		"the backend sees the request line and headers and then an aborted body, and golb answers 502 instead of 400. "+
		"PRD section 6 requires that it never reaches a backend")
	rec := newRecorder(t, nil)
	in := start(t, webConfig(poolOf("round_robin", rec.backend).with(outlierOff), ""))
	request := "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nZZ\r\nabc\r\n0\r\n\r\n"
	response := exchange(t, in.addr(web), request, time.Second)
	if line := statusLine(response); !strings.HasPrefix(line, "HTTP/1.1 400") {
		t.Errorf("status line %q, want 400", line)
	}
	if got := rec.hits.Load(); got != 0 {
		t.Errorf("request reached the backend %d times", got)
	}
	if got := in.metrics().sum("golb_upstream_requests_total", "result", "error"); got != 0 {
		t.Errorf("the client's malformed body was counted as %v backend failures", got)
	}
}

func TestAbsoluteFormTargetStaysOnThePool(t *testing.T) {
	rec := newRecorder(t, nil)
	in := start(t, webConfig(poolOf("round_robin", rec.backend), ""))
	// The address in the request target refuses connections: if golb
	// dialled it, the request would fail.
	elsewhere := deadAddr(t)
	response := exchange(t, in.addr(web), "GET http://"+elsewhere+"/secret HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", time.Second)
	if statusLine(response) != "HTTP/1.1 200 OK" || !strings.HasSuffix(response, "recorded") {
		t.Errorf("absolute-form request was not served by the pool: %q", statusLine(response))
	}
	if rec.hits.Load() != 1 {
		t.Errorf("backend saw %d requests, want 1", rec.hits.Load())
	}
}

func TestSlowHeadersHitReadHeaderTimeout(t *testing.T) {
	const readHeader = 400 * time.Millisecond
	rec := newRecorder(t, nil)
	in := start(t, webConfig(poolOf("round_robin", rec.backend), "    timeouts:\n      read_header: 400ms\n"))

	conn, err := net.DialTimeout("tcp", in.addr(web), waitTimeout)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer closeConn(conn)
	began := time.Now()

	// One header byte every 50 ms, forever: the connection must be cut at
	// the read-header timeout however regularly bytes arrive.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		trickle := "GET / HTTP/1.1\r\nHost: x\r\nX-Slow: " + strings.Repeat("a", 1000)
		for i := 0; i < len(trickle); i++ {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
			}
			_, err := conn.Write([]byte{trickle[i]})
			if err != nil {
				return
			}
		}
	})

	err = conn.SetReadDeadline(time.Now().Add(waitTimeout))
	if err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	_, err = io.ReadAll(conn)
	closedAfter := time.Since(began)
	close(stop)
	wg.Wait()

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("connection still open %v after it started trickling headers; read_header is %v", closedAfter, readHeader)
	}
	if closedAfter < readHeader-50*time.Millisecond || closedAfter > readHeader+time.Second {
		t.Errorf("connection closed after %v, want about %v", closedAfter, readHeader)
	}
	if rec.hits.Load() != 0 {
		t.Errorf("incomplete request reached the backend")
	}
}

// timeouts.request is documented as the limit for the whole request
// including retries (PRD 5.4). A client that sends its headers and then
// stops in the middle of the body must not be able to hold a backend
// connection and an in-flight slot beyond it.
func TestStalledRequestBodyIsBoundedByRequestTimeout(t *testing.T) {
	knownBug(t, "BUG: a request whose body upload stalls is not ended by timeouts.request when that timeout is longer than "+
		"dial_timeout + per_try_timeout (true for the defaults: 60 s vs 2 s + 10 s); nothing else bounds a stalled body, so "+
		"the client connection, an upstream connection and a backend in-flight slot are held for as long as the client likes")
	const request = time.Second
	rec := newRecorder(t, nil)
	retry := "    retry:\n      per_try_timeout: 300ms\n    transport:\n      dial_timeout: 200ms\n"
	in := start(t, webConfig(poolOf("round_robin", rec.backend).with(outlierOff+retry), "    timeouts:\n      request: 1s\n"))

	conn, err := net.DialTimeout("tcp", in.addr(web), waitTimeout)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer closeConn(conn)
	// Half of the announced body, then silence.
	_, err = io.WriteString(conn, "POST /upload HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\n"+strings.Repeat("a", 500))
	if err != nil {
		t.Fatalf("write request: %v", err)
	}
	began := time.Now()
	err = conn.SetReadDeadline(began.Add(request + 2*time.Second))
	if err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	response, err := io.ReadAll(conn)
	waited := time.Since(began)
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() && len(response) == 0 {
		t.Errorf("no response and connection still open %v after the body stalled; timeouts.request is %v (in flight on the backend: %d)",
			waited, request, in.backend(rec.addr).Inflight)
	}
	if len(response) > 0 && !strings.HasPrefix(statusLine(string(response)), "HTTP/1.1 504") && !strings.HasPrefix(statusLine(string(response)), "HTTP/1.1 408") {
		t.Errorf("status line %q, want 504 or 408", statusLine(string(response)))
	}
}

// Result.Failed is documented as "never client cancellation" (PRD 5.2): a
// backend must not pay for what a client does. A request whose own body is
// malformed is the client's failure, yet each one is fed to outlier
// detection as a failure of the backend it was being forwarded to.
func TestMalformedClientBodiesDoNotEjectBackends(t *testing.T) {
	knownBug(t, "BUG: a client can get healthy backends ejected: every request with a malformed chunked body is counted as a "+
		"failure of the backend it was forwarded to, so consecutive_failures such requests per backend eject up to "+
		"max_ejected_percent of the pool for everyone")
	backends := newBackends(t, 4)
	in := start(t, webConfig(poolOf("round_robin", backends...), "")) // default outlier settings
	malformed := "POST / HTTP/1.1\r\nHost: x\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\nZZ\r\nabc\r\n0\r\n\r\n"
	for range 40 {
		exchange(t, in.addr(web), malformed, 200*time.Millisecond)
	}
	m := in.metrics()
	for _, b := range backends {
		row := in.backend(b.addr)
		failures := m.sum("golb_upstream_requests_total", "backend", b.addr, "result", "error")
		if row.Ejected || failures != 0 {
			t.Errorf("backend %s: ejected=%v after %v attempts counted as its failures; it never failed", b.name, row.Ejected, failures)
		}
	}
	if got := m.sum("golb_backend_ejections_total"); got != 0 {
		t.Errorf("golb_backend_ejections_total = %v after 40 malformed client requests, want 0", got)
	}
}
