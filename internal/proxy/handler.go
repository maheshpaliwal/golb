// Package proxy is the HTTP data path: routing, forwarding through
// httputil.ReverseProxy, and retries across the backends of a pool.
package proxy

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

const (
	copyBufferBytes = 32 << 10
	upstreamScheme  = "http"
	schemeHTTPS     = "https"
	retryAfterValue = "1"
	closeToken      = "close"

	// statusClientClosed records a request the client abandoned; no response
	// reaches anyone, the code exists for metrics and the access log.
	statusClientClosed = 499

	maxInlineAttempts = 4

	forwardedPrefix = "X-Forwarded-"
)

var (
	errNoBackend      = errors.New("no backend available")
	errRequestTimeout = errors.New("request timeout waiting for upstream")
	errClientStalled  = errors.New("request body not received in time")
	errClientBody     = errors.New("request body could not be read")

	// Shared header values. The transport only reads request headers.
	protoHTTP  = []string{upstreamScheme}
	protoHTTPS = []string{schemeHTTPS}

	// Headers a backend may trust to describe the client. Whatever the
	// client sent under these names is dropped; golb sets the ones it owns.
	clientIdentityHeaders = []string{"Forwarded", "X-Real-Ip", "True-Client-Ip"}
)

// Handler serves one HTTP listener. It loads the listener's router once per
// request, so a reload never changes the rules under a request in flight.
type Handler struct {
	source    func() *Router
	logger    *slog.Logger
	errorLog  *log.Logger
	exchanges sync.Pool

	// Upgraded connections are invisible to http.Server.Shutdown, so the
	// handler keeps its own list for Drain.
	mu       sync.Mutex
	upgrades map[*upstreamBody]struct{}
	drained  chan struct{}
}

// NewHandler returns a handler that routes with whatever source returns at
// the start of each request.
func NewHandler(source func() *Router, logger *slog.Logger) *Handler {
	h := &Handler{
		source:   source,
		logger:   logger,
		errorLog: slog.NewLogLogger(logger.Handler(), slog.LevelDebug),
		upgrades: make(map[*upstreamBody]struct{}),
	}
	h.exchanges.New = func() any { return h.newExchange() }
	return h
}

// Drain waits for upgraded connections (WebSocket and the like) to end.
// When ctx ends first it closes the ones still open and returns ctx's error.
func (h *Handler) Drain(ctx context.Context) error {
	h.mu.Lock()
	if len(h.upgrades) == 0 {
		h.mu.Unlock()
		return nil
	}
	drained := make(chan struct{})
	h.drained = drained
	h.mu.Unlock()

	select {
	case <-drained:
		return nil
	case <-ctx.Done():
	}
	h.mu.Lock()
	open := make([]*upstreamBody, 0, len(h.upgrades))
	for body := range h.upgrades {
		open = append(open, body)
	}
	h.mu.Unlock()
	for _, body := range open {
		// Closing the backend side ends ReverseProxy's copy, which closes
		// the client side.
		_ = body.Close()
	}
	return ctx.Err()
}

func (h *Handler) track(body *upstreamBody) {
	h.mu.Lock()
	h.upgrades[body] = struct{}{}
	h.mu.Unlock()
}

func (h *Handler) untrack(body *upstreamBody) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.upgrades, body)
	if len(h.upgrades) == 0 && h.drained != nil {
		close(h.drained)
		h.drained = nil
	}
}

// exchange is the per-request state. It is pooled, so nothing may hold a
// reference to it once ServeHTTP returns; anything that can outlive the
// handler (bodies handed to the transport) is allocated separately.
type exchange struct {
	forward httputil.ReverseProxy
	writer  responseWriter
	handler *Handler
	timer   *time.Timer // the watchdog, see watchdog.go

	// mu serialises the watchdog with the start and end of a request. The
	// fields below it are written under it and read by the watchdog.
	mu       sync.Mutex
	finished bool
	router   *Router
	route    *Route
	start    time.Time
	watch    watchState

	// Set by the request goroutine, read by the watchdog.
	sent    atomic.Pointer[requestBody]
	body    atomic.Pointer[upstreamBody]
	expired atomic.Bool

	// Request goroutine only.
	armed          bool
	attempts       int
	backend        *core.Backend
	tried          []*core.Backend
	inline         [maxInlineAttempts]*core.Backend
	upstreamStatus int // for upgrades, which bypass the writer
	owed           owedResult
}

// owedResult is an attempt whose response has no body: there is nothing to
// close, so it is reported to the pool when the request finishes.
type owedResult struct {
	backend *core.Backend
	began   time.Duration
	failed  bool
}

func (h *Handler) newExchange() *exchange {
	x := &exchange{handler: h}
	x.forward = httputil.ReverseProxy{
		Rewrite:      rewrite,
		Transport:    x,
		BufferPool:   copyBuffers,
		ErrorHandler: x.fail,
		ErrorLog:     h.errorLog,
	}
	x.timer = time.AfterFunc(time.Hour, x.check)
	x.timer.Stop()
	return x
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	router := h.source()
	// net/http accepts a chunked request that also carries Content-Length
	// by dropping the length (RFC 9112 6.3) and does not tell the handler.
	// The same RFC requires the connection to be closed after such a
	// request; as the case cannot be told apart, every chunked HTTP/1
	// request ends its connection.
	if r.ProtoMajor == 1 && len(r.TransferEncoding) > 0 {
		w.Header().Set("Connection", closeToken)
	}
	// Only origin-form targets are proxied. CONNECT and "OPTIONS *" address
	// the proxy itself and have no meaning for a backend.
	if r.Method == http.MethodConnect || !strings.HasPrefix(r.URL.Path, "/") {
		router.reject(w, r, router.unmatched, http.StatusBadRequest, start)
		return
	}
	route := router.match(r.Host, r.URL.Path)
	if route == nil {
		router.reject(w, r, router.unmatched, http.StatusNotFound, start)
		return
	}
	if route.limiter != nil && !route.limiter.Allow(start.UnixNano()) {
		route.limited.Inc()
		w.Header().Set("Retry-After", retryAfterValue)
		router.reject(w, r, route, http.StatusTooManyRequests, start)
		return
	}

	x := h.exchanges.Get().(*exchange)
	x.begin(w, router, route, start)
	// Deferred because ReverseProxy aborts a broken response stream by
	// panicking with http.ErrAbortHandler, which must still release the backend.
	defer x.finish(r)
	x.forward.ServeHTTP(&x.writer, r)
}

func (x *exchange) begin(w http.ResponseWriter, router *Router, route *Route, start time.Time) {
	x.mu.Lock()
	x.finished = false
	x.router = router
	x.route = route
	x.start = start
	x.writer.ResponseWriter = w
	x.watch = watchState{phase: phaseUpload}
	x.mu.Unlock()
	x.tried = x.inline[:0]
	x.arm()
}

func (x *exchange) finish(r *http.Request) {
	x.mu.Lock()
	x.finished = true
	x.mu.Unlock()
	// A watchdog run that already started sees finished and does nothing,
	// but it still holds the exchange, which then must not be reused.
	reusable := !x.armed || x.timer.Stop()

	var elapsed time.Duration
	body := x.body.Load()
	if body != nil {
		// Reached with the body open only when ReverseProxy gave up early.
		_ = body.Close()
		elapsed = time.Duration(body.elapsed.Load())
	}
	if elapsed == 0 {
		elapsed = time.Since(x.start)
	}
	if x.owed.backend != nil {
		x.route.upstream.done(x.owed.backend, max(elapsed-x.owed.began, 1), x.owed.failed)
	}

	status := x.writer.status
	if status == 0 {
		status = x.upstreamStatus
	}
	if status == 0 {
		status = http.StatusOK
	}
	x.route.observe(status, elapsed)
	addr := ""
	if x.backend != nil {
		addr = x.backend.Addr
	}
	x.router.record(r, x.route, entry{
		start:    x.start,
		elapsed:  elapsed,
		status:   status,
		bytes:    x.writer.bytes.Load(),
		backend:  addr,
		attempts: x.attempts,
	})
	if !reusable {
		return
	}
	x.reset()
	x.handler.exchanges.Put(x)
}

func (x *exchange) reset() {
	x.mu.Lock()
	x.router = nil
	x.route = nil
	x.writer.ResponseWriter = nil
	x.mu.Unlock()
	x.writer.status = 0
	x.writer.bytes.Store(0)
	x.sent.Store(nil)
	x.body.Store(nil)
	x.expired.Store(false)
	x.armed = false
	x.attempts = 0
	x.backend = nil
	x.inline = [maxInlineAttempts]*core.Backend{}
	x.tried = nil
	x.upstreamStatus = 0
	x.owed = owedResult{}
}

// fail is the ReverseProxy error handler: it turns a forwarding error into a
// status without revealing backend addresses or error text to the client.
func (x *exchange) fail(w http.ResponseWriter, r *http.Request, err error) {
	status := failureStatus(err)
	if status == http.StatusBadRequest || status == http.StatusRequestTimeout {
		// The request body is in an unknown state; the connection cannot
		// carry another request.
		w.Header().Set("Connection", closeToken)
	}
	if status == http.StatusBadGateway && r.Context().Err() != nil {
		w.WriteHeader(statusClientClosed)
		return
	}
	if x.handler.logger.Enabled(r.Context(), slog.LevelDebug) {
		x.handler.logger.Debug("upstream request", "listener", x.router.listener, "route", x.route.label, "status", status, "error", err)
	}
	writeStatus(w, status)
}

func failureStatus(err error) int {
	if errors.Is(err, errNoBackend) {
		return http.StatusServiceUnavailable
	}
	if errors.Is(err, errClientStalled) {
		return http.StatusRequestTimeout
	}
	if errors.Is(err, errClientBody) {
		return http.StatusBadRequest
	}
	if errors.Is(err, errRequestTimeout) {
		return http.StatusGatewayTimeout
	}
	if isDialError(err) {
		return http.StatusBadGateway
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return http.StatusGatewayTimeout
	}
	return http.StatusBadGateway
}

func isDialError(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

// writeStatus sends a proxy-generated response: the status text and nothing else.
func writeStatus(w http.ResponseWriter, status int) {
	http.Error(w, http.StatusText(status), status)
}

// rewrite prepares the outbound request. ReverseProxy has already removed
// hop-by-hop headers. Every header by which a client could pass itself off
// as another address, host or scheme is dropped, then golb sets
// X-Forwarded-For, -Host and -Proto from the connection. The URL host is
// filled in per attempt from the picked backend and never from the request;
// the Host header is passed through unchanged.
func rewrite(pr *httputil.ProxyRequest) {
	pr.Out.URL.Scheme = upstreamScheme
	// The server fills the inbound trailer map when the body has been read;
	// sharing it (the clone is a snapshot taken before that) lets the
	// transport send the values after the body.
	pr.Out.Trailer = pr.In.Trailer

	header := pr.Out.Header
	for name := range header {
		if strings.HasPrefix(name, forwardedPrefix) {
			delete(header, name)
		}
	}
	for _, name := range clientIdentityHeaders {
		delete(header, name)
	}
	// One allocation for both values. It cannot live in the pooled
	// exchange: the transport may still read the headers after the handler
	// has returned.
	values := new([2]string)
	clientIP, _, err := net.SplitHostPort(pr.In.RemoteAddr)
	if err == nil {
		values[0] = clientIP
		header["X-Forwarded-For"] = values[0:1:1]
	}
	values[1] = pr.In.Host
	header["X-Forwarded-Host"] = values[1:2:2]
	header["X-Forwarded-Proto"] = protoHTTP
	if pr.In.TLS != nil {
		header["X-Forwarded-Proto"] = protoHTTPS
	}
}

// responseWriter records the status and size of the response. ReverseProxy
// reaches Flush and Hijack on the underlying writer through Unwrap.
type responseWriter struct {
	http.ResponseWriter
	status int
	bytes  atomic.Int64 // also read by the watchdog as progress
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *responseWriter) WriteHeader(status int) {
	if w.status == 0 && status >= http.StatusOK {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes.Add(int64(n))
	return n, err
}

type entry struct {
	start    time.Time
	elapsed  time.Duration
	status   int
	bytes    int64
	backend  string
	attempts int
}

// reject answers a request that is never forwarded.
func (rt *Router) reject(w http.ResponseWriter, r *http.Request, route *Route, status int, start time.Time) {
	writeStatus(w, status)
	elapsed := time.Since(start)
	route.observe(status, elapsed)
	rt.record(r, route, entry{start: start, elapsed: elapsed, status: status})
}

func (rt *Router) record(r *http.Request, route *Route, e entry) {
	if rt.log == nil {
		return
	}
	rec := rt.log.Acquire()
	if rec == nil {
		return
	}
	rec.Time = e.start
	rec.Listener = rt.listener
	rec.Route = route.label
	rec.Client = r.RemoteAddr
	rec.Method = r.Method
	rec.Host = r.Host
	rec.Path = r.URL.Path
	rec.Proto = r.Proto
	rec.Status = e.status
	rec.Bytes = e.bytes
	rec.DurationMs = float64(e.elapsed) / float64(time.Millisecond)
	rec.Backend = e.backend
	rec.Attempts = e.attempts
	rt.log.Submit(rec)
}

// bufferPool hands ReverseProxy its copy buffers. Arrays are pooled by
// pointer so neither Get nor Put allocates.
type bufferPool struct {
	arrays sync.Pool
}

var copyBuffers = &bufferPool{arrays: sync.Pool{New: func() any { return new([copyBufferBytes]byte) }}}

func (p *bufferPool) Get() []byte {
	return p.arrays.Get().(*[copyBufferBytes]byte)[:]
}

func (p *bufferPool) Put(buf []byte) {
	if cap(buf) < copyBufferBytes {
		return
	}
	p.arrays.Put((*[copyBufferBytes]byte)(buf[:copyBufferBytes]))
}
