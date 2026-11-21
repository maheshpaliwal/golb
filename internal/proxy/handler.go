// Package proxy is the HTTP data path: routing, forwarding through
// httputil.ReverseProxy, and retries across the backends of a pool.
package proxy

import (
	"errors"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"sync"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

const (
	copyBufferBytes = 32 << 10
	upstreamScheme  = "http"
	schemeHTTPS     = "https"
	retryAfterValue = "1"

	// statusClientClosed records a request the client abandoned; no response
	// reaches anyone, the code exists for metrics and the access log.
	statusClientClosed = 499

	maxInlineAttempts = 4
)

var (
	errNoBackend      = errors.New("no backend available")
	errRequestTimeout = errors.New("request timeout waiting for upstream")

	// Shared header values. The transport only reads request headers.
	protoHTTP  = []string{upstreamScheme}
	protoHTTPS = []string{schemeHTTPS}
)

// Handler serves one HTTP listener. It loads the listener's router once per
// request, so a reload never changes the rules under a request in flight.
type Handler struct {
	source    func() *Router
	logger    *slog.Logger
	errorLog  *log.Logger
	exchanges sync.Pool
}

// NewHandler returns a handler that routes with whatever source returns at
// the start of each request.
func NewHandler(source func() *Router, logger *slog.Logger) *Handler {
	h := &Handler{
		source:   source,
		logger:   logger,
		errorLog: slog.NewLogLogger(logger.Handler(), slog.LevelDebug),
	}
	h.exchanges.New = func() any { return h.newExchange() }
	return h
}

// exchange is the per-request state. It is pooled, so nothing may hold a
// reference to it once ServeHTTP returns; anything that can outlive the
// handler (bodies handed to the transport) is allocated separately.
type exchange struct {
	forward httputil.ReverseProxy
	writer  responseWriter
	handler *Handler

	router         *Router
	route          *Route
	start          time.Time
	attempts       int
	backend        *core.Backend
	tried          []*core.Backend
	inline         [maxInlineAttempts]*core.Backend
	body           *upstreamBody
	upstreamStatus int // for upgrades, which bypass the writer
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
	return x
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	router := h.source()
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
	x.router = router
	x.route = route
	x.start = start
	x.tried = x.inline[:0]
	x.writer.ResponseWriter = w
	// Deferred because ReverseProxy aborts a broken response stream by
	// panicking with http.ErrAbortHandler, which must still release the backend.
	defer x.finish(r)
	x.forward.ServeHTTP(&x.writer, r)
}

func (x *exchange) finish(r *http.Request) {
	if x.body != nil {
		x.body.Close()
	}
	status := x.writer.status
	if status == 0 {
		status = x.upstreamStatus
	}
	if status == 0 {
		status = http.StatusOK
	}
	elapsed := time.Since(x.start)
	x.route.observe(status, elapsed)
	addr := ""
	if x.backend != nil {
		addr = x.backend.Addr
	}
	x.router.record(r, x.route, entry{
		start:    x.start,
		elapsed:  elapsed,
		status:   status,
		bytes:    x.writer.bytes,
		backend:  addr,
		attempts: x.attempts,
	})

	*x = exchange{forward: x.forward, handler: x.handler}
	x.handler.exchanges.Put(x)
}

// fail is the ReverseProxy error handler: it turns a forwarding error into a
// status without revealing backend addresses or error text to the client.
func (x *exchange) fail(w http.ResponseWriter, r *http.Request, err error) {
	if r.Context().Err() != nil {
		w.WriteHeader(statusClientClosed)
		return
	}
	status := failureStatus(err)
	if x.handler.logger.Enabled(r.Context(), slog.LevelDebug) {
		x.handler.logger.Debug("upstream request", "listener", x.router.listener, "route", x.route.label, "status", status, "error", err)
	}
	writeStatus(w, status)
}

func failureStatus(err error) int {
	if errors.Is(err, errNoBackend) {
		return http.StatusServiceUnavailable
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
// hop-by-hop headers and any client-supplied Forwarded and X-Forwarded-*
// headers, so the values set here replace them rather than extend them. The
// URL host is filled in per attempt from the picked backend and never from
// the request; the Host header is passed through unchanged.
func rewrite(pr *httputil.ProxyRequest) {
	pr.Out.URL.Scheme = upstreamScheme
	header := pr.Out.Header
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
	bytes  int64
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
	w.bytes += int64(n)
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
