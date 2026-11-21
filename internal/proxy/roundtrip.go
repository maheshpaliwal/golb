package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

// RoundTrip sends the request to a backend of the route's pool and retries
// on another one when that is safe:
//
//   - a failed connect is retried for any method, because nothing was sent;
//   - any other error, and the configured statuses, are retried only for an
//     idempotent method without a body and never for an upgrade;
//   - a failure caused by the client (it went away, its body was malformed
//     or stalled) is neither retried nor held against the backend.
//
// Nothing has been written to the client while RoundTrip runs, so a retry
// can never follow response bytes.
func (x *exchange) RoundTrip(req *http.Request) (*http.Response, error) {
	u := x.route.upstream
	u.budget.recordRequest(uint64(x.start.Unix()))

	info := core.PickInfo{}
	if u.ringHash {
		info.Key = affinityKey(req, u.hashHeader)
	}
	var sent *requestBody
	if req.Body != nil && req.Body != http.NoBody {
		sent = &requestBody{ReadCloser: req.Body, start: x.start}
		req.Body = sent
		x.sent.Store(sent)
	}

	b := u.pool.Pick(info)
	if b == nil {
		return nil, errNoBackend
	}
	var began time.Duration // offset of the current attempt from the request start
	for {
		x.attempts++
		x.backend = b
		try := x.newAttempt(req, b, sent, began)
		res, err := u.conns.transport.RoundTrip(try.request)
		err = try.settle(res, err)
		if err == nil && !x.retriableStatus(req, res.StatusCode) {
			return x.deliver(res, delivery{backend: b, sent: sent, began: began, cancel: try.cancel, bounded: try.limit != nil}), nil
		}

		elapsed := time.Since(x.start)
		latency := elapsed - began
		if err != nil {
			fault := x.clientFault(req, sent, err)
			if fault != nil {
				u.abandon(b)
				return nil, fault
			}
		}

		var next *core.Backend
		if x.retriable(req, sent, err) && elapsed < x.deadline() {
			x.tried = append(x.tried, b)
			info.Exclude = x.tried
			next = u.pool.Pick(info)
		}
		// The budget is spent only on a retry that will really be sent.
		if next != nil && !u.budget.allowRetry(uint64(x.start.Add(elapsed).Unix())) {
			u.budgetExhausted.Inc()
			u.abandon(next)
			next = nil
		}
		if next == nil && err == nil {
			return x.deliver(res, delivery{backend: b, sent: sent, began: began, cancel: try.cancel, latency: latency}), nil
		}
		if err == nil {
			// Closed unread: draining it could block on a backend that
			// stalls after its headers, and the retry must not wait for that.
			_ = res.Body.Close()
			try.release()
		}
		u.done(b, latency, true)
		if next == nil {
			return nil, err
		}
		u.retries.Inc()
		b = next
		began = elapsed
	}
}

// clientFault reports whether a failed try is the client's doing, returning
// the error to answer with. Such a try says nothing about the backend.
func (x *exchange) clientFault(req *http.Request, sent *requestBody, err error) error {
	if x.expired.Load() {
		return errClientStalled
	}
	if req.Context().Err() != nil {
		return err
	}
	if sent != nil && sent.failed.Load() {
		return errClientBody
	}
	return nil
}

// attempt is one try against one backend.
type attempt struct {
	request *http.Request
	cancel  context.CancelFunc
	limit   *tryLimit // nil when the try cannot overrun the request timeout
}

// tryLimit cancels a try that would overrun the request timeout. The time
// the client spends uploading the body is not the pool's and does not
// count: while the upload runs (the watchdog polices that) the limit is
// pushed back, and afterwards the try gets what was left when it began.
type tryLimit struct {
	mu      sync.Mutex
	timer   *time.Timer
	cancel  context.CancelFunc
	sent    *requestBody
	start   time.Time
	began   time.Duration // offset of the try from the request start
	budget  time.Duration // what the try may spend waiting for the pool
	settled bool
	fired   bool
}

func (l *tryLimit) check() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.settled {
		return
	}
	if l.sent != nil && !l.sent.done.Load() {
		l.timer.Reset(l.budget)
		return
	}
	waited := time.Since(l.start) - l.began
	if l.sent != nil {
		waited = time.Since(l.start) - max(l.began, time.Duration(l.sent.doneAt.Load()))
	}
	if waited < l.budget {
		l.timer.Reset(l.budget - waited)
		return
	}
	l.fired = true
	l.cancel()
}

// newAttempt points the request at b. The first try reuses the outbound
// request; a retry gets its own copy because the transport may still hold
// the previous one. A try that could overrun the request timeout runs under
// a tryLimit.
func (x *exchange) newAttempt(req *http.Request, b *core.Backend, sent *requestBody, began time.Duration) attempt {
	u := x.route.upstream
	try := attempt{request: req}
	remaining := x.deadline() - began
	bounded := x.route.requestTimeout > 0 && remaining < u.dialTimeout+u.perTry
	if x.attempts == 1 && !bounded {
		req.URL.Host = b.Addr
		return try
	}

	ctx := req.Context()
	if bounded {
		ctx, try.cancel = context.WithCancel(ctx)
		try.limit = &tryLimit{cancel: try.cancel, sent: sent, start: x.start, began: began, budget: remaining}
		try.limit.mu.Lock()
		try.limit.timer = time.AfterFunc(remaining, try.limit.check)
		try.limit.mu.Unlock()
	}
	target := *req.URL
	target.Host = b.Addr
	try.request = req.WithContext(ctx)
	try.request.URL = &target
	return try
}

// settle stops the try's limit once response headers (or an error) are in,
// and reports a limit that fired as errRequestTimeout. On success the
// cancel function stays armed for the body: cancelling when the headers
// arrive would truncate the response.
func (a *attempt) settle(res *http.Response, err error) error {
	if a.limit == nil {
		return err
	}
	a.limit.mu.Lock()
	a.limit.settled = true
	fired := a.limit.fired
	a.limit.mu.Unlock()
	a.limit.timer.Stop()
	if !fired && err == nil {
		return nil
	}
	if err == nil {
		_ = res.Body.Close()
	}
	a.release()
	if fired {
		return errRequestTimeout
	}
	return err
}

func (a *attempt) release() {
	if a.cancel != nil {
		a.cancel()
		a.cancel = nil
	}
}

// deadline is the offset from the request start after which no new try
// begins; without a request timeout it never arrives.
func (x *exchange) deadline() time.Duration {
	if x.route.requestTimeout > 0 {
		return x.route.requestTimeout
	}
	return time.Duration(1<<63 - 1)
}

func (x *exchange) retriableStatus(req *http.Request, status int) bool {
	if status < http.StatusInternalServerError || x.attempts >= x.route.upstream.maxAttempts {
		return false
	}
	return slices.Contains(x.route.upstream.retryOn, status) && repeatable(req)
}

// retriable applies the safety rules to a failed try (err != nil) or a
// response already accepted by retriableStatus (err == nil).
func (x *exchange) retriable(req *http.Request, sent *requestBody, err error) bool {
	if x.attempts >= x.route.upstream.maxAttempts || errors.Is(err, errRequestTimeout) {
		return false
	}
	if err == nil {
		return true
	}
	if isDialError(err) {
		return sent == nil || !sent.touched.Load()
	}
	return repeatable(req)
}

// repeatable reports whether sending the request a second time is harmless
// even if the first copy was processed.
func repeatable(req *http.Request) bool {
	if req.Body != nil && req.Body != http.NoBody {
		return false
	}
	if len(req.Header["Upgrade"]) > 0 {
		return false
	}
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete:
		return true
	}
	return false
}

// delivery is a response accepted for the client and the attempt behind it.
type delivery struct {
	backend *core.Backend
	sent    *requestBody
	began   time.Duration
	latency time.Duration // 0 when not measured yet
	cancel  context.CancelFunc
	bounded bool // the attempt ran under the request-timeout timer
}

// deliver hands the response to ReverseProxy and arranges for the attempt
// to be reported to the pool exactly once: when the body is closed, or at
// the end of the request for a response without one.
//
// The clock is read here only when the time to the headers differs from the
// time to the end of the response. For a first-try response that fits one
// copy buffer the two are the same to within the copy, so the reading taken
// when the body closes serves as both the latency sample and the request
// duration.
func (x *exchange) deliver(res *http.Response, d delivery) *http.Response {
	x.upstreamStatus = res.StatusCode
	failed := res.StatusCode >= http.StatusInternalServerError
	if res.Body == http.NoBody && d.cancel == nil && d.latency == 0 {
		x.owed = owedResult{backend: d.backend, began: d.began, failed: failed}
		return res
	}
	short := d.began == 0 && !d.bounded && !failed && res.ContentLength >= 0 && res.ContentLength <= copyBufferBytes
	if d.latency == 0 && !short {
		d.latency = max(time.Since(x.start)-d.began, 1)
	}
	body := &upstreamBody{
		ReadCloser: res.Body,
		upstream:   x.route.upstream,
		backend:    d.backend,
		sent:       d.sent,
		start:      x.start,
		latency:    d.latency,
		failed:     failed,
		cancel:     d.cancel,
	}
	res.Body = body
	conn, ok := body.ReadCloser.(io.ReadWriteCloser)
	if ok && res.StatusCode == http.StatusSwitchingProtocols {
		// An upgraded connection: ReverseProxy needs to write to it too,
		// and Drain needs to find it.
		body.upgrades = x.handler
		x.handler.track(body)
		res.Body = &upgradedBody{upstreamBody: body, conn: conn}
	}
	x.body.Store(body)
	return res
}

// affinityKey hashes the configured header, or the client IP when the
// header is not configured or not sent.
func affinityKey(req *http.Request, header string) uint64 {
	if header != "" {
		values := req.Header[header]
		if len(values) > 0 && values[0] != "" {
			return core.Hash(values[0])
		}
	}
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		host = req.RemoteAddr
	}
	return core.Hash(host)
}

// requestBody stands between the transport and the client's body. It keeps
// the transport from closing the body on a failed try (the server closes it
// when the handler returns), and it records what the retry rules, the
// watchdog and the failure classification need to know.
type requestBody struct {
	io.ReadCloser
	touched atomic.Bool  // a read was attempted: the body cannot be replayed
	n       atomic.Int64 // bytes read, the watchdog's measure of progress
	done    atomic.Bool  // the body ended, cleanly or not
	doneAt  atomic.Int64 // when it ended, as an offset from start
	start   time.Time
	failed  atomic.Bool // reading it failed: a client fault, never a backend's
}

func (b *requestBody) Read(p []byte) (int, error) {
	b.touched.Store(true)
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.n.Add(int64(n))
	}
	if err != nil {
		if err != io.EOF {
			b.failed.Store(true)
		}
		b.doneAt.Store(int64(time.Since(b.start)))
		b.done.Store(true)
	}
	return n, err
}

func (b *requestBody) Close() error { return nil }

// upstreamBody releases the backend exactly once, when the response body is
// closed. It is never pooled: after an upgrade a copy goroutine can still
// hold it when the handler has returned, and the watchdog or Drain may close
// it from another goroutine.
type upstreamBody struct {
	io.ReadCloser
	upstream *Upstream
	backend  *core.Backend
	sent     *requestBody
	upgrades *Handler // set for an upgraded connection
	start    time.Time
	latency  time.Duration // 0: measured when the body closes
	failed   bool
	cancel   context.CancelFunc

	n       atomic.Int64 // bytes moved, the watchdog's measure of progress
	elapsed atomic.Int64 // request duration read at Close, when latency was 0
	broken  atomic.Bool
	closed  atomic.Bool
}

func (b *upstreamBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.n.Add(int64(n))
	}
	// A backend that dies mid-response counts against it; the client going
	// away, or an upgraded connection ending, does not.
	if err != nil && err != io.EOF && b.upgrades == nil && !errors.Is(err, context.Canceled) {
		b.broken.Store(true)
	}
	return n, err
}

func (b *upstreamBody) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	err := b.ReadCloser.Close()
	if b.cancel != nil {
		b.cancel()
	}
	latency := b.latency
	if latency == 0 {
		latency = max(time.Since(b.start), 1)
		b.elapsed.Store(int64(latency))
	}
	// A broken request body takes the upstream exchange down with it; that
	// is the client's failure.
	clientFault := b.sent != nil && b.sent.failed.Load()
	b.upstream.done(b.backend, latency, (b.failed || b.broken.Load()) && !clientFault)
	if b.upgrades != nil {
		b.upgrades.untrack(b)
	}
	return err
}

// upgradedBody is the backend side of an upgraded (for example WebSocket)
// connection.
type upgradedBody struct {
	*upstreamBody
	conn io.ReadWriteCloser
}

func (b *upgradedBody) Write(p []byte) (int, error) {
	n, err := b.conn.Write(p)
	if n > 0 {
		b.n.Add(int64(n))
	}
	return n, err
}

// CloseWrite lets ReverseProxy pass a client half-close on to the backend.
func (b *upgradedBody) CloseWrite() error {
	half, ok := b.conn.(interface{ CloseWrite() error })
	if !ok {
		return http.ErrNotSupported
	}
	return half.CloseWrite()
}
