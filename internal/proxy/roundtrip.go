package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

// discardLimit is how much of a retried response is read so its connection
// can be reused; a larger body is cheaper to drop with the connection.
const discardLimit = 4 << 10

// RoundTrip sends the request to a backend of the route's pool and retries
// on another one when that is safe:
//
//   - a failed connect is retried for any method, because nothing was sent;
//   - any other error, and the configured statuses, are retried only for an
//     idempotent method without a body and never for an upgrade;
//   - the client going away is not a failure and is never retried.
//
// Nothing has been written to the client while RoundTrip runs, so a retry
// can never follow response bytes.
func (x *exchange) RoundTrip(req *http.Request) (*http.Response, error) {
	u := x.route.upstream
	second := uint64(x.start.Unix())
	u.budget.recordRequest(second)

	info := core.PickInfo{}
	if u.ringHash {
		info.Key = affinityKey(req, u.hashHeader)
	}
	var sent *requestBody
	if req.Body != nil && req.Body != http.NoBody && u.maxAttempts > 1 {
		sent = &requestBody{ReadCloser: req.Body}
		req.Body = sent
	}

	b := u.pool.Pick(info)
	if b == nil {
		return nil, errNoBackend
	}
	var began time.Duration // offset of the current attempt from the request start
	for {
		x.attempts++
		x.backend = b
		try := x.newAttempt(req, b, began)
		res, err := u.transport.RoundTrip(try.request)
		elapsed := time.Since(x.start)
		latency := elapsed - began
		err = try.settle(res, err)

		if err == nil && !x.retriableStatus(req, res.StatusCode) {
			return x.deliver(res, b, latency, try.cancel), nil
		}
		if err != nil && req.Context().Err() != nil {
			u.abandon(b)
			return nil, err
		}

		var next *core.Backend
		if x.retriable(req, sent, err) && elapsed < x.deadline() && x.reserveRetry(second) {
			x.tried = append(x.tried, b)
			info.Exclude = x.tried
			next = u.pool.Pick(info)
		}
		if next == nil && err == nil {
			return x.deliver(res, b, latency, try.cancel), nil
		}
		if err == nil {
			discard(res.Body)
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

// attempt is one try against one backend.
type attempt struct {
	request *http.Request
	parent  context.Context
	ctx     context.Context
	cancel  context.CancelFunc
	timer   *time.Timer
}

// newAttempt points the request at b. The first try reuses the outbound
// request; a retry gets its own copy because the transport may still hold
// the previous one. A try that could overrun the request timeout runs under
// a timer that cancels it.
func (x *exchange) newAttempt(req *http.Request, b *core.Backend, began time.Duration) attempt {
	u := x.route.upstream
	try := attempt{request: req, parent: req.Context()}
	remaining := x.deadline() - began
	bounded := x.route.requestTimeout > 0 && remaining < u.dialTimeout+u.perTry
	if x.attempts == 1 && !bounded {
		req.URL.Host = b.Addr
		return try
	}

	ctx := try.parent
	if bounded {
		try.ctx, try.cancel = context.WithCancel(ctx)
		try.timer = time.AfterFunc(remaining, try.cancel)
		ctx = try.ctx
	}
	target := *req.URL
	target.Host = b.Addr
	try.request = req.WithContext(ctx)
	try.request.URL = &target
	return try
}

// settle stops the request-timeout timer once response headers (or an
// error) are in, and reports a timer that fired as errRequestTimeout. On
// success the cancel function stays armed for the body: cancelling when the
// headers arrive would truncate the response.
func (a *attempt) settle(res *http.Response, err error) error {
	if a.timer == nil {
		return err
	}
	stopped := a.timer.Stop()
	if stopped && err == nil {
		return nil
	}
	expired := !stopped || (a.ctx.Err() != nil && a.parent.Err() == nil)
	if err == nil {
		res.Body.Close()
	}
	a.release()
	if expired {
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
		return sent == nil || !sent.read.Load()
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

func (x *exchange) reserveRetry(second uint64) bool {
	u := x.route.upstream
	if u.budget.allowRetry(second) {
		return true
	}
	u.budgetExhausted.Inc()
	return false
}

// deliver hands the response to ReverseProxy with a body that reports the
// attempt to the pool when it is closed.
func (x *exchange) deliver(res *http.Response, b *core.Backend, latency time.Duration, cancel context.CancelFunc) *http.Response {
	body := &upstreamBody{
		ReadCloser: res.Body,
		upstream:   x.route.upstream,
		backend:    b,
		latency:    latency,
		failed:     res.StatusCode >= http.StatusInternalServerError,
		cancel:     cancel,
	}
	x.body = body
	x.upstreamStatus = res.StatusCode
	res.Body = body
	conn, ok := body.ReadCloser.(io.ReadWriteCloser)
	if ok && res.StatusCode == http.StatusSwitchingProtocols {
		// An upgraded connection: ReverseProxy needs to write to it too.
		body.upgraded = true
		res.Body = &upgradedBody{upstreamBody: body, conn: conn}
	}
	return res
}

func discard(body io.ReadCloser) {
	// Both errors only decide whether the connection is reused.
	_, _ = io.CopyN(io.Discard, body, discardLimit)
	_ = body.Close()
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

// requestBody shields the client's body from the transport, which closes
// the body of every failed try, and records whether any of it was consumed.
// The server closes the real body when the handler returns.
type requestBody struct {
	io.ReadCloser
	read atomic.Bool
}

func (b *requestBody) Read(p []byte) (int, error) {
	b.read.Store(true)
	return b.ReadCloser.Read(p)
}

func (b *requestBody) Close() error { return nil }

// upstreamBody releases the backend exactly once, when the response body is
// closed. It is never pooled: after an upgrade a copy goroutine can still
// hold it when the handler has returned.
type upstreamBody struct {
	io.ReadCloser
	upstream *Upstream
	backend  *core.Backend
	latency  time.Duration
	failed   bool
	upgraded bool
	cancel   context.CancelFunc
	broken   atomic.Bool
	closed   atomic.Bool
}

func (b *upstreamBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	// A backend that dies mid-response counts against it; the client going
	// away, or an upgraded connection ending, does not.
	if err != nil && err != io.EOF && !b.upgraded && !errors.Is(err, context.Canceled) {
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
	b.upstream.done(b.backend, b.latency, b.failed || b.broken.Load())
	return err
}

// upgradedBody is the backend side of an upgraded (for example WebSocket)
// connection.
type upgradedBody struct {
	*upstreamBody
	conn io.ReadWriteCloser
}

func (b *upgradedBody) Write(p []byte) (int, error) { return b.conn.Write(p) }

// CloseWrite lets ReverseProxy pass a client half-close on to the backend.
func (b *upgradedBody) CloseWrite() error {
	half, ok := b.conn.(interface{ CloseWrite() error })
	if !ok {
		return http.ErrNotSupported
	}
	return half.CloseWrite()
}
