package proxy

import (
	"net/http"
	"time"
)

// The watchdog is one timer per exchange that ends a request which has
// stopped moving. Two limits apply:
//
//   - timeouts.body_idle: once bytes are expected to flow (a request body
//     being uploaded, a response being streamed, an upgraded connection)
//     a full interval without a single byte in any direction ends the
//     exchange. A stream that keeps moving is never cut, however long it
//     lasts.
//   - timeouts.request: before the response headers no single wait may
//     exceed it, so an upload is held to the shorter of the two. An upload
//     that stalls is answered with 408. (The wait for a backend is bounded
//     by the retry loop, see newAttempt.)
//
// Progress is counted, not timed: the data path only adds to counters and
// the timer compares them once per interval, so no clock is read per chunk.
// The price is precision: a stall is noticed between one and two intervals
// after it began.

type phase uint8

const (
	// phaseWaiting is the wait for a backend: nothing is expected to move
	// and the per-try timeout governs.
	phaseWaiting phase = iota
	phaseUpload
	phaseStream
)

type watchState struct {
	phase    phase
	progress int64         // counters at the last sample
	at       time.Duration // offset from the request start of that sample
}

// arm starts the watchdog for a request, if the route has any limit for it.
func (x *exchange) arm() {
	interval := x.idleLimit(phaseUpload)
	if interval == 0 {
		return
	}
	x.armed = true
	x.timer.Reset(interval)
}

// idleLimit is how long the given phase may go without progress, which is
// also how often it is checked; 0 means no limit.
func (x *exchange) idleLimit(now phase) time.Duration {
	idle := x.route.bodyIdle
	request := x.route.requestTimeout
	if now != phaseStream && request > 0 && (idle == 0 || request < idle) {
		return request
	}
	return idle
}

// check runs on the timer goroutine.
func (x *exchange) check() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.finished {
		return
	}
	elapsed := time.Since(x.start)
	sent := x.sent.Load()
	body := x.body.Load()

	now := phaseWaiting
	switch {
	case body != nil:
		now = phaseStream
	case sent != nil && !sent.done.Load():
		now = phaseUpload
	}

	progress := x.writer.bytes.Load()
	if sent != nil {
		progress += sent.n.Load()
	}
	if body != nil {
		progress += body.n.Load()
	}
	idle := x.idleLimit(now)
	// A phase that only just began is sampled, not judged: it has not had
	// its interval yet.
	stalled := idle > 0 && now != phaseWaiting && now == x.watch.phase &&
		progress == x.watch.progress && elapsed-x.watch.at >= idle
	if stalled {
		x.expire(body)
		return
	}
	if now != x.watch.phase || progress != x.watch.progress {
		x.watch = watchState{phase: now, progress: progress, at: elapsed}
	}
	if idle > 0 {
		x.timer.Reset(idle)
	}
}

// expire ends the exchange from the timer goroutine by making the blocked
// reads and writes fail. body is the response being streamed, or nil while
// the request is still being uploaded; then only reading is cut, so the 408
// can still be written.
func (x *exchange) expire(body *upstreamBody) {
	x.expired.Store(true)
	// Any time in the past works as an already-expired deadline.
	past := time.Unix(1, 0)
	control := http.NewResponseController(x.writer.ResponseWriter)
	// A writer that has no deadlines (or was hijacked) reports an error;
	// closing the response body below still ends the exchange.
	_ = control.SetReadDeadline(past)
	if body == nil {
		return
	}
	_ = control.SetWriteDeadline(past)
	// Not a backend failure: a stream that went quiet is not evidence the
	// backend is broken. Close decides the outcome before the blocked Read
	// returns its error.
	_ = body.Close()
}
