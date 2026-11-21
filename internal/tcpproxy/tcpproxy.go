// Package tcpproxy is the L4 data path: accept, pick a backend, connect
// (trying another backend on failure) and copy bytes both ways with
// half-close and an idle timeout.
package tcpproxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maheshpaliwal/golb/internal/config"
	"github.com/maheshpaliwal/golb/internal/core"
	"github.com/maheshpaliwal/golb/internal/metrics"
)

const (
	copyBufferBytes = 32 << 10
	dialKeepAlive   = 30 * time.Second
	acceptBackoff   = 5 * time.Millisecond
	acceptBackoffTo = time.Second

	directionIn  = "in"  // client to backend
	directionOut = "out" // backend to client
)

var errNoBackend = errors.New("no backend available")

var copyBuffers = sync.Pool{New: func() any { return new([copyBufferBytes]byte) }}

// Target is the immutable forwarding state of one TCP listener.
type Target struct {
	pool        core.Pool
	dialer      net.Dialer
	maxAttempts int
	idle        time.Duration
	ringHash    bool

	connections *metrics.Counter
	bytesIn     *metrics.Counter
	bytesOut    *metrics.Counter
}

type TargetOptions struct {
	Listener config.Listener
	Config   config.Pool
	Pool     core.Pool
	Registry *metrics.Registry
}

func NewTarget(opts TargetOptions) *Target {
	name := opts.Listener.Name
	reg := opts.Registry
	const bytesHelp = "Bytes copied by TCP listeners."
	return &Target{
		pool:        opts.Pool,
		dialer:      net.Dialer{Timeout: opts.Config.Transport.DialTimeout, KeepAlive: dialKeepAlive},
		maxAttempts: opts.Config.Retry.MaxAttempts,
		idle:        opts.Listener.Timeouts.Idle,
		ringHash:    opts.Config.Policy == core.RingHash,
		connections: reg.Counter("golb_tcp_connections_total", "Connections accepted by TCP listeners.", "listener", name),
		bytesIn:     reg.Counter("golb_tcp_bytes_total", bytesHelp, "listener", name, "direction", directionIn),
		bytesOut:    reg.Counter("golb_tcp_bytes_total", bytesHelp, "listener", name, "direction", directionOut),
	}
}

// Proxy serves one TCP listener. A connection uses the Target that was
// current when it was accepted until it closes.
type Proxy struct {
	source func() *Target
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	listener net.Listener
	conns    map[net.Conn]struct{}
	closing  bool
	handlers sync.WaitGroup
}

// New returns a proxy that forwards with whatever source returns when a
// connection is accepted. ctx bounds every connection: cancelling it aborts
// connects in progress.
func New(ctx context.Context, source func() *Target, logger *slog.Logger) *Proxy {
	p := &Proxy{
		source: source,
		logger: logger,
		conns:  make(map[net.Conn]struct{}),
	}
	p.ctx, p.cancel = context.WithCancel(ctx)
	return p
}

// Serve accepts connections until Shutdown closes the listener, then returns nil.
func (p *Proxy) Serve(l net.Listener) error {
	p.mu.Lock()
	p.listener = l
	p.mu.Unlock()

	backoff := acceptBackoff
	for {
		conn, err := l.Accept()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		if err != nil {
			// Typically the process is out of file descriptors; wait for some to free up.
			p.logger.Error("accept tcp connection", "error", err)
			select {
			case <-time.After(backoff):
			case <-p.ctx.Done():
				return nil
			}
			backoff = min(2*backoff, acceptBackoffTo)
			continue
		}
		backoff = acceptBackoff
		if !p.track(conn) {
			closeQuietly(conn)
			continue
		}
		go p.handle(conn)
	}
}

// Shutdown stops accepting and waits for open connections. When ctx ends
// first, the remaining connections are closed and ctx's error is returned.
func (p *Proxy) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.closing = true
	listener := p.listener
	p.mu.Unlock()
	if listener != nil {
		closeQuietly(listener)
	}

	drained := make(chan struct{})
	go func() {
		p.handlers.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		p.cancel()
		return nil
	case <-ctx.Done():
	}

	p.cancel()
	p.mu.Lock()
	for conn := range p.conns {
		closeQuietly(conn)
	}
	p.mu.Unlock()
	<-drained
	return ctx.Err()
}

func (p *Proxy) track(conn net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return false
	}
	p.conns[conn] = struct{}{}
	p.handlers.Add(1)
	return true
}

func (p *Proxy) untrack(conn net.Conn) {
	p.mu.Lock()
	delete(p.conns, conn)
	p.mu.Unlock()
	p.handlers.Done()
}

func (p *Proxy) handle(client net.Conn) {
	defer p.untrack(client)
	defer closeQuietly(client)
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("tcp connection handler", "panic", r)
		}
	}()

	target := p.source()
	target.connections.Inc()
	link, err := target.connect(p.ctx, client)
	if err != nil {
		p.logger.Debug("connect tcp upstream", "client", client.RemoteAddr().String(), "error", err)
		return
	}
	defer target.pool.Done(link.backend, core.Result{Latency: link.latency})
	defer closeQuietly(link.conn)

	// Shutdown closes the client side; closing the backend side with it
	// unblocks the copy that is reading from the backend.
	stop := context.AfterFunc(p.ctx, func() { closeQuietly(link.conn) })
	defer stop()

	pipe := &relay{idle: target.idle}
	pipe.touch()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				p.logger.Error("tcp copy", "panic", r)
			}
		}()
		pipe.copy(link.conn, client, target.bytesIn)
	}()
	pipe.copy(client, link.conn, target.bytesOut)
	<-done
}

type link struct {
	conn    net.Conn
	backend *core.Backend
	latency time.Duration
}

// connect picks a backend and dials it, moving on to another backend when
// the dial fails. The returned backend must be passed to Done once.
func (t *Target) connect(ctx context.Context, client net.Conn) (link, error) {
	info := core.PickInfo{}
	if t.ringHash {
		info.Key = core.Hash(clientIP(client))
	}
	err := errNoBackend
	for range t.maxAttempts {
		b := t.pool.Pick(info)
		if b == nil {
			return link{}, err
		}
		began := time.Now()
		var conn net.Conn
		conn, err = t.dialer.DialContext(ctx, "tcp", b.Addr)
		if err == nil {
			return link{conn: conn, backend: b, latency: time.Since(began)}, nil
		}
		if ctx.Err() != nil {
			t.pool.Done(b, core.Result{})
			return link{}, err
		}
		t.pool.Done(b, core.Result{Latency: time.Since(began), Failed: true})
		info.Exclude = append(info.Exclude, b)
	}
	return link{}, err
}

func clientIP(conn net.Conn) string {
	addr := conn.RemoteAddr().String()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// relay is the shared state of the two copy directions of one connection.
// The idle timeout applies to the connection as a whole: a direction with
// nothing to read stays open while the other one is moving bytes.
type relay struct {
	idle time.Duration
	last atomic.Int64 // unix nanoseconds of the latest byte in either direction
}

func (r *relay) touch() time.Time {
	now := time.Now()
	r.last.Store(now.UnixNano())
	return now
}

// copy moves bytes from src to dst until src ends. A clean end is passed on
// as a half-close so the other direction can finish; an error or an idle
// connection closes both sides, which stops the other direction too.
func (r *relay) copy(dst, src net.Conn, bytes *metrics.Counter) {
	array := copyBuffers.Get().(*[copyBufferBytes]byte)
	defer copyBuffers.Put(array)
	buf := array[:]
	for {
		// Deadline errors surface as a failed Read or Write below.
		_ = src.SetReadDeadline(time.Now().Add(r.idle))
		n, err := src.Read(buf)
		if n > 0 {
			now := r.touch()
			_ = dst.SetWriteDeadline(now.Add(r.idle))
			_, writeErr := dst.Write(buf[:n])
			if writeErr != nil {
				closeQuietly(src)
				closeQuietly(dst)
				return
			}
			bytes.Add(uint64(n))
		}
		if err == nil || r.stillActive(err) {
			continue
		}
		if errors.Is(err, io.EOF) && closeWrite(dst) {
			return
		}
		closeQuietly(src)
		closeQuietly(dst)
		return
	}
}

// stillActive reports whether a read timed out only because this direction
// was quiet while the other one was not.
func (r *relay) stillActive(err error) bool {
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		return false
	}
	return time.Since(time.Unix(0, r.last.Load())) < r.idle
}

func closeWrite(conn net.Conn) bool {
	half, ok := conn.(interface{ CloseWrite() error })
	return ok && half.CloseWrite() == nil
}

// closeQuietly closes a connection whose close error says nothing useful:
// the peer or the other copy direction usually closed it first.
func closeQuietly(c io.Closer) {
	_ = c.Close()
}
