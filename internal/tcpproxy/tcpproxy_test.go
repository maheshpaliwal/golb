package tcpproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/golb/internal/config"
	"github.com/maheshpaliwal/golb/internal/core"
	"github.com/maheshpaliwal/golb/internal/metrics"
)

const waitLimit = 5 * time.Second

// orderedPool hands out backends in config order, skipping excluded ones.
type orderedPool struct {
	backends []*core.Backend

	mu      sync.Mutex
	results []core.Result
}

func (p *orderedPool) Pick(info core.PickInfo) *core.Backend {
	for _, b := range p.backends {
		excluded := false
		for _, tried := range info.Exclude {
			excluded = excluded || tried == b
		}
		if !excluded {
			b.Acquire()
			return b
		}
	}
	return nil
}

func (p *orderedPool) Done(b *core.Backend, res core.Result) {
	b.Release()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.results = append(p.results, res)
}

func (p *orderedPool) Backends() []*core.Backend { return p.backends }
func (p *orderedPool) Close()                    {}

func (p *orderedPool) calls() []core.Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]core.Result(nil), p.results...)
}

func (p *orderedPool) waitIdle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(waitLimit)
	for {
		var inflight int64
		for _, b := range p.backends {
			inflight += b.Inflight()
		}
		if inflight == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("in-flight count is %d, want 0", inflight)
		}
		time.Sleep(time.Millisecond)
	}
}

type fixture struct {
	pool     *orderedPool
	registry *metrics.Registry
	proxy    *Proxy
	addr     string
	served   chan error
}

// start runs a TCP proxy on a loopback port in front of the given backends.
func start(t *testing.T, idle string, backends ...string) *fixture {
	t.Helper()
	var doc strings.Builder
	fmt.Fprintf(&doc, "listeners:\n  - name: raw\n    addr: 127.0.0.1:0\n    protocol: tcp\n    pool: db\n    timeouts:\n      idle: %s\n", idle)
	doc.WriteString("pools:\n  - name: db\n    transport:\n      dial_timeout: 1s\n    backends:\n")
	f := &fixture{pool: new(orderedPool), registry: metrics.NewRegistry(), served: make(chan error, 1)}
	for _, addr := range backends {
		fmt.Fprintf(&doc, "      - addr: %s\n", addr)
		f.pool.backends = append(f.pool.backends, core.NewBackend(addr, 1))
	}
	cfg, err := config.Parse([]byte(doc.String()))
	if err != nil {
		t.Fatalf("parse fixture config: %v", err)
	}
	target := NewTarget(TargetOptions{Listener: cfg.Listeners[0], Config: cfg.Pools[0], Pool: f.pool, Registry: f.registry})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	f.proxy = New(context.Background(), func() *Target { return target }, logger)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f.addr = listener.Addr().String()
	go func() { f.served <- f.proxy.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		f.proxy.Shutdown(ctx)
	})
	return f
}

func (f *fixture) dial(t *testing.T) *net.TCPConn {
	t.Helper()
	conn, err := net.Dial("tcp", f.addr)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(waitLimit))
	return conn.(*net.TCPConn)
}

// tcpBackend runs handler for every accepted connection and returns its address.
func tcpBackend(t *testing.T, handler func(*net.TCPConn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				handler(conn.(*net.TCPConn))
			}()
		}
	}()
	return listener.Addr().String()
}

func refusedAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func echo(conn *net.TCPConn) { io.Copy(conn, conn) }

func TestEcho(t *testing.T) {
	f := start(t, "30s", tcpBackend(t, echo))
	conn := f.dial(t)

	for _, message := range []string{"hello", strings.Repeat("x", 100_000)} {
		go io.WriteString(conn, message)
		got := make([]byte, len(message))
		_, err := io.ReadFull(conn, got)
		if err != nil {
			t.Fatalf("read echo: %v", err)
		}
		if string(got) != message {
			t.Errorf("echo of %d bytes came back different", len(message))
		}
	}
	conn.Close()
	f.pool.waitIdle(t)

	const total = 5 + 100_000
	in := f.registry.Counter("golb_tcp_bytes_total", "", "listener", "raw", "direction", "in").Value()
	out := f.registry.Counter("golb_tcp_bytes_total", "", "listener", "raw", "direction", "out").Value()
	if in != total || out != total {
		t.Errorf("bytes in=%d out=%d, want %d each", in, out, total)
	}
	if got := f.registry.Counter("golb_tcp_connections_total", "", "listener", "raw").Value(); got != 1 {
		t.Errorf("connections = %d, want 1", got)
	}
	calls := f.pool.calls()
	if len(calls) != 1 || calls[0].Failed || calls[0].Latency <= 0 {
		t.Errorf("Done calls = %+v, want one success with the connect time", calls)
	}
}

func TestHalfClose(t *testing.T) {
	// The backend answers only after the client has finished sending, so the
	// reply arrives only if the client's FIN is passed on and the return
	// direction stays open.
	summarise := func(conn *net.TCPConn) {
		data, err := io.ReadAll(conn)
		if err != nil {
			return
		}
		fmt.Fprintf(conn, "received %d bytes", len(data))
		conn.CloseWrite()
	}
	f := start(t, "30s", tcpBackend(t, summarise))
	conn := f.dial(t)

	io.WriteString(conn, "request body")
	err := conn.CloseWrite()
	if err != nil {
		t.Fatalf("half-close: %v", err)
	}
	reply, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read reply after half-close: %v", err)
	}
	if string(reply) != "received 12 bytes" {
		t.Errorf("reply = %q, want %q", reply, "received 12 bytes")
	}
	f.pool.waitIdle(t)
}

func TestConnectRetriesAnotherBackend(t *testing.T) {
	tests := []struct {
		name      string
		dead      int
		wantReply bool
		wantDone  []bool // Failed flag of each Done, in order
	}{
		{name: "first backend refuses", dead: 1, wantReply: true, wantDone: []bool{true, false}},
		{name: "two backends refuse", dead: 2, wantReply: true, wantDone: []bool{true, true, false}},
		{name: "attempts run out before a live backend", dead: 3, wantReply: false, wantDone: []bool{true, true, true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var addrs []string
			for range tc.dead {
				addrs = append(addrs, refusedAddr(t))
			}
			addrs = append(addrs, tcpBackend(t, echo))
			f := start(t, "30s", addrs...)
			conn := f.dial(t)

			io.WriteString(conn, "ping")
			got := make([]byte, 4)
			_, err := io.ReadFull(conn, got)
			if tc.wantReply && (err != nil || string(got) != "ping") {
				t.Errorf("echo = %q, %v; want ping", got, err)
			}
			if !tc.wantReply && err == nil {
				t.Errorf("got a reply %q with every tried backend down", got)
			}
			conn.Close()
			f.pool.waitIdle(t)
			calls := f.pool.calls()
			if len(calls) != len(tc.wantDone) {
				t.Fatalf("Done called %d times, want %d: %+v", len(calls), len(tc.wantDone), calls)
			}
			for i, want := range tc.wantDone {
				if calls[i].Failed != want {
					t.Errorf("Done %d Failed = %v, want %v", i, calls[i].Failed, want)
				}
			}
		})
	}
}

func TestIdleTimeoutClosesConnection(t *testing.T) {
	f := start(t, "100ms", tcpBackend(t, echo))
	conn := f.dial(t)

	// Traffic in one direction keeps the whole connection open past the timeout.
	for range 4 {
		time.Sleep(50 * time.Millisecond)
		io.WriteString(conn, "k")
		_, err := io.ReadFull(conn, make([]byte, 1))
		if err != nil {
			t.Fatalf("connection closed while active: %v", err)
		}
	}

	began := time.Now()
	_, err := conn.Read(make([]byte, 1))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("read on an idle connection = %v, want EOF from the proxy closing it", err)
	}
	if waited := time.Since(began); waited > 2*time.Second {
		t.Errorf("idle connection closed after %v, want about 100ms", waited)
	}
	f.pool.waitIdle(t)
}

func TestShutdown(t *testing.T) {
	tests := []struct {
		name       string
		grace      time.Duration
		closeEarly bool
		wantErr    error
	}{
		{name: "waits for an open connection to finish", grace: waitLimit, closeEarly: true},
		{name: "closes connections still open at the deadline", grace: 50 * time.Millisecond, wantErr: context.DeadlineExceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := start(t, "30s", tcpBackend(t, echo))
			conn := f.dial(t)
			io.WriteString(conn, "x")
			_, err := io.ReadFull(conn, make([]byte, 1))
			if err != nil {
				t.Fatalf("echo before shutdown: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), tc.grace)
			defer cancel()
			finished := make(chan error, 1)
			go func() { finished <- f.proxy.Shutdown(ctx) }()

			select {
			case err := <-f.served:
				if err != nil {
					t.Errorf("Serve returned %v, want nil", err)
				}
			case <-time.After(waitLimit):
				t.Fatal("Serve did not return after Shutdown")
			}
			_, err = net.DialTimeout("tcp", f.addr, time.Second)
			if err == nil {
				t.Error("proxy still accepts connections after Shutdown")
			}

			if tc.closeEarly {
				// The open connection must still work while draining.
				io.WriteString(conn, "y")
				_, err = io.ReadFull(conn, make([]byte, 1))
				if err != nil {
					t.Errorf("echo during drain: %v", err)
				}
				conn.Close()
			}
			select {
			case err := <-finished:
				if !errors.Is(err, tc.wantErr) {
					t.Errorf("Shutdown = %v, want %v", err, tc.wantErr)
				}
			case <-time.After(waitLimit):
				t.Fatal("Shutdown did not return")
			}
			if !tc.closeEarly {
				_, err = conn.Read(make([]byte, 1))
				if err == nil {
					t.Error("connection survived a forced shutdown")
				}
			}
			f.pool.waitIdle(t)
		})
	}
}

// An open connection that is not moving bytes must not hold a pooled copy
// buffer; a bulk transfer borrows them and gives them back.
func TestCopyBuffersAreHeldOnlyWhileCopying(t *testing.T) {
	var held atomic.Int64
	original := copyBuffers
	copyBuffers = &countingBuffers{held: &held}
	t.Cleanup(func() { copyBuffers = original })

	f := start(t, "30s", tcpBackend(t, echo))
	conn := f.dial(t)
	exchange := func(message string) {
		t.Helper()
		go io.WriteString(conn, message)
		_, err := io.ReadFull(conn, make([]byte, len(message)))
		if err != nil {
			t.Fatalf("echo of %d bytes: %v", len(message), err)
		}
	}
	settle := func(what string) {
		t.Helper()
		deadline := time.Now().Add(waitLimit)
		for held.Load() != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("%s: %d copy buffers still held by an idle connection, want 0", what, held.Load())
			}
			time.Sleep(time.Millisecond)
		}
	}

	exchange("small message")
	settle("after a small message")
	exchange(strings.Repeat("x", 1<<20))
	settle("after a bulk transfer")
	if got := f.registry.Counter("golb_tcp_bytes_total", "", "listener", "raw", "direction", "out").Value(); got != 13+1<<20 {
		t.Errorf("bytes out = %d, want %d", got, 13+1<<20)
	}
}

type countingBuffers struct {
	held *atomic.Int64
}

func (c *countingBuffers) Get() any {
	c.held.Add(1)
	return new([copyBufferBytes]byte)
}

func (c *countingBuffers) Put(any) { c.held.Add(-1) }
