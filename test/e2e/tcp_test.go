package e2e

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// tcpBackend is a fake L4 upstream; serve handles one accepted connection.
type tcpBackend struct {
	addr     string
	accepted atomic.Int64
}

func newTCPBackend(t *testing.T, serve func(conn *net.TCPConn)) *tcpBackend {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	b := &tcpBackend{addr: listener.Addr().String()}
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			b.accepted.Add(1)
			wg.Go(func() {
				defer closeConn(conn)
				serve(conn.(*net.TCPConn))
			})
		}
	})
	t.Cleanup(func() {
		closeConn(listener)
		wg.Wait()
	})
	return b
}

func echoConn(conn *net.TCPConn) {
	// Ends when the proxy closes its side.
	_, _ = io.Copy(conn, conn)
}

func dialTCP(t *testing.T, addr string) *net.TCPConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, waitTimeout)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	err = conn.SetDeadline(time.Now().Add(waitTimeout))
	if err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	return conn.(*net.TCPConn)
}

func TestTCPEcho(t *testing.T) {
	echo := newTCPBackend(t, echoConn)
	in := start(t, tcpConfig(pool{policy: "round_robin", addrs: []string{echo.addr}}, ""))

	const connections = 8
	payload := bytes.Repeat([]byte("0123456789abcdef"), 16<<10) // 256 KiB
	var wg sync.WaitGroup
	var failed atomic.Int64
	for range connections {
		wg.Go(func() {
			conn, err := net.DialTimeout("tcp", in.addr(raw), waitTimeout)
			if err != nil {
				failed.Add(1)
				return
			}
			defer closeConn(conn)
			go func() {
				// A write error shows up as a short read below.
				_, _ = conn.Write(payload)
			}()
			got := make([]byte, len(payload))
			_, err = io.ReadFull(conn, got)
			if err != nil || !bytes.Equal(got, payload) {
				failed.Add(1)
			}
		})
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.Fatalf("%d of %d connections did not get their bytes echoed", failed.Load(), connections)
	}

	want := float64(connections * len(payload))
	var m *exposition
	counted := eventually(waitTimeout, func() bool {
		m = in.metrics()
		return m.sum("golb_tcp_bytes_total", "listener", raw, "direction", "out") == want
	})
	if !counted {
		t.Errorf("golb_tcp_bytes_total out = %v, want %v", m.sum("golb_tcp_bytes_total", "listener", raw, "direction", "out"), want)
	}
	if got := m.sum("golb_tcp_bytes_total", "listener", raw, "direction", "in"); got != want {
		t.Errorf("golb_tcp_bytes_total in = %v, want %v", got, want)
	}
	if got := m.sum("golb_tcp_connections_total", "listener", raw); got != connections {
		t.Errorf("golb_tcp_connections_total = %v, want %d", got, connections)
	}
}

func TestTCPHalfClose(t *testing.T) {
	// The backend answers only after it has seen the client's FIN.
	summarise := newTCPBackend(t, func(conn *net.TCPConn) {
		n, err := io.Copy(io.Discard, conn)
		if err != nil {
			return
		}
		_, _ = fmt.Fprintf(conn, "received %d bytes", n)
	})
	in := start(t, tcpConfig(pool{policy: "round_robin", addrs: []string{summarise.addr}}, ""))

	conn := dialTCP(t, in.addr(raw))
	defer closeConn(conn)
	payload := bytes.Repeat([]byte("x"), 100_000)
	_, err := conn.Write(payload)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	err = conn.CloseWrite()
	if err != nil {
		t.Fatalf("half-close: %v", err)
	}
	answer, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read after half-close: %v", err)
	}
	if want := fmt.Sprintf("received %d bytes", len(payload)); string(answer) != want {
		t.Errorf("answer after half-close: %q, want %q", answer, want)
	}
}

func TestTCPBackendDown(t *testing.T) {
	t.Run("connect is retried on another backend", func(t *testing.T) {
		echo := newTCPBackend(t, echoConn)
		p := pool{policy: "round_robin", addrs: []string{deadAddr(t), echo.addr}, extra: outlierOff}
		in := start(t, tcpConfig(p, ""))
		for i := range 10 {
			conn := dialTCP(t, in.addr(raw))
			message := fmt.Sprintf("ping %d", i)
			_, err := io.WriteString(conn, message)
			if err != nil {
				t.Fatalf("connection %d: write: %v", i, err)
			}
			got := make([]byte, len(message))
			_, err = io.ReadFull(conn, got)
			closeConn(conn)
			if err != nil || string(got) != message {
				t.Fatalf("connection %d: got %q err %v, want the echo of %q", i, got, err, message)
			}
		}
		if echo.accepted.Load() != 10 {
			t.Errorf("live backend accepted %d connections, want 10", echo.accepted.Load())
		}
	})

	t.Run("client is disconnected when every backend is down", func(t *testing.T) {
		p := pool{policy: "round_robin", addrs: []string{deadAddr(t), deadAddr(t)}, extra: outlierOff}
		in := start(t, tcpConfig(p, ""))
		conn := dialTCP(t, in.addr(raw))
		defer closeConn(conn)
		began := time.Now()
		n, err := conn.Read(make([]byte, 1))
		var netErr net.Error
		if n != 0 || err == nil || (errors.As(err, &netErr) && netErr.Timeout()) {
			t.Errorf("read returned %d bytes, err %v; want the connection closed by golb", n, err)
		}
		if waited := time.Since(began); waited > time.Second {
			t.Errorf("connection closed after %v, want it at once", waited)
		}
	})
}

func TestTCPIdleTimeout(t *testing.T) {
	const idle = 600 * time.Millisecond
	echo := newTCPBackend(t, echoConn)
	in := start(t, tcpConfig(pool{policy: "round_robin", addrs: []string{echo.addr}}, "    timeouts:\n      idle: 600ms\n"))

	conn := dialTCP(t, in.addr(raw))
	defer closeConn(conn)
	// Traffic keeps the connection open past the idle time.
	for range 5 {
		time.Sleep(idle / 4)
		_, err := io.WriteString(conn, "x")
		if err != nil {
			t.Fatalf("write on an active connection: %v", err)
		}
		_, err = io.ReadFull(conn, make([]byte, 1))
		if err != nil {
			t.Fatalf("active connection was closed: %v", err)
		}
	}
	began := time.Now()
	_, err := conn.Read(make([]byte, 1))
	var netErr net.Error
	if err == nil || (errors.As(err, &netErr) && netErr.Timeout()) {
		t.Fatalf("idle connection was not closed (err %v)", err)
	}
	if waited := time.Since(began); waited < idle-50*time.Millisecond || waited > idle+time.Second {
		t.Errorf("idle connection closed after %v, want about %v", waited, idle)
	}
	if !eventually(2*time.Second, func() bool { return in.backend(echo.addr).Inflight == 0 }) {
		t.Errorf("in-flight stayed at %d after the idle connection was closed", in.backend(echo.addr).Inflight)
	}
}
