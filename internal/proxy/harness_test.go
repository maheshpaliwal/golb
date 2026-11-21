package proxy

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
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

// rotatingPool is a core.Pool that hands out backends in order, skipping
// excluded ones, and records every Done.
type rotatingPool struct {
	backends []*core.Backend
	next     atomic.Uint64

	mu      sync.Mutex
	results []doneCall
}

type doneCall struct {
	addr   string
	result core.Result
}

func newRotatingPool(addrs ...string) *rotatingPool {
	p := new(rotatingPool)
	for _, addr := range addrs {
		p.backends = append(p.backends, core.NewBackend(addr, 1))
	}
	return p
}

func (p *rotatingPool) Pick(info core.PickInfo) *core.Backend {
	n := uint64(len(p.backends))
	if n == 0 {
		return nil
	}
	start := p.next.Add(1) - 1
	for i := range n {
		b := p.backends[(start+i)%n]
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

func (p *rotatingPool) Done(b *core.Backend, res core.Result) {
	// Recorded before the release so a test that waits for idle sees the call.
	p.mu.Lock()
	p.results = append(p.results, doneCall{addr: b.Addr, result: res})
	p.mu.Unlock()
	b.Release()
}

func (p *rotatingPool) Backends() []*core.Backend { return p.backends }
func (p *rotatingPool) Close()                    {}

func (p *rotatingPool) calls() []doneCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]doneCall(nil), p.results...)
}

func (p *rotatingPool) inflight() int64 {
	var total int64
	for _, b := range p.backends {
		total += b.Inflight()
	}
	return total
}

// waitIdle polls until every picked backend was released.
func (p *rotatingPool) waitIdle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(waitLimit)
	for p.inflight() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("in-flight count is %d, want 0: a Pick was never passed to Done", p.inflight())
		}
		time.Sleep(time.Millisecond)
	}
}

// fixture is a proxy handler in front of in-process backends.
type fixture struct {
	pool     *rotatingPool
	registry *metrics.Registry
	upstream *Upstream
	router   *Router
	handler  *Handler
	server   *httptest.Server
}

type fixtureOptions struct {
	backends []string // host:port of each backend
	pool     string   // extra YAML under the pool, indented four spaces
	listener string   // extra YAML under the listener, indented four spaces
	routes   string   // replaces the default catch-all route
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func parseConfig(t testing.TB, opts fixtureOptions) *config.Config {
	t.Helper()
	routes := opts.routes
	if routes == "" {
		routes = "      - pool: app\n"
	}
	var doc strings.Builder
	doc.WriteString("listeners:\n  - name: web\n    addr: 127.0.0.1:0\n    protocol: http\n")
	doc.WriteString(opts.listener)
	doc.WriteString("    routes:\n" + routes)
	doc.WriteString("pools:\n  - name: app\n    backends:\n")
	for _, addr := range opts.backends {
		fmt.Fprintf(&doc, "      - addr: %s\n", addr)
	}
	doc.WriteString(opts.pool)
	cfg, err := config.Parse([]byte(doc.String()))
	if err != nil {
		t.Fatalf("parse fixture config: %v\n%s", err, doc.String())
	}
	return cfg
}

func newFixture(t testing.TB, opts fixtureOptions) *fixture {
	t.Helper()
	cfg := parseConfig(t, opts)
	f := &fixture{
		pool:     newRotatingPool(opts.backends...),
		registry: metrics.NewRegistry(),
	}
	f.upstream = NewUpstream(UpstreamOptions{Config: cfg.Pools[0], Pool: f.pool, Registry: f.registry})
	f.router = NewRouter(RouterOptions{
		Listener:  cfg.Listeners[0],
		Upstreams: map[string]*Upstream{"app": f.upstream},
		Registry:  f.registry,
	})
	f.handler = NewHandler(func() *Router { return f.router }, quietLogger())
	t.Cleanup(func() { f.upstream.Retire(nil) })
	return f
}

// serve starts the proxy on a loopback port.
func (f *fixture) serve(t testing.TB) *fixture {
	t.Helper()
	f.server = httptest.NewServer(f.handler)
	t.Cleanup(f.server.Close)
	return f
}

func (f *fixture) counter(name string, labels ...string) uint64 {
	return f.registry.Counter(name, "", labels...).Value()
}

// backend starts an in-process upstream and returns its host:port.
func backend(t testing.TB, handler http.HandlerFunc) string {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server.Listener.Addr().String()
}

// refusedAddr returns a loopback address nothing listens on.
func refusedAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	err = l.Close()
	if err != nil {
		t.Fatalf("release port: %v", err)
	}
	return addr
}

func get(t *testing.T, target string) (status int, body string) {
	t.Helper()
	res, err := http.Get(target)
	if err != nil {
		t.Fatalf("GET %s: %v", target, err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body of %s: %v", target, err)
	}
	return res.StatusCode, string(data)
}
