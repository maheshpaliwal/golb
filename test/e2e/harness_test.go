// Package e2e holds black-box tests: each one starts a real golb server from
// a generated YAML file and drives it over loopback sockets against fake
// backends in the same process. Nothing here listens on anything but
// 127.0.0.1 with an ephemeral port.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/golb/internal/server"
)

const (
	waitTimeout  = 5 * time.Second
	pollInterval = 10 * time.Millisecond
	healthPath   = "/healthz"
	backendName  = "X-Backend"

	web = "web" // name of the HTTP listener in generated configs
	raw = "raw" // name of the TCP listener in generated configs

	outlierOff = "    outlier:\n      disabled: true\n"
)

// lockedBuffer is a bytes.Buffer that the server's log goroutines and the
// test can use at the same time.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// instance is one running golb server.
type instance struct {
	t      *testing.T
	srv    *server.Server
	path   string
	logs   *lockedBuffer
	access *lockedBuffer
	ports  []string // addresses reserved for the config's listeners, in order
	admin  *http.Client

	goroutinesBefore int
	stopOnce         sync.Once
	stopErr          error
	stopped          atomic.Bool
}

// start runs golb with cfg (an admin listener on an ephemeral loopback port
// is added) and no file watcher. On cleanup it checks that no request is
// still counted in flight, shuts the server down and checks that the
// goroutine count is back to where it was before the server started.
func start(t *testing.T, cfg string) *instance {
	t.Helper()
	return startWatching(t, cfg, -1)
}

func startWatching(t *testing.T, cfg string, watch time.Duration) *instance {
	t.Helper()
	in := &instance{
		t:      t,
		path:   filepath.Join(t.TempDir(), "golb.yaml"),
		logs:   new(lockedBuffer),
		access: new(lockedBuffer),
		admin:  newAdminClient(),
	}
	in.writeConfig(cfg)
	in.goroutinesBefore = runtime.NumGoroutine()
	logger := slog.New(slog.NewTextHandler(in.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	srv, err := server.New(context.Background(), server.Options{
		ConfigPath:    in.path,
		Logger:        logger,
		AccessLog:     in.access,
		WatchInterval: watch,
	})
	if err != nil {
		t.Fatalf("start golb: %v\nconfig:\n%s", err, cfg)
	}
	in.srv = srv
	srv.Start()
	t.Cleanup(in.cleanup)
	return in
}

func withAdmin(cfg string) string {
	if strings.Contains(cfg, "admin:") {
		return cfg
	}
	return "admin:\n  addr: 127.0.0.1:0\n" + cfg
}

// anyPort is what generated configs write for a listener address. golb's
// validation treats two listeners on "127.0.0.1:0" as the same address, so
// each occurrence is replaced by a port reserved for this instance; the
// n-th occurrence gets the same port on every rewrite, which keeps the
// listener set stable across reloads.
const anyPort = "127.0.0.1:0"

func (in *instance) resolve(cfg string) string {
	in.t.Helper()
	parts := strings.Split(withAdmin(cfg), anyPort)
	for len(in.ports) < len(parts)-1 {
		in.ports = append(in.ports, deadAddr(in.t))
	}
	var b strings.Builder
	for i, part := range parts {
		b.WriteString(part)
		if i < len(parts)-1 {
			b.WriteString(in.ports[i])
		}
	}
	return b.String()
}

func (in *instance) writeConfig(cfg string) {
	in.t.Helper()
	err := os.WriteFile(in.path, []byte(in.resolve(cfg)), 0o600)
	if err != nil {
		in.t.Fatalf("write config: %v", err)
	}
}

func (in *instance) cleanup() {
	in.t.Helper()
	if !in.stopped.Load() {
		in.waitIdle()
		err := in.shutdown()
		if err != nil {
			in.t.Errorf("shutdown: %v", err)
		}
	}
	in.admin.CloseIdleConnections()
	in.checkGoroutines()
	if in.t.Failed() {
		in.t.Logf("golb log (tail):\n%s", tail(in.logs.String(), 4000))
	}
}

// shutdown drains the server once; later calls return the first result.
func (in *instance) shutdown() error {
	in.stopOnce.Do(func() {
		in.stopErr = in.srv.Shutdown(context.Background())
		in.stopped.Store(true)
	})
	return in.stopErr
}

// waitIdle fails the test if any backend still has requests in flight once
// the test body is over: every Pick must have reached its Done.
func (in *instance) waitIdle() {
	in.t.Helper()
	var busy []backendRow
	ok := eventually(3*time.Second, func() bool {
		busy = busy[:0]
		for _, row := range in.backends() {
			if row.Inflight != 0 {
				busy = append(busy, row)
			}
		}
		return len(busy) == 0
	})
	if !ok {
		in.t.Errorf("in-flight did not return to 0 after the scenario: %+v", busy)
	}
}

func (in *instance) checkGoroutines() {
	in.t.Helper()
	ok := eventually(waitTimeout, func() bool { return runtime.NumGoroutine() <= in.goroutinesBefore })
	if ok {
		return
	}
	var dump bytes.Buffer
	err := pprof.Lookup("goroutine").WriteTo(&dump, 1)
	if err != nil {
		in.t.Logf("goroutine dump: %v", err)
	}
	in.t.Errorf("goroutine leak: %d before golb started, %d after shutdown\n%s",
		in.goroutinesBefore, runtime.NumGoroutine(), tail(dump.String(), 6000))
}

func (in *instance) addr(listener string) string {
	in.t.Helper()
	addr := in.srv.Addr(listener)
	if addr == nil {
		in.t.Fatalf("no listener named %q", listener)
	}
	return addr.String()
}

func (in *instance) url(path string) string { return "http://" + in.addr(web) + path }

func (in *instance) adminURL(path string) string { return "http://" + in.addr("admin") + path }

// backendRow mirrors one element of GET /backends.
type backendRow struct {
	Pool      string  `json:"pool"`
	Addr      string  `json:"addr"`
	Weight    uint32  `json:"weight"`
	Healthy   bool    `json:"healthy"`
	Ejected   bool    `json:"ejected"`
	Inflight  int64   `json:"inflight"`
	LatencyMs float64 `json:"latency_ms"`
}

func (in *instance) backends() []backendRow {
	in.t.Helper()
	status, body := in.adminGet("/backends")
	if status != http.StatusOK {
		in.t.Fatalf("GET /backends: status %d: %s", status, body)
	}
	var rows []backendRow
	err := json.Unmarshal([]byte(body), &rows)
	if err != nil {
		in.t.Fatalf("GET /backends: %v\n%s", err, body)
	}
	return rows
}

func (in *instance) backend(addr string) backendRow {
	in.t.Helper()
	for _, row := range in.backends() {
		if row.Addr == addr {
			return row
		}
	}
	in.t.Fatalf("backend %s is not in /backends", addr)
	return backendRow{}
}

func (in *instance) adminGet(path string) (int, string) {
	in.t.Helper()
	return in.adminDo(http.MethodGet, path)
}

func (in *instance) adminDo(method, path string) (int, string) {
	in.t.Helper()
	req, err := http.NewRequest(method, in.adminURL(path), nil)
	if err != nil {
		in.t.Fatalf("admin request: %v", err)
	}
	res, err := in.admin.Do(req)
	if err != nil {
		in.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		in.t.Fatalf("%s %s: read body: %v", method, path, err)
	}
	return res.StatusCode, string(body)
}

// newAdminClient returns a client for one instance's admin listener. It
// keeps its connection alive: the tests poll the admin endpoints often, and
// a connection per poll would use up the machine's ephemeral ports.
func newAdminClient() *http.Client {
	return &http.Client{Timeout: waitTimeout, Transport: &http.Transport{MaxIdleConnsPerHost: 2}}
}

// reload rewrites the config file and asks the server to load it.
func (in *instance) reload(cfg string) (int, string) {
	in.t.Helper()
	in.writeConfig(cfg)
	return in.adminDo(http.MethodPost, "/reload")
}

func (in *instance) mustReload(cfg string) {
	in.t.Helper()
	status, body := in.reload(cfg)
	if status != http.StatusOK {
		in.t.Fatalf("reload: status %d: %s\nlog:\n%s", status, body, tail(in.logs.String(), 2000))
	}
}

func (in *instance) metrics() *exposition {
	in.t.Helper()
	status, body := in.adminGet("/metrics")
	if status != http.StatusOK {
		in.t.Fatalf("GET /metrics: status %d", status)
	}
	exp, err := parseExposition(body)
	if err != nil {
		in.t.Fatalf("/metrics is not valid Prometheus text: %v\n%s", err, body)
	}
	return exp
}

// backend is a fake upstream HTTP server on 127.0.0.1 with an ephemeral port.
type backend struct {
	t       *testing.T
	name    string
	addr    string
	handler http.HandlerFunc
	srv     *httptest.Server

	hits atomic.Int64 // requests other than health probes
	open atomic.Int64 // connections currently open to this backend
	sick atomic.Bool  // health probes answer 500 while set
}

// newBackend starts a backend. A nil handler answers 200 with the backend's
// name as the body. Every response carries the name in X-Backend.
func newBackend(t *testing.T, name string, handler http.HandlerFunc) *backend {
	t.Helper()
	b := &backend{t: t, name: name, handler: handler}
	if handler == nil {
		b.handler = func(w http.ResponseWriter, r *http.Request) { writeString(w, name) }
	}
	b.srv = httptest.NewUnstartedServer(b)
	b.srv.Config.ConnState = b.track
	b.srv.Start()
	b.addr = b.srv.Listener.Addr().String()
	t.Cleanup(func() { b.srv.Close() })
	return b
}

func newBackends(t *testing.T, n int) []*backend {
	t.Helper()
	out := make([]*backend, n)
	for i := range out {
		out[i] = newBackend(t, fmt.Sprintf("b%d", i), nil)
	}
	return out
}

func (b *backend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == healthPath {
		if b.sick.Load() {
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}
	b.hits.Add(1)
	w.Header().Set(backendName, b.name)
	b.handler(w, r)
}

func (b *backend) track(_ net.Conn, state http.ConnState) {
	switch state {
	case http.StateNew:
		b.open.Add(1)
	case http.StateClosed, http.StateHijacked:
		b.open.Add(-1)
	}
}

// stop closes the listener and every connection, as a crashed process would.
func (b *backend) stop() {
	b.srv.CloseClientConnections()
	b.srv.Close()
}

// restart brings a stopped backend back on the same address.
func (b *backend) restart() {
	b.t.Helper()
	var listener net.Listener
	ok := eventually(waitTimeout, func() bool {
		var err error
		listener, err = net.Listen("tcp", b.addr)
		return err == nil
	})
	if !ok {
		b.t.Fatalf("restart backend %s: address %s is still in use", b.name, b.addr)
	}
	b.srv = httptest.NewUnstartedServer(b)
	b.srv.Config.ConnState = b.track
	b.srv.Listener = listener
	b.srv.Start()
	srv := b.srv
	b.t.Cleanup(srv.Close)
}

func writeString(w io.Writer, s string) {
	// The peer going away is part of several scenarios; the reader's side
	// asserts on what arrived.
	_, _ = io.WriteString(w, s)
}

// deadAddr returns a loopback address that refuses connections.
func deadAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	addr := listener.Addr().String()
	err = listener.Close()
	if err != nil {
		t.Fatalf("release address: %v", err)
	}
	return addr
}

// pool describes one pool of a generated config.
type pool struct {
	name    string // "app" when empty
	policy  string
	addrs   []string
	weights []int  // parallel to addrs; missing or 0 means the default
	extra   string // further pool-level YAML, indented by four spaces
}

func poolOf(policy string, backends ...*backend) pool {
	return pool{policy: policy, addrs: addrsOf(backends...)}
}

func addrsOf(backends ...*backend) []string {
	addrs := make([]string, len(backends))
	for i, b := range backends {
		addrs[i] = b.addr
	}
	return addrs
}

func (p pool) with(extra string) pool {
	p.extra += extra
	return p
}

func (p pool) weighted(weights ...int) pool {
	p.weights = weights
	return p
}

func (p pool) yaml() string {
	var b strings.Builder
	name := p.name
	if name == "" {
		name = "app"
	}
	fmt.Fprintf(&b, "  - name: %s\n", name)
	if p.policy != "" {
		fmt.Fprintf(&b, "    policy: %s\n", p.policy)
	}
	b.WriteString("    backends:\n")
	for i, addr := range p.addrs {
		fmt.Fprintf(&b, "      - addr: %s\n", addr)
		if i < len(p.weights) && p.weights[i] > 0 {
			fmt.Fprintf(&b, "        weight: %d\n", p.weights[i])
		}
	}
	b.WriteString(p.extra)
	return b.String()
}

// webConfig is one HTTP listener named "web" that sends everything to p.
// listenerExtra is further listener-level YAML, indented by four spaces.
func webConfig(p pool, listenerExtra string) string {
	return "listeners:\n  - name: web\n    addr: 127.0.0.1:0\n    protocol: http\n" + listenerExtra +
		"    routes:\n      - path_prefix: /\n        pool: app\npools:\n" + p.yaml()
}

// tcpConfig is one TCP listener named "raw" in front of p.
func tcpConfig(p pool, listenerExtra string) string {
	return "listeners:\n  - name: raw\n    addr: 127.0.0.1:0\n    protocol: tcp\n    pool: app\n" + listenerExtra +
		"pools:\n" + p.yaml()
}

// newClient returns an HTTP client whose idle connections are closed before
// the server it talks to is shut down, so they do not count as leaks.
func newClient(t *testing.T) *http.Client {
	t.Helper()
	transport := &http.Transport{MaxIdleConnsPerHost: 64, DisableCompression: true}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 2 * waitTimeout}
}

type reply struct {
	status  int
	body    string
	header  http.Header
	elapsed time.Duration
}

// do sends one request and reads the whole response.
func do(client *http.Client, req *http.Request) (reply, error) {
	began := time.Now()
	res, err := client.Do(req)
	if err != nil {
		return reply{elapsed: time.Since(began)}, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	return reply{status: res.StatusCode, body: string(body), header: res.Header, elapsed: time.Since(began)}, err
}

func get(t *testing.T, client *http.Client, url string) reply {
	t.Helper()
	return send(t, client, http.MethodGet, url, "")
}

func send(t *testing.T, client *http.Client, method, url, body string, headers ...string) reply {
	t.Helper()
	r, err := trySend(client, method, url, body, headers...)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return r
}

// trySend is send for callers on other goroutines, which must not call t.Fatal.
func trySend(client *http.Client, method, url, body string, headers ...string) (reply, error) {
	var payload io.Reader
	if body != "" {
		payload = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return reply{}, err
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	return do(client, req)
}

// knownBug marks a test that demonstrates an open defect. The test is
// skipped so the suite stays green; GOLB_E2E_KNOWN_BUGS=1 runs it, and it is
// expected to fail until the defect is fixed. Every reason starts with
// "BUG" so the list can be found with grep.
func knownBug(t *testing.T, reason string) {
	t.Helper()
	if os.Getenv("GOLB_E2E_KNOWN_BUGS") == "" {
		t.Skip(reason)
	}
}

// eventually polls cond until it holds or the deadline passes.
func eventually(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(pollInterval)
	}
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}

// within reports whether got is inside want ± tolerance (a fraction of total).
func within(got, want, total int64, tolerance float64) bool {
	diff := float64(got - want)
	if diff < 0 {
		diff = -diff
	}
	return diff <= tolerance*float64(total)
}
