package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/maheshpaliwal/golb/internal/config"
	"github.com/maheshpaliwal/golb/internal/server"
)

// heldRequests starts n requests against a backend that answers after
// delay and returns once all of them are in flight.
func heldRequests(t *testing.T, client *http.Client, url string, n int, arrived *atomic.Int64) (statuses chan int) {
	t.Helper()
	statuses = make(chan int, n)
	for range n {
		go func() {
			r, err := trySend(client, http.MethodGet, url, "")
			if err != nil {
				statuses <- 0
				return
			}
			statuses <- r.status
		}()
	}
	if !eventually(waitTimeout, func() bool { return arrived.Load() == int64(n) }) {
		t.Fatalf("%d of %d requests reached the backend", arrived.Load(), n)
	}
	return statuses
}

// PRD B3: a drain drops nothing that was in flight.
func TestGracefulShutdownDrainsInFlight(t *testing.T) {
	const (
		inFlight = 8
		delay    = 600 * time.Millisecond
	)
	var arrived atomic.Int64
	slow := newBackend(t, "slow", func(w http.ResponseWriter, r *http.Request) {
		arrived.Add(1)
		time.Sleep(delay)
		writeString(w, "done")
	})
	in := start(t, "shutdown_grace: 10s\n"+webConfig(poolOf("round_robin", slow), ""))
	client := newClient(t)
	webAddr, adminURL := in.addr(web), in.adminURL("/readyz")
	if status, _ := in.adminGet("/readyz"); status != http.StatusOK {
		t.Fatalf("/readyz before the drain: %d, want 200", status)
	}
	statuses := heldRequests(t, client, in.url("/"), inFlight, &arrived)

	began := time.Now()
	drained := make(chan error, 1)
	go func() { drained <- in.shutdown() }()

	// While draining, readiness goes to 503 so a balancer in front stops
	// sending, and new connections are refused.
	sawDraining := eventually(delay, func() bool {
		res, err := in.admin.Get(adminURL)
		if err != nil {
			return false
		}
		defer res.Body.Close()
		return res.StatusCode == http.StatusServiceUnavailable
	})
	if !sawDraining {
		t.Errorf("/readyz never answered 503 while the server was draining")
	}
	refused := eventually(delay, func() bool {
		conn, err := net.DialTimeout("tcp", webAddr, time.Second)
		if err != nil {
			return true
		}
		closeConn(conn)
		return false
	})
	if !refused {
		t.Errorf("new connections were still accepted while draining")
	}

	for range inFlight {
		if status := <-statuses; status != http.StatusOK {
			t.Errorf("in-flight request ended with status %d (0 = transport error), want 200", status)
		}
	}
	err := <-drained
	took := time.Since(began)
	if err != nil {
		t.Errorf("shutdown: %v", err)
	}
	if took < delay/2 || took > delay+3*time.Second {
		t.Errorf("shutdown took %v, want about the %v the slowest request needed", took, delay)
	}
}

func TestShutdownDeadlineCutsOffStuckRequests(t *testing.T) {
	const grace = 300 * time.Millisecond
	var arrived atomic.Int64
	stuck := newBackend(t, "stuck", func(w http.ResponseWriter, r *http.Request) {
		arrived.Add(1)
		select {
		case <-r.Context().Done():
		case <-time.After(2 * waitTimeout):
		}
	})
	in := start(t, "shutdown_grace: 300ms\n"+webConfig(poolOf("round_robin", stuck), ""))
	client := newClient(t)
	statuses := heldRequests(t, client, in.url("/"), 4, &arrived)

	began := time.Now()
	err := in.shutdown()
	took := time.Since(began)
	if err == nil {
		t.Errorf("shutdown reported a clean drain although requests were cut off")
	}
	if took < grace-50*time.Millisecond || took > grace+2*time.Second {
		t.Errorf("shutdown took %v, want about the grace period of %v", took, grace)
	}
	for range 4 {
		select {
		case status := <-statuses:
			if status == http.StatusOK {
				t.Errorf("a stuck request was answered 200")
			}
		case <-time.After(waitTimeout):
			t.Fatalf("client still waiting %v after the grace period ended", waitTimeout)
		}
	}
}

// Load on every kind of listener, with health checks and the access log
// running, then a shutdown: nothing may be left behind. start's cleanup
// makes the same check after every other test in this package as well.
func TestNoGoroutineLeakAfterLoadAndShutdown(t *testing.T) {
	backends := newBackends(t, 3)
	echo := newTCPBackend(t, echoConn)
	check := "    health_check:\n      type: http\n      path: /healthz\n" + fastCheck
	cfg := "access_log:\n  enabled: true\n" +
		"listeners:\n" +
		"  - name: web\n    addr: 127.0.0.1:0\n    protocol: http\n    routes:\n      - path_prefix: /\n        pool: app\n" +
		"  - name: raw\n    addr: 127.0.0.1:0\n    protocol: tcp\n    pool: tcp\n" +
		"pools:\n" + poolOf("", backends...).with(check).yaml() +
		pool{name: "tcp", addrs: []string{echo.addr}, extra: "    health_check:\n      type: tcp\n" + fastCheck}.yaml()

	before := runtime.NumGoroutine()
	in := start(t, cfg)
	client := newClient(t)
	l := startLoad(client, in.url("/"), 16)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			conn, err := net.DialTimeout("tcp", in.addr(raw), waitTimeout)
			if err != nil {
				return
			}
			defer closeConn(conn)
			for range 50 {
				_, err = io.WriteString(conn, "ping")
				if err != nil {
					return
				}
				_, err = io.ReadFull(conn, make([]byte, 4))
				if err != nil {
					return
				}
			}
		})
	}
	in.mustReload(cfg + "    transport:\n      dial_timeout: 1s\n")
	l.progress(t, 3000)
	wg.Wait()
	l.finish()
	if l.failed.Load() != 0 {
		t.Errorf("%d of %d requests failed (first: %v)", l.failed.Load(), l.total.Load(), l.first.Load())
	}
	requests := l.total.Load()
	client.CloseIdleConnections()

	in.waitIdle()
	err := in.shutdown()
	if err != nil {
		t.Errorf("shutdown: %v", err)
	}
	if !eventually(waitTimeout, func() bool { return runtime.NumGoroutine() <= before }) {
		t.Errorf("goroutines: %d before golb started, %d after load and shutdown", before, runtime.NumGoroutine())
	}

	// The access log is flushed by the shutdown: one JSON line per request.
	lines := strings.Split(strings.TrimSpace(in.access.String()), "\n")
	dropped := in.logs.String()
	if int64(len(lines)) != requests && !strings.Contains(dropped, "dropped") {
		t.Errorf("access log has %d lines for %d requests", len(lines), requests)
	}
	var entry map[string]any
	err = json.Unmarshal([]byte(lines[len(lines)-1]), &entry)
	if err != nil {
		t.Fatalf("access log line is not JSON: %v\n%s", err, lines[len(lines)-1])
	}
	for _, field := range []string{"listener", "route", "method", "path", "status", "backend", "duration_ms"} {
		if _, ok := entry[field]; !ok {
			t.Errorf("access log line lacks %q: %s", field, lines[len(lines)-1])
		}
	}
}

func TestAdminEndpoints(t *testing.T) {
	b := newBackend(t, "b0", nil)
	in := start(t, webConfig(poolOf("round_robin", b).weighted(7), ""))

	tests := []struct {
		name     string
		method   string
		path     string
		want     int
		wantBody string
	}{
		{name: "healthz", method: http.MethodGet, path: "/healthz", want: http.StatusOK, wantBody: "ok\n"},
		{name: "readyz", method: http.MethodGet, path: "/readyz", want: http.StatusOK, wantBody: "ready\n"},
		{name: "reload", method: http.MethodPost, path: "/reload", want: http.StatusOK, wantBody: "reloaded\n"},
		{name: "reload needs POST", method: http.MethodGet, path: "/reload", want: http.StatusMethodNotAllowed},
		{name: "metrics is read-only", method: http.MethodPost, path: "/metrics", want: http.StatusMethodNotAllowed},
		{name: "pprof is off by default", method: http.MethodGet, path: "/debug/pprof/", want: http.StatusNotFound},
		{name: "unknown path", method: http.MethodGet, path: "/nope", want: http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, body := in.adminDo(tc.method, tc.path)
			if status != tc.want {
				t.Errorf("%s %s: status %d, want %d", tc.method, tc.path, status, tc.want)
			}
			if tc.wantBody != "" && body != tc.wantBody {
				t.Errorf("%s %s: body %q, want %q", tc.method, tc.path, body, tc.wantBody)
			}
		})
	}

	rows := in.backends()
	want := backendRow{Pool: "app", Addr: b.addr, Weight: 7, Healthy: true}
	if len(rows) != 1 || rows[0] != want {
		t.Errorf("/backends = %+v, want [%+v]", rows, want)
	}

	// The admin listener is not a proxy and the proxy is not an admin.
	if status, _ := in.adminGet("/"); status != http.StatusNotFound {
		t.Errorf("admin listener answered / with %d, want 404", status)
	}
	if r := get(t, newClient(t), in.url("/metrics")); r.body != "b0" {
		t.Errorf("/metrics on the traffic listener was not proxied to the backend: %q", r.body)
	}
}

func TestPprofOnlyWhenEnabled(t *testing.T) {
	b := newBackend(t, "b0", nil)
	in := start(t, "admin:\n  addr: 127.0.0.1:0\n  pprof: true\n"+webConfig(poolOf("round_robin", b), ""))
	status, body := in.adminGet("/debug/pprof/cmdline")
	if status != http.StatusOK || body == "" {
		t.Errorf("/debug/pprof/cmdline with pprof on: status %d body %q", status, body)
	}
}

func TestMetricsMatchRequestsSent(t *testing.T) {
	const (
		ok       = 37
		notFound = 5
		broken   = 3
	)
	b := newBackend(t, "b0", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/missing":
			w.WriteHeader(http.StatusNotFound)
		case "/broken":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			writeString(w, "ok")
		}
	})
	in := start(t, webConfig(poolOf("round_robin", b).with(outlierOff), ""))
	client := newClient(t)
	for range ok {
		get(t, client, in.url("/"))
	}
	for range notFound {
		get(t, client, in.url("/missing"))
	}
	for range broken {
		get(t, client, in.url("/broken"))
	}

	status, text := in.adminGet("/metrics")
	if status != http.StatusOK {
		t.Fatalf("GET /metrics: %d", status)
	}
	m, err := parseExposition(text)
	if err != nil {
		t.Fatalf("/metrics is not valid Prometheus text: %v\n%s", err, text)
	}

	const total = ok + notFound + broken
	counters := []struct {
		name   string
		labels []string
		want   float64
	}{
		{name: "golb_requests_total", labels: []string{"listener", web, "route", "/", "code_class", "2xx"}, want: ok},
		{name: "golb_requests_total", labels: []string{"listener", web, "route", "/", "code_class", "4xx"}, want: notFound},
		{name: "golb_requests_total", labels: []string{"listener", web, "route", "/", "code_class", "5xx"}, want: broken},
		{name: "golb_requests_total", labels: []string{"listener", web}, want: total},
		{name: "golb_request_duration_seconds_count", labels: []string{"listener", web, "route", "/"}, want: total},
		{name: "golb_upstream_requests_total", labels: []string{"pool", "app", "backend", b.addr, "result", "ok"}, want: ok + notFound},
		{name: "golb_upstream_requests_total", labels: []string{"pool", "app", "backend", b.addr, "result", "error"}, want: broken},
		{name: "golb_upstream_duration_seconds_count", labels: []string{"pool", "app"}, want: total},
		{name: "golb_retries_total", labels: []string{"pool", "app"}, want: 0},
		{name: "golb_backend_healthy", labels: []string{"pool", "app", "backend", b.addr}, want: 1},
		{name: "golb_backend_inflight", labels: []string{"pool", "app", "backend", b.addr}, want: 0},
		{name: "golb_reload_total", labels: []string{"result", "ok"}, want: 0},
		{name: "golb_reload_total", labels: []string{"result", "error"}, want: 0},
		{name: "golb_accesslog_dropped_total", want: 0},
	}
	for _, tc := range counters {
		if !m.has(tc.name, tc.labels...) {
			t.Errorf("%s%v is not exported", tc.name, tc.labels)
			continue
		}
		if got := m.sum(tc.name, tc.labels...); got != tc.want {
			t.Errorf("%s%v = %v, want %v", tc.name, tc.labels, got, tc.want)
		}
	}
	if got := int64(b.hits.Load()); got != total {
		t.Errorf("backend saw %d requests, %d were sent", got, total)
	}

	// Every family the PRD lists (5.5) is declared with a type, including
	// those with no traffic yet.
	families := map[string]string{
		"golb_requests_total":                               "counter",
		"golb_request_duration_seconds":                     "histogram",
		"golb_upstream_requests_total":                      "counter",
		"golb_upstream_duration_seconds":                    "histogram",
		"golb_backend_inflight":                             "gauge",
		"golb_backend_healthy":                              "gauge",
		"golb_backend_ejections_total":                      "counter",
		"golb_retries_total":                                "counter",
		"golb_retry_budget_exhausted_total":                 "counter",
		"golb_ratelimited_total":                            "counter",
		"golb_reload_total":                                 "counter",
		"golb_accesslog_dropped_total":                      "counter",
		"golb_config_last_reload_success_timestamp_seconds": "gauge",
	}
	for name, kind := range families {
		if m.types[name] != kind {
			t.Errorf("metric %s has type %q, want %q", name, m.types[name], kind)
		}
		if m.help[name] == "" {
			t.Errorf("metric %s has no HELP text", name)
		}
	}
	loaded := m.sum("golb_config_last_reload_success_timestamp_seconds")
	if age := float64(time.Now().Unix()) - loaded; age < 0 || age > 60 {
		t.Errorf("golb_config_last_reload_success_timestamp_seconds is %v, %v s from now", loaded, age)
	}
	// Labels come from configuration only, never from the request.
	if strings.Contains(text, "/missing") || strings.Contains(text, "/broken") {
		t.Errorf("request paths appear in metric labels")
	}
}

// PRD B8: no listener without an explicit host, admin on loopback.
func TestUnsafeAddressesAreRejected(t *testing.T) {
	b := newBackend(t, "b0", nil)
	valid := webConfig(poolOf("round_robin", b), "")
	tests := []struct {
		name    string
		cfg     string
		wantErr string
	}{
		{name: "listener without host", cfg: strings.Replace(valid, "addr: "+anyPort, "addr: \":8080\"", 1), wantErr: "has no host"},
		{name: "listener without port", cfg: strings.Replace(valid, "addr: "+anyPort, "addr: 127.0.0.1", 1), wantErr: "not host:port"},
		{name: "admin without host", cfg: "admin:\n  addr: \":9901\"\n" + valid, wantErr: "has no host"},
		{name: "admin on a non-loopback address", cfg: "admin:\n  addr: 192.0.2.1:9901\n" + valid, wantErr: "not loopback"},
		{name: "admin on every interface", cfg: "admin:\n  addr: 0.0.0.0:9901\n" + valid, wantErr: "not loopback"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "golb.yaml")
			cfg := tc.cfg
			if !strings.Contains(cfg, "admin:") {
				cfg = "admin:\n  addr: \"off\"\n" + cfg
			}
			err := os.WriteFile(path, []byte(cfg), 0o600)
			if err != nil {
				t.Fatalf("write config: %v", err)
			}
			srv, err := server.New(context.Background(), server.Options{
				ConfigPath: path,
				Logger:     slog.New(slog.DiscardHandler),
				AccessLog:  io.Discard,
			})
			if err == nil {
				shutdownErr := srv.Shutdown(context.Background())
				t.Fatalf("server started with %s (shutdown: %v)", tc.name, shutdownErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

// PRD B8: a config with only listeners, routes and backends gets safe values.
func TestMinimalConfigHasSafeDefaults(t *testing.T) {
	cfg, err := config.Parse([]byte("listeners:\n  - name: web\n    addr: 127.0.0.1:8080\n    protocol: http\n    routes:\n      - pool: app\n" +
		"pools:\n  - name: app\n    backends:\n      - addr: 127.0.0.1:9001\n"))
	if err != nil {
		t.Fatalf("parse minimal config: %v", err)
	}
	listener, p := cfg.Listeners[0], cfg.Pools[0]
	tests := []struct {
		name string
		got  any
		want any
	}{
		{name: "admin address", got: cfg.Admin.Addr, want: "127.0.0.1:9901"},
		{name: "pprof", got: cfg.Admin.Pprof, want: false},
		{name: "shutdown grace", got: cfg.ShutdownGrace, want: 30 * time.Second},
		{name: "read-header timeout", got: listener.Timeouts.ReadHeader, want: 5 * time.Second},
		{name: "idle timeout", got: listener.Timeouts.Idle, want: 60 * time.Second},
		{name: "request timeout", got: listener.Timeouts.Request, want: 60 * time.Second},
		{name: "max header bytes", got: listener.Timeouts.MaxHeaderBytes, want: 65536},
		{name: "policy", got: string(p.Policy), want: "p2c_ewma"},
		{name: "max attempts", got: p.Retry.MaxAttempts, want: 3},
		{name: "per-try timeout", got: p.Retry.PerTryTimeout, want: 10 * time.Second},
		{name: "retry budget", got: p.Retry.BudgetPercent, want: 20},
		{name: "dial timeout", got: p.Transport.DialTimeout, want: 2 * time.Second},
		{name: "outlier detection on", got: p.Outlier.Disabled, want: false},
	}
	for _, tc := range tests {
		if tc.got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, tc.got, tc.want)
		}
	}
}

var (
	buildOnce sync.Once
	binary    string
	buildErr  error
	buildDir  string
)

// golbBinary builds cmd/golb once per test run.
func golbBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		buildDir, buildErr = os.MkdirTemp("", "golb-e2e-")
		if buildErr != nil {
			return
		}
		binary = filepath.Join(buildDir, "golb")
		out, err := exec.Command("go", "build", "-o", binary, "github.com/maheshpaliwal/golb/cmd/golb").CombinedOutput()
		if err != nil {
			buildErr = fmt.Errorf("go build: %w\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatalf("build golb: %v", buildErr)
	}
	return binary
}

func TestMain(m *testing.M) {
	code := m.Run()
	if buildDir != "" {
		err := os.RemoveAll(buildDir)
		if err != nil {
			fmt.Fprintln(os.Stderr, "remove build directory:", err)
		}
	}
	os.Exit(code)
}

// The real binary: SIGHUP reloads, SIGTERM under load drains and exits 0
// within the grace period (PRD B3).
func TestBinarySignals(t *testing.T) {
	bin := golbBinary(t)
	var arrived atomic.Int64
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			arrived.Add(1)
			time.Sleep(700 * time.Millisecond)
		}
		writeString(w, "ok")
	}
	slow := newBackend(t, "slow", handler)
	extra := newBackend(t, "extra", handler)

	// The process cannot report an ephemeral port, so two free ones are
	// reserved first; nothing else on loopback is likely to take them in
	// the moment before golb binds.
	webAddr, adminAddr := deadAddr(t), deadAddr(t)
	cfg := func(backends ...*backend) string {
		body := webConfig(poolOf("round_robin", backends...), "")
		return "admin:\n  addr: " + adminAddr + "\nshutdown_grace: 10s\n" + strings.Replace(body, "addr: "+anyPort, "addr: "+webAddr, 1)
	}
	path := filepath.Join(t.TempDir(), "golb.yaml")
	write := func(content string) {
		t.Helper()
		err := os.WriteFile(path, []byte(content), 0o600)
		if err != nil {
			t.Fatalf("write config: %v", err)
		}
	}
	write(cfg(slow))

	stderr := new(lockedBuffer)
	cmd := exec.Command(bin, "-config", path)
	cmd.Stderr = stderr
	err := cmd.Start()
	if err != nil {
		t.Fatalf("start golb: %v", err)
	}
	exited := make(chan error, 1)
	var gone atomic.Bool
	go func() {
		err := cmd.Wait()
		gone.Store(true)
		exited <- err
	}()
	t.Cleanup(func() {
		// The process is only still alive here when the test failed.
		if gone.Load() {
			return
		}
		killErr := cmd.Process.Kill()
		t.Logf("killed golb pid %d (err %v)\n%s", cmd.Process.Pid, killErr, tail(stderr.String(), 2000))
	})

	adminHTTP := newAdminClient()
	t.Cleanup(adminHTTP.CloseIdleConnections)
	admin := func(method, p string) (int, string) {
		req, err := http.NewRequest(method, "http://"+adminAddr+p, nil)
		if err != nil {
			return 0, err.Error()
		}
		res, err := adminHTTP.Do(req)
		if err != nil {
			return 0, err.Error()
		}
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return 0, err.Error()
		}
		return res.StatusCode, string(body)
	}
	if !eventually(waitTimeout, func() bool { status, _ := admin(http.MethodGet, "/healthz"); return status == http.StatusOK }) {
		t.Fatalf("golb did not come up\n%s", stderr.String())
	}
	client := newClient(t)
	url := "http://" + webAddr

	// SIGHUP loads the changed file.
	write(cfg(slow, extra))
	err = cmd.Process.Signal(syscall.SIGHUP)
	if err != nil {
		t.Fatalf("send SIGHUP: %v", err)
	}
	reloaded := eventually(waitTimeout, func() bool {
		_, body := admin(http.MethodGet, "/backends")
		return strings.Contains(body, extra.addr)
	})
	if !reloaded {
		t.Fatalf("SIGHUP did not load the new backend\n%s", stderr.String())
	}

	// A SIGHUP with a broken file leaves the process serving.
	write("listeners: [\n")
	err = cmd.Process.Signal(syscall.SIGHUP)
	if err != nil {
		t.Fatalf("send SIGHUP: %v", err)
	}
	rejected := eventually(waitTimeout, func() bool {
		_, body := admin(http.MethodGet, "/metrics")
		return strings.Contains(body, `golb_reload_total{result="error"} 1`)
	})
	if !rejected {
		t.Errorf("rejected reload was not counted")
	}
	if r := get(t, client, url+"/"); r.status != http.StatusOK {
		t.Errorf("after a rejected SIGHUP reload: status %d", r.status)
	}

	// SIGTERM with slow requests in flight.
	const inFlight = 8
	statuses := heldRequests(t, client, url+"/slow", inFlight, &arrived)
	began := time.Now()
	err = cmd.Process.Signal(syscall.SIGTERM)
	if err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}
	for range inFlight {
		if status := <-statuses; status != http.StatusOK {
			t.Errorf("request in flight at SIGTERM ended with status %d (0 = transport error), want 200", status)
		}
	}
	select {
	case err = <-exited:
	case <-time.After(10 * time.Second):
		t.Fatalf("golb still running 10 s after SIGTERM\n%s", stderr.String())
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		t.Errorf("exit code %d after SIGTERM, want 0\n%s", exitErr.ExitCode(), tail(stderr.String(), 2000))
	} else if err != nil {
		t.Errorf("wait: %v", err)
	}
	if took := time.Since(began); took > 5*time.Second {
		t.Errorf("exit took %v after SIGTERM; the slowest request needed 700 ms", took)
	}
	_, err = net.DialTimeout("tcp", webAddr, time.Second)
	if err == nil {
		t.Errorf("listener still accepts connections after exit")
	}
	if !strings.Contains(stderr.String(), "golb stopped") {
		t.Errorf("no clean-stop line in the process log\n%s", tail(stderr.String(), 2000))
	}
}

func TestBinaryCheckFlag(t *testing.T) {
	bin := golbBinary(t)
	dir := t.TempDir()
	valid := filepath.Join(dir, "valid.yaml")
	invalid := filepath.Join(dir, "invalid.yaml")
	body := "listeners:\n  - name: web\n    addr: %s\n    protocol: http\n    routes:\n      - pool: app\npools:\n  - name: app\n    backends:\n      - addr: 127.0.0.1:9001\n"
	for path, addr := range map[string]string{valid: "127.0.0.1:8080", invalid: `":8080"`} {
		err := os.WriteFile(path, fmt.Appendf(nil, body, addr), 0o600)
		if err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	tests := []struct {
		name     string
		path     string
		wantCode int
		wantOut  string
	}{
		{name: "valid file", path: valid, wantCode: 0, wantOut: "ok"},
		{name: "listener without host", path: invalid, wantCode: 1, wantOut: "has no host"},
		{name: "example config", path: filepath.Join("..", "..", "configs", "golb.example.yaml"), wantCode: 0, wantOut: "ok"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, err := exec.Command(bin, "-check", "-config", tc.path).CombinedOutput()
			code := 0
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				code = exitErr.ExitCode()
			} else if err != nil {
				t.Fatalf("run golb -check: %v", err)
			}
			if code != tc.wantCode || !strings.Contains(string(out), tc.wantOut) {
				t.Errorf("exit code %d, output %q; want code %d and output containing %q", code, out, tc.wantCode, tc.wantOut)
			}
		})
	}
}

// An upgraded connection is not a request http.Server.Shutdown knows about.
// Like a TCP connection (PRD 4.5) it should get the grace period and then
// be closed, so that Shutdown leaves nothing behind.
func TestShutdownClosesUpgradedConnections(t *testing.T) {
	knownBug(t, "BUG: Shutdown returns at once with a WebSocket connection open and never closes it: the connection keeps "+
		"working after Shutdown returned, its goroutines and the backend's in-flight count are left behind "+
		"(in the binary the connection is cut without notice when the process exits, with no grace period)")
	ws := newBackend(t, "ws", func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("backend: hijack: %v", err)
			return
		}
		defer closeConn(conn)
		_, err = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		if err == nil {
			err = rw.Flush()
		}
		if err != nil {
			return
		}
		// Raw echo is enough here; framing is covered by TestWebSocketEcho.
		_, _ = io.Copy(conn, rw)
	})
	in := start(t, "shutdown_grace: 300ms\n"+webConfig(poolOf("round_robin", ws), ""))

	conn, err := net.DialTimeout("tcp", in.addr(web), waitTimeout)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer closeConn(conn)
	_, err = io.WriteString(conn, "GET /chat HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
	if err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	reader := bufio.NewReader(conn)
	res, err := http.ReadResponse(reader, nil)
	if err != nil || res.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake: %v (response %v)", err, res)
	}

	began := time.Now()
	err = in.shutdown()
	t.Logf("shutdown returned %v after %v", err, time.Since(began))

	// After Shutdown has returned, the connection must be gone.
	err = conn.SetDeadline(time.Now().Add(time.Second))
	if err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	_, err = io.WriteString(conn, "still here?")
	if err == nil {
		echoed := make([]byte, len("still here?"))
		_, err = io.ReadFull(reader, echoed)
		if err == nil {
			t.Errorf("WebSocket connection still echoes %q after Shutdown returned", echoed)
		}
	}
}
