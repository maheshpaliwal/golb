package server

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/golb/internal/admin"
)

const waitLimit = 5 * time.Second

// settings fills the config template; zero values give a plain HTTP
// listener, a TCP listener and the admin listener, all on 127.0.0.1 port 0.
type settings struct {
	backends []string
	listener string // extra YAML under the HTTP listener, indented four spaces
	pool     string // extra YAML under the pool, indented four spaces
	top      string // extra top-level YAML
}

func (s settings) yaml() string {
	var doc strings.Builder
	doc.WriteString("admin:\n  addr: 127.0.0.1:0\n")
	doc.WriteString(s.top)
	doc.WriteString("listeners:\n  - name: web\n    addr: 127.0.0.1:0\n    protocol: http\n")
	doc.WriteString(s.listener)
	doc.WriteString("    routes:\n      - pool: app\n")
	doc.WriteString("  - name: raw\n    addr: 127.0.0.1:0\n    protocol: tcp\n    pool: app\n")
	doc.WriteString("pools:\n  - name: app\n    backends:\n")
	for _, addr := range s.backends {
		fmt.Fprintf(&doc, "      - addr: %s\n", addr)
	}
	doc.WriteString(s.pool)
	return doc.String()
}

type instance struct {
	*Server
	path string
}

func writeConfig(t *testing.T, path string, s settings) {
	t.Helper()
	// Write-then-rename, as deploy tools do, so a poll never sees half a file.
	tmp := path + ".tmp"
	err := os.WriteFile(tmp, []byte(s.yaml()), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}
	err = os.Rename(tmp, path)
	if err != nil {
		t.Fatalf("replace config: %v", err)
	}
}

func start(t *testing.T, s settings, watch time.Duration) *instance {
	t.Helper()
	path := filepath.Join(t.TempDir(), "golb.yaml")
	writeConfig(t, path, s)
	srv, err := New(context.Background(), Options{
		ConfigPath:    path,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		AccessLog:     io.Discard,
		WatchInterval: watch,
	})
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	srv.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		srv.Shutdown(ctx)
	})
	return &instance{Server: srv, path: path}
}

func (in *instance) url(listener string) string {
	return "http://" + in.Addr(listener).String()
}

func (in *instance) metric(t *testing.T, series string) string {
	t.Helper()
	_, body := fetch(t, in.url("admin")+"/metrics")
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, series+" ") {
			return strings.TrimPrefix(line, series+" ")
		}
	}
	return ""
}

func named(t *testing.T, name string) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, name)
	}))
	t.Cleanup(server.Close)
	return server.Listener.Addr().String()
}

func fetch(t *testing.T, target string) (int, string) {
	t.Helper()
	client := &http.Client{Timeout: waitLimit}
	defer client.CloseIdleConnections()
	res, err := client.Get(target)
	if err != nil {
		t.Fatalf("GET %s: %v", target, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read %s: %v", target, err)
	}
	return res.StatusCode, string(body)
}

// eventually polls check until it passes or the deadline is reached.
func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitLimit)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestServesTrafficAndAdmin(t *testing.T) {
	in := start(t, settings{backends: []string{named(t, "alpha")}}, -1)

	status, body := fetch(t, in.url("web")+"/hello")
	if status != http.StatusOK || body != "alpha" {
		t.Fatalf("proxied request = %d %q, want 200 alpha", status, body)
	}

	if got := in.metric(t, `golb_requests_total{listener="web",route="/",code_class="2xx"}`); got != "1" {
		t.Errorf("golb_requests_total 2xx = %q, want 1", got)
	}
	_, exposition := fetch(t, in.url("admin")+"/metrics")
	for _, family := range []string{
		"golb_request_duration_seconds_bucket", "golb_upstream_requests_total", "golb_upstream_duration_seconds_count",
		"golb_backend_inflight", "golb_backend_healthy", "golb_backend_ejections_total", "golb_retries_total",
		"golb_retry_budget_exhausted_total", "golb_tcp_connections_total", "golb_tcp_bytes_total",
		"golb_reload_total", "golb_accesslog_dropped_total", "golb_config_last_reload_success_timestamp_seconds",
	} {
		if !strings.Contains(exposition, family) {
			t.Errorf("/metrics lacks %s", family)
		}
	}

	status, body = fetch(t, in.url("admin")+"/backends")
	var rows []admin.Backend
	err := json.Unmarshal([]byte(body), &rows)
	if err != nil || status != http.StatusOK {
		t.Fatalf("/backends = %d, %v: %s", status, err, body)
	}
	if len(rows) != 1 || rows[0].Pool != "app" || !rows[0].Healthy || rows[0].Weight != 1 {
		t.Errorf("/backends = %+v, want one healthy backend of pool app", rows)
	}

	if status, _ = fetch(t, in.url("admin")+"/readyz"); status != http.StatusOK {
		t.Errorf("/readyz = %d, want 200", status)
	}
	if got := in.Addr("admin").(*net.TCPAddr); !got.IP.IsLoopback() {
		t.Errorf("admin bound %v, want loopback", got)
	}
}

func TestTCPListener(t *testing.T) {
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
				io.Copy(conn, conn)
			}()
		}
	}()
	in := start(t, settings{backends: []string{listener.Addr().String()}}, -1)

	conn, err := net.Dial("tcp", in.Addr("raw").String())
	if err != nil {
		t.Fatalf("dial tcp listener: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(waitLimit))
	io.WriteString(conn, "through the tcp listener")
	got := make([]byte, len("through the tcp listener"))
	_, err = io.ReadFull(conn, got)
	if err != nil || string(got) != "through the tcp listener" {
		t.Errorf("echo = %q, %v", got, err)
	}
}

func TestReload(t *testing.T) {
	alpha, beta := named(t, "alpha"), named(t, "beta")
	in := start(t, settings{backends: []string{alpha}}, -1)
	ctx := context.Background()

	steps := []struct {
		name      string
		change    func()
		wantErr   string // substring; empty means the reload succeeds
		wantBody  string
		wantOK    string
		wantError string
	}{
		{
			name:     "valid change switches backends",
			change:   func() { writeConfig(t, in.path, settings{backends: []string{beta}}) },
			wantBody: "beta", wantOK: "1", wantError: "0",
		},
		{
			name: "invalid file keeps the running configuration",
			change: func() {
				writeConfig(t, in.path, settings{backends: []string{alpha}, top: "no_such_key: true\n"})
			},
			wantErr: "no_such_key", wantBody: "beta", wantOK: "1", wantError: "1",
		},
		{
			name: "validation error keeps the running configuration",
			change: func() {
				writeConfig(t, in.path, settings{backends: []string{"not-an-address"}})
			},
			wantErr: "not host:port", wantBody: "beta", wantOK: "1", wantError: "2",
		},
		{
			name: "listener changes are refused",
			change: func() {
				writeConfig(t, in.path, settings{backends: []string{alpha}, listener: "    timeouts:\n      read_header: 1s\n"})
			},
			wantErr: "cannot change on reload", wantBody: "beta", wantOK: "1", wantError: "3",
		},
		{
			name: "unreadable file keeps the running configuration",
			change: func() {
				err := os.Remove(in.path)
				if err != nil {
					t.Fatalf("remove config: %v", err)
				}
			},
			wantErr: "read config", wantBody: "beta", wantOK: "1", wantError: "4",
		},
		{
			name:     "a later valid file is accepted",
			change:   func() { writeConfig(t, in.path, settings{backends: []string{alpha}}) },
			wantBody: "alpha", wantOK: "2", wantError: "4",
		},
	}
	for _, step := range steps {
		step.change()
		err := in.Reload(ctx)
		if step.wantErr == "" && err != nil {
			t.Fatalf("%s: Reload: %v", step.name, err)
		}
		if step.wantErr != "" && (err == nil || !strings.Contains(err.Error(), step.wantErr)) {
			t.Fatalf("%s: Reload = %v, want an error mentioning %q", step.name, err, step.wantErr)
		}
		status, body := fetch(t, in.url("web"))
		if status != http.StatusOK || body != step.wantBody {
			t.Errorf("%s: request = %d %q, want 200 %q", step.name, status, body, step.wantBody)
		}
		if got := in.metric(t, `golb_reload_total{result="ok"}`); got != step.wantOK {
			t.Errorf("%s: reloads ok = %q, want %s", step.name, got, step.wantOK)
		}
		if got := in.metric(t, `golb_reload_total{result="error"}`); got != step.wantError {
			t.Errorf("%s: reloads error = %q, want %s", step.name, got, step.wantError)
		}
	}
}

func TestReloadUnderLoadDropsNothing(t *testing.T) {
	const (
		workers = 16
		reloads = 20
	)
	alpha, beta := named(t, "alpha"), named(t, "beta")
	in := start(t, settings{backends: []string{alpha}}, -1)
	target := in.url("web")

	stop := make(chan struct{})
	var served, failed atomic.Int64
	var seen sync.Map
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: waitLimit}
			defer client.CloseIdleConnections()
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := client.Get(target)
				if err != nil {
					failed.Add(1)
					continue
				}
				body, err := io.ReadAll(res.Body)
				res.Body.Close()
				if err != nil || res.StatusCode != http.StatusOK {
					failed.Add(1)
					continue
				}
				seen.Store(string(body), true)
				served.Add(1)
			}
		}()
	}

	for i := range reloads {
		next := settings{backends: []string{beta}}
		if i%2 == 1 {
			next = settings{backends: []string{alpha}, pool: "    transport:\n      dial_timeout: 1s\n"}
		}
		writeConfig(t, in.path, next)
		err := in.Reload(context.Background())
		if err != nil {
			t.Errorf("reload %d: %v", i, err)
		}
		before := served.Load()
		eventually(t, "traffic after a reload", func() bool { return served.Load() > before+workers })
	}
	close(stop)
	wg.Wait()

	if failed.Load() != 0 {
		t.Errorf("%d requests failed across %d reloads (%d served), want 0", failed.Load(), reloads, served.Load())
	}
	for _, name := range []string{"alpha", "beta"} {
		if _, ok := seen.Load(name); !ok {
			t.Errorf("no response from %s: the reloads did not take effect", name)
		}
	}
	eventually(t, "in-flight to return to 0", func() bool {
		for _, row := range in.Backends() {
			if row.Inflight != 0 {
				return false
			}
		}
		return true
	})
}

func TestFileWatcherReloads(t *testing.T) {
	alpha, beta := named(t, "alpha"), named(t, "beta")
	in := start(t, settings{backends: []string{alpha}}, 10*time.Millisecond)

	writeConfig(t, in.path, settings{backends: []string{beta}})
	eventually(t, "the watcher to load the new file", func() bool {
		_, body := fetch(t, in.url("web"))
		return body == "beta"
	})

	// A broken file is tried once and counted, and traffic is unaffected.
	writeConfig(t, in.path, settings{backends: []string{alpha}, top: "broken: [\n"})
	eventually(t, "the rejected reload to be counted", func() bool {
		return in.metric(t, `golb_reload_total{result="error"}`) == "1"
	})
	if _, body := fetch(t, in.url("web")); body != "beta" {
		t.Errorf("after a broken file the proxy answers from %q, want beta", body)
	}
	if got := in.metric(t, `golb_reload_total{result="ok"}`); got != "1" {
		t.Errorf("reloads ok = %q, want 1", got)
	}
}

// A file replaced between start-up's read and the watcher's first poll is
// still loaded: the watcher compares with what was loaded, not with what it
// first finds.
func TestFileChangedBeforeWatcherStartsIsLoaded(t *testing.T) {
	alpha, beta := named(t, "alpha"), named(t, "beta")
	path := filepath.Join(t.TempDir(), "golb.yaml")
	writeConfig(t, path, settings{backends: []string{alpha}})
	srv, err := New(context.Background(), Options{
		ConfigPath:    path,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		AccessLog:     io.Discard,
		WatchInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { srv.Shutdown(context.Background()) })
	writeConfig(t, path, settings{backends: []string{beta}})
	srv.Start()

	in := &instance{Server: srv, path: path}
	eventually(t, "the file written during start-up to be loaded", func() bool {
		_, body := fetch(t, in.url("web"))
		return body == "beta"
	})
}

func TestReloadRemovesSeriesOfRemovedBackends(t *testing.T) {
	alpha, beta := named(t, "alpha"), named(t, "beta")
	in := start(t, settings{backends: []string{alpha, beta}}, -1)
	for range 4 {
		fetch(t, in.url("web"))
	}
	families := []string{"golb_upstream_requests_total", "golb_backend_inflight", "golb_backend_healthy", "golb_backend_ejections_total"}
	series := func(addr string) []string {
		_, exposition := fetch(t, in.url("admin")+"/metrics")
		var found []string
		for _, line := range strings.Split(exposition, "\n") {
			for _, family := range families {
				if strings.HasPrefix(line, family+"{") && strings.Contains(line, `backend="`+addr+`"`) {
					found = append(found, line)
				}
			}
		}
		return found
	}
	if got := series(beta); len(got) != 5 {
		t.Fatalf("before the reload %s has %d series, want 5: %q", beta, len(got), got)
	}

	writeConfig(t, in.path, settings{backends: []string{alpha}})
	err := in.Reload(context.Background())
	if err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if got := series(beta); len(got) != 0 {
		t.Errorf("removed backend still exported: %q", got)
	}
	if got := series(alpha); len(got) != 5 {
		t.Errorf("kept backend has %d series, want 5: %q", len(got), got)
	}
}

func TestShutdownClosesUpgradedConnectionsAfterGrace(t *testing.T) {
	echo := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		io.WriteString(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		rw.Flush()
		io.Copy(conn, rw)
	}))
	t.Cleanup(echo.Close)
	in := start(t, settings{backends: []string{echo.Listener.Addr().String()}, top: "shutdown_grace: 300ms\n"}, -1)

	conn, err := net.Dial("tcp", in.Addr("web").String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(waitLimit))
	io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	reader := bufio.NewReader(conn)
	res, err := http.ReadResponse(reader, nil)
	if err != nil || res.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake: %v, %v", res, err)
	}

	began := time.Now()
	stopped := make(chan error, 1)
	go func() { stopped <- in.Shutdown(context.Background()) }()
	// During the grace period the connection still works.
	io.WriteString(conn, "m")
	_, err = io.ReadFull(reader, make([]byte, 1))
	if err != nil {
		t.Errorf("echo during the grace period: %v", err)
	}
	select {
	case err = <-stopped:
	case <-time.After(waitLimit):
		t.Fatal("Shutdown did not return")
	}
	if err == nil {
		t.Error("Shutdown reported a clean drain with a connection it had to close")
	}
	if waited := time.Since(began); waited < 250*time.Millisecond {
		t.Errorf("Shutdown returned after %v, before the 300ms grace period", waited)
	}
	_, err = reader.ReadByte()
	if err == nil {
		t.Error("upgraded connection still open after Shutdown returned")
	}
	eventually(t, "the backend's in-flight count to return to 0", func() bool { return in.Backends()[0].Inflight == 0 })
}

func TestShutdownDrainsInFlightRequests(t *testing.T) {
	tests := []struct {
		name       string
		grace      string
		release    bool
		wantStatus int
		wantErr    bool
	}{
		{name: "in-flight request completes within the grace period", grace: "5s", release: true, wantStatus: http.StatusOK},
		{name: "grace period ends with the request still running", grace: "100ms", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			arrived := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(arrived)
				<-release
				io.WriteString(w, "finished")
			}))
			t.Cleanup(slow.Close)
			in := start(t, settings{
				backends: []string{slow.Listener.Addr().String()},
				top:      "shutdown_grace: " + tc.grace + "\n",
				pool:     "    retry:\n      max_attempts: 1\n",
			}, -1)
			web := in.Addr("web").String()

			type result struct {
				status int
				body   string
				err    error
			}
			inflight := make(chan result, 1)
			go func() {
				client := &http.Client{Timeout: waitLimit}
				defer client.CloseIdleConnections()
				res, err := client.Get("http://" + web)
				if err != nil {
					inflight <- result{err: err}
					return
				}
				defer res.Body.Close()
				body, err := io.ReadAll(res.Body)
				inflight <- result{status: res.StatusCode, body: string(body), err: err}
			}()
			<-arrived

			stopped := make(chan error, 1)
			go func() { stopped <- in.Shutdown(context.Background()) }()
			eventually(t, "the listener to stop accepting", func() bool {
				conn, err := net.DialTimeout("tcp", web, time.Second)
				if err != nil {
					return true
				}
				conn.Close()
				return false
			})
			if in.Ready() {
				t.Error("Ready() is true while draining")
			}
			if err := in.Reload(context.Background()); err == nil {
				t.Error("Reload succeeded while draining")
			}

			if tc.release {
				unblock()
			}
			var err error
			select {
			case err = <-stopped:
			case <-time.After(waitLimit):
				t.Fatal("Shutdown did not return")
			}
			if (err != nil) != tc.wantErr {
				t.Errorf("Shutdown = %v, want error %v", err, tc.wantErr)
			}
			unblock()
			got := <-inflight
			if tc.release && (got.err != nil || got.status != tc.wantStatus || got.body != "finished") {
				t.Errorf("in-flight request = %+v, want 200 finished", got)
			}
			if !tc.release && got.err == nil && got.status == http.StatusOK {
				t.Errorf("request cut off at the deadline still got %+v", got)
			}
		})
	}
}

func TestSlowHeadersAreCutOff(t *testing.T) {
	in := start(t, settings{
		backends: []string{named(t, "alpha")},
		listener: "    timeouts:\n      read_header: 100ms\n",
	}, -1)
	conn, err := net.Dial("tcp", in.Addr("web").String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(waitLimit))
	io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\nX-Slow: ")

	began := time.Now()
	reply, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("connection was not closed by the proxy: %v", err)
	}
	if waited := time.Since(began); waited > 2*time.Second {
		t.Errorf("slow request held the connection for %v, want about 100ms", waited)
	}
	if strings.Contains(string(reply), "200 OK") {
		t.Errorf("incomplete request was answered: %q", reply)
	}
}

func TestOversizedHeadersAreRejected(t *testing.T) {
	var reached atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Add(1) }))
	t.Cleanup(backend.Close)
	in := start(t, settings{
		backends: []string{backend.Listener.Addr().String()},
		listener: "    timeouts:\n      max_header_bytes: 1024\n",
	}, -1)
	conn, err := net.Dial("tcp", in.Addr("web").String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(waitLimit))
	io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\nX-Big: "+strings.Repeat("a", 16<<10)+"\r\n\r\n")
	res, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Errorf("status = %d, want 431", res.StatusCode)
	}
	if reached.Load() != 0 {
		t.Error("oversized request reached the backend")
	}
}

func TestTLSTermination(t *testing.T) {
	dir := t.TempDir()
	roots := x509.NewCertPool()
	one := writeCertificate(t, dir, "one", "one.golb.test", roots)
	two := writeCertificate(t, dir, "two", "two.golb.test", roots)
	seen := make(chan string, 8)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Get("X-Forwarded-Proto")
	}))
	t.Cleanup(backend.Close)
	tlsYAML := func(pairs ...[2]string) string {
		var doc strings.Builder
		doc.WriteString("    tls:\n      certificates:\n")
		for _, pair := range pairs {
			fmt.Fprintf(&doc, "        - cert_file: %s\n          key_file: %s\n", pair[0], pair[1])
		}
		return doc.String()
	}
	base := settings{backends: []string{backend.Listener.Addr().String()}}
	withTLS := base
	withTLS.listener = tlsYAML(one, two)
	in := start(t, withTLS, 10*time.Millisecond)
	addr := in.Addr("web").String()

	// handshake returns the DNS name of the certificate the server presented.
	handshake := func(serverName string, maxVersion uint16) (string, *tls.ConnectionState, error) {
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: waitLimit}, "tcp", addr, &tls.Config{
			ServerName: serverName,
			RootCAs:    roots,
			MaxVersion: maxVersion,
			NextProtos: []string{"h2", "http/1.1"},
		})
		if err != nil {
			return "", nil, err
		}
		defer conn.Close()
		state := conn.ConnectionState()
		return state.PeerCertificates[0].DNSNames[0], &state, nil
	}

	for _, name := range []string{"one.golb.test", "two.golb.test"} {
		got, state, err := handshake(name, 0)
		if err != nil {
			t.Fatalf("handshake for %s: %v", name, err)
		}
		if got != name {
			t.Errorf("SNI %s was served the certificate for %s", name, got)
		}
		if state.NegotiatedProtocol != "h2" {
			t.Errorf("ALPN negotiated %q, want h2", state.NegotiatedProtocol)
		}
	}
	if _, _, err := handshake("one.golb.test", tls.VersionTLS11); err == nil {
		t.Error("TLS 1.1 handshake succeeded, want it refused")
	}

	client := &http.Client{
		Timeout: waitLimit,
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{ServerName: "one.golb.test", RootCAs: roots},
			ForceAttemptHTTP2: true,
		},
	}
	defer client.CloseIdleConnections()
	res, err := client.Get("https://" + addr + "/")
	if err != nil {
		t.Fatalf("HTTPS request: %v", err)
	}
	res.Body.Close()
	if res.ProtoMajor != 2 {
		t.Errorf("client spoke HTTP/%d over TLS, want 2", res.ProtoMajor)
	}
	if proto := <-seen; proto != "https" {
		t.Errorf("backend saw X-Forwarded-Proto %q, want https", proto)
	}

	// A certificate replaced on disk is picked up without touching the config.
	rotated := writeCertificate(t, dir, "one", "rotated.golb.test", roots)
	if rotated != one {
		t.Fatalf("rotation wrote %v, want to overwrite %v", rotated, one)
	}
	eventually(t, "the rotated certificate to be served", func() bool {
		got, _, err := handshake("rotated.golb.test", 0)
		return err == nil && got == "rotated.golb.test"
	})

	// Certificates are re-read on reload: swap the files' order and require 1.3.
	reloaded := base
	reloaded.listener = tlsYAML(two) + "      min_version: \"1.3\"\n"
	writeConfig(t, in.path, reloaded)
	err = in.Reload(context.Background())
	if err != nil {
		t.Fatalf("Reload: %v", err)
	}
	got, _, err := handshake("one.golb.test", 0)
	if err == nil {
		t.Errorf("after the reload removed its certificate, one.golb.test still verified against %s", got)
	}
	if _, _, err = handshake("two.golb.test", tls.VersionTLS12); err == nil {
		t.Error("TLS 1.2 handshake succeeded after min_version was raised to 1.3")
	}
	if _, _, err = handshake("two.golb.test", 0); err != nil {
		t.Errorf("handshake after reload: %v", err)
	}
}

func TestNewRejectsBadStart(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { taken.Close() })
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "invalid config", yaml: "listeners: []\n", want: "no listeners"},
		{
			name: "address in use",
			yaml: strings.Replace(settings{backends: []string{"127.0.0.1:1"}}.yaml(), "name: web\n    addr: 127.0.0.1:0", "name: web\n    addr: "+taken.Addr().String(), 1),
			want: `listener "web": listen`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "golb.yaml")
			err := os.WriteFile(path, []byte(tc.yaml), 0o600)
			if err != nil {
				t.Fatalf("write config: %v", err)
			}
			srv, err := New(context.Background(), Options{
				ConfigPath: path,
				Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
				AccessLog:  io.Discard,
			})
			if err == nil {
				srv.Shutdown(context.Background())
				t.Fatal("New succeeded")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// writeCertificate creates a throwaway self-signed certificate for host in
// dir, adds it to roots and returns the certificate and key paths.
func writeCertificate(t *testing.T, dir, name, host string, roots *x509.CertPool) [2]string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: host},
		DNSNames:              []string{host},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	roots.AddCert(cert)
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	paths := [2]string{filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")}
	err = os.WriteFile(paths[0], pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	if err != nil {
		t.Fatalf("write certificate: %v", err)
	}
	err = os.WriteFile(paths[1], pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600)
	if err != nil {
		t.Fatalf("write key: %v", err)
	}
	return paths
}
