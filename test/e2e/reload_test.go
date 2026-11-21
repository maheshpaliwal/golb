package e2e

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/golb/internal/server"
)

// load keeps a fixed number of workers sending GET requests until stopped
// and counts every request that did not come back 200.
type load struct {
	total  atomic.Int64
	failed atomic.Int64
	first  atomic.Value // first failure, as a string
	stop   chan struct{}
	wg     sync.WaitGroup
}

func startLoad(client *http.Client, url string, workers int) *load {
	l := &load{stop: make(chan struct{})}
	for range workers {
		l.wg.Go(func() {
			for {
				select {
				case <-l.stop:
					return
				default:
				}
				r, err := trySend(client, http.MethodGet, url, "")
				l.total.Add(1)
				switch {
				case err != nil:
					l.failed.Add(1)
					l.first.CompareAndSwap(nil, err.Error())
				case r.status != http.StatusOK:
					l.failed.Add(1)
					l.first.CompareAndSwap(nil, "status "+http.StatusText(r.status))
				}
			}
		})
	}
	return l
}

func (l *load) finish() {
	close(l.stop)
	l.wg.Wait()
}

// progress waits until n more requests have completed.
func (l *load) progress(t *testing.T, n int64) {
	t.Helper()
	from := l.total.Load()
	if !eventually(2*waitTimeout, func() bool { return l.total.Load() >= from+n }) {
		t.Fatalf("load stalled: %d requests in %v, want %d (first failure: %v)", l.total.Load()-from, 2*waitTimeout, n, l.first.Load())
	}
}

// PRD B2: reloads under sustained load drop nothing.
func TestHotReloadUnderLoadDropsNothing(t *testing.T) {
	const (
		workers = 16
		settle  = 400 // requests between steps
	)
	backends := newBackends(t, 4)
	b0, b1, b2, b3 := backends[0], backends[1], backends[2], backends[3]
	initial := webConfig(poolOf("round_robin", b0, b1, b2), "")
	in := start(t, initial)
	client := newClient(t)
	l := startLoad(client, in.url("/"), workers)
	defer func() {
		select {
		case <-l.stop:
		default:
			l.finish()
		}
	}()
	l.progress(t, settle)

	// share is the fraction of the next 1000 requests each backend serves.
	share := func() []float64 {
		t.Helper()
		before := make([]int64, len(backends))
		for i, b := range backends {
			before[i] = b.hits.Load()
		}
		l.progress(t, 1000)
		shares := make([]float64, len(backends))
		var sum int64
		for i, b := range backends {
			before[i] = b.hits.Load() - before[i]
			sum += before[i]
		}
		for i := range shares {
			shares[i] = float64(before[i]) / float64(sum)
		}
		return shares
	}
	near := func(got, want float64) bool { return got > want-0.06 && got < want+0.06 }

	t.Log("step 1: change weights to 8:1:1")
	in.mustReload(webConfig(poolOf("round_robin", b0, b1, b2).weighted(8, 1, 1), ""))
	l.progress(t, settle)
	if s := share(); !near(s[0], 0.8) || !near(s[1], 0.1) || !near(s[2], 0.1) || s[3] != 0 {
		t.Errorf("shares after reweighting to 8:1:1: %.2f", s)
	}
	if got := in.backend(b0.addr).Weight; got != 8 {
		t.Errorf("/backends weight of the first backend: %d, want 8", got)
	}

	t.Log("step 2: add a backend")
	in.mustReload(webConfig(poolOf("round_robin", b0, b1, b2, b3), ""))
	l.progress(t, settle)
	if s := share(); !near(s[0], 0.25) || !near(s[1], 0.25) || !near(s[2], 0.25) || !near(s[3], 0.25) {
		t.Errorf("shares after adding a fourth backend: %.2f", s)
	}

	t.Log("step 3: remove a backend")
	in.mustReload(webConfig(poolOf("round_robin", b1, b2, b3), ""))
	l.progress(t, settle)
	if s := share(); s[0] != 0 || !near(s[1], 1.0/3) || !near(s[2], 1.0/3) || !near(s[3], 1.0/3) {
		t.Errorf("shares after removing the first backend: %.2f", s)
	}
	if len(in.backends()) != 3 {
		t.Errorf("/backends lists %d backends after the removal, want 3", len(in.backends()))
	}

	t.Log("step 4: 20 reloads alternating two configs")
	alternate := webConfig(poolOf("least_conn", b0, b2).with("    retry:\n      max_attempts: 2\n"), "")
	for i := range 20 {
		next := initial
		if i%2 == 0 {
			next = alternate
		}
		in.mustReload(next)
		l.progress(t, 100)
	}

	t.Log("step 5: invalid configs are rejected and the old one keeps serving")
	okBefore := in.metrics().sum("golb_reload_total", "result", "ok")
	running := in.backends()
	invalid := []struct {
		name string
		cfg  string
	}{
		{name: "not YAML", cfg: "listeners: [\n"},
		{name: "unknown key", cfg: initial + "    surprise: true\n"},
		{name: "no backends", cfg: webConfig(pool{policy: "round_robin"}, "")},
		{name: "listener without host", cfg: strings.Replace(initial, "addr: "+anyPort, "addr: \":0\"", 1)},
		{name: "listener added", cfg: "listeners:\n  - name: extra\n    addr: 127.0.0.1:0\n    protocol: http\n    routes:\n      - path_prefix: /\n        pool: app\n" +
			strings.TrimPrefix(initial, "listeners:\n")},
		{name: "listener renamed", cfg: strings.Replace(initial, "name: web", "name: other", 1)},
		{name: "unknown pool in route", cfg: strings.Replace(initial, "pool: app\n", "pool: missing\n", 1)},
	}
	for i, tc := range invalid {
		status, body := in.reload(tc.cfg)
		if status != http.StatusInternalServerError {
			t.Errorf("reload with %s: status %d, want 500", tc.name, status)
		}
		if strings.Contains(body, "surprise") || strings.Contains(body, "127.0.0.1") {
			t.Errorf("reload with %s: the response quotes the file: %q", tc.name, body)
		}
		if got := in.metrics().sum("golb_reload_total", "result", "error"); got != float64(i+1) {
			t.Errorf("after %s: golb_reload_total{result=error} = %v, want %d", tc.name, got, i+1)
		}
		l.progress(t, 100)
	}
	if got := in.metrics().sum("golb_reload_total", "result", "ok"); got != okBefore {
		t.Errorf("golb_reload_total{result=ok} moved from %v to %v across rejected reloads", okBefore, got)
	}
	after := in.backends()
	if len(after) != len(running) {
		t.Fatalf("/backends lists %d backends after the rejected reloads, %d before", len(after), len(running))
	}
	for i := range after {
		if after[i].Addr != running[i].Addr || after[i].Weight != running[i].Weight {
			t.Errorf("backend %d changed across rejected reloads: %+v -> %+v", i, running[i], after[i])
		}
	}
	if s := share(); !near(s[0], 1.0/3) || !near(s[1], 1.0/3) || !near(s[2], 1.0/3) || s[3] != 0 {
		t.Errorf("shares after the rejected reloads: %.2f, want the last valid config (three equal backends)", s)
	}

	l.finish()
	t.Logf("%d requests across the reloads, %d failed", l.total.Load(), l.failed.Load())
	if l.failed.Load() != 0 {
		t.Errorf("%d of %d requests failed during reloads, want 0 (first: %v)", l.failed.Load(), l.total.Load(), l.first.Load())
	}
}

func TestFileChangeIsPickedUpByTheWatcher(t *testing.T) {
	backends := newBackends(t, 2)
	in := startWatching(t, webConfig(poolOf("round_robin", backends[0]), ""), 20*time.Millisecond)
	client := newClient(t)
	if r := get(t, client, in.url("/")); r.body != "b0" {
		t.Fatalf("before the change: served by %q, want b0", r.body)
	}
	// Give the watcher its first look at the unchanged file; a change that
	// lands before that look is the subject of the next test.
	time.Sleep(200 * time.Millisecond)

	in.writeConfig(webConfig(poolOf("round_robin", backends[1]), ""))
	if !eventually(waitTimeout, func() bool { return get(t, client, in.url("/")).body == "b1" }) {
		t.Fatalf("file change was not loaded")
	}

	in.writeConfig("listeners: [\n")
	if !eventually(waitTimeout, func() bool { return in.metrics().sum("golb_reload_total", "result", "error") == 1 }) {
		t.Fatalf("broken file was not counted as a failed reload")
	}
	if r := get(t, client, in.url("/")); r.status != http.StatusOK || r.body != "b1" {
		t.Errorf("after a broken file: status %d from %q, want 200 from b1", r.status, r.body)
	}
}

// A config file that changes after golb has read it but before the watcher
// takes its first look must still be loaded: the watcher exists so that the
// running config follows the file.
func TestFileChangedDuringStartupIsPickedUp(t *testing.T) {
	knownBug(t, "BUG: a config file modified between start-up's read and the file watcher's first poll is never loaded; "+
		"the watcher takes the changed file as its starting point and only reacts to a later modification "+
		"(also the cause of the flaky internal/server TestFileWatcherReloads)")
	backends := newBackends(t, 2)
	listen := "addr: " + deadAddr(t)
	path := filepath.Join(t.TempDir(), "golb.yaml")
	write := func(cfg string) {
		t.Helper()
		err := os.WriteFile(path, []byte("admin:\n  addr: \"off\"\n"+cfg), 0o600)
		if err != nil {
			t.Fatalf("write config: %v", err)
		}
	}
	write(strings.Replace(webConfig(poolOf("round_robin", backends[0]), ""), "addr: "+anyPort, listen, 1))
	srv, err := server.New(context.Background(), server.Options{
		ConfigPath:    path,
		Logger:        slog.New(slog.DiscardHandler),
		AccessLog:     new(lockedBuffer),
		WatchInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start golb: %v", err)
	}
	t.Cleanup(func() {
		err := srv.Shutdown(context.Background())
		if err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	// The file changes while the process is still starting.
	write(strings.Replace(webConfig(poolOf("round_robin", backends[1]), ""), "addr: "+anyPort, listen, 1))
	srv.Start()

	client := newClient(t)
	url := "http://" + srv.Addr(web).String() + "/"
	if !eventually(2*time.Second, func() bool { return get(t, client, url).body == "b1" }) {
		t.Errorf("golb still serves the config it read at start-up 2 s after the file changed (served by %q)", get(t, client, url).body)
	}
}

func TestRateLimitReturns429AndRecovers(t *testing.T) {
	const (
		burst    = 5
		perToken = 200 * time.Millisecond // requests_per_second: 5
		requests = 20
	)
	b := newBackend(t, "b0", nil)
	cfg := "listeners:\n  - name: web\n    addr: 127.0.0.1:0\n    protocol: http\n    routes:\n" +
		"      - path_prefix: /\n        pool: app\n" +
		"      - path_prefix: /limited\n        pool: app\n        rate_limit:\n          requests_per_second: 5\n          burst: 5\n" +
		"pools:\n" + poolOf("round_robin", b).yaml()
	in := start(t, cfg)
	client := newClient(t)

	// A token bucket never admits more than its burst plus one token per
	// interval since it was created, however the requests are spread.
	began := time.Now()
	allowed, limited := 0, 0
	volley := func() (ok int) {
		t.Helper()
		for range requests {
			r := get(t, client, in.url("/limited"))
			switch r.status {
			case http.StatusOK:
				ok++
			case http.StatusTooManyRequests:
				limited++
				if r.header.Get("Retry-After") != "1" || r.body != "Too Many Requests\n" {
					t.Errorf("429 with Retry-After %q and body %q", r.header.Get("Retry-After"), r.body)
				}
			default:
				t.Errorf("status %d, want 200 or 429", r.status)
			}
		}
		allowed += ok
		if ceiling := burst + int(time.Since(began)/perToken) + 1; allowed > ceiling {
			t.Errorf("%d requests allowed in %v, the bucket permits at most %d", allowed, time.Since(began), ceiling)
		}
		return ok
	}

	if ok := volley(); ok < burst {
		t.Errorf("%d of %d requests allowed, want at least the burst of %d", ok, requests, burst)
	}
	if limited == 0 {
		t.Fatalf("no request was limited")
	}
	if got := b.hits.Load(); got != int64(allowed) {
		t.Errorf("backend saw %d requests, %d were allowed", got, allowed)
	}
	m := in.metrics()
	if got := m.sum("golb_ratelimited_total", "route", "/limited"); got != float64(limited) {
		t.Errorf("golb_ratelimited_total = %v, clients saw %d x 429", got, limited)
	}
	if got := m.sum("golb_requests_total", "route", "/limited", "code_class", "4xx"); got != float64(limited) {
		t.Errorf("golb_requests_total 4xx = %v, want %d", got, limited)
	}

	// The limit belongs to its route only.
	for range requests {
		if r := get(t, client, in.url("/other")); r.status != http.StatusOK {
			t.Fatalf("unlimited route answered %d", r.status)
		}
	}

	// A reload that leaves the limit unchanged must not hand out a new
	// burst: the ceiling in volley still holds.
	in.mustReload(cfg)
	volley()

	// After a second without requests the bucket is full again.
	time.Sleep(time.Second + perToken)
	if ok := volley(); ok < burst {
		t.Errorf("after a pause: %d of %d requests allowed, want at least the burst of %d", ok, requests, burst)
	}
}

// Review checklist, "Leaks": old transports release their connections after
// a reload. A reload that changes transport settings builds a new transport
// and closes the idle connections of the old one. A request that had already
// loaded the old configuration but sends afterwards opens a fresh connection
// on the old transport, and that connection is then parked in an idle pool
// nothing will close.
func TestUpstreamConnectionsOfReplacedTransportAreClosed(t *testing.T) {
	knownBug(t, "BUG: a request that races with a reload changing transport settings leaves its upstream connection in the "+
		"replaced transport's idle pool; such connections stay open until idle_conn_timeout (90 s by default) and are "+
		"still open after Shutdown returns")
	const (
		workers = 16
		reloads = 40
	)
	b := newBackend(t, "b0", nil)
	cfg := func(dial string) string {
		return webConfig(poolOf("round_robin", b).with("    transport:\n      dial_timeout: "+dial+"\n"), "")
	}
	in := start(t, cfg("2s"))
	client := newClient(t)
	l := startLoad(client, in.url("/"), workers)
	l.progress(t, 100)
	for i := range reloads {
		dial := "1s"
		if i%2 == 1 {
			dial = "2s"
		}
		in.mustReload(cfg(dial))
		l.progress(t, 100)
	}
	l.finish()
	if l.failed.Load() != 0 {
		t.Errorf("%d of %d requests failed across the reloads (first: %v)", l.failed.Load(), l.total.Load(), l.first.Load())
	}

	// Only the current transport may keep connections, at most one per worker.
	if !eventually(2*time.Second, func() bool { return b.open.Load() <= workers }) {
		t.Errorf("backend has %d connections open from golb after %d reloads, want at most %d (the current transport's idle pool)",
			b.open.Load(), reloads, workers)
	}
	in.waitIdle()
	err := in.shutdown()
	if err != nil {
		t.Errorf("shutdown: %v", err)
	}
	if !eventually(2*time.Second, func() bool { return b.open.Load() == 0 }) {
		t.Errorf("backend still has %d connections open from golb after Shutdown returned", b.open.Load())
	}
}
