package e2e

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const distributionTolerance = 0.05 // PRD 6: within 5 % of the weights

func TestRoundRobinDistribution(t *testing.T) {
	tests := []struct {
		name     string
		weights  []int
		requests int
		want     []int64
	}{
		{name: "equal weights", weights: []int{1, 1, 1}, requests: 300, want: []int64{100, 100, 100}},
		{name: "weights 1:2:5", weights: []int{1, 2, 5}, requests: 800, want: []int64{100, 200, 500}},
		{name: "weights 3:1", weights: []int{3, 1}, requests: 400, want: []int64{300, 100}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backends := newBackends(t, len(tc.weights))
			in := start(t, webConfig(poolOf("round_robin", backends...).weighted(tc.weights...), ""))
			client := newClient(t)
			for range tc.requests {
				r := get(t, client, in.url("/"))
				if r.status != http.StatusOK {
					t.Fatalf("status %d, want 200", r.status)
				}
			}
			for i, b := range backends {
				got := b.hits.Load()
				if !within(got, tc.want[i], int64(tc.requests), distributionTolerance) {
					t.Errorf("backend %d (weight %d): %d of %d requests, want %d ± 5%%", i, tc.weights[i], got, tc.requests, tc.want[i])
				}
			}
		})
	}
}

// holding returns backends whose requests block until release is closed,
// and a gauge per backend of the requests currently held.
func holding(t *testing.T, n int) (backends []*backend, held []*atomic.Int64, release chan struct{}) {
	t.Helper()
	release = make(chan struct{})
	for i := range n {
		gauge := new(atomic.Int64)
		held = append(held, gauge)
		backends = append(backends, newBackend(t, fmt.Sprintf("b%d", i), func(w http.ResponseWriter, r *http.Request) {
			gauge.Add(1)
			defer gauge.Add(-1)
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}))
	}
	return backends, held, release
}

func TestLeastConnSpreadsByInflight(t *testing.T) {
	tests := []struct {
		name     string
		weights  []int
		requests int
		want     []int64
		slack    int64
	}{
		{name: "equal weights", weights: []int{1, 1}, requests: 20, want: []int64{10, 10}, slack: 2},
		{name: "weights 1:3", weights: []int{1, 3}, requests: 24, want: []int64{6, 18}, slack: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backends, held, release := holding(t, len(tc.weights))
			in := start(t, webConfig(poolOf("least_conn", backends...).weighted(tc.weights...), ""))
			client := newClient(t)

			var wg sync.WaitGroup
			var failed atomic.Int64
			for range tc.requests {
				wg.Go(func() {
					r, err := trySend(client, http.MethodGet, in.url("/"), "")
					if err != nil || r.status != http.StatusOK {
						failed.Add(1)
					}
				})
			}
			total := func() int64 {
				var n int64
				for _, gauge := range held {
					n += gauge.Load()
				}
				return n
			}
			if !eventually(waitTimeout, func() bool { return total() == int64(tc.requests) }) {
				t.Errorf("only %d of %d requests reached a backend", total(), tc.requests)
			}
			for i, gauge := range held {
				got := gauge.Load()
				if got < tc.want[i]-tc.slack || got > tc.want[i]+tc.slack {
					t.Errorf("backend %d (weight %d) holds %d requests, want %d ± %d", i, tc.weights[i], got, tc.want[i], tc.slack)
				}
				if row := in.backend(backends[i].addr); row.Inflight != got {
					t.Errorf("backend %d: /backends reports %d in flight, the backend holds %d", i, row.Inflight, got)
				}
			}
			close(release)
			wg.Wait()
			if failed.Load() != 0 {
				t.Errorf("%d held requests failed", failed.Load())
			}
		})
	}
}

func TestP2CEWMAAvoidsSlowBackend(t *testing.T) {
	const (
		requests = 240
		workers  = 4
		delay    = 80 * time.Millisecond
	)
	backends := newBackends(t, 3)
	slow := newBackend(t, "slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		writeString(w, "slow")
	})
	// p2c_ewma is the default policy, so none is written.
	in := start(t, webConfig(poolOf("", append(backends, slow)...), ""))
	client := newClient(t)

	var wg sync.WaitGroup
	var failed atomic.Int64
	for range workers {
		wg.Go(func() {
			for range requests / workers {
				r, err := trySend(client, http.MethodGet, in.url("/"), "")
				if err != nil || r.status != http.StatusOK {
					failed.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.Fatalf("%d requests failed", failed.Load())
	}

	if got := slow.hits.Load(); got < 1 || got > requests/10 {
		t.Errorf("slow backend served %d of %d requests, want at least 1 (it must be probed) and at most 10%%", got, requests)
	}
	for i, b := range backends {
		if got := b.hits.Load(); got < requests/6 {
			t.Errorf("fast backend %d served %d of %d requests, want at least %d", i, got, requests, requests/6)
		}
	}
	if row := in.backend(slow.addr); row.LatencyMs < float64(delay/time.Millisecond)/2 {
		t.Errorf("slow backend latency in /backends is %.1f ms, want about %d ms", row.LatencyMs, delay/time.Millisecond)
	}
}

func TestRingHashStickyByHeader(t *testing.T) {
	const (
		keys    = 60
		repeats = 4
	)
	backends := newBackends(t, 4)
	cfg := func(members ...*backend) string {
		return webConfig(poolOf("ring_hash", members...).with("    hash_header: X-User\n"), "")
	}
	in := start(t, cfg(backends...))
	client := newClient(t)

	owner := make(map[string]string, keys)
	used := make(map[string]int)
	for k := range keys {
		key := fmt.Sprintf("user-%d", k)
		for range repeats {
			r := send(t, client, http.MethodGet, in.url("/"), "", "X-User", key)
			if owner[key] == "" {
				owner[key] = r.body
				used[r.body]++
			}
			if r.body != owner[key] {
				t.Fatalf("key %s moved from %s to %s", key, owner[key], r.body)
			}
		}
	}
	if len(used) != len(backends) {
		t.Errorf("%d keys landed on %d of %d backends: %v", keys, len(used), len(backends), used)
	}

	// Removing one backend moves only the keys it owned.
	removed := backends[3]
	in.mustReload(cfg(backends[:3]...))
	moved := 0
	for key, before := range owner {
		r := send(t, client, http.MethodGet, in.url("/"), "", "X-User", key)
		if r.body == removed.name {
			t.Fatalf("key %s still reaches the removed backend", key)
		}
		if before == removed.name {
			moved++
			continue
		}
		if r.body != before {
			t.Errorf("key %s was on %s, which stayed in the pool, but moved to %s", key, before, r.body)
		}
	}
	if moved != used[removed.name] {
		t.Errorf("%d keys moved, want the %d owned by the removed backend", moved, used[removed.name])
	}
}

func TestRingHashStickyByClientIP(t *testing.T) {
	tests := []struct {
		name  string
		extra string
	}{
		{name: "no hash header configured"},
		{name: "configured header absent from the request", extra: "    hash_header: X-User\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backends := newBackends(t, 4)
			in := start(t, webConfig(poolOf("ring_hash", backends...).with(tc.extra), ""))
			// A new connection per request gives every request another
			// source port; the key must be the IP alone.
			client := newClient(t)
			client.Transport.(*http.Transport).DisableKeepAlives = true
			first := ""
			for i := range 30 {
				r := get(t, client, in.url("/"))
				if first == "" {
					first = r.body
				}
				if r.body != first {
					t.Fatalf("request %d from the same client IP went to %s, earlier ones to %s", i, r.body, first)
				}
			}
		})
	}
}

func TestRouting(t *testing.T) {
	root := newBackend(t, "root", nil)
	api := newBackend(t, "api", nil)
	host := newBackend(t, "host", nil)
	wild := newBackend(t, "wild", nil)
	cfg := "listeners:\n  - name: web\n    addr: 127.0.0.1:0\n    protocol: http\n    routes:\n" +
		"      - path_prefix: /\n        pool: root\n" +
		"      - path_prefix: /api\n        pool: api\n" +
		"      - host: shop.test\n        pool: host\n" +
		"      - host: \"*.shop.test\"\n        pool: wild\n" +
		"pools:\n" +
		pool{name: "root", addrs: addrsOf(root)}.yaml() +
		pool{name: "api", addrs: addrsOf(api)}.yaml() +
		pool{name: "host", addrs: addrsOf(host)}.yaml() +
		pool{name: "wild", addrs: addrsOf(wild)}.yaml()
	in := start(t, cfg)
	client := newClient(t)

	tests := []struct {
		name string
		host string
		path string
		want string
	}{
		{name: "root", path: "/", want: "root"},
		{name: "longest prefix", path: "/api/v1", want: "api"},
		{name: "prefix matches whole segments only", path: "/apiary", want: "root"},
		{name: "double slash cannot dodge the prefix", path: "//api/v1", want: "api"},
		{name: "dot segments cannot dodge the prefix", path: "/x/../api", want: "api"},
		{name: "exact host", host: "shop.test", path: "/api", want: "host"},
		{name: "host with port and upper case", host: "SHOP.test:8080", path: "/", want: "host"},
		{name: "wildcard host", host: "eu.shop.test", path: "/", want: "wild"},
		{name: "wildcard does not match the bare suffix", host: "xshop.test", path: "/", want: "root"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, in.url("/"), nil)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			req.URL.Path = tc.path // sent as written; the client does not clean it
			req.Host = tc.host
			r, err := do(client, req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			if r.status != http.StatusOK || r.body != tc.want {
				t.Errorf("host %q path %q: status %d from %q, want 200 from %q", tc.host, tc.path, r.status, r.body, tc.want)
			}
		})
	}
}
