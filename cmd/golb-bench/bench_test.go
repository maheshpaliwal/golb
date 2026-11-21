package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const anyLoopbackPort = "127.0.0.1:0"

// startServers serves handler on n loopback listeners with free ports and
// returns their addresses. The servers stop when the test ends.
func startServers(t *testing.T, n int, handler func(addr string) http.Handler) []string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = anyLoopbackPort
	}
	listeners, err := listenAll(ctx, addrs)
	if err != nil {
		cancel()
		t.Fatalf("listen: %v", err)
	}
	for i, listener := range listeners {
		addrs[i] = listener.Addr().String()
	}
	done := make(chan error, 1)
	go func() { done <- serve(ctx, listeners, handler) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	})
	return addrs
}

func startEcho(t *testing.T, n int, cfg echoConfig) []string {
	t.Helper()
	return startServers(t, n, func(addr string) http.Handler { return newEcho(addr, cfg) })
}

func testClient(t *testing.T) *http.Client {
	t.Helper()
	transport := &http.Transport{DisableKeepAlives: true}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 5 * time.Second}
}

func get(t *testing.T, client *http.Client, target string) (status int, header http.Header, body []byte) {
	t.Helper()
	resp, err := client.Get(target)
	if err != nil {
		t.Fatalf("GET %s: %v", target, err)
	}
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body of %s: %v", target, err)
	}
	err = resp.Body.Close()
	if err != nil {
		t.Fatalf("close body of %s: %v", target, err)
	}
	return resp.StatusCode, resp.Header, body
}

func load(t *testing.T, args ...string) report {
	t.Helper()
	var out bytes.Buffer
	err := run(context.Background(), append([]string{"load", "-format", "json"}, args...), &out)
	if err != nil {
		t.Fatalf("load %v: %v", args, err)
	}
	var rep report
	err = json.Unmarshal(out.Bytes(), &rep)
	if err != nil {
		t.Fatalf("decode load report %q: %v", out.String(), err)
	}
	return rep
}

func TestLoopback(t *testing.T) {
	tests := []struct {
		addr    string
		wantErr bool
	}{
		{addr: "127.0.0.1:9001"},
		{addr: "127.8.9.10:80"},
		{addr: "[::1]:9001"},
		{addr: "localhost:9001"},
		{addr: "0.0.0.0:9001", wantErr: true},
		{addr: "[::]:9001", wantErr: true},
		{addr: ":9001", wantErr: true},
		{addr: "192.168.1.10:9001", wantErr: true},
		{addr: "example.com:80", wantErr: true},
		{addr: "127.0.0.1", wantErr: true},
		{addr: "127.0.0.1:", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.addr, func(t *testing.T) {
			if err := loopback(tc.addr); (err != nil) != tc.wantErr {
				t.Errorf("got error %v, want error: %v", err, tc.wantErr)
			}
		})
	}
}

func TestCommandsRejectBadInput(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "no command", args: nil},
		{name: "unknown command", args: []string{"serve"}},
		{name: "echo on all interfaces", args: []string{"echo", "-addr", "0.0.0.0:0"}},
		{name: "echo without a host", args: []string{"echo", "-addr", ":0"}},
		{name: "echo with one bad address in the list", args: []string{"echo", "-addr", "127.0.0.1:0,10.0.0.1:0"}},
		{name: "echo fail rate above one", args: []string{"echo", "-addr", anyLoopbackPort, "-fail", "1.5"}},
		{name: "baseline on all interfaces", args: []string{"baseline", "-addr", "[::]:0", "-backends", "127.0.0.1:1"}},
		{name: "baseline without backends", args: []string{"baseline", "-addr", anyLoopbackPort}},
		{name: "baseline with a remote backend", args: []string{"baseline", "-addr", anyLoopbackPort, "-backends", "192.0.2.1:80"}},
		{name: "load without url", args: []string{"load"}},
		{name: "load https", args: []string{"load", "-url", "https://127.0.0.1:1/"}},
		{name: "load remote target", args: []string{"load", "-url", "http://192.0.2.1/"}},
		{name: "load zero connections", args: []string{"load", "-url", "http://127.0.0.1:1/", "-c", "0"}},
		{name: "load unknown format", args: []string{"load", "-url", "http://127.0.0.1:1/", "-format", "xml"}},
		{name: "load malformed header", args: []string{"load", "-url", "http://127.0.0.1:1/", "-H", "no-colon"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A command that wrongly accepts the input would block serving,
			// so the context is already cancelled to make that a failure
			// rather than a hang.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var out bytes.Buffer
			if err := run(ctx, tc.args, &out); err == nil {
				t.Errorf("got no error, output %q", out.String())
			}
		})
	}
}

func TestHelp(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"help"}, &out); err != nil {
		t.Fatalf("help: %v", err)
	}
	for _, command := range []string{"echo", "load", "baseline"} {
		if !strings.Contains(out.String(), command) {
			t.Errorf("help output lacks %q", command)
		}
	}
	if err := run(context.Background(), []string{"load", "-h"}, io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("load -h: got %v, want flag.ErrHelp", err)
	}
}

func TestEcho(t *testing.T) {
	tests := []struct {
		name       string
		cfg        echoConfig
		path       string
		wantStatus int
		wantBody   int
		minElapsed time.Duration
	}{
		{name: "default body", cfg: echoConfig{size: defaultBodySize}, path: "/", wantStatus: http.StatusOK, wantBody: defaultBodySize},
		{name: "empty body", cfg: echoConfig{}, path: "/any/path?q=1", wantStatus: http.StatusOK},
		{name: "large body", cfg: echoConfig{size: 1 << 20}, path: "/", wantStatus: http.StatusOK, wantBody: 1 << 20},
		{name: "always failing", cfg: echoConfig{size: 10, failRate: 1}, path: "/", wantStatus: http.StatusServiceUnavailable, wantBody: len("injected failure\n")},
		{name: "delayed", cfg: echoConfig{size: 10, delay: 30 * time.Millisecond}, path: "/", wantStatus: http.StatusOK, wantBody: 10, minElapsed: 30 * time.Millisecond},
		{name: "health is neither failed nor delayed", cfg: echoConfig{size: 10, failRate: 1, delay: time.Hour}, path: healthPath, wantStatus: http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addr := startEcho(t, 1, tc.cfg)[0]
			began := time.Now()
			status, header, body := get(t, testClient(t), "http://"+addr+tc.path)
			if elapsed := time.Since(began); elapsed < tc.minElapsed {
				t.Errorf("answered in %v, want at least %v", elapsed, tc.minElapsed)
			}
			if status != tc.wantStatus {
				t.Errorf("status: got %d, want %d", status, tc.wantStatus)
			}
			if len(body) != tc.wantBody {
				t.Errorf("body: got %d bytes, want %d", len(body), tc.wantBody)
			}
			if tc.path != healthPath && header.Get(backendHeader) != addr {
				t.Errorf("%s: got %q, want %q", backendHeader, header.Get(backendHeader), addr)
			}
		})
	}
}

func TestEchoFailRate(t *testing.T) {
	addr := startEcho(t, 1, echoConfig{size: 1, failRate: 0.3})[0]
	rep := load(t, "-url", "http://"+addr+"/", "-c", "4", "-d", "300ms", "-warmup", "0")
	run := rep.Median
	total := run.Status[2] + run.Status[5]
	if total < 200 {
		t.Fatalf("only %d responses in 300ms, too few to judge the failure rate", total)
	}
	if share := float64(run.Status[5]) / float64(total); math.Abs(share-0.3) > 0.1 {
		t.Errorf("got %.2f of responses failed, want about 0.30", share)
	}
}

func TestLoadClosedLoop(t *testing.T) {
	addr := startEcho(t, 1, echoConfig{size: 2048})[0]
	rep := load(t, "-url", "http://"+addr+"/x", "-c", "8", "-d", "200ms", "-warmup", "50ms", "-runs", "3", "-label", "direct")

	if rep.Mode != "closed-loop" || rep.Connections != 8 || rep.Label != "direct" || rep.GOMAXPROCS < 1 || rep.GoVersion == "" {
		t.Errorf("report header: %+v", rep)
	}
	if len(rep.Runs) != 3 {
		t.Fatalf("got %d runs, want 3", len(rep.Runs))
	}
	for i, run := range rep.Runs {
		if run.Requests == 0 || run.Errors != 0 || run.Status[2] != run.Requests {
			t.Errorf("run %d: requests=%d errors=%d 2xx=%d (%s), want all requests answered 2xx", i, run.Requests, run.Errors, run.Status[2], run.FirstErr)
		}
		if want := float64(run.Requests) / 0.2; math.Abs(run.ReqPerSec-want) > want*0.001 {
			t.Errorf("run %d: req/s %.1f does not equal requests over the 200ms window (%.1f)", i, run.ReqPerSec, want)
		}
		if !(0 < run.P50 && run.P50 <= run.P90 && run.P90 <= run.P99 && run.P99 <= run.P999 && run.P999 <= run.Max) {
			t.Errorf("run %d: percentiles not ordered: p50=%v p90=%v p99=%v p99.9=%v max=%v", i, run.P50, run.P90, run.P99, run.P999, run.Max)
		}
	}
	if rep.MinRPS > rep.MedianRPS || rep.MedianRPS > rep.MaxRPS || rep.Median.ReqPerSec != rep.MedianRPS {
		t.Errorf("summary: min=%v median=%v max=%v", rep.MinRPS, rep.MedianRPS, rep.MaxRPS)
	}
}

func TestLoadFixedRate(t *testing.T) {
	addr := startEcho(t, 1, echoConfig{size: 16})[0]
	rep := load(t, "-url", "http://"+addr+"/", "-c", "4", "-rate", "400", "-d", "500ms", "-warmup", "100ms")
	run := rep.Median
	if rep.Mode != "fixed-rate" || rep.Rate != 400 {
		t.Errorf("report header: mode=%q rate=%v", rep.Mode, rep.Rate)
	}
	// 400 req/s for 500ms is 200 requests; the schedule is fixed, so only
	// the edges of the window can differ.
	if run.Requests < 190 || run.Requests > 210 {
		t.Errorf("got %d requests, want about 200", run.Requests)
	}
	if run.Errors != 0 || run.Status[2] != run.Requests {
		t.Errorf("errors=%d 2xx=%d of %d (%s)", run.Errors, run.Status[2], run.Requests, run.FirstErr)
	}
}

// A backend slower than the schedule must show the queueing delay, not just
// its own service time.
func TestLoadFixedRateCountsQueueing(t *testing.T) {
	addr := startEcho(t, 1, echoConfig{size: 1, delay: 20 * time.Millisecond})[0]
	// One connection asked for 200 req/s can complete 50 req/s, so by the
	// end of 400ms the backlog is about 300ms.
	rep := load(t, "-url", "http://"+addr+"/", "-c", "1", "-rate", "200", "-d", "400ms", "-warmup", "0")
	run := rep.Median
	if run.Requests == 0 || run.Errors != 0 {
		t.Fatalf("requests=%d errors=%d (%s)", run.Requests, run.Errors, run.FirstErr)
	}
	if run.Max < 200_000 {
		t.Errorf("max latency %vµs, want well above the 20ms service time because requests queued", run.Max)
	}
	if run.P50 < 20_000 {
		t.Errorf("p50 %vµs is below the 20ms service time", run.P50)
	}
}

// A target far slower than the schedule must not stretch the run by its
// backlog: requests later than the timeout are failed without being sent.
func TestLoadFixedRateGivesUpOnHopelessBacklog(t *testing.T) {
	addr := startEcho(t, 1, echoConfig{size: 1, delay: 50 * time.Millisecond})[0]
	began := time.Now()
	rep := load(t, "-url", "http://"+addr+"/", "-c", "1", "-rate", "1000", "-d", "300ms", "-warmup", "0", "-timeout", "100ms")
	if elapsed := time.Since(began); elapsed > 2*time.Second {
		t.Errorf("run took %v; sending the whole 300-request backlog at 50ms each would take 15s", elapsed)
	}
	run := rep.Median
	if run.Requests != 300 {
		t.Errorf("got %d requests accounted for, want the 300 scheduled", run.Requests)
	}
	if run.Errors < 250 || run.FirstErr != errBehindSchedule.Error() {
		t.Errorf("errors=%d first=%q, want most of the schedule failed as behind schedule", run.Errors, run.FirstErr)
	}
}

func TestLoadCountsErrors(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		check   func(t *testing.T, run runStats)
	}{
		{
			name:    "5xx",
			handler: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) },
			check: func(t *testing.T, run runStats) {
				if run.Errors != 0 || run.Status[5] != run.Requests || run.nonSuccess() != run.Requests {
					t.Errorf("errors=%d 5xx=%d of %d, want every response counted as 5xx", run.Errors, run.Status[5], run.Requests)
				}
			},
		},
		{
			name: "connection closed without a response",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					return
				}
				if err := conn.Close(); err != nil {
					return
				}
			},
			check: func(t *testing.T, run runStats) {
				if run.Errors == 0 || run.Errors != run.Requests || run.FirstErr == "" {
					t.Errorf("errors=%d of %d first=%q, want every request an error with a message", run.Errors, run.Requests, run.FirstErr)
				}
				if run.ReqPerSec != 0 {
					t.Errorf("req/s %v counts failed requests", run.ReqPerSec)
				}
			},
		},
		{
			name: "connection close header forces reconnects",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Connection", "close")
				w.WriteHeader(http.StatusOK)
			},
			check: func(t *testing.T, run runStats) {
				if run.Errors != 0 || run.Status[2] != run.Requests {
					t.Errorf("errors=%d 2xx=%d of %d (%s), want all answered after reconnecting", run.Errors, run.Status[2], run.Requests, run.FirstErr)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addr := startServers(t, 1, func(string) http.Handler { return tc.handler })[0]
			// A fixed low rate keeps the number of short-lived connections,
			// and so of ports left in TIME_WAIT, to a few dozen.
			rep := load(t, "-url", "http://"+addr+"/", "-c", "2", "-rate", "200", "-d", "100ms", "-warmup", "0")
			if rep.Median.Requests == 0 {
				t.Fatal("no requests were made")
			}
			tc.check(t, rep.Median)
		})
	}
}

func TestLoadNothingListening(t *testing.T) {
	// The echo is started only to obtain a port that is certainly free
	// again once it has shut down.
	ctx, cancel := context.WithCancel(context.Background())
	listeners, err := listenAll(ctx, []string{anyLoopbackPort})
	if err != nil {
		cancel()
		t.Fatalf("listen: %v", err)
	}
	addr := listeners[0].Addr().String()
	cancel()
	closeListeners(listeners)

	rep := load(t, "-url", "http://"+addr+"/", "-c", "2", "-d", "100ms", "-warmup", "0", "-timeout", "1s")
	run := rep.Median
	if run.Errors == 0 || run.ReqPerSec != 0 {
		t.Errorf("errors=%d req/s=%v, want only connection errors", run.Errors, run.ReqPerSec)
	}
	// Two workers backing off 10ms after each refusal make about 20 attempts.
	if run.Errors > 40 {
		t.Errorf("got %d attempts in 100ms, want the error backoff to keep it near 20", run.Errors)
	}
}

func TestLoadSendsHeaders(t *testing.T) {
	type seen struct{ host, agent, tenant, uri string }
	got := make(chan seen, 1)
	addr := startServers(t, 1, func(string) http.Handler {
		return http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			select {
			case got <- seen{host: r.Host, agent: r.UserAgent(), tenant: r.Header.Get("X-Tenant"), uri: r.RequestURI}:
			default:
			}
		})
	})[0]
	load(t, "-url", "http://"+addr+"/a/b?c=d", "-c", "1", "-d", "30ms", "-warmup", "0", "-H", "Host: app.test", "-H", "X-Tenant: blue")
	want := seen{host: "app.test", agent: "golb-bench", tenant: "blue", uri: "/a/b?c=d"}
	if first := <-got; first != want {
		t.Errorf("request: got %+v, want %+v", first, want)
	}
}

func TestRequestBytes(t *testing.T) {
	target, err := url.Parse("http://127.0.0.1:8080")
	if err != nil {
		t.Fatal(err)
	}
	got := string(requestBytes(loadConfig{target: target}))
	want := "GET / HTTP/1.1\r\nHost: 127.0.0.1:8080\r\nUser-Agent: golb-bench\r\n\r\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestLoadOutputFormats(t *testing.T) {
	addr := startEcho(t, 1, echoConfig{size: 8})[0]
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{name: "text", args: []string{"-label", "golb"}, want: []string{"label        golb", "mode         closed-loop, 2 connections", "req/s", "p99.9=", "status     2xx="}},
		{name: "text with several runs", args: []string{"-runs", "2"}, want: []string{"run 1", "run 2", "median run"}},
		{name: "markdown row", args: []string{"-format", "markdown", "-label", "golb"}, want: []string{"| golb | "}},
		{name: "markdown with header", args: []string{"-format", "markdown", "-header"}, want: []string{"| scenario | req/s (median) |", "|---|"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			args := append([]string{"load", "-url", "http://" + addr + "/", "-c", "2", "-d", "30ms", "-warmup", "0"}, tc.args...)
			if err := run(context.Background(), args, &out); err != nil {
				t.Fatalf("load: %v", err)
			}
			for _, want := range tc.want {
				if !strings.Contains(out.String(), want) {
					t.Errorf("output lacks %q:\n%s", want, out.String())
				}
			}
			if tc.name == "markdown row" && strings.Count(out.String(), "\n") != 1 {
				t.Errorf("markdown row is not a single line:\n%s", out.String())
			}
		})
	}
}

func TestLoadSavesReport(t *testing.T) {
	addr := startEcho(t, 1, echoConfig{size: 8})[0]
	path := filepath.Join(t.TempDir(), "report.json")
	var out bytes.Buffer
	err := run(context.Background(), []string{"load", "-url", "http://" + addr + "/", "-c", "1", "-d", "30ms", "-warmup", "0", "-format", "markdown", "-save", path}, &out)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read saved report: %v", err)
	}
	var saved report
	err = json.Unmarshal(data, &saved)
	if err != nil {
		t.Fatalf("decode saved report: %v", err)
	}
	if saved.MedianRPS <= 0 || len(saved.Runs) != 1 {
		t.Errorf("saved report: median_rps=%v runs=%d", saved.MedianRPS, len(saved.Runs))
	}
	if !strings.HasPrefix(out.String(), "| ") {
		t.Errorf("stdout is not the markdown row: %q", out.String())
	}
}

func TestBaselineProxiesRoundRobin(t *testing.T) {
	backends := startEcho(t, 3, echoConfig{size: 64})
	handler, err := newBaseline(backends, http.DefaultMaxIdleConnsPerHost)
	if err != nil {
		t.Fatalf("build baseline: %v", err)
	}
	proxy := startServers(t, 1, func(string) http.Handler { return handler })[0]

	client := testClient(t)
	hits := make(map[string]int)
	for range 30 {
		status, header, body := get(t, client, "http://"+proxy+"/")
		if status != http.StatusOK || len(body) != 64 {
			t.Fatalf("through the baseline: status %d, %d body bytes; want 200 and 64", status, len(body))
		}
		hits[header.Get(backendHeader)]++
	}
	for _, backend := range backends {
		if hits[backend] != 10 {
			t.Errorf("backend %s served %d of 30 requests, want 10", backend, hits[backend])
		}
	}

	rep := load(t, "-url", "http://"+proxy+"/", "-c", "4", "-d", "100ms", "-warmup", "0")
	if run := rep.Median; run.Requests == 0 || run.Errors != 0 || run.Status[2] != run.Requests {
		t.Errorf("load through the baseline: requests=%d errors=%d 2xx=%d (%s)", run.Requests, run.Errors, run.Status[2], run.FirstErr)
	}
}

func TestHistogramBuckets(t *testing.T) {
	for _, nanos := range []uint64{0, 1, 127, 255, 256, 257, 511, 512, 1000, 123_456, 1_000_000, 987_654_321, 1 << 40, math.MaxInt64} {
		i := bucketOf(nanos)
		if i < 0 || i >= bucketCount {
			t.Fatalf("value %d: bucket %d outside 0..%d", nanos, i, bucketCount-1)
		}
		top := bucketTop(i)
		if top < nanos {
			t.Errorf("value %d: bucket top %d is below the value", nanos, top)
		}
		if bucketOf(top) != i {
			t.Errorf("value %d: bucket top %d maps to bucket %d, not %d", nanos, top, bucketOf(top), i)
		}
		if i+1 < bucketCount && bucketOf(top+1) != i+1 {
			t.Errorf("value %d: %d should open bucket %d, got %d", nanos, top+1, i+1, bucketOf(top+1))
		}
		if width := float64(top-nanos) / float64(max(nanos, 1)); nanos >= linearLimit && width > 1.0/subBuckets {
			t.Errorf("value %d: rounded up by %.3f%%, want at most %.3f%%", nanos, width*100, 100.0/subBuckets)
		}
	}
}

func TestHistogramQuantiles(t *testing.T) {
	var h histogram
	if h.quantile(0.5) != 0 || h.mean() != 0 {
		t.Error("empty histogram does not report zero")
	}
	// 1µs..10ms in 1µs steps: quantile q is q*10ms.
	const samples = 10_000
	var half histogram
	for i := 1; i <= samples; i++ {
		target := &h
		if i%2 == 0 {
			target = &half
		}
		target.record(time.Duration(i) * time.Microsecond)
	}
	h.merge(&half)

	tests := []struct {
		q    float64
		want time.Duration
	}{
		{q: 0.5, want: 5 * time.Millisecond},
		{q: 0.9, want: 9 * time.Millisecond},
		{q: 0.99, want: 9900 * time.Microsecond},
		{q: 0.999, want: 9990 * time.Microsecond},
		{q: 1, want: 10 * time.Millisecond},
	}
	for _, tc := range tests {
		got := h.quantile(tc.q)
		if got < tc.want || float64(got-tc.want) > float64(tc.want)/subBuckets {
			t.Errorf("quantile %v: got %v, want %v rounded up by at most %.1f%%", tc.q, got, tc.want, 100.0/subBuckets)
		}
	}
	if h.total != samples || h.max != 10*time.Millisecond {
		t.Errorf("total=%d max=%v, want %d and 10ms", h.total, h.max, samples)
	}
	if want := 5000500 * time.Nanosecond; h.mean() != want {
		t.Errorf("mean: got %v, want %v", h.mean(), want)
	}
	h.record(-time.Second)
	if h.quantile(0) != 0 {
		t.Error("negative duration was not recorded as zero")
	}
}
