package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maheshpaliwal/golb/internal/metrics"
)

type stubSource struct {
	ready     bool
	reloadErr error
	reloads   int
}

func (s *stubSource) Ready() bool { return s.ready }

func (s *stubSource) Backends() []Backend {
	return []Backend{{Pool: "app", Addr: "127.0.0.1:9001", Weight: 2, Healthy: true, Inflight: 3, LatencyMs: 1.5}}
}

func (s *stubSource) Reload(context.Context) error {
	s.reloads++
	return s.reloadErr
}

func serve(t *testing.T, source *stubSource, pprof bool) *httptest.Server {
	t.Helper()
	reg := metrics.NewRegistry()
	reg.Counter("golb_reload_total", "Reloads.", "result", "ok").Add(7)
	server := httptest.NewServer(NewHandler(Options{
		Source:   source,
		Registry: reg,
		Pprof:    pprof,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}))
	t.Cleanup(server.Close)
	return server
}

func TestEndpoints(t *testing.T) {
	tests := []struct {
		name       string
		source     stubSource
		pprof      bool
		method     string
		path       string
		wantStatus int
		wantBody   string // substring
		wantReload int
	}{
		{name: "metrics", method: http.MethodGet, path: "/metrics", wantStatus: 200, wantBody: `golb_reload_total{result="ok"} 7`},
		{name: "healthz", method: http.MethodGet, path: "/healthz", wantStatus: 200, wantBody: "ok"},
		{name: "ready", source: stubSource{ready: true}, method: http.MethodGet, path: "/readyz", wantStatus: 200, wantBody: "ready"},
		{name: "draining", method: http.MethodGet, path: "/readyz", wantStatus: 503, wantBody: "draining"},
		{name: "backends", method: http.MethodGet, path: "/backends", wantStatus: 200, wantBody: `"addr": "127.0.0.1:9001"`},
		{name: "reload", method: http.MethodPost, path: "/reload", wantStatus: 200, wantBody: "reloaded", wantReload: 1},
		{name: "reload needs POST", method: http.MethodGet, path: "/reload", wantStatus: 405},
		{name: "metrics is read-only", method: http.MethodPost, path: "/metrics", wantStatus: 405},
		{name: "pprof off by default", method: http.MethodGet, path: "/debug/pprof/", wantStatus: 404},
		{name: "pprof when enabled", pprof: true, method: http.MethodGet, path: "/debug/pprof/cmdline", wantStatus: 200},
		{name: "unknown path", method: http.MethodGet, path: "/", wantStatus: 404},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := tc.source
			server := serve(t, &source, tc.pprof)
			req, err := http.NewRequest(tc.method, server.URL+tc.path, nil)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			res, err := server.Client().Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if res.StatusCode != tc.wantStatus {
				t.Errorf("status = %d, want %d", res.StatusCode, tc.wantStatus)
			}
			if !strings.Contains(string(body), tc.wantBody) {
				t.Errorf("body %q does not contain %q", body, tc.wantBody)
			}
			if source.reloads != tc.wantReload {
				t.Errorf("Reload called %d times, want %d", source.reloads, tc.wantReload)
			}
		})
	}
}

func TestBackendsJSON(t *testing.T) {
	server := serve(t, new(stubSource), false)
	res, err := server.Client().Get(server.URL + "/backends")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer res.Body.Close()
	var got []Backend
	err = json.NewDecoder(res.Body).Decode(&got)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := new(stubSource).Backends()
	if len(got) != 1 || got[0] != want[0] {
		t.Errorf("backends = %+v, want %+v", got, want)
	}
}

func TestReloadFailureHidesTheReason(t *testing.T) {
	const secret = "line 3: cannot unmarshal `placeholder-value`"
	source := &stubSource{reloadErr: errors.New(secret)}
	server := serve(t, source, false)
	res, err := server.Client().Post(server.URL+"/reload", "", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if res.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", res.StatusCode)
	}
	if strings.Contains(string(body), "placeholder-value") {
		t.Errorf("response quotes the configuration: %q", body)
	}
}
