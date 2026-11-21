// Package admin serves the operator endpoints: metrics, health, backend
// state and reload. It never shares a listener with proxied traffic.
package admin

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/pprof"

	"github.com/maheshpaliwal/golb/internal/metrics"
)

const (
	metricsContentType = "text/plain; version=0.0.4; charset=utf-8"
	jsonContentType    = "application/json"
)

// Backend is one row of GET /backends.
type Backend struct {
	Pool      string  `json:"pool"`
	Addr      string  `json:"addr"`
	Weight    uint32  `json:"weight"`
	Healthy   bool    `json:"healthy"`
	Ejected   bool    `json:"ejected"`
	Inflight  int64   `json:"inflight"`
	LatencyMs float64 `json:"latency_ms"`
}

// Source is the running server as the admin endpoints see it.
type Source interface {
	// Ready reports whether a configuration is loaded and the server is not draining.
	Ready() bool
	Backends() []Backend
	Reload(ctx context.Context) error
}

type Options struct {
	Source   Source
	Registry *metrics.Registry
	Pprof    bool
	Logger   *slog.Logger
}

func NewHandler(opts Options) http.Handler {
	h := &handler{source: opts.Source, registry: opts.Registry, logger: opts.Logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", h.metrics)
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /readyz", h.readyz)
	mux.HandleFunc("GET /backends", h.backends)
	mux.HandleFunc("POST /reload", h.reload)
	if opts.Pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return mux
}

type handler struct {
	source   Source
	registry *metrics.Registry
	logger   *slog.Logger
}

func (h *handler) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", metricsContentType)
	_, err := h.registry.WriteTo(w)
	if err != nil {
		h.logger.Error("write metrics", "error", err)
	}
}

func (h *handler) healthz(w http.ResponseWriter, r *http.Request) {
	h.text(w, http.StatusOK, "ok\n")
}

func (h *handler) readyz(w http.ResponseWriter, r *http.Request) {
	if !h.source.Ready() {
		h.text(w, http.StatusServiceUnavailable, "draining\n")
		return
	}
	h.text(w, http.StatusOK, "ready\n")
}

func (h *handler) backends(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", jsonContentType)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	err := enc.Encode(h.source.Backends())
	if err != nil {
		h.logger.Error("write backends", "error", err)
	}
}

// reload answers without the error text: a parse error can quote the file,
// and the full reason is in the process log.
func (h *handler) reload(w http.ResponseWriter, r *http.Request) {
	err := h.source.Reload(r.Context())
	if err != nil {
		h.text(w, http.StatusInternalServerError, "reload rejected, the running configuration is unchanged; see the log\n")
		return
	}
	h.text(w, http.StatusOK, "reloaded\n")
}

func (h *handler) text(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, err := w.Write([]byte(body))
	if err != nil {
		h.logger.Error("write admin response", "error", err)
	}
}
