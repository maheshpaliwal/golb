package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"
)

const (
	defaultEchoAddr = "127.0.0.1:9001"
	defaultBodySize = 1024
	healthPath      = "/healthz"
	backendHeader   = "X-Backend"
	bodyFiller      = 'x'
)

type echoConfig struct {
	size     int
	delay    time.Duration
	failRate float64
}

// echo answers every request with the same body. The delay and failure rate
// turn it into the slow or broken backend of a scenario.
type echo struct {
	addr string
	body []byte
	echoConfig
}

func runEcho(ctx context.Context, args []string, out io.Writer) error {
	var cfg echoConfig
	flags := flag.NewFlagSet("echo", flag.ContinueOnError)
	addrs := flags.String("addr", defaultEchoAddr, "comma-separated loopback host:port list; one backend per address")
	flags.IntVar(&cfg.size, "size", defaultBodySize, "response body size in bytes")
	flags.DurationVar(&cfg.delay, "delay", 0, "time to wait before answering")
	flags.Float64Var(&cfg.failRate, "fail", 0, "fraction of requests answered with 503, 0..1")
	err := flags.Parse(args)
	if err != nil {
		return err
	}
	if cfg.size < 0 || cfg.delay < 0 || cfg.failRate < 0 || cfg.failRate > 1 {
		return errors.New("echo: -size and -delay must not be negative and -fail must be within 0..1")
	}
	listeners, err := listenAll(ctx, splitAddrs(*addrs))
	if err != nil {
		return fmt.Errorf("echo: %w", err)
	}
	if len(listeners) == 0 {
		return errors.New("echo: -addr is empty")
	}
	for _, listener := range listeners {
		_, err = fmt.Fprintf(out, "echo listening on %s (body %d bytes, delay %s, fail %g)\n", listener.Addr(), cfg.size, cfg.delay, cfg.failRate)
		if err != nil {
			return err
		}
	}
	return serve(ctx, listeners, func(addr string) http.Handler { return newEcho(addr, cfg) })
}

func newEcho(addr string, cfg echoConfig) *echo {
	return &echo{addr: addr, body: bytes.Repeat([]byte{bodyFiller}, cfg.size), echoConfig: cfg}
}

func (e *echo) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Health probes are answered at once so a slow or failing scenario
	// backend is still seen as up by active checks.
	if r.URL.Path == healthPath {
		w.WriteHeader(http.StatusOK)
		return
	}
	if e.delay > 0 {
		timer := time.NewTimer(e.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}
	w.Header().Set(backendHeader, e.addr)
	if e.failRate > 0 && rand.Float64() < e.failRate {
		http.Error(w, "injected failure", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, err := w.Write(e.body)
	if err != nil {
		slog.Debug("write echo response", "backend", e.addr, "error", err)
	}
}
