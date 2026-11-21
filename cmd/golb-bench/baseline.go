package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
)

// The transport values below are the ones net/http's default transport
// uses, written out so the baseline is "ReverseProxy as most people first
// deploy it" without sharing the process-wide default transport.
const (
	baselineDialTimeout     = 30 * time.Second
	baselineKeepAlive       = 30 * time.Second
	baselineMaxIdleConns    = 100
	baselineIdleConnTimeout = 90 * time.Second
	baselineExpectContinue  = time.Second
	defaultBaselineAddr     = "127.0.0.1:8081"
)

func runBaseline(ctx context.Context, args []string, out io.Writer) error {
	flags := flag.NewFlagSet("baseline", flag.ContinueOnError)
	addr := flags.String("addr", defaultBaselineAddr, "loopback host:port to listen on")
	backends := flags.String("backends", "", "comma-separated loopback host:port list, used round robin")
	idlePerHost := flags.Int("idle-per-host", http.DefaultMaxIdleConnsPerHost, "idle upstream connections kept per backend; the default is net/http's, which reconnects constantly under load")
	err := flags.Parse(args)
	if err != nil {
		return err
	}
	handler, err := newBaseline(splitAddrs(*backends), *idlePerHost)
	if err != nil {
		return fmt.Errorf("baseline: %w", err)
	}
	listeners, err := listenAll(ctx, []string{*addr})
	if err != nil {
		return fmt.Errorf("baseline: %w", err)
	}
	_, err = fmt.Fprintf(out, "baseline listening on %s -> %s (idle per host %d)\n", listeners[0].Addr(), *backends, *idlePerHost)
	if err != nil {
		return err
	}
	return serve(ctx, listeners, func(string) http.Handler { return handler })
}

func newBaseline(backends []string, idlePerHost int) (http.Handler, error) {
	if len(backends) == 0 {
		return nil, errors.New("-backends is empty")
	}
	targets := make([]*url.URL, len(backends))
	for i, backend := range backends {
		err := loopback(backend)
		if err != nil {
			return nil, err
		}
		targets[i] = &url.URL{Scheme: "http", Host: backend}
	}
	var next atomic.Uint64
	dialer := &net.Dialer{Timeout: baselineDialTimeout, KeepAlive: baselineKeepAlive}
	return &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(targets[next.Add(1)%uint64(len(targets))])
			r.SetXForwarded()
		},
		Transport: &http.Transport{
			DialContext: dialer.DialContext,
			// Raising -idle-per-host must not be undone by the total cap.
			MaxIdleConns:          max(baselineMaxIdleConns, idlePerHost*len(targets)),
			MaxIdleConnsPerHost:   idlePerHost,
			IdleConnTimeout:       baselineIdleConnTimeout,
			ExpectContinueTimeout: baselineExpectContinue,
		},
	}, nil
}
