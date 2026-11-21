// Command golb-bench is the benchmark toolkit for golb: a fixed-response
// upstream (echo), a keep-alive HTTP/1.1 load generator (load) and a bare
// httputil.ReverseProxy to compare against (baseline). Everything it starts
// listens on loopback only.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	readHeaderTimeout = 5 * time.Second
	serverIdleTimeout = 2 * time.Minute
	shutdownGrace     = 5 * time.Second

	usage = `usage: golb-bench <command> [flags]

commands:
  echo      fixed-response HTTP upstream; -addr takes a comma-separated list
  load      keep-alive HTTP/1.1 load generator, closed-loop (-c) or fixed-rate (-rate)
  baseline  bare httputil.ReverseProxy over -backends, the unoptimised reference

run "golb-bench <command> -h" for the flags of a command`
)

var errUsage = errors.New("usage")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout)
	stop()
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return
	}
	fmt.Fprintln(os.Stderr, "golb-bench:", err)
	os.Exit(1)
}

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("%w: no command\n%s", errUsage, usage)
	}
	switch args[0] {
	case "echo":
		return runEcho(ctx, args[1:], out)
	case "load":
		return runLoad(ctx, args[1:], out)
	case "baseline":
		return runBaseline(ctx, args[1:], out)
	case "-h", "-help", "--help", "help":
		_, err := fmt.Fprintln(out, usage)
		return err
	}
	return fmt.Errorf("%w: unknown command %q\n%s", errUsage, args[0], usage)
}

// loopback rejects any address this tool must not bind or, by default,
// send load to: only 127.0.0.0/8, ::1 and localhost are accepted.
func loopback(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("address %q is not host:port: %w", addr, err)
	}
	if port == "" {
		return fmt.Errorf("address %q has no port", addr)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("address %q is not a loopback address", addr)
	}
	return nil
}

func splitAddrs(list string) []string {
	var addrs []string
	for addr := range strings.SplitSeq(list, ",") {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

// listenAll opens one loopback listener per address, or none if any fails.
func listenAll(ctx context.Context, addrs []string) ([]net.Listener, error) {
	listeners := make([]net.Listener, 0, len(addrs))
	for _, addr := range addrs {
		listener, err := listen(ctx, addr)
		if err != nil {
			closeListeners(listeners)
			return nil, err
		}
		listeners = append(listeners, listener)
	}
	return listeners, nil
}

func listen(ctx context.Context, addr string) (net.Listener, error) {
	err := loopback(addr)
	if err != nil {
		return nil, err
	}
	var config net.ListenConfig
	return config.Listen(ctx, "tcp", addr)
}

func closeListeners(listeners []net.Listener) {
	for _, listener := range listeners {
		err := listener.Close()
		if err != nil {
			slog.Error("close listener", "addr", listener.Addr().String(), "error", err)
		}
	}
}

// serve runs handler(addr) on every listener until ctx ends or one server
// fails, then shuts all of them down.
func serve(ctx context.Context, listeners []net.Listener, handler func(addr string) http.Handler) error {
	servers := make([]*http.Server, len(listeners))
	failed := make(chan error, len(listeners))
	var wg sync.WaitGroup
	for i, listener := range listeners {
		servers[i] = &http.Server{
			Handler:           handler(listener.Addr().String()),
			ReadHeaderTimeout: readHeaderTimeout,
			IdleTimeout:       serverIdleTimeout,
		}
		wg.Go(func() {
			defer logPanic("server")
			err := servers[i].Serve(listener)
			if !errors.Is(err, http.ErrServerClosed) {
				failed <- fmt.Errorf("serve %s: %w", listener.Addr(), err)
			}
		})
	}

	var err error
	select {
	case <-ctx.Done():
	case err = <-failed:
	}
	// ctx is already cancelled here, so the grace period needs its own root.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	for _, server := range servers {
		if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
			slog.Error("shut down server", "addr", server.Addr, "error", shutdownErr)
		}
	}
	wg.Wait()
	return err
}

// logPanic is deferred at the top of every goroutine this tool starts.
func logPanic(what string) {
	if r := recover(); r != nil {
		slog.Error("goroutine panicked", "goroutine", what, "panic", r)
	}
}
