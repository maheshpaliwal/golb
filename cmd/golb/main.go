// Command golb is an HTTP and TCP load balancer.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/maheshpaliwal/golb/internal/config"
	"github.com/maheshpaliwal/golb/internal/server"
)

const (
	defaultConfigPath = "golb.yaml"

	exitOK      = 0
	exitFailure = 1
	exitUsage   = 2
)

func main() {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	os.Exit(run(context.Background(), os.Args[1:], signals, os.Stdout, os.Stderr))
}

// run is the whole program apart from process-level wiring, so it can be
// driven from a test with its own signals and output.
func run(ctx context.Context, args []string, signals <-chan os.Signal, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("golb", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("config", defaultConfigPath, "path to the YAML configuration file")
	check := flags.Bool("check", false, "validate the configuration and exit")
	err := flags.Parse(args)
	if err != nil {
		return exitUsage
	}

	if *check {
		_, err = config.Load(*path)
		if err != nil {
			fmt.Fprintf(stderr, "%s: %v\n", *path, err)
			return exitFailure
		}
		fmt.Fprintf(stdout, "%s: ok\n", *path)
		return exitOK
	}

	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	srv, err := server.New(ctx, server.Options{ConfigPath: *path, Logger: logger, AccessLog: stdout})
	if err != nil {
		logger.Error("start", "config", *path, "error", err)
		return exitFailure
	}
	srv.Start()
	logger.Info("golb started", "config", *path)

	for {
		select {
		case err = <-srv.Failed():
			logger.Error("stopping after a listener failure", "error", err)
			return shutdown(ctx, srv, signals, logger, exitFailure)
		case sig := <-signals:
			if sig != syscall.SIGHUP {
				logger.Info("draining", "signal", sig.String())
				return shutdown(ctx, srv, signals, logger, exitOK)
			}
			// A rejected reload is logged and counted by the server; the
			// process keeps running on the old configuration.
			_ = srv.Reload(ctx)
		}
	}
}

// shutdown drains the server. A second termination signal while draining
// stops waiting and exits at once.
func shutdown(ctx context.Context, srv *server.Server, signals <-chan os.Signal, logger *slog.Logger, code int) int {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	drained := make(chan error, 1)
	go func() { drained <- srv.Shutdown(ctx) }()
	for {
		select {
		case err := <-drained:
			if err != nil {
				// The grace period ran out; the process still stops cleanly.
				logger.Error("drain incomplete", "error", err)
			}
			logger.Info("golb stopped")
			return code
		case sig := <-signals:
			if sig == syscall.SIGHUP {
				continue
			}
			logger.Error("second signal, exiting without draining", "signal", sig.String())
			return exitFailure
		}
	}
}
