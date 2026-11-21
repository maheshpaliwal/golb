// Package server owns the process-level pieces: listeners, TLS, the
// configuration snapshot, hot reload, the file watcher and graceful drain.
package server

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maheshpaliwal/golb/internal/accesslog"
	"github.com/maheshpaliwal/golb/internal/admin"
	"github.com/maheshpaliwal/golb/internal/config"
	"github.com/maheshpaliwal/golb/internal/core"
	"github.com/maheshpaliwal/golb/internal/metrics"
	"github.com/maheshpaliwal/golb/internal/proxy"
	"github.com/maheshpaliwal/golb/internal/tcpproxy"
)

const (
	defaultWatchInterval = time.Second

	// HTTP/2 lets one connection carry many requests; this caps how many a
	// single client can have open at once.
	maxConcurrentStreams = 250

	adminReadHeaderTimeout = 5 * time.Second
	adminReadTimeout       = 10 * time.Second
	adminWriteTimeout      = 90 * time.Second // a CPU profile takes 30s by default
	adminIdleTimeout       = 60 * time.Second
	adminMaxHeaderBytes    = 64 << 10
	adminShutdownTimeout   = 2 * time.Second

	resultOK    = "ok"
	resultError = "error"

	reloadHelp = "Configuration reloads by result."
)

var errDraining = errors.New("server is shutting down")

type Options struct {
	ConfigPath string
	Logger     *slog.Logger
	// AccessLog receives access log lines.
	AccessLog io.Writer
	// WatchInterval is how often the config file is checked for changes;
	// zero means the default and a negative value disables watching.
	WatchInterval time.Duration
}

// Server is a running golb instance.
type Server struct {
	path      string
	logger    *slog.Logger
	registry  *metrics.Registry
	accessLog *accesslog.Logger
	watch     time.Duration

	current atomic.Pointer[snapshot]

	// reloadMu serialises reloads with each other and with shutdown.
	reloadMu   sync.Mutex
	loadedHash [sha256.Size]byte
	draining   atomic.Bool

	ctx      context.Context
	cancel   context.CancelFunc
	routines sync.WaitGroup
	failures chan error

	frontends []*frontend
	admin     *frontend

	reloadOK    *metrics.Counter
	reloadError *metrics.Counter
	lastReload  *metrics.Gauge
}

// frontend is one bound listener and whatever serves it.
type frontend struct {
	name     string
	listener net.Listener
	http     *http.Server    // HTTP and admin listeners
	tcp      *tcpproxy.Proxy // TCP listeners
}

// New loads the configuration and binds every listener. Nothing is served
// until Start. ctx bounds the server's background work.
func New(ctx context.Context, opts Options) (*Server, error) {
	data, err := os.ReadFile(opts.ConfigPath)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return nil, err
	}

	s := &Server{
		path:       opts.ConfigPath,
		logger:     opts.Logger,
		registry:   metrics.NewRegistry(),
		watch:      opts.WatchInterval,
		loadedHash: sha256.Sum256(data),
		failures:   make(chan error, len(cfg.Listeners)+1),
	}
	if s.watch == 0 {
		s.watch = defaultWatchInterval
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.reloadOK = s.registry.Counter("golb_reload_total", reloadHelp, "result", resultOK)
	s.reloadError = s.registry.Counter("golb_reload_total", reloadHelp, "result", resultError)
	s.lastReload = s.registry.Gauge("golb_config_last_reload_success_timestamp_seconds",
		"Unix time of the last configuration load that succeeded.")
	s.accessLog = accesslog.New(accesslog.Options{
		Out:        opts.AccessLog,
		BufferSize: cfg.AccessLog.BufferSize,
		SampleRate: cfg.AccessLog.SampleRate,
		Dropped:    s.registry.Counter("golb_accesslog_dropped_total", "Access log records dropped because the queue was full."),
		Logger:     s.logger,
	})

	snap, err := s.build(cfg, nil)
	if err != nil {
		s.abandon(nil)
		return nil, err
	}
	s.current.Store(snap)
	err = s.bind(cfg)
	if err != nil {
		s.abandon(snap)
		return nil, err
	}
	s.lastReload.Set(time.Now().Unix())
	return s, nil
}

// abandon undoes a partly constructed server.
func (s *Server) abandon(snap *snapshot) {
	s.cancel()
	for _, fe := range s.frontends {
		s.closeListener(fe)
	}
	if s.admin != nil {
		s.closeListener(s.admin)
	}
	if snap != nil {
		snap.retire(nil)
	}
	s.accessLog.Close()
}

func (s *Server) closeListener(fe *frontend) {
	err := fe.listener.Close()
	if err != nil {
		s.logger.Error("close listener", "listener", fe.name, "error", err)
	}
}

func (s *Server) bind(cfg *config.Config) error {
	for i, listener := range cfg.Listeners {
		raw, err := net.Listen("tcp", listener.Addr)
		if err != nil {
			return fmt.Errorf("listener %q: listen: %w", listener.Name, err)
		}
		fe := &frontend{name: listener.Name, listener: raw}
		s.frontends = append(s.frontends, fe)
		if listener.Protocol == config.ProtocolTCP {
			fe.tcp = tcpproxy.New(s.ctx, func() *tcpproxy.Target { return s.current.Load().targets[i] }, s.logger)
			continue
		}
		if listener.TLS != nil {
			fe.listener = tls.NewListener(raw, &tls.Config{
				MinVersion: tls.VersionTLS12,
				NextProtos: []string{protoH2, protoHTTP1},
				// Certificates and the minimum version come from the current
				// snapshot, so a reload replaces them for new handshakes.
				GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
					return s.current.Load().tls[i], nil
				},
			})
		}
		fe.http = s.httpServer(i, listener)
	}

	if !cfg.Admin.Enabled() {
		return nil
	}
	raw, err := net.Listen("tcp", cfg.Admin.Addr)
	if err != nil {
		return fmt.Errorf("admin: listen: %w", err)
	}
	s.admin = &frontend{
		name:     "admin",
		listener: raw,
		http: &http.Server{
			Handler: admin.NewHandler(admin.Options{
				Source:   s,
				Registry: s.registry,
				Pprof:    cfg.Admin.Pprof,
				Logger:   s.logger,
			}),
			ReadHeaderTimeout: adminReadHeaderTimeout,
			ReadTimeout:       adminReadTimeout,
			WriteTimeout:      adminWriteTimeout,
			IdleTimeout:       adminIdleTimeout,
			MaxHeaderBytes:    adminMaxHeaderBytes,
			ErrorLog:          slog.NewLogLogger(s.logger.Handler(), slog.LevelError),
		},
	}
	return nil
}

func (s *Server) httpServer(index int, listener config.Listener) *http.Server {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(listener.H2C)
	source := func() *proxy.Router { return s.current.Load().routers[index] }
	return &http.Server{
		Handler: proxy.NewHandler(source, s.logger),
		// No ReadTimeout or WriteTimeout: they would cut off streaming
		// bodies and upgraded connections. Slow headers are bounded here,
		// slow upstreams by the pool's timeouts.
		ReadHeaderTimeout: listener.Timeouts.ReadHeader,
		IdleTimeout:       listener.Timeouts.Idle,
		MaxHeaderBytes:    listener.Timeouts.MaxHeaderBytes,
		Protocols:         protocols,
		HTTP2:             &http.HTTP2Config{MaxConcurrentStreams: maxConcurrentStreams},
		ErrorLog:          slog.NewLogLogger(s.logger.Handler(), slog.LevelError),
	}
}

// Start begins serving every listener and watching the config file.
func (s *Server) Start() {
	for _, fe := range s.frontends {
		s.serve(fe)
	}
	if s.admin != nil {
		s.serve(s.admin)
	}
	if s.watch > 0 {
		s.spawn("config watcher", s.watchFile)
	}
}

// Failed delivers the error of a listener that stopped serving on its own.
func (s *Server) Failed() <-chan error { return s.failures }

// Addr returns the bound address of a listener by name ("admin" for the
// admin listener), or nil. It matters when the config asks for port 0.
func (s *Server) Addr(name string) net.Addr {
	if s.admin != nil && name == s.admin.name {
		return s.admin.listener.Addr()
	}
	for _, fe := range s.frontends {
		if fe.name == name {
			return fe.listener.Addr()
		}
	}
	return nil
}

func (s *Server) serve(fe *frontend) {
	s.spawn("listener "+fe.name, func() {
		var err error
		if fe.tcp != nil {
			err = fe.tcp.Serve(fe.listener)
		} else {
			err = fe.http.Serve(fe.listener)
		}
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return
		}
		s.logger.Error("listener stopped", "listener", fe.name, "error", err)
		s.failures <- fmt.Errorf("listener %q: %w", fe.name, err)
	})
}

// spawn runs fn on a goroutine that Shutdown waits for.
func (s *Server) spawn(name string, fn func()) {
	s.routines.Add(1)
	go func() {
		defer s.routines.Done()
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error("background task stopped", "task", name, "panic", r)
			}
		}()
		fn()
	}()
}

// Ready reports whether the server is accepting new work.
func (s *Server) Ready() bool { return !s.draining.Load() }

// Backends lists every backend of the running configuration.
func (s *Server) Backends() []admin.Backend { return s.current.Load().backends() }

// Reload re-reads the config file and, if it is valid, switches to it
// without interrupting requests. On any error the running configuration
// stays in force.
func (s *Server) Reload(ctx context.Context) error {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		return s.reloadFailed(fmt.Errorf("read config: %w", err))
	}
	return s.apply(ctx, data)
}

// reloadIfChanged is Reload for the file watcher: a file whose content was
// already tried, successfully or not, is left alone.
func (s *Server) reloadIfChanged(ctx context.Context) {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		// Editors and deploy tools replace files by rename; the next poll retries.
		return
	}
	if sha256.Sum256(data) == s.loadedHash {
		return
	}
	// The error is already logged and counted by apply.
	_ = s.apply(ctx, data)
}

func (s *Server) apply(ctx context.Context, data []byte) error {
	if s.draining.Load() {
		return errDraining
	}
	s.loadedHash = sha256.Sum256(data)
	err := ctx.Err()
	if err != nil {
		return s.reloadFailed(err)
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return s.reloadFailed(err)
	}
	running := s.current.Load()
	err = sameListeners(running.cfg, cfg)
	if err != nil {
		return s.reloadFailed(err)
	}
	next, err := s.build(cfg, running)
	if err != nil {
		return s.reloadFailed(err)
	}
	if cfg.AccessLog.BufferSize != running.cfg.AccessLog.BufferSize {
		s.logger.Error("access_log buffer_size is fixed at start-up; the change takes effect after a restart")
	}
	s.accessLog.SetSampleRate(cfg.AccessLog.SampleRate)
	s.current.Store(next)
	running.retire(next)
	s.reloadOK.Inc()
	s.lastReload.Set(time.Now().Unix())
	s.logger.Info("configuration reloaded", "path", s.path)
	return nil
}

func (s *Server) reloadFailed(err error) error {
	s.reloadError.Inc()
	s.logger.Error("reload rejected, running configuration unchanged", "path", s.path, "error", err)
	return err
}

// Shutdown stops accepting, lets in-flight requests and TCP connections
// finish within the configured shutdown_grace, closes what is left and
// flushes the access log. A cancelled ctx cuts the grace period short.
func (s *Server) Shutdown(ctx context.Context) error {
	// Taking the lock waits out a reload in progress; every later reload
	// sees draining and is refused, so the snapshot read here is the last.
	s.reloadMu.Lock()
	already := s.draining.Swap(true)
	s.reloadMu.Unlock()
	if already {
		return nil
	}
	snap := s.current.Load()
	graceCtx, cancel := context.WithTimeout(ctx, snap.cfg.ShutdownGrace)
	defer cancel()

	errs := make([]error, len(s.frontends))
	var drains sync.WaitGroup
	for i, fe := range s.frontends {
		drains.Add(1)
		go func() {
			defer drains.Done()
			errs[i] = s.drain(graceCtx, fe)
		}()
	}
	drains.Wait()

	if s.admin != nil {
		adminCtx, cancelAdmin := context.WithTimeout(ctx, adminShutdownTimeout)
		errs = append(errs, s.drain(adminCtx, s.admin))
		cancelAdmin()
	}
	s.cancel()
	s.routines.Wait()
	snap.retire(nil)
	s.accessLog.Close()
	return errors.Join(errs...)
}

// drain shuts one listener down gracefully and, if the context ends first,
// closes its remaining connections.
func (s *Server) drain(ctx context.Context, fe *frontend) error {
	if fe.tcp != nil {
		err := fe.tcp.Shutdown(ctx)
		if err != nil {
			return fmt.Errorf("listener %q: connections closed at the deadline: %w", fe.name, err)
		}
		return nil
	}
	err := fe.http.Shutdown(ctx)
	if err == nil {
		return nil
	}
	closeErr := fe.http.Close()
	if closeErr != nil {
		s.logger.Error("close listener", "listener", fe.name, "error", closeErr)
	}
	return fmt.Errorf("listener %q: requests cut off at the deadline: %w", fe.name, err)
}

// watchFile polls the config file's size and modification time. A change is
// acted on one poll later, once the file has stopped changing, so a burst
// of writes causes one reload.
func (s *Server) watchFile() {
	ticker := time.NewTicker(s.watch)
	defer ticker.Stop()
	var seen fileStamp
	info, err := os.Stat(s.path)
	if err == nil {
		seen = stampOf(info)
	}
	pending := false
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
		}
		info, err = os.Stat(s.path)
		if err != nil {
			continue
		}
		stamp := stampOf(info)
		if stamp != seen {
			seen = stamp
			pending = true
			continue
		}
		if !pending {
			continue
		}
		pending = false
		s.reloadIfChanged(s.ctx)
	}
}

type fileStamp struct {
	size    int64
	modTime time.Time
}

func stampOf(info os.FileInfo) fileStamp {
	return fileStamp{size: info.Size(), modTime: info.ModTime()}
}

// observe receives backend state changes from health checking. It runs on
// checker and request goroutines; registration is a map lookup for a series
// that build already created.
func (s *Server) observe(ev core.Event) {
	switch ev.Kind {
	case core.BackendUp:
		s.logger.Info("backend up", "pool", ev.Pool, "backend", ev.Backend.Addr)
	case core.BackendDown:
		s.logger.Error("backend down", "pool", ev.Pool, "backend", ev.Backend.Addr)
	case core.BackendEjected:
		s.ejections(ev.Pool, ev.Backend.Addr).Inc()
		s.logger.Error("backend ejected", "pool", ev.Pool, "backend", ev.Backend.Addr)
	}
}

func (s *Server) ejections(pool, addr string) *metrics.Counter {
	return s.registry.Counter("golb_backend_ejections_total", "Times a backend was ejected by outlier detection.",
		"pool", pool, "backend", addr)
}

func (s *Server) registerBackendGauges(name string, pool core.Pool) {
	for _, b := range pool.Backends() {
		s.ejections(name, b.Addr)
		s.registry.GaugeFunc("golb_backend_inflight", "Requests and connections in flight per backend.",
			func() float64 { return float64(b.Inflight()) }, "pool", name, "backend", b.Addr)
		s.registry.GaugeFunc("golb_backend_healthy", "1 when the backend passes its active health check, else 0.",
			func() float64 {
				if b.Healthy() {
					return 1
				}
				return 0
			}, "pool", name, "backend", b.Addr)
	}
}
