package server

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/maheshpaliwal/golb/internal/accesslog"
	"github.com/maheshpaliwal/golb/internal/admin"
	"github.com/maheshpaliwal/golb/internal/config"
	"github.com/maheshpaliwal/golb/internal/core"
	"github.com/maheshpaliwal/golb/internal/proxy"
	"github.com/maheshpaliwal/golb/internal/tcpproxy"
	"github.com/maheshpaliwal/golb/internal/upstream"
)

const (
	protoH2    = "h2"
	protoHTTP1 = "http/1.1"
)

// snapshot is all runtime state derived from one configuration. It is
// immutable once published; a reload builds a new one and swaps the pointer.
type snapshot struct {
	cfg       *config.Config
	pools     map[string]core.Pool
	upstreams map[string]*proxy.Upstream

	// Indexed like cfg.Listeners; only the entry matching the protocol is set.
	routers []*proxy.Router
	targets []*tcpproxy.Target
	tls     []*tls.Config
}

// build turns a validated configuration into a snapshot, carrying backend
// state, transports, retry budgets and rate limiters over from prev (nil at
// start-up). On error nothing it started is left running.
func (s *Server) build(cfg *config.Config, prev *snapshot) (*snapshot, error) {
	next := &snapshot{
		cfg:       cfg,
		pools:     make(map[string]core.Pool, len(cfg.Pools)),
		upstreams: make(map[string]*proxy.Upstream, len(cfg.Pools)),
		routers:   make([]*proxy.Router, len(cfg.Listeners)),
		targets:   make([]*tcpproxy.Target, len(cfg.Listeners)),
		tls:       make([]*tls.Config, len(cfg.Listeners)),
	}
	// Certificates first: a bad file must not cost a round of health checkers.
	for i, listener := range cfg.Listeners {
		if listener.TLS == nil {
			continue
		}
		tlsConfig, err := loadTLS(*listener.TLS)
		if err != nil {
			return nil, fmt.Errorf("listener %q: %w", listener.Name, err)
		}
		next.tls[i] = tlsConfig
	}

	byName := make(map[string]config.Pool, len(cfg.Pools))
	for _, poolCfg := range cfg.Pools {
		var prevPool core.Pool
		var prevUpstream *proxy.Upstream
		if prev != nil {
			// A missing key must stay a nil interface, not a typed nil.
			if old, ok := prev.pools[poolCfg.Name]; ok {
				prevPool = old
			}
			prevUpstream = prev.upstreams[poolCfg.Name]
		}
		pool, err := upstream.New(poolCfg.PoolSpec, prevPool, s.observe)
		if err != nil {
			next.closePools()
			return nil, fmt.Errorf("build pool %q: %w", poolCfg.Name, err)
		}
		next.pools[poolCfg.Name] = pool
		next.upstreams[poolCfg.Name] = proxy.NewUpstream(proxy.UpstreamOptions{
			Config:   poolCfg,
			Pool:     pool,
			Registry: s.registry,
			Prev:     prevUpstream,
		})
		byName[poolCfg.Name] = poolCfg
		s.registerBackendGauges(poolCfg.Name, pool)
	}

	var log *accesslog.Logger
	if cfg.AccessLog.Enabled {
		log = s.accessLog
	}
	for i, listener := range cfg.Listeners {
		if listener.Protocol == config.ProtocolTCP {
			next.targets[i] = tcpproxy.NewTarget(tcpproxy.TargetOptions{
				Listener: listener,
				Config:   byName[listener.Pool],
				Pool:     next.pools[listener.Pool],
				Registry: s.registry,
			})
			continue
		}
		var prevRouter *proxy.Router
		if prev != nil {
			prevRouter = prev.routers[i]
		}
		next.routers[i] = proxy.NewRouter(proxy.RouterOptions{
			Listener:  listener,
			Upstreams: next.upstreams,
			Registry:  s.registry,
			Log:       log,
			Prev:      prevRouter,
		})
	}
	return next, nil
}

// retire stops what next (the snapshot replacing this one, or nil at
// shutdown) no longer uses. Requests still running on this snapshot finish
// normally: a closed pool keeps accepting Done.
func (snap *snapshot) retire(next *snapshot) {
	snap.closePools()
	for name, old := range snap.upstreams {
		var successor *proxy.Upstream
		if next != nil {
			successor = next.upstreams[name]
		}
		old.Retire(successor)
	}
}

// forget removes the metric series of backends and pools that next no
// longer has, so a scrape does not report them for ever.
func (s *Server) forget(old, next *snapshot) {
	for name, pool := range old.pools {
		kept := make(map[string]bool)
		if successor, ok := next.pools[name]; ok {
			for _, b := range successor.Backends() {
				kept[b.Addr] = true
			}
		}
		for _, b := range pool.Backends() {
			if kept[b.Addr] {
				continue
			}
			s.registry.Delete(metricBackendInflight, "pool", name, "backend", b.Addr)
			s.registry.Delete(metricBackendHealthy, "pool", name, "backend", b.Addr)
			s.registry.Delete(metricBackendEjections, "pool", name, "backend", b.Addr)
		}
		old.upstreams[name].Forget(next.upstreams[name])
	}
}

// watched lists the files whose content decides what is running: the
// certificates and keys the configuration points at.
func (snap *snapshot) watched() []string {
	var paths []string
	for _, listener := range snap.cfg.Listeners {
		if listener.TLS == nil {
			continue
		}
		for _, files := range listener.TLS.Certificates {
			paths = append(paths, files.CertFile, files.KeyFile)
		}
	}
	return paths
}

func (snap *snapshot) closePools() {
	for _, pool := range snap.pools {
		pool.Close()
	}
}

func (snap *snapshot) backends() []admin.Backend {
	now := time.Now().UnixNano()
	var rows []admin.Backend
	for _, poolCfg := range snap.cfg.Pools {
		for _, b := range snap.pools[poolCfg.Name].Backends() {
			rows = append(rows, admin.Backend{
				Pool:      poolCfg.Name,
				Addr:      b.Addr,
				Weight:    b.Weight,
				Healthy:   b.Healthy(),
				Ejected:   b.Ejected(now),
				Inflight:  b.Inflight(),
				LatencyMs: float64(b.Latency()) / float64(time.Millisecond),
			})
		}
	}
	return rows
}

func loadTLS(cfg config.TLS) (*tls.Config, error) {
	out := &tls.Config{
		MinVersion: cfg.Version(),
		NextProtos: []string{protoH2, protoHTTP1},
	}
	for _, files := range cfg.Certificates {
		cert, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load certificate %s: %w", files.CertFile, err)
		}
		out.Certificates = append(out.Certificates, cert)
	}
	return out, nil
}

// sameListeners rejects a reload that would need a socket to be opened,
// closed or re-created: those settings are fixed when the listener starts.
func sameListeners(running, next *config.Config) error {
	var errs []error
	if running.Admin != next.Admin {
		errs = append(errs, errors.New("admin settings cannot change on reload"))
	}
	if len(running.Listeners) != len(next.Listeners) {
		errs = append(errs, fmt.Errorf("listeners cannot be added or removed on reload (running %d, file has %d)",
			len(running.Listeners), len(next.Listeners)))
		return errors.Join(errs...)
	}
	for i, old := range running.Listeners {
		updated := next.Listeners[i]
		if old.Name != updated.Name || old.Addr != updated.Addr || old.Protocol != updated.Protocol {
			errs = append(errs, fmt.Errorf("listener %d (%q): name, addr and protocol cannot change on reload", i, old.Name))
			continue
		}
		if (old.TLS == nil) != (updated.TLS == nil) || old.H2C != updated.H2C {
			errs = append(errs, fmt.Errorf("listener %q: tls and h2c cannot be switched on or off on reload", old.Name))
		}
		if old.Protocol != config.ProtocolHTTP {
			continue
		}
		before, after := old.Timeouts, updated.Timeouts
		if before.ReadHeader != after.ReadHeader || before.Idle != after.Idle || before.MaxHeaderBytes != after.MaxHeaderBytes {
			errs = append(errs, fmt.Errorf("listener %q: read_header, idle and max_header_bytes cannot change on reload", old.Name))
		}
	}
	return errors.Join(errs...)
}
