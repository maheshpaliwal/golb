package health

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

const (
	userAgent    = "golb-healthcheck"
	minOKStatus  = http.StatusOK
	maxOKStatus  = 399
	probeNetwork = "tcp"
)

var errStatus = errors.New("unhealthy status")

type CheckerConfig struct {
	Pool     string
	Spec     core.HealthCheck // type http or tcp, defaults applied, valid
	Backends []*core.Backend
	Observer core.Observer // required
}

// Checker probes every backend on its own goroutine and flips the backend's
// health verdict after enough consecutive results.
type Checker struct {
	pool     string
	spec     core.HealthCheck
	observer core.Observer
	probe    func(ctx context.Context, target string) error
	client   *http.Client
	dialer   net.Dialer

	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// streak counts consecutive probe results for one backend.
type streak struct{ oks, fails int }

// StartChecker starts one probing goroutine per backend. Close stops them.
func StartChecker(cfg CheckerConfig) *Checker {
	c := &Checker{
		pool:     cfg.Pool,
		spec:     cfg.Spec,
		observer: cfg.Observer,
		dialer:   net.Dialer{Timeout: cfg.Spec.Timeout},
	}
	// A pool is built at configuration time and outlives every request, so
	// there is no caller context to inherit; the checker owns this one and
	// Close cancels it.
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	switch cfg.Spec.Type {
	case core.CheckHTTP:
		c.client = c.newClient()
		c.probe = c.probeHTTP
	case core.CheckTCP:
		c.probe = c.probeTCP
	default:
		return c
	}
	for _, b := range cfg.Backends {
		c.wg.Go(func() { c.run(ctx, b) })
	}
	return c
}

// Close stops probing and waits for every goroutine to exit. Idempotent.
func (c *Checker) Close() {
	c.closeOnce.Do(func() {
		c.cancel()
		c.wg.Wait()
		if c.client != nil {
			c.client.CloseIdleConnections()
		}
	})
}

// newClient builds a client used by this checker only. Keep-alives are off
// so every probe also proves the backend still accepts connections, and so
// no idle connection outlives the pool. Redirects are not followed: a 3xx is
// itself a healthy answer.
func (c *Checker) newClient() *http.Client {
	return &http.Client{
		Timeout: c.spec.Timeout,
		Transport: &http.Transport{
			DialContext:           c.dialer.DialContext,
			DisableKeepAlives:     true,
			DisableCompression:    true,
			ResponseHeaderTimeout: c.spec.Timeout,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func (c *Checker) run(ctx context.Context, b *core.Backend) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("health check goroutine panicked", "pool", c.pool, "backend", b.Addr, "panic", r)
		}
	}()

	// The first probe is delayed by a random part of the interval so the
	// backends of a pool, and pools rebuilt by one reload, are not probed in
	// the same instant.
	if !wait(ctx, rand.N(c.spec.Interval)) {
		return
	}
	target := c.target(b)
	ticker := time.NewTicker(c.spec.Interval)
	defer ticker.Stop()
	var results streak
	for {
		err := c.probeOnce(ctx, target)
		if ctx.Err() != nil {
			return
		}
		c.apply(b, &results, err)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Checker) probeOnce(ctx context.Context, target string) error {
	ctx, cancel := context.WithTimeout(ctx, c.spec.Timeout)
	defer cancel()
	return c.probe(ctx, target)
}

func (c *Checker) apply(b *core.Backend, results *streak, probeErr error) {
	healthy := results.next(probeErr == nil, b.Healthy(), c.spec)
	if !b.SetHealthy(healthy) {
		return
	}
	kind := core.BackendUp
	if !healthy {
		kind = core.BackendDown
	}
	c.observer(core.Event{Pool: c.pool, Backend: b, Kind: kind})
	slog.Info("backend health changed", "pool", c.pool, "backend", b.Addr, "healthy", healthy, "cause", probeErr)
}

// next records one probe result and returns the verdict that follows from
// it: the current one until the opposite result has repeated threshold
// times in a row.
func (s *streak) next(ok, healthy bool, spec core.HealthCheck) bool {
	if ok {
		s.fails = 0
		s.oks++
		return healthy || s.oks >= spec.HealthyThreshold
	}
	s.oks = 0
	s.fails++
	return healthy && s.fails < spec.UnhealthyThreshold
}

func (c *Checker) target(b *core.Backend) string {
	if c.spec.Type == core.CheckTCP {
		return b.Addr
	}
	path := c.spec.Path
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return "http://" + b.Addr + path
}

func (c *Checker) probeHTTP(ctx context.Context, target string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("build probe request: %w", err)
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	err = resp.Body.Close()
	if err != nil {
		return fmt.Errorf("close probe response: %w", err)
	}
	if resp.StatusCode < minOKStatus || resp.StatusCode > maxOKStatus {
		return fmt.Errorf("%w %d", errStatus, resp.StatusCode)
	}
	return nil
}

func (c *Checker) probeTCP(ctx context.Context, target string) error {
	conn, err := c.dialer.DialContext(ctx, probeNetwork, target)
	if err != nil {
		return err
	}
	return conn.Close()
}

// wait sleeps for d and reports false when ctx ended first.
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
