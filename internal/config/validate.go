package config

import (
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/maheshpaliwal/golb/internal/core"
)

const (
	localhost      = "localhost"
	wildcardPrefix = "*."
	maxPort        = 65535
)

// validate reports every problem at once; each message names the listener,
// route or pool it belongs to.
func (c *Config) validate() error {
	var errs []error
	errs = append(errs, c.Admin.validate(), c.AccessLog.validate())
	if c.ShutdownGrace < 0 {
		errs = append(errs, errors.New("shutdown_grace is negative"))
	}

	pools := make(map[string]bool, len(c.Pools))
	for i := range c.Pools {
		pool := &c.Pools[i]
		errs = append(errs, pool.validate())
		if pools[pool.Name] {
			errs = append(errs, fmt.Errorf("pool %q: duplicate name", pool.Name))
		}
		pools[pool.Name] = true
	}

	if len(c.Listeners) == 0 {
		errs = append(errs, errors.New("no listeners"))
	}
	names := make(map[string]bool, len(c.Listeners))
	addrs := make(map[string]string, len(c.Listeners)+1)
	if c.Admin.Enabled() {
		addrs[c.Admin.Addr] = "admin"
	}
	for i := range c.Listeners {
		listener := &c.Listeners[i]
		errs = append(errs, listener.validate(pools))
		if names[listener.Name] {
			errs = append(errs, fmt.Errorf("listener %q: duplicate name", listener.Name))
		}
		names[listener.Name] = true
		other, taken := addrs[listener.Addr]
		// Port 0 asks the kernel for a free port, so two such addresses never collide.
		if taken && !ephemeral(listener.Addr) {
			errs = append(errs, fmt.Errorf("listener %q: address %s already used by %s", listener.Name, listener.Addr, other))
		}
		addrs[listener.Addr] = "listener " + strconv.Quote(listener.Name)
	}
	return errors.Join(errs...)
}

func (a Admin) validate() error {
	if !a.Enabled() {
		return nil
	}
	host, err := explicitHost(a.Addr)
	if err != nil {
		return fmt.Errorf("admin: %w", err)
	}
	if a.AllowNonLoopback || isLoopback(host) {
		return nil
	}
	return fmt.Errorf("admin: address %s is not loopback; set allow_non_loopback: true to expose it", a.Addr)
}

func (a AccessLog) validate() error {
	var errs []error
	// Written as a positive test so NaN fails it.
	if !(a.SampleRate >= 0 && a.SampleRate <= 1) {
		errs = append(errs, fmt.Errorf("access_log: sample_rate %v outside 0..1", a.SampleRate))
	}
	if a.BufferSize < 1 {
		errs = append(errs, fmt.Errorf("access_log: buffer_size %d must be at least 1", a.BufferSize))
	}
	return errors.Join(errs...)
}

func (l *Listener) validate(pools map[string]bool) error {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("listener %q: "+format, append([]any{l.Name}, args...)...))
	}
	if l.Name == "" {
		fail("name is empty")
	}
	_, err := explicitHost(l.Addr)
	if err != nil {
		fail("%v", err)
	}
	t := l.Timeouts
	if t.ReadHeader <= 0 || t.Idle <= 0 || t.Request < 0 || t.BodyIdle < 0 || t.MaxHeaderBytes <= 0 {
		fail("timeouts need read_header > 0, idle > 0, request >= 0, body_idle >= 0 and max_header_bytes > 0")
	}

	switch l.Protocol {
	case ProtocolHTTP:
		if l.Pool != "" {
			fail("pool is for tcp listeners; http listeners use routes")
		}
		if len(l.Routes) == 0 {
			fail("no routes")
		}
		if l.H2C && l.TLS != nil {
			fail("h2c and tls are mutually exclusive")
		}
		errs = append(errs, l.validateRoutes(pools), l.TLS.validate(l.Name))
	case ProtocolTCP:
		if len(l.Routes) > 0 {
			fail("routes are for http listeners; tcp listeners use pool")
		}
		if l.TLS != nil || l.H2C {
			fail("tls and h2c are for http listeners")
		}
		if !pools[l.Pool] {
			fail("unknown pool %q", l.Pool)
		}
	default:
		fail("protocol %q is not %q or %q", l.Protocol, ProtocolHTTP, ProtocolTCP)
	}
	return errors.Join(errs...)
}

func (l *Listener) validateRoutes(pools map[string]bool) error {
	var errs []error
	seen := make(map[string]bool, len(l.Routes))
	for i, route := range l.Routes {
		fail := func(format string, args ...any) {
			prefix := fmt.Sprintf("listener %q route %d (%s): ", l.Name, i, route.Label())
			errs = append(errs, fmt.Errorf(prefix+format, args...))
		}
		if !pools[route.Pool] {
			fail("unknown pool %q", route.Pool)
		}
		if !strings.HasPrefix(route.PathPrefix, "/") {
			fail("path_prefix must start with /")
		}
		if !validRouteHost(route.Host) {
			fail("host must be a hostname or *.suffix, without a port or trailing dot")
		}
		// "/api" and "/api/" select the same requests, so they are one rule.
		key := strings.ToLower(route.Host) + " " + strings.TrimRight(route.PathPrefix, "/")
		if seen[key] {
			fail("duplicate host and path_prefix")
		}
		seen[key] = true
		limit := route.RateLimit
		if limit != nil && !validRate(limit.RequestsPerSecond, limit.Burst) {
			fail("rate_limit needs a finite requests_per_second > 0 and burst >= 1")
		}
	}
	return errors.Join(errs...)
}

// Label identifies a route in metrics and logs. It is built only from
// configuration, never from request data.
func (r Route) Label() string {
	return strings.ToLower(r.Host) + r.PathPrefix
}

func validRouteHost(host string) bool {
	if host == "" {
		return true
	}
	name := strings.TrimPrefix(host, wildcardPrefix)
	// Request hosts are compared without their trailing dot, so a rule
	// written with one could never match.
	return name != "" && !strings.ContainsAny(name, "*:/ ") && !strings.HasSuffix(name, ".")
}

func validRate(perSecond float64, burst int) bool {
	return perSecond > 0 && !math.IsInf(perSecond, 0) && burst >= 1
}

func ephemeral(addr string) bool {
	_, port, err := net.SplitHostPort(addr)
	return err == nil && port == "0"
}

func (t *TLS) validate(listener string) error {
	if t == nil {
		return nil
	}
	var errs []error
	if t.MinVersion != TLS12 && t.MinVersion != TLS13 {
		errs = append(errs, fmt.Errorf("listener %q: tls min_version %q is not %q or %q", listener, t.MinVersion, TLS12, TLS13))
	}
	if len(t.Certificates) == 0 {
		errs = append(errs, fmt.Errorf("listener %q: tls has no certificates", listener))
	}
	for _, cert := range t.Certificates {
		_, err := tls.LoadX509KeyPair(cert.CertFile, cert.KeyFile)
		if err != nil {
			errs = append(errs, fmt.Errorf("listener %q: load certificate %s: %w", listener, cert.CertFile, err))
		}
	}
	return errors.Join(errs...)
}

func (p *Pool) validate() error {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("pool %q: "+format, append([]any{p.Name}, args...)...))
	}
	errs = append(errs, p.PoolSpec.Validate())
	if p.Retry.MaxAttempts < 1 || p.Retry.MaxAttempts > maxAttemptsLimit {
		fail("retry max_attempts %d outside 1..%d", p.Retry.MaxAttempts, maxAttemptsLimit)
	}
	if p.Retry.PerTryTimeout <= 0 {
		fail("retry per_try_timeout must be positive")
	}
	if p.Retry.BudgetPercent < 0 || p.Retry.BudgetPercent > 100 {
		fail("retry budget_percent %d outside 0..100", p.Retry.BudgetPercent)
	}
	for _, status := range p.Retry.RetryOn {
		if status < http.StatusInternalServerError || status > 599 {
			fail("retry_on status %d outside 500..599", status)
		}
	}
	if p.HashHeader != "" && p.Policy != core.RingHash {
		fail("hash_header needs policy %q", core.RingHash)
	}
	tr := p.Transport
	if tr.DialTimeout <= 0 || tr.MaxIdleConnsPerHost < 1 || tr.IdleConnTimeout <= 0 {
		fail("transport needs dial_timeout > 0, max_idle_conns_per_host >= 1 and idle_conn_timeout > 0")
	}
	return errors.Join(errs...)
}

// explicitHost returns the host of a host:port address. An empty host would
// bind every interface, which golb only does when it is spelled out.
func explicitHost(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("address %q is not host:port", addr)
	}
	if host == "" {
		return "", fmt.Errorf("address %q has no host; write the interface explicitly, for example 127.0.0.1:%s", addr, port)
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > maxPort {
		return "", fmt.Errorf("address %q has an invalid port", addr)
	}
	return host, nil
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, localhost) {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
