// Package config defines the golb configuration file: its structure, strict
// loading, defaults and validation.
package config

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/maheshpaliwal/golb/internal/core"
)

const (
	ProtocolHTTP = "http"
	ProtocolTCP  = "tcp"

	// AdminOff as the admin address disables the admin listener.
	AdminOff = "off"

	TLS12 = "1.2"
	TLS13 = "1.3"
)

const (
	defaultAdminAddr       = "127.0.0.1:9901"
	defaultShutdownGrace   = 30 * time.Second
	defaultSampleRate      = 1.0
	defaultLogBuffer       = 8192
	defaultReadHeader      = 5 * time.Second
	defaultIdle            = 60 * time.Second
	defaultRequest         = 60 * time.Second
	defaultBodyIdle        = 60 * time.Second
	defaultMaxHeaderBytes  = 64 << 10
	defaultPathPrefix      = "/"
	defaultMaxAttempts     = 3
	defaultPerTryTimeout   = 10 * time.Second
	defaultBudgetPercent   = 20
	defaultDialTimeout     = 2 * time.Second
	defaultMaxIdlePerHost  = 256
	defaultIdleConnTimeout = 90 * time.Second

	// maxAttemptsLimit bounds the per-request list of tried backends.
	maxAttemptsLimit = 10
)

var defaultRetryOn = []int{502, 503, 504}

type Config struct {
	Admin         Admin         `yaml:"admin"`
	AccessLog     AccessLog     `yaml:"access_log"`
	ShutdownGrace time.Duration `yaml:"shutdown_grace"`
	Listeners     []Listener    `yaml:"listeners"`
	Pools         []Pool        `yaml:"pools"`
}

type Admin struct {
	Addr             string `yaml:"addr"`
	Pprof            bool   `yaml:"pprof"`
	AllowNonLoopback bool   `yaml:"allow_non_loopback"`
}

func (a Admin) Enabled() bool { return a.Addr != AdminOff }

type AccessLog struct {
	Enabled    bool    `yaml:"enabled"`
	SampleRate float64 `yaml:"sample_rate"`
	BufferSize int     `yaml:"buffer_size"`

	// sampleRateSet distinguishes an explicit 0 from an omitted key.
	sampleRateSet bool
}

type Listener struct {
	Name     string   `yaml:"name"`
	Addr     string   `yaml:"addr"`
	Protocol string   `yaml:"protocol"`
	TLS      *TLS     `yaml:"tls"`
	H2C      bool     `yaml:"h2c"`
	Timeouts Timeouts `yaml:"timeouts"`
	Routes   []Route  `yaml:"routes"`
	Pool     string   `yaml:"pool"`
}

type TLS struct {
	Certificates []Certificate `yaml:"certificates"`
	MinVersion   string        `yaml:"min_version"`
}

// Version maps the configured minimum to the crypto/tls constant.
func (t TLS) Version() uint16 {
	if t.MinVersion == TLS13 {
		return tls.VersionTLS13
	}
	return tls.VersionTLS12
}

type Certificate struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type Timeouts struct {
	ReadHeader     time.Duration `yaml:"read_header"`
	Idle           time.Duration `yaml:"idle"`
	Request        time.Duration `yaml:"request"`
	BodyIdle       time.Duration `yaml:"body_idle"`
	MaxHeaderBytes int           `yaml:"max_header_bytes"`

	// The set flags distinguish an explicit 0 (no limit) from an omitted key.
	requestSet  bool
	bodyIdleSet bool
}

type Route struct {
	Host       string     `yaml:"host"`
	PathPrefix string     `yaml:"path_prefix"`
	Pool       string     `yaml:"pool"`
	RateLimit  *RateLimit `yaml:"rate_limit"`
}

type RateLimit struct {
	RequestsPerSecond float64 `yaml:"requests_per_second"`
	Burst             int     `yaml:"burst"`
}

type Pool struct {
	core.PoolSpec `yaml:",inline"`
	HashHeader    string    `yaml:"hash_header"`
	Retry         Retry     `yaml:"retry"`
	Transport     Transport `yaml:"transport"`
}

type Retry struct {
	MaxAttempts   int           `yaml:"max_attempts"`
	PerTryTimeout time.Duration `yaml:"per_try_timeout"`
	BudgetPercent int           `yaml:"budget_percent"`
	RetryOn       []int         `yaml:"retry_on"`

	// budgetSet distinguishes "budget_percent: 0" (floor only) from an omitted key.
	budgetSet bool
}

type Transport struct {
	DialTimeout         time.Duration `yaml:"dial_timeout"`
	MaxIdleConnsPerHost int           `yaml:"max_idle_conns_per_host"`
	IdleConnTimeout     time.Duration `yaml:"idle_conn_timeout"`
}

// UnmarshalYAML records whether sample_rate was written, so 0 can mean "log nothing".
func (a *AccessLog) UnmarshalYAML(node *yaml.Node) error {
	type plain AccessLog
	err := decodeKnown(node, (*plain)(a))
	if err != nil {
		return err
	}
	a.sampleRateSet = hasKey(node, "sample_rate")
	return nil
}

// UnmarshalYAML records whether request and body_idle were written, so 0 can
// mean "no limit".
func (t *Timeouts) UnmarshalYAML(node *yaml.Node) error {
	type plain Timeouts
	err := decodeKnown(node, (*plain)(t))
	if err != nil {
		return err
	}
	t.requestSet = hasKey(node, "request")
	t.bodyIdleSet = hasKey(node, "body_idle")
	return nil
}

// UnmarshalYAML records whether budget_percent was written, so 0 can mean "floor only".
func (r *Retry) UnmarshalYAML(node *yaml.Node) error {
	type plain Retry
	err := decodeKnown(node, (*plain)(r))
	if err != nil {
		return err
	}
	r.budgetSet = hasKey(node, "budget_percent")
	return nil
}

// Load reads, strictly parses, defaults and validates the file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(data)
}

// Parse is Load for bytes already in memory.
func Parse(data []byte) (*Config, error) {
	cfg := new(Config)
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	err := dec.Decode(cfg)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	var extra yaml.Node
	err = dec.Decode(&extra)
	if !errors.Is(err, io.EOF) {
		return nil, errors.New("parse config: more than one YAML document")
	}
	cfg.applyDefaults()
	err = cfg.validate()
	if err != nil {
		return nil, fmt.Errorf("invalid config:\n%w", err)
	}
	return cfg, nil
}

// decodeKnown decodes a mapping node rejecting unknown keys. A custom
// UnmarshalYAML bypasses the decoder's KnownFields setting, so the strictness
// has to be re-applied here.
func decodeKnown(node *yaml.Node, out any) error {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	err := enc.Encode(node)
	if err != nil {
		return err
	}
	err = enc.Close()
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(&buf)
	dec.KnownFields(true)
	err = dec.Decode(out)
	if err != nil {
		return fmt.Errorf("line %d: %w", node.Line, err)
	}
	return nil
}

func hasKey(node *yaml.Node, key string) bool {
	if node.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return true
		}
	}
	return false
}

func (c *Config) applyDefaults() {
	c.Admin.Addr = orDefault(c.Admin.Addr, defaultAdminAddr)
	c.ShutdownGrace = orDefault(c.ShutdownGrace, defaultShutdownGrace)
	if !c.AccessLog.sampleRateSet {
		c.AccessLog.SampleRate = defaultSampleRate
	}
	c.AccessLog.BufferSize = orDefault(c.AccessLog.BufferSize, defaultLogBuffer)
	for i := range c.Listeners {
		c.Listeners[i].applyDefaults()
	}
	for i := range c.Pools {
		c.Pools[i].applyDefaults()
	}
}

func (l *Listener) applyDefaults() {
	l.Timeouts.ReadHeader = orDefault(l.Timeouts.ReadHeader, defaultReadHeader)
	l.Timeouts.Idle = orDefault(l.Timeouts.Idle, defaultIdle)
	l.Timeouts.MaxHeaderBytes = orDefault(l.Timeouts.MaxHeaderBytes, defaultMaxHeaderBytes)
	if !l.Timeouts.requestSet {
		l.Timeouts.Request = defaultRequest
	}
	if !l.Timeouts.bodyIdleSet {
		l.Timeouts.BodyIdle = defaultBodyIdle
	}
	if l.TLS != nil {
		l.TLS.MinVersion = orDefault(l.TLS.MinVersion, TLS12)
	}
	for i := range l.Routes {
		l.Routes[i].PathPrefix = orDefault(l.Routes[i].PathPrefix, defaultPathPrefix)
	}
}

func (p *Pool) applyDefaults() {
	p.PoolSpec.ApplyDefaults()
	p.Retry.MaxAttempts = orDefault(p.Retry.MaxAttempts, defaultMaxAttempts)
	p.Retry.PerTryTimeout = orDefault(p.Retry.PerTryTimeout, defaultPerTryTimeout)
	if !p.Retry.budgetSet {
		p.Retry.BudgetPercent = defaultBudgetPercent
	}
	if p.Retry.RetryOn == nil {
		p.Retry.RetryOn = defaultRetryOn
	}
	p.Transport.DialTimeout = orDefault(p.Transport.DialTimeout, defaultDialTimeout)
	p.Transport.MaxIdleConnsPerHost = orDefault(p.Transport.MaxIdleConnsPerHost, defaultMaxIdlePerHost)
	p.Transport.IdleConnTimeout = orDefault(p.Transport.IdleConnTimeout, defaultIdleConnTimeout)
}

func orDefault[T comparable](v, fallback T) T {
	var zero T
	if v == zero {
		return fallback
	}
	return v
}
