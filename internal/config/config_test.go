package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

const minimal = `
listeners:
  - name: web
    addr: 127.0.0.1:8080
    protocol: http
    routes:
      - pool: app
pools:
  - name: app
    backends:
      - addr: 127.0.0.1:9001
`

func TestParseAppliesDefaults(t *testing.T) {
	cfg, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	listener := cfg.Listeners[0]
	pool := cfg.Pools[0]
	checks := []struct {
		name      string
		got, want any
	}{
		{name: "admin addr", got: cfg.Admin.Addr, want: "127.0.0.1:9901"},
		{name: "pprof", got: cfg.Admin.Pprof, want: false},
		{name: "shutdown grace", got: cfg.ShutdownGrace, want: 30 * time.Second},
		{name: "access log enabled", got: cfg.AccessLog.Enabled, want: false},
		{name: "sample rate", got: cfg.AccessLog.SampleRate, want: 1.0},
		{name: "log buffer", got: cfg.AccessLog.BufferSize, want: 8192},
		{name: "read header", got: listener.Timeouts.ReadHeader, want: 5 * time.Second},
		{name: "idle", got: listener.Timeouts.Idle, want: 60 * time.Second},
		{name: "request", got: listener.Timeouts.Request, want: 60 * time.Second},
		{name: "body idle", got: listener.Timeouts.BodyIdle, want: 60 * time.Second},
		{name: "max header bytes", got: listener.Timeouts.MaxHeaderBytes, want: 65536},
		{name: "path prefix", got: listener.Routes[0].PathPrefix, want: "/"},
		{name: "policy", got: pool.Policy, want: core.P2CEWMA},
		{name: "backend weight", got: pool.Backends[0].Weight, want: uint32(1)},
		{name: "max attempts", got: pool.Retry.MaxAttempts, want: 3},
		{name: "per try timeout", got: pool.Retry.PerTryTimeout, want: 10 * time.Second},
		{name: "budget percent", got: pool.Retry.BudgetPercent, want: 20},
		{name: "dial timeout", got: pool.Transport.DialTimeout, want: 2 * time.Second},
		{name: "max idle per host", got: pool.Transport.MaxIdleConnsPerHost, want: 256},
		{name: "idle conn timeout", got: pool.Transport.IdleConnTimeout, want: 90 * time.Second},
		{name: "outlier consecutive failures", got: pool.Outlier.ConsecutiveFailures, want: 5},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if !slices.Equal(pool.Retry.RetryOn, []int{502, 503, 504}) {
		t.Errorf("retry_on = %v, want [502 503 504]", pool.Retry.RetryOn)
	}
}

func TestParseExplicitZeroIsNotDefaulted(t *testing.T) {
	input := `
access_log:
  enabled: true
  sample_rate: 0
listeners:
  - name: web
    addr: 127.0.0.1:8080
    protocol: http
    timeouts:
      request: 0s
      body_idle: 0s
      idle: 90s
    routes:
      - pool: app
pools:
  - name: app
    backends:
      - addr: 127.0.0.1:9001
    retry:
      budget_percent: 0
      retry_on: []
      per_try_timeout: 250ms
`
	cfg, err := Parse([]byte(input))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.AccessLog.SampleRate != 0 {
		t.Errorf("sample_rate = %v, want explicit 0", cfg.AccessLog.SampleRate)
	}
	if got := cfg.Listeners[0].Timeouts.Request; got != 0 {
		t.Errorf("request timeout = %v, want explicit 0", got)
	}
	if got := cfg.Listeners[0].Timeouts.BodyIdle; got != 0 {
		t.Errorf("body_idle = %v, want explicit 0", got)
	}
	if got := cfg.Listeners[0].Timeouts.Idle; got != 90*time.Second {
		t.Errorf("idle = %v, want 90s", got)
	}
	retry := cfg.Pools[0].Retry
	if retry.BudgetPercent != 0 {
		t.Errorf("budget_percent = %d, want explicit 0", retry.BudgetPercent)
	}
	if len(retry.RetryOn) != 0 {
		t.Errorf("retry_on = %v, want explicit empty list", retry.RetryOn)
	}
	if retry.PerTryTimeout != 250*time.Millisecond {
		t.Errorf("per_try_timeout = %v, want 250ms", retry.PerTryTimeout)
	}
}

func TestParseRejects(t *testing.T) {
	base := func(replacements ...string) string {
		return strings.NewReplacer(replacements...).Replace(minimal)
	}
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "unknown top-level key",
			input: minimal + "surprise: 1\n",
			want:  []string{"surprise"},
		},
		{
			name:  "unknown key in a listener",
			input: base("protocol: http", "protocol: http\n    colour: blue"),
			want:  []string{"colour"},
		},
		{
			name:  "unknown key in timeouts",
			input: base("protocol: http", "protocol: http\n    timeouts:\n      write: 5s"),
			want:  []string{"write"},
		},
		{
			name:  "unknown key in retry",
			input: minimal + "    retry:\n      attempts: 2\n",
			want:  []string{"attempts"},
		},
		{
			name:  "unknown key in access log",
			input: minimal + "access_log:\n  file: /tmp/x\n",
			want:  []string{"file"},
		},
		{
			name:  "bad duration",
			input: minimal + "shutdown_grace: soon\n",
			want:  []string{"line 12", "soon", "time.Duration"},
		},
		{
			name:  "second document",
			input: minimal + "---\nadmin:\n  addr: 127.0.0.1:1\n",
			want:  []string{"more than one YAML document"},
		},
		{
			name:  "listener without host",
			input: base("addr: 127.0.0.1:8080", `addr: ":8080"`),
			want:  []string{`listener "web"`, "has no host"},
		},
		{
			name:  "listener address not host:port",
			input: base("addr: 127.0.0.1:8080", "addr: localhost"),
			want:  []string{`listener "web"`, "not host:port"},
		},
		{
			name:  "admin without host",
			input: minimal + "admin:\n  addr: \":9901\"\n",
			want:  []string{"admin:", "has no host"},
		},
		{
			name:  "admin on non-loopback",
			input: minimal + "admin:\n  addr: 192.0.2.10:9901\n",
			want:  []string{"admin:", "not loopback", "allow_non_loopback"},
		},
		{
			name:  "admin shares a listener address",
			input: minimal + "admin:\n  addr: 127.0.0.1:8080\n",
			want:  []string{`listener "web"`, "already used by admin"},
		},
		{
			name:  "unknown protocol",
			input: base("protocol: http", "protocol: udp"),
			want:  []string{`listener "web"`, `protocol "udp"`},
		},
		{
			name:  "route names a missing pool",
			input: base("- pool: app", "- pool: nope"),
			want:  []string{`listener "web" route 0`, `unknown pool "nope"`},
		},
		{
			name:  "route prefix without slash",
			input: base("- pool: app", "- pool: app\n        path_prefix: api"),
			want:  []string{"route 0", "path_prefix must start with /"},
		},
		{
			name:  "route host with port",
			input: base("- pool: app", "- pool: app\n        host: example.test:80"),
			want:  []string{"route 0", "host must be"},
		},
		{
			name:  "duplicate route",
			input: base("- pool: app", "- pool: app\n      - pool: app"),
			want:  []string{"route 1", "duplicate host and path_prefix"},
		},
		{
			name:  "bad rate limit",
			input: base("- pool: app", "- pool: app\n        rate_limit:\n          requests_per_second: 0\n          burst: 0"),
			want:  []string{"route 0", "rate_limit needs"},
		},
		{
			name:  "rate limit that is not a number",
			input: base("- pool: app", "- pool: app\n        rate_limit:\n          requests_per_second: .nan\n          burst: 1"),
			want:  []string{"route 0", "rate_limit needs"},
		},
		{
			name:  "infinite rate limit",
			input: base("- pool: app", "- pool: app\n        rate_limit:\n          requests_per_second: .inf\n          burst: 1"),
			want:  []string{"route 0", "rate_limit needs"},
		},
		{
			name:  "sample rate that is not a number",
			input: minimal + "access_log:\n  sample_rate: .nan\n",
			want:  []string{"access_log: sample_rate"},
		},
		{
			name:  "prefixes differing only by a trailing slash",
			input: base("- pool: app", "- pool: app\n        path_prefix: /api\n      - pool: app\n        path_prefix: /api/"),
			want:  []string{"route 1", "duplicate host and path_prefix"},
		},
		{
			name:  "route host with a trailing dot",
			input: base("- pool: app", "- pool: app\n        host: example.test."),
			want:  []string{"route 0", "trailing dot"},
		},
		{
			name:  "negative body idle",
			input: base("protocol: http", "protocol: http\n    timeouts:\n      body_idle: -1s"),
			want:  []string{`listener "web"`, "body_idle >= 0"},
		},
		{
			name:  "http listener without routes",
			input: base("    routes:\n      - pool: app\n", ""),
			want:  []string{`listener "web"`, "no routes"},
		},
		{
			name:  "tcp listener with a missing pool",
			input: base("protocol: http\n    routes:\n      - pool: app", "protocol: tcp\n    pool: nope"),
			want:  []string{`listener "web"`, `unknown pool "nope"`},
		},
		{
			name:  "duplicate listener name and address",
			input: base("pools:", "  - name: web\n    addr: 127.0.0.1:8080\n    protocol: tcp\n    pool: app\npools:"),
			want:  []string{"duplicate name", "already used by listener"},
		},
		{
			name:  "duplicate pool",
			input: minimal + "  - name: app\n    backends:\n      - addr: 127.0.0.1:9002\n",
			want:  []string{`pool "app": duplicate name`},
		},
		{
			name:  "pool without backends",
			input: base("    backends:\n      - addr: 127.0.0.1:9001\n", ""),
			want:  []string{`pool "app": no backends`},
		},
		{
			name:  "unknown policy",
			input: minimal + "    policy: fastest\n",
			want:  []string{`unknown policy "fastest"`},
		},
		{
			name:  "retry settings out of range",
			input: minimal + "    retry:\n      max_attempts: 50\n      budget_percent: 150\n      retry_on: [404]\n",
			want:  []string{"max_attempts 50", "budget_percent 150", "retry_on status 404"},
		},
		{
			name:  "hash header without ring hash",
			input: minimal + "    hash_header: X-User\n",
			want:  []string{`pool "app"`, "hash_header needs policy"},
		},
		{
			name:  "negative transport values",
			input: minimal + "    transport:\n      dial_timeout: -1s\n",
			want:  []string{`pool "app"`, "transport needs"},
		},
		{
			name:  "missing certificate files",
			input: base("protocol: http", "protocol: http\n    tls:\n      certificates:\n        - cert_file: /nonexistent/cert.pem\n          key_file: /nonexistent/key.pem"),
			want:  []string{`listener "web"`, "load certificate /nonexistent/cert.pem"},
		},
		{
			name:  "tls without certificates and bad version",
			input: base("protocol: http", "protocol: http\n    tls:\n      min_version: \"1.0\""),
			want:  []string{"tls has no certificates", `min_version "1.0"`},
		},
		{
			name:  "h2c with tls",
			input: base("protocol: http", "protocol: http\n    h2c: true\n    tls:\n      certificates: []"),
			want:  []string{"h2c and tls are mutually exclusive"},
		},
		{
			name: "all errors reported together",
			input: `
listeners:
  - name: a
    addr: ":1"
    protocol: http
    routes:
      - pool: missing
  - name: b
    addr: 127.0.0.1:2
    protocol: tcp
    pool: gone
pools:
  - name: p
    backends:
      - addr: nowhere
`,
			want: []string{`listener "a"`, "has no host", `unknown pool "missing"`, `listener "b"`, `unknown pool "gone"`, `backend "nowhere" is not host:port`},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Parse([]byte(tc.input))
			if err == nil {
				t.Fatalf("Parse accepted the config: %+v", cfg)
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error does not mention %q:\n%v", want, err)
				}
			}
		})
	}
}

func TestParseAccepts(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "admin off", input: minimal + "admin:\n  addr: \"off\"\n"},
		{name: "admin on localhost", input: minimal + "admin:\n  addr: localhost:9901\n"},
		{name: "admin on IPv6 loopback", input: minimal + "admin:\n  addr: \"[::1]:9901\"\n"},
		{name: "admin non-loopback when allowed", input: minimal + "admin:\n  addr: 192.0.2.10:9901\n  allow_non_loopback: true\n"},
		{name: "wildcard route host", input: strings.Replace(minimal, "- pool: app", "- pool: app\n        host: \"*.example.test\"", 1)},
		{name: "ring hash with header", input: minimal + "    policy: ring_hash\n    hash_header: X-User\n"},
		{
			name: "several listeners on port 0",
			input: strings.Replace(minimal, "addr: 127.0.0.1:8080", "addr: 127.0.0.1:0", 1) +
				"admin:\n  addr: 127.0.0.1:0\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.input))
			if err != nil {
				t.Errorf("Parse: %v", err)
			}
		})
	}
}

func TestParseLoadsCertificates(t *testing.T) {
	certFile, keyFile := writeCertificate(t)
	input := strings.Replace(minimal, "protocol: http",
		"protocol: http\n    tls:\n      certificates:\n        - cert_file: "+certFile+"\n          key_file: "+keyFile, 1)
	cfg, err := Parse([]byte(input))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := cfg.Listeners[0].TLS.MinVersion; got != TLS12 {
		t.Errorf("min_version = %q, want default %q", got, TLS12)
	}
}

func TestLoadExampleFile(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "configs", "golb.example.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Listeners) != 2 || len(cfg.Pools) != 1 {
		t.Fatalf("example has %d listeners and %d pools, want 2 and 1", len(cfg.Listeners), len(cfg.Pools))
	}
	if got := cfg.Pools[0].Backends[1].Weight; got != 2 {
		t.Errorf("second backend weight = %d, want 2", got)
	}
	if got := cfg.Pools[0].Health.Path; got != "/healthz" {
		t.Errorf("health check path = %q, want /healthz", got)
	}
	addrs := []string{cfg.Admin.Addr}
	for _, l := range cfg.Listeners {
		addrs = append(addrs, l.Addr)
	}
	for _, addr := range addrs {
		if !strings.HasPrefix(addr, "127.0.0.1:") {
			t.Errorf("example binds %s, want loopback only", addr)
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "absent.yaml"))
	if err == nil {
		t.Fatal("Load succeeded on a missing file")
	}
}

// writeCertificate creates a throwaway self-signed pair in the test's temp dir.
func writeCertificate(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "golb.test"},
		DNSNames:     []string{"golb.test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	err = os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	if err != nil {
		t.Fatalf("write certificate: %v", err)
	}
	err = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600)
	if err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}
