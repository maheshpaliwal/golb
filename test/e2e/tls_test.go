package e2e

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testCert is a self-signed certificate written to disk for golb to load.
type testCert struct {
	certFile string
	keyFile  string
	leaf     *x509.Certificate
}

var certSerial atomic.Int64

// writeCert generates a self-signed certificate for host in memory and
// writes it to dir under the given file stem.
func writeCert(t *testing.T, dir, stem, host string) testCert {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(certSerial.Add(1)),
		Subject:               pkix.Name{CommonName: host},
		DNSNames:              []string{host},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	c := testCert{
		certFile: filepath.Join(dir, stem+".pem"),
		keyFile:  filepath.Join(dir, stem+".key"),
		leaf:     leaf,
	}
	writePEM(t, c.certFile, "CERTIFICATE", der)
	writePEM(t, c.keyFile, "EC PRIVATE KEY", keyDER)
	return c
}

func writePEM(t *testing.T, path, kind string, der []byte) {
	t.Helper()
	err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600)
	if err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func rootsOf(certs ...testCert) *x509.CertPool {
	roots := x509.NewCertPool()
	for _, c := range certs {
		roots.AddCert(c.leaf)
	}
	return roots
}

func tlsConfig(p pool, minVersion string, certs ...testCert) string {
	var b strings.Builder
	b.WriteString("    tls:\n")
	if minVersion != "" {
		fmt.Fprintf(&b, "      min_version: %q\n", minVersion)
	}
	b.WriteString("      certificates:\n")
	for _, c := range certs {
		fmt.Fprintf(&b, "        - cert_file: %s\n          key_file: %s\n", c.certFile, c.keyFile)
	}
	return webConfig(p, b.String())
}

// handshake connects with the given client settings and returns the state
// of the established TLS session.
func handshake(addr string, cfg *tls.Config) (tls.ConnectionState, error) {
	dialer := &net.Dialer{Timeout: waitTimeout}
	conn, err := tls.DialWithDialer(dialer, "tcp", addr, cfg)
	if err != nil {
		return tls.ConnectionState{}, err
	}
	defer closeConn(conn)
	return conn.ConnectionState(), nil
}

func TestHTTP2OverTLS(t *testing.T) {
	var proto, forwardedProto, forwardedHost atomic.Value
	echo := newBackend(t, "echo", func(w http.ResponseWriter, r *http.Request) {
		proto.Store(r.Proto)
		forwardedProto.Store(r.Header.Get("X-Forwarded-Proto"))
		forwardedHost.Store(r.Header.Get("X-Forwarded-Host"))
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		writeString(w, string(body))
	})
	cert := writeCert(t, t.TempDir(), "a", "a.test")
	in := start(t, tlsConfig(poolOf("round_robin", echo), "", cert))

	tests := []struct {
		name      string
		protocols []string
		wantMajor int
	}{
		{name: "HTTP/2 by ALPN", protocols: []string{"h2", "http/1.1"}, wantMajor: 2},
		{name: "HTTP/1.1 when the client offers nothing else", protocols: []string{"http/1.1"}, wantMajor: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transport := &http.Transport{
				TLSClientConfig:   &tls.Config{RootCAs: rootsOf(cert), ServerName: "a.test", NextProtos: tc.protocols},
				ForceAttemptHTTP2: tc.wantMajor == 2,
			}
			t.Cleanup(transport.CloseIdleConnections)
			client := &http.Client{Transport: transport, Timeout: waitTimeout}
			url := "https://" + in.addr(web) + "/"

			res, err := client.Post(url, "text/plain", strings.NewReader("over tls"))
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			body, err := io.ReadAll(res.Body)
			closeConn(res.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if res.ProtoMajor != tc.wantMajor {
				t.Errorf("downstream protocol %s, want HTTP/%d", res.Proto, tc.wantMajor)
			}
			if res.StatusCode != http.StatusOK || string(body) != "over tls" {
				t.Errorf("status %d body %q, want 200 and the echoed body", res.StatusCode, body)
			}
			if got := proto.Load(); got != "HTTP/1.1" {
				t.Errorf("upstream protocol %v, want HTTP/1.1", got)
			}
			if got := forwardedProto.Load(); got != "https" {
				t.Errorf("X-Forwarded-Proto %v, want https", got)
			}
			if got := forwardedHost.Load(); got != in.addr(web) {
				t.Errorf("X-Forwarded-Host %v, want %s", got, in.addr(web))
			}

			// Many requests multiplexed on the one connection.
			var wg sync.WaitGroup
			var failed atomic.Int64
			for i := range 40 {
				wg.Go(func() {
					want := fmt.Sprintf("stream %d", i)
					r, err := trySend(client, http.MethodPost, url, want)
					if err != nil || r.status != http.StatusOK || r.body != want {
						failed.Add(1)
					}
				})
			}
			wg.Wait()
			if failed.Load() != 0 {
				t.Errorf("%d of 40 concurrent requests failed or got another request's body", failed.Load())
			}
		})
	}
}

func TestH2COptIn(t *testing.T) {
	var proto atomic.Value
	b := newBackend(t, "b", func(w http.ResponseWriter, r *http.Request) {
		proto.Store(r.Proto)
		writeString(w, "ok")
	})
	in := start(t, webConfig(poolOf("round_robin", b), "    h2c: true\n"))

	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{Protocols: protocols}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: waitTimeout}
	res, err := client.Get(in.url("/"))
	if err != nil {
		t.Fatalf("h2c request: %v", err)
	}
	defer res.Body.Close()
	if res.ProtoMajor != 2 || res.StatusCode != http.StatusOK {
		t.Errorf("status %d over %s, want 200 over HTTP/2", res.StatusCode, res.Proto)
	}
	if got := proto.Load(); got != "HTTP/1.1" {
		t.Errorf("upstream protocol %v, want HTTP/1.1", got)
	}
	// HTTP/1.1 clients still work on the same listener.
	if r := get(t, newClient(t), in.url("/")); r.status != http.StatusOK {
		t.Errorf("HTTP/1.1 on the h2c listener: status %d", r.status)
	}
}

func TestSNISelectsCertificate(t *testing.T) {
	dir := t.TempDir()
	a := writeCert(t, dir, "a", "a.test")
	b := writeCert(t, dir, "b", "b.test")
	in := start(t, tlsConfig(poolOf("round_robin", newBackend(t, "b0", nil)), "", a, b))

	tests := []struct {
		serverName string
		want       testCert
	}{
		{serverName: "a.test", want: a},
		{serverName: "b.test", want: b},
	}
	for _, tc := range tests {
		t.Run(tc.serverName, func(t *testing.T) {
			// Verification against both roots succeeds only if the
			// certificate presented is the one for the requested name.
			state, err := handshake(in.addr(web), &tls.Config{RootCAs: rootsOf(a, b), ServerName: tc.serverName, NextProtos: []string{"h2", "http/1.1"}})
			if err != nil {
				t.Fatalf("handshake: %v", err)
			}
			got := state.PeerCertificates[0]
			if got.SerialNumber.Cmp(tc.want.leaf.SerialNumber) != 0 {
				t.Errorf("certificate for %v presented, want the one for %s", got.DNSNames, tc.serverName)
			}
			if state.NegotiatedProtocol != "h2" {
				t.Errorf("ALPN chose %q, want h2", state.NegotiatedProtocol)
			}
			if state.Version < tls.VersionTLS12 {
				t.Errorf("negotiated TLS version %x, want at least 1.2", state.Version)
			}
		})
	}
}

func TestOldTLSVersionsRefused(t *testing.T) {
	tests := []struct {
		name       string
		minVersion string
		client     uint16
		wantOK     bool
	}{
		{name: "default minimum refuses TLS 1.0", client: tls.VersionTLS10},
		{name: "default minimum refuses TLS 1.1", client: tls.VersionTLS11},
		{name: "default minimum accepts TLS 1.2", client: tls.VersionTLS12, wantOK: true},
		{name: "minimum 1.3 refuses TLS 1.2", minVersion: "1.3", client: tls.VersionTLS12},
		{name: "minimum 1.3 accepts TLS 1.3", minVersion: "1.3", client: tls.VersionTLS13, wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cert := writeCert(t, t.TempDir(), "a", "a.test")
			in := start(t, tlsConfig(poolOf("round_robin", newBackend(t, "b0", nil)), tc.minVersion, cert))
			state, err := handshake(in.addr(web), &tls.Config{
				RootCAs:    rootsOf(cert),
				ServerName: "a.test",
				MinVersion: tls.VersionTLS10,
				MaxVersion: tc.client,
			})
			if tc.wantOK && (err != nil || state.Version != tc.client) {
				t.Errorf("handshake at version %x: err %v, negotiated %x", tc.client, err, state.Version)
			}
			if !tc.wantOK && err == nil {
				t.Errorf("handshake at version %x succeeded, want it refused", tc.client)
			}
		})
	}
}

func TestCertificateReplacedOnReload(t *testing.T) {
	dir := t.TempDir()
	before := writeCert(t, dir, "a", "a.test")
	cfg := tlsConfig(poolOf("round_robin", newBackend(t, "b0", nil)), "", before)
	in := start(t, cfg)

	serial := func(roots *x509.CertPool) *big.Int {
		t.Helper()
		state, err := handshake(in.addr(web), &tls.Config{RootCAs: roots, ServerName: "a.test"})
		if err != nil {
			t.Fatalf("handshake: %v", err)
		}
		return state.PeerCertificates[0].SerialNumber
	}
	if got := serial(rootsOf(before)); got.Cmp(before.leaf.SerialNumber) != 0 {
		t.Fatalf("serial %v before the reload, want %v", got, before.leaf.SerialNumber)
	}

	after := writeCert(t, dir, "a", "a.test") // same paths, new key pair
	in.mustReload(cfg)
	if got := serial(rootsOf(after)); got.Cmp(after.leaf.SerialNumber) != 0 {
		t.Errorf("serial %v after the reload, want the new certificate %v", got, after.leaf.SerialNumber)
	}

	// A reload whose certificate does not load is rejected and the running
	// certificate stays.
	err := os.WriteFile(after.certFile, []byte("not a certificate"), 0o600)
	if err != nil {
		t.Fatalf("corrupt certificate: %v", err)
	}
	status, _ := in.reload(cfg)
	if status != http.StatusInternalServerError {
		t.Errorf("reload with a broken certificate: status %d, want 500", status)
	}
	if got := serial(rootsOf(after)); got.Cmp(after.leaf.SerialNumber) != 0 {
		t.Errorf("serial %v after the rejected reload, want %v still", got, after.leaf.SerialNumber)
	}
}
