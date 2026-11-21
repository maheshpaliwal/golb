package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"

	"github.com/maheshpaliwal/golb/internal/accesslog"
	"github.com/maheshpaliwal/golb/internal/metrics"
)

const benchResponseBytes = 1 << 10

// discardWriter is a ResponseWriter that costs nothing, so the benchmark
// measures the handler rather than a recorder.
type discardWriter struct {
	header http.Header
	status int
	bytes  int
}

func (w *discardWriter) Header() http.Header         { return w.header }
func (w *discardWriter) WriteHeader(status int)      { w.status = status }
func (w *discardWriter) Write(p []byte) (int, error) { w.bytes += len(p); return len(p), nil }

func (w *discardWriter) reset() {
	clear(w.header)
	w.status = 0
	w.bytes = 0
}

func benchBackend(b *testing.B) string {
	b.Helper()
	payload := make([]byte, benchResponseBytes)
	return backend(b, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(payload)
	})
}

func runHandler(b *testing.B, handler http.Handler) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(benchResponseBytes)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		req := httptest.NewRequest(http.MethodGet, "http://bench.example.test/items/42?page=1", nil)
		req.Header.Set("User-Agent", "bench")
		req.Header.Set("Accept", "*/*")
		w := &discardWriter{header: make(http.Header)}
		for pb.Next() {
			w.reset()
			handler.ServeHTTP(w, req)
			if w.status != http.StatusOK || w.bytes != benchResponseBytes {
				b.Errorf("got status %d with %d bytes, want 200 with %d", w.status, w.bytes, benchResponseBytes)
				return
			}
		}
	})
}

// BenchmarkHandler is the full golb request path (routing, pick, retry
// transport, metrics) against in-process backends over loopback.
func BenchmarkHandler(b *testing.B) {
	addrs := []string{benchBackend(b), benchBackend(b), benchBackend(b), benchBackend(b)}
	f := newFixture(b, fixtureOptions{backends: addrs})
	runHandler(b, f.handler)
}

// BenchmarkHandlerAccessLog is BenchmarkHandler with every request logged.
func BenchmarkHandlerAccessLog(b *testing.B) {
	addrs := []string{benchBackend(b), benchBackend(b), benchBackend(b), benchBackend(b)}
	f := newFixture(b, fixtureOptions{backends: addrs})
	f.router.log = discardLog(b)
	runHandler(b, f.handler)
}

// BenchmarkBareReverseProxy is the baseline from the same harness: a stock
// httputil.ReverseProxy to one backend, with the same connection pool size.
func BenchmarkBareReverseProxy(b *testing.B) {
	target, err := url.Parse("http://" + benchBackend(b))
	if err != nil {
		b.Fatalf("parse backend address: %v", err)
	}
	bare := httputil.NewSingleHostReverseProxy(target)
	transport := &http.Transport{MaxIdleConnsPerHost: 256}
	b.Cleanup(transport.CloseIdleConnections)
	bare.Transport = transport
	runHandler(b, bare)
}

func BenchmarkRouterMatch(b *testing.B) {
	const routes = `
      - pool: app
      - path_prefix: /api
        pool: app
      - host: shop.example.test
        path_prefix: /cart
        pool: app
      - host: "*.example.test"
        pool: app
`
	f := newFixture(b, fixtureOptions{backends: []string{"127.0.0.1:1"}, routes: routes})
	b.ReportAllocs()
	var matched *Route
	for b.Loop() {
		matched = f.router.match("shop.example.test:8080", "/cart/items/42")
	}
	if matched == nil || matched.label != "shop.example.test/cart" {
		b.Fatalf("matched %v, want the cart route", matched)
	}
}

func discardLog(b *testing.B) *accesslog.Logger {
	b.Helper()
	logger := accesslog.New(accesslog.Options{
		Out:        io.Discard,
		BufferSize: 8192,
		SampleRate: 1,
		Dropped:    metrics.NewRegistry().Counter("dropped", ""),
		Logger:     quietLogger(),
	})
	b.Cleanup(logger.Close)
	return logger
}
