package metrics

import (
	"bytes"
	"errors"
	"math"
	"regexp"
	"strings"
	"sync"
	"testing"
)

func render(t *testing.T, r *Registry) string {
	t.Helper()
	var buf bytes.Buffer
	n, err := r.WriteTo(&buf)
	if err != nil {
		t.Fatalf("write metrics: %v", err)
	}
	if n != int64(buf.Len()) {
		t.Fatalf("WriteTo reported %d bytes, wrote %d", n, buf.Len())
	}
	return buf.String()
}

func TestWriteToTextFormat(t *testing.T) {
	r := NewRegistry()
	r.Counter("golb_requests_total", "Requests handled.", "listener", "web", "code_class", "2xx").Add(7)
	r.Counter("golb_requests_total", "Requests handled.", "listener", "web", "code_class", "5xx").Inc()
	r.Gauge("golb_backend_inflight", "In-flight requests.", "backend", "127.0.0.1:9001").Set(-3)
	r.GaugeFunc("golb_backend_healthy", "Health state.", func() float64 { return 1 }, "backend", "127.0.0.1:9001")
	r.Counter("golb_accesslog_dropped_total", "Dropped records.")
	h := r.Histogram("golb_request_duration_seconds", "Request duration.", []float64{0.1, 1}, "route", "/")
	for _, v := range []float64{0.05, 0.1, 0.5, 3} {
		h.Observe(v)
	}

	want := `# HELP golb_accesslog_dropped_total Dropped records.
# TYPE golb_accesslog_dropped_total counter
golb_accesslog_dropped_total 0
# HELP golb_backend_healthy Health state.
# TYPE golb_backend_healthy gauge
golb_backend_healthy{backend="127.0.0.1:9001"} 1
# HELP golb_backend_inflight In-flight requests.
# TYPE golb_backend_inflight gauge
golb_backend_inflight{backend="127.0.0.1:9001"} -3
# HELP golb_request_duration_seconds Request duration.
# TYPE golb_request_duration_seconds histogram
golb_request_duration_seconds_bucket{route="/",le="0.1"} 2
golb_request_duration_seconds_bucket{route="/",le="1"} 3
golb_request_duration_seconds_bucket{route="/",le="+Inf"} 4
golb_request_duration_seconds_sum{route="/"} 3.65
golb_request_duration_seconds_count{route="/"} 4
# HELP golb_requests_total Requests handled.
# TYPE golb_requests_total counter
golb_requests_total{listener="web",code_class="2xx"} 7
golb_requests_total{listener="web",code_class="5xx"} 1
`
	if got := render(t, r); got != want {
		t.Errorf("text output mismatch\n got:\n%s\nwant:\n%s", got, want)
	}
}

func TestWriteToLinesParse(t *testing.T) {
	r := NewRegistry()
	r.Counter("a_total", "help with \\ and\nnewline", "k", "quote \" slash \\ nl \n end").Inc()
	r.Histogram("b_seconds", "h", nil).Observe(0.3)
	r.GaugeFunc("c", "nan", math.NaN)
	r.GaugeFunc("d", "inf", func() float64 { return math.Inf(1) })

	comment := regexp.MustCompile(`^# (HELP|TYPE) [a-zA-Z_:][a-zA-Z0-9_:]* .+$`)
	sample := regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*(\{([a-zA-Z_][a-zA-Z0-9_]*="([^"\\\n]|\\\\|\\"|\\n)*",?)+\})? (NaN|[+-]Inf|-?[0-9.e+-]+)$`)
	out := render(t, r)
	if !strings.HasSuffix(out, "\n") {
		t.Error("output does not end with a newline")
	}
	for line := range strings.SplitSeq(strings.TrimSuffix(out, "\n"), "\n") {
		if comment.MatchString(line) || sample.MatchString(line) {
			continue
		}
		t.Errorf("line is not valid text format: %q", line)
	}
	for _, want := range []string{
		`# HELP a_total help with \\ and\nnewline`,
		`a_total{k="quote \" slash \\ nl \n end"} 1`,
		`b_seconds_bucket{le="0.0005"} 0`,
		`b_seconds_bucket{le="0.5"} 1`,
		`b_seconds_bucket{le="+Inf"} 1`,
		`c NaN`,
		`d +Inf`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("output lacks line %q", want)
		}
	}
}

func TestRegistrationReturnsExisting(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("c_total", "h", "pool", "a")
	c.Add(5)
	if again := r.Counter("c_total", "h", "pool", "a"); again != c || again.Value() != 5 {
		t.Errorf("same name and labels: got a different counter or value %d, want the original with 5", again.Value())
	}
	if other := r.Counter("c_total", "h", "pool", "b"); other == c {
		t.Error("different labels returned the same counter")
	}
	g := r.Gauge("g", "h")
	if r.Gauge("g", "h") != g {
		t.Error("same gauge registration returned a different gauge")
	}
	h := r.Histogram("h_seconds", "h", []float64{1, 2})
	if r.Histogram("h_seconds", "h", []float64{5}) != h {
		t.Error("same histogram registration returned a different histogram")
	}
}

func TestRegistrationConflicts(t *testing.T) {
	r := NewRegistry()
	r.Counter("x", "h").Add(2)
	r.GaugeFunc("fn", "h", func() float64 { return 4 })

	g := r.Gauge("x", "h")
	g.Set(9)
	hist := r.Histogram("x", "h", nil)
	hist.Observe(1)
	r.Gauge("fn", "h").Set(8)
	r.GaugeFunc("x", "h", func() float64 { return 5 })
	odd := r.Counter("odd_total", "h", "k", "v", "dangling")
	odd.Inc()

	want := `# HELP fn h
# TYPE fn gauge
fn 4
# HELP odd_total h
# TYPE odd_total counter
odd_total{k="v"} 1
# HELP x h
# TYPE x counter
x 2
`
	if got := render(t, r); got != want {
		t.Errorf("conflicting registrations changed the output\n got:\n%s\nwant:\n%s", got, want)
	}
}

func TestGaugeFuncReplaced(t *testing.T) {
	r := NewRegistry()
	r.GaugeFunc("up", "h", func() float64 { return 1 }, "pool", "a")
	r.GaugeFunc("up", "h", func() float64 { return 2.5 }, "pool", "a")
	if got, want := render(t, r), "# HELP up h\n# TYPE up gauge\nup{pool=\"a\"} 2.5\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestHistogramBuckets(t *testing.T) {
	tests := []struct {
		name    string
		buckets []float64
		observe []float64
		want    []uint64 // non-cumulative, last is +Inf
		wantSum float64
	}{
		{name: "bound is inclusive", buckets: []float64{1, 2}, observe: []float64{1, 2}, want: []uint64{1, 1, 0}, wantSum: 3},
		{name: "below first and above last", buckets: []float64{1, 2}, observe: []float64{-1, 0, 2.01, 300}, want: []uint64{2, 0, 2}, wantSum: 301.01},
		{name: "infinite samples are clamped, not overflowed", buckets: []float64{1}, observe: []float64{math.Inf(1), math.Inf(-1), 2}, want: []uint64{1, 2}, wantSum: 2},
		{name: "nanosecond resolution", buckets: []float64{1}, observe: []float64{1e-9, 2e-9, 0.4e-9, 0.6e-9}, want: []uint64{4, 0}, wantSum: 4e-9},
		{name: "unsorted duplicate buckets are normalised", buckets: []float64{2, 1, 2}, observe: []float64{1.5}, want: []uint64{0, 1, 0}, wantSum: 1.5},
		{name: "nan ignored", buckets: []float64{1}, observe: []float64{math.NaN(), 0.5}, want: []uint64{1, 0}, wantSum: 0.5},
		{name: "default buckets", observe: []float64{0.0005, 0.00051, 10, 11}, want: []uint64{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1}, wantSum: 21.00101},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHistogram(tc.buckets)
			for _, v := range tc.observe {
				h.Observe(v)
			}
			if len(h.counts) != len(tc.want) {
				t.Fatalf("got %d buckets, want %d", len(h.counts), len(tc.want))
			}
			for i := range tc.want {
				if got := h.counts[i].Load(); got != tc.want[i] {
					t.Errorf("bucket %d: got %d, want %d", i, got, tc.want[i])
				}
			}
			if got := h.sumSeconds(); math.Abs(got-tc.wantSum) > 1e-9 {
				t.Errorf("sum: got %v, want %v", got, tc.wantSum)
			}
		})
	}
}

func TestConcurrentUpdatesSumExactly(t *testing.T) {
	const goroutines, perGoroutine = 64, 5000
	r := NewRegistry()
	c := r.Counter("c_total", "h")
	g := r.Gauge("g", "h")
	h := r.Histogram("h_seconds", "h", []float64{1})

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range perGoroutine {
				c.Inc()
				g.Add(2)
				g.Add(-1)
				h.Observe(0.5)
			}
		})
	}
	wg.Wait()

	const total = goroutines * perGoroutine
	if got := c.Value(); got != total {
		t.Errorf("counter: got %d, want %d", got, total)
	}
	if got := g.v.Load(); got != total {
		t.Errorf("gauge: got %d, want %d", got, total)
	}
	if got := h.counts[0].Load(); got != total {
		t.Errorf("histogram count: got %d, want %d", got, total)
	}
	if got := h.sumSeconds(); got != total/2 {
		t.Errorf("histogram sum: got %v, want %v", got, total/2)
	}
}

func TestConcurrentRegistrationAndScrape(t *testing.T) {
	r := NewRegistry()
	names := []string{"a", "b", "c", "d"}
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			for j := range 200 {
				r.Counter("reg_total", "h", "k", names[(i+j)%len(names)]).Inc()
				r.GaugeFunc("reg_fn", "h", func() float64 { return 1 }, "k", names[j%len(names)])
				if _, err := r.WriteTo(&bytes.Buffer{}); err != nil {
					t.Errorf("write metrics: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()
	var total uint64
	for _, name := range names {
		total += r.Counter("reg_total", "h", "k", name).Value()
	}
	if total != 16*200 {
		t.Errorf("got %d increments across label sets, want %d", total, 16*200)
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestWriteToReturnsWriterError(t *testing.T) {
	r := NewRegistry()
	r.Counter("c_total", "h").Inc()
	want := errors.New("closed")
	if _, err := r.WriteTo(failingWriter{err: want}); !errors.Is(err, want) {
		t.Errorf("got error %v, want %v", err, want)
	}
}

func TestHotPathDoesNotAllocate(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("c_total", "h")
	g := r.Gauge("g", "h")
	h := r.Histogram("h_seconds", "h", nil)
	allocs := testing.AllocsPerRun(1000, func() {
		c.Inc()
		c.Add(3)
		g.Add(1)
		g.Set(4)
		h.Observe(0.012)
	})
	if allocs != 0 {
		t.Errorf("got %v allocs per update, want 0", allocs)
	}
}

func BenchmarkCounterInc(b *testing.B) {
	c := NewRegistry().Counter("c_total", "h")
	b.ReportAllocs()
	for b.Loop() {
		c.Inc()
	}
	if c.Value() == 0 {
		b.Fatal("counter did not advance")
	}
}

func BenchmarkCounterIncParallel(b *testing.B) {
	c := NewRegistry().Counter("c_total", "h")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
	if c.Value() == 0 {
		b.Fatal("counter did not advance")
	}
}

func BenchmarkGaugeAdd(b *testing.B) {
	g := NewRegistry().Gauge("g", "h")
	b.ReportAllocs()
	for b.Loop() {
		g.Add(1)
	}
	if g.v.Load() == 0 {
		b.Fatal("gauge did not advance")
	}
}

func BenchmarkHistogramObserve(b *testing.B) {
	h := NewRegistry().Histogram("h_seconds", "h", LatencyBuckets)
	values := latencySamples()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		h.Observe(values[i&(len(values)-1)])
		i++
	}
	if h.sum.Load() == 0 {
		b.Fatal("histogram did not record")
	}
}

func BenchmarkHistogramObserveParallel(b *testing.B) {
	h := NewRegistry().Histogram("h_seconds", "h", LatencyBuckets)
	values := latencySamples()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			h.Observe(values[i&(len(values)-1)])
			i++
		}
	})
	if h.sum.Load() == 0 {
		b.Fatal("histogram did not record")
	}
}

func BenchmarkWriteTo(b *testing.B) {
	r := NewRegistry()
	backends := []string{"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003", "127.0.0.1:9004"}
	for _, backend := range backends {
		r.Counter("golb_upstream_requests_total", "h", "pool", "app", "backend", backend, "result", "ok").Add(10)
		r.Gauge("golb_backend_inflight", "h", "pool", "app", "backend", backend).Set(3)
		r.Histogram("golb_upstream_duration_seconds", "h", LatencyBuckets, "pool", "app", "backend", backend).Observe(0.01)
	}
	var buf bytes.Buffer
	b.ReportAllocs()
	for b.Loop() {
		buf.Reset()
		if _, err := r.WriteTo(&buf); err != nil {
			b.Fatal(err)
		}
	}
}

// latencySamples spreads values over every bucket; the length is a power of
// two so the benchmark loop can mask instead of dividing.
func latencySamples() []float64 {
	samples := make([]float64, 64)
	for i := range samples {
		samples[i] = 0.0002 * math.Pow(1.2, float64(i))
	}
	return samples
}
