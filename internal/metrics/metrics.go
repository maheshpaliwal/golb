// Package metrics provides lock-free counters, gauges and histograms and
// writes them in the Prometheus text exposition format.
package metrics

import (
	"io"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// LatencyBuckets are histogram upper bounds in seconds.
var LatencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type kind uint8

const (
	kindCounter kind = iota + 1
	kindGauge
	kindHistogram
)

func (k kind) String() string {
	switch k {
	case kindCounter:
		return "counter"
	case kindGauge:
		return "gauge"
	default:
		return "histogram"
	}
}

type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Add(n uint64)  { c.v.Add(n) }
func (c *Counter) Value() uint64 { return c.v.Load() }

type Gauge struct{ v atomic.Int64 }

func (g *Gauge) Set(v int64)     { g.v.Store(v) }
func (g *Gauge) Add(delta int64) { g.v.Add(delta) }

const (
	// The sum is kept as an integer count of nanos so Observe is two
	// wait-free adds. A float sum needs a compare-and-swap loop, which
	// measured 230 ns/op with 11 writers against 7 ns/op with one.
	sumUnitsPerSecond = 1e9
	// maxObservation keeps one sample from overflowing the integer sum.
	maxObservation = float64(math.MaxInt64) / sumUnitsPerSecond / 2
)

// Histogram counts observations per bucket. Buckets are stored
// non-cumulative so Observe touches one counter; WriteTo accumulates.
// Values are seconds; the sum has nanosecond resolution and each sample is
// clamped to about +-146 years.
type Histogram struct {
	bounds []float64
	counts []atomic.Uint64 // len(bounds)+1; the last is the +Inf bucket
	sum    atomic.Int64    // nanoseconds
}

func newHistogram(buckets []float64) *Histogram {
	if len(buckets) == 0 {
		buckets = LatencyBuckets
	}
	bounds := slices.Clone(buckets)
	slices.Sort(bounds)
	bounds = slices.Compact(bounds)
	return &Histogram{bounds: bounds, counts: make([]atomic.Uint64, len(bounds)+1)}
}

func (h *Histogram) Observe(seconds float64) {
	if math.IsNaN(seconds) {
		return
	}
	lo, hi := 0, len(h.bounds)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if h.bounds[mid] < seconds {
			lo = mid + 1
			continue
		}
		hi = mid
	}
	h.counts[lo].Add(1)
	h.sum.Add(int64(math.Round(min(max(seconds, -maxObservation), maxObservation) * sumUnitsPerSecond)))
}

func (h *Histogram) sumSeconds() float64 {
	return float64(h.sum.Load()) / sumUnitsPerSecond
}

type series struct {
	labels    string // rendered `k="v",k2="v2"`, empty when there are none
	counter   *Counter
	gauge     *Gauge
	fn        atomic.Pointer[func() float64]
	histogram *Histogram
}

type family struct {
	name   string
	help   string
	kind   kind
	series []*series
	index  map[string]*series
}

type Registry struct {
	mu       sync.RWMutex
	families map[string]*family
}

func NewRegistry() *Registry {
	return &Registry{families: make(map[string]*family)}
}

// Counter returns the counter for name and labels, creating it on first use.
// A name already registered as another type yields a working counter that is
// not exported, so callers never receive nil.
func (r *Registry) Counter(name, help string, labels ...string) *Counter {
	s := r.register(name, help, kindCounter, labels, func(s *series) { s.counter = new(Counter) })
	if s == nil {
		return new(Counter)
	}
	return s.counter
}

func (r *Registry) Gauge(name, help string, labels ...string) *Gauge {
	s := r.register(name, help, kindGauge, labels, func(s *series) { s.gauge = new(Gauge) })
	if s == nil {
		return new(Gauge)
	}
	if s.gauge == nil {
		slog.Error("metric already registered as a gauge function", "name", name)
		return new(Gauge)
	}
	return s.gauge
}

// GaugeFunc registers a gauge read from fn at scrape time. Registering the
// same name and labels again replaces fn, because a reload builds new state
// for the function to read.
func (r *Registry) GaugeFunc(name, help string, fn func() float64, labels ...string) {
	s := r.register(name, help, kindGauge, labels, func(*series) {})
	if s == nil {
		return
	}
	if s.gauge != nil {
		slog.Error("metric already registered as a settable gauge", "name", name)
		return
	}
	s.fn.Store(&fn)
}

// Histogram returns the histogram for name and labels. Empty buckets mean
// LatencyBuckets. The buckets of an existing histogram are not changed.
func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *Histogram {
	s := r.register(name, help, kindHistogram, labels, func(s *series) { s.histogram = newHistogram(buckets) })
	if s == nil {
		return newHistogram(buckets)
	}
	return s.histogram
}

// WriteTo writes every metric in Prometheus text format 0.0.4, families
// sorted by name and series in registration order.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	var buf []byte
	for _, f := range r.snapshot() {
		buf = f.appendText(buf)
	}
	n, err := w.Write(buf)
	return int64(n), err
}

func (r *Registry) register(name, help string, k kind, labels []string, fill func(*series)) *series {
	if len(labels)%2 != 0 {
		slog.Error("metric labels are not key, value pairs; last key dropped", "name", name)
		labels = labels[:len(labels)-1]
	}
	key := seriesKey(labels)

	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.families[name]
	if f == nil {
		f = &family{name: name, help: help, kind: k, index: make(map[string]*series)}
		r.families[name] = f
	}
	if f.kind != k {
		slog.Error("metric already registered with another type", "name", name, "registered", f.kind.String(), "requested", k.String())
		return nil
	}
	s := f.index[key]
	if s != nil {
		return s
	}
	s = &series{labels: renderLabels(labels)}
	fill(s)
	f.index[key] = s
	f.series = append(f.series, s)
	return s
}

// Delete removes the series registered under name and labels, in any label
// order. It does nothing when there is none. A handle obtained earlier keeps
// working but is no longer exported; registering the same name and labels
// again creates a fresh series starting at zero.
func (r *Registry) Delete(name string, labels ...string) {
	if len(labels)%2 != 0 {
		labels = labels[:len(labels)-1]
	}
	key := seriesKey(labels)

	r.mu.Lock()
	defer r.mu.Unlock()
	f := r.families[name]
	if f == nil {
		return
	}
	s := f.index[key]
	if s == nil {
		return
	}
	delete(f.index, key)
	if len(f.index) == 0 {
		delete(r.families, name)
		return
	}
	// A scrape in progress still reads the old slice outside the lock, so
	// the survivors go into a new one instead of being shifted in place.
	kept := make([]*series, 0, len(f.series)-1)
	for _, other := range f.series {
		if other != s {
			kept = append(kept, other)
		}
	}
	f.series = kept
}

// snapshot copies the family list so formatting, which calls gauge
// functions, runs without the registry lock.
func (r *Registry) snapshot() []family {
	r.mu.RLock()
	defer r.mu.RUnlock()
	families := make([]family, 0, len(r.families))
	for _, f := range r.families {
		families = append(families, family{name: f.name, help: f.help, kind: f.kind, series: f.series})
	}
	slices.SortFunc(families, func(a, b family) int { return strings.Compare(a.name, b.name) })
	return families
}

func (f family) appendText(buf []byte) []byte {
	buf = append(buf, "# HELP "...)
	buf = append(buf, f.name...)
	buf = append(buf, ' ')
	buf = appendEscaped(buf, f.help, false)
	buf = append(buf, "\n# TYPE "...)
	buf = append(buf, f.name...)
	buf = append(buf, ' ')
	buf = append(buf, f.kind.String()...)
	buf = append(buf, '\n')
	for _, s := range f.series {
		buf = s.appendText(buf, f.name)
	}
	return buf
}

func (s *series) appendText(buf []byte, name string) []byte {
	switch {
	case s.counter != nil:
		buf = appendName(buf, name, "", s.labels, "")
		buf = strconv.AppendUint(buf, s.counter.Value(), 10)
		return append(buf, '\n')
	case s.gauge != nil:
		buf = appendName(buf, name, "", s.labels, "")
		buf = strconv.AppendInt(buf, s.gauge.v.Load(), 10)
		return append(buf, '\n')
	case s.histogram != nil:
		return s.histogram.appendText(buf, name, s.labels)
	}
	fn := s.fn.Load()
	if fn == nil {
		return buf
	}
	buf = appendName(buf, name, "", s.labels, "")
	buf = appendFloat(buf, (*fn)())
	return append(buf, '\n')
}

func (h *Histogram) appendText(buf []byte, name, labels string) []byte {
	var cumulative uint64
	for i := range h.counts {
		cumulative += h.counts[i].Load()
		le := "+Inf"
		if i < len(h.bounds) {
			le = strconv.FormatFloat(h.bounds[i], 'g', -1, 64)
		}
		buf = appendName(buf, name, "_bucket", labels, le)
		buf = strconv.AppendUint(buf, cumulative, 10)
		buf = append(buf, '\n')
	}
	buf = appendName(buf, name, "_sum", labels, "")
	buf = appendFloat(buf, h.sumSeconds())
	buf = append(buf, '\n')
	buf = appendName(buf, name, "_count", labels, "")
	buf = strconv.AppendUint(buf, cumulative, 10)
	return append(buf, '\n')
}

// appendName writes `name+suffix{labels,le="le"} ` with the braces and each
// part only when present.
func appendName(buf []byte, name, suffix, labels, le string) []byte {
	buf = append(buf, name...)
	buf = append(buf, suffix...)
	if labels == "" && le == "" {
		return append(buf, ' ')
	}
	buf = append(buf, '{')
	buf = append(buf, labels...)
	if le != "" {
		if labels != "" {
			buf = append(buf, ',')
		}
		buf = append(buf, `le="`...)
		buf = append(buf, le...)
		buf = append(buf, '"')
	}
	return append(buf, "} "...)
}

func appendFloat(buf []byte, v float64) []byte {
	switch {
	case math.IsNaN(v):
		return append(buf, "NaN"...)
	case math.IsInf(v, 1):
		return append(buf, "+Inf"...)
	case math.IsInf(v, -1):
		return append(buf, "-Inf"...)
	}
	return strconv.AppendFloat(buf, v, 'g', -1, 64)
}

// seriesKey identifies a series independently of the order its labels were
// given in, so {a,b} and {b,a} are one series, exported in the order of the
// first registration.
func seriesKey(labels []string) string {
	if len(labels) <= 2 {
		return renderLabels(labels)
	}
	type pair struct{ name, value string }
	pairs := make([]pair, 0, len(labels)/2)
	for i := 0; i < len(labels); i += 2 {
		pairs = append(pairs, pair{name: labels[i], value: labels[i+1]})
	}
	slices.SortStableFunc(pairs, func(a, b pair) int { return strings.Compare(a.name, b.name) })
	sorted := make([]string, 0, len(labels))
	for _, p := range pairs {
		sorted = append(sorted, p.name, p.value)
	}
	return renderLabels(sorted)
}

func renderLabels(labels []string) string {
	var buf []byte
	for i := 0; i < len(labels); i += 2 {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, labels[i]...)
		buf = append(buf, `="`...)
		buf = appendEscaped(buf, labels[i+1], true)
		buf = append(buf, '"')
	}
	return string(buf)
}

// appendEscaped applies the text-format escapes: backslash and newline
// everywhere, double quote in label values only.
func appendEscaped(buf []byte, s string, quotes bool) []byte {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\\':
			buf = append(buf, `\\`...)
		case c == '\n':
			buf = append(buf, `\n`...)
		case c == '"' && quotes:
			buf = append(buf, `\"`...)
		default:
			buf = append(buf, c)
		}
	}
	return buf
}
