package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	defaultConnections = 64
	defaultDuration    = 15 * time.Second
	defaultWarmup      = 3 * time.Second
	defaultTimeout     = 10 * time.Second
	// errorBackoff slows a closed-loop worker after a failed request. Without
	// it a target that refuses or drops connections is hit by a tight
	// reconnect loop that can use up the machine's ephemeral ports.
	errorBackoff   = 10 * time.Millisecond
	readBufferSize = 16 << 10
	statusClasses  = 6 // index 0 collects anything outside 1xx..5xx
	classDivisor   = 100
	successClass   = 2

	formatText     = "text"
	formatJSON     = "json"
	formatMarkdown = "markdown"
)

var errBehindSchedule = errors.New("not sent: already behind schedule by more than the timeout")

type loadConfig struct {
	target      *url.URL
	headers     []string
	connections int
	rate        float64 // requests per second over all connections; 0 means closed loop
	duration    time.Duration
	warmup      time.Duration
	timeout     time.Duration
	runs        int
	format      string
	header      bool
	label       string
	savePath    string
}

// headerFlags collects repeated -H values.
type headerFlags []string

func (h *headerFlags) String() string { return strings.Join(*h, ", ") }

func (h *headerFlags) Set(value string) error {
	name, _, found := strings.Cut(value, ":")
	if !found || strings.TrimSpace(name) == "" || strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("header %q is not \"Name: value\"", value)
	}
	*h = append(*h, value)
	return nil
}

// runStats is the outcome of one measured window. The latency figures
// cover every request of the window, failed ones included at the time they
// took to fail: leaving out timeouts and requests that could not be sent
// would report the tail of the survivors only.
type runStats struct {
	Requests        uint64                `json:"requests"`
	Errors          uint64                `json:"errors"` // no complete response: connect, write, read, timeout, not sent
	Status          [statusClasses]uint64 `json:"status_classes"`
	ReqPerSec       float64               `json:"ok_per_sec"`        // responses with a status below 400
	ResponsesPerSec float64               `json:"responses_per_sec"` // complete responses of any status
	P50             float64               `json:"p50_us"`
	P90             float64               `json:"p90_us"`
	P99             float64               `json:"p99_us"`
	P999            float64               `json:"p999_us"`
	Max             float64               `json:"max_us"`
	Mean            float64               `json:"mean_us"`
	FirstErr        string                `json:"first_error,omitempty"`
}

// report is what one invocation prints: every run, and the run with the
// median successful throughput as the representative one. Rate is what was
// offered in fixed-rate mode; what was achieved is in the runs.
type report struct {
	Label       string     `json:"label,omitempty"`
	URL         string     `json:"url"`
	Mode        string     `json:"mode"`
	Connections int        `json:"connections"`
	Rate        float64    `json:"rate"`
	DurationSec float64    `json:"duration_s"`
	WarmupSec   float64    `json:"warmup_s"`
	GoVersion   string     `json:"go"`
	GOMAXPROCS  int        `json:"gomaxprocs"`
	Runs        []runStats `json:"runs"`
	Median      runStats   `json:"median"`
	MedianRPS   float64    `json:"median_rps"`
	MinRPS      float64    `json:"min_rps"`
	MaxRPS      float64    `json:"max_rps"`
}

func runLoad(ctx context.Context, args []string, out io.Writer) error {
	cfg, err := parseLoadFlags(args)
	if err != nil {
		return err
	}
	rep := report{
		Label:       cfg.label,
		URL:         cfg.target.String(),
		Mode:        "closed-loop",
		Connections: cfg.connections,
		Rate:        cfg.rate,
		DurationSec: cfg.duration.Seconds(),
		WarmupSec:   cfg.warmup.Seconds(),
		GoVersion:   runtime.Version(),
		GOMAXPROCS:  runtime.GOMAXPROCS(0),
	}
	if cfg.rate > 0 {
		rep.Mode = "fixed-rate"
	}
	for range cfg.runs {
		stats := measure(ctx, cfg)
		if ctx.Err() != nil {
			return fmt.Errorf("load: interrupted: %w", ctx.Err())
		}
		rep.Runs = append(rep.Runs, stats)
	}
	rep.summarise()
	if cfg.savePath != "" {
		err = rep.save(cfg.savePath)
		if err != nil {
			return fmt.Errorf("load: %w", err)
		}
	}
	return rep.write(out, cfg)
}

func parseLoadFlags(args []string) (loadConfig, error) {
	var cfg loadConfig
	var headers headerFlags
	flags := flag.NewFlagSet("load", flag.ContinueOnError)
	target := flags.String("url", "", "http URL to request, for example http://127.0.0.1:8080/")
	flags.IntVar(&cfg.connections, "c", defaultConnections, "keep-alive connections")
	flags.Float64Var(&cfg.rate, "rate", 0, "total requests per second at a fixed rate; 0 sends as fast as responses return")
	flags.DurationVar(&cfg.duration, "d", defaultDuration, "measured time per run")
	flags.DurationVar(&cfg.warmup, "warmup", defaultWarmup, "time before each run whose requests are not counted")
	flags.DurationVar(&cfg.timeout, "timeout", defaultTimeout, "per-request timeout, including connect")
	flags.IntVar(&cfg.runs, "runs", 1, "number of runs; the median run by req/s is the result")
	flags.StringVar(&cfg.format, "format", formatText, "output: text, json or markdown (one table row)")
	flags.BoolVar(&cfg.header, "header", false, "with -format markdown, print the table header before the row")
	flags.StringVar(&cfg.label, "label", "", "name shown in the output")
	flags.StringVar(&cfg.savePath, "save", "", "also write the JSON report to this file")
	flags.Var(&headers, "H", "extra request header \"Name: value\"; repeatable")
	allowRemote := flags.Bool("allow-remote", false, "permit a target that is not on loopback")
	err := flags.Parse(args)
	if err != nil {
		return cfg, err
	}
	cfg.headers = headers
	cfg.target, err = url.Parse(*target)
	if err != nil {
		return cfg, fmt.Errorf("load: parse -url: %w", err)
	}
	if cfg.target.Scheme != "http" || cfg.target.Host == "" {
		return cfg, fmt.Errorf("load: -url %q must be http://host:port/path", *target)
	}
	if cfg.target.Port() == "" {
		cfg.target.Host = net.JoinHostPort(cfg.target.Hostname(), "80")
	}
	if !*allowRemote {
		err = loopback(cfg.target.Host)
		if err != nil {
			return cfg, fmt.Errorf("load: %w (pass -allow-remote to override)", err)
		}
	}
	if cfg.connections < 1 || cfg.runs < 1 || cfg.duration <= 0 || cfg.warmup < 0 || cfg.timeout <= 0 || cfg.rate < 0 {
		return cfg, errors.New("load: -c and -runs must be at least 1, -d and -timeout positive, -warmup and -rate not negative")
	}
	if !slices.Contains([]string{formatText, formatJSON, formatMarkdown}, cfg.format) {
		return cfg, fmt.Errorf("load: unknown -format %q", cfg.format)
	}
	return cfg, nil
}

// requestBytes is the request every worker sends, built once. The load
// generator writes these bytes itself and parses responses with net/http's
// reader, which costs far less than http.Transport and so leaves more of
// the machine to the proxy under test.
func requestBytes(cfg loadConfig) []byte {
	var b strings.Builder
	b.WriteString("GET ")
	b.WriteString(cfg.target.RequestURI())
	b.WriteString(" HTTP/1.1\r\n")
	hasHost, hasAgent := false, false
	for _, header := range cfg.headers {
		name, _, _ := strings.Cut(header, ":")
		hasHost = hasHost || strings.EqualFold(strings.TrimSpace(name), "Host")
		hasAgent = hasAgent || strings.EqualFold(strings.TrimSpace(name), "User-Agent")
	}
	if !hasHost {
		b.WriteString("Host: " + cfg.target.Host + "\r\n")
	}
	if !hasAgent {
		b.WriteString("User-Agent: golb-bench\r\n")
	}
	for _, header := range cfg.headers {
		b.WriteString(header + "\r\n")
	}
	b.WriteString("\r\n")
	return []byte(b.String())
}

// schedule fixes the time window of one run.
type schedule struct {
	start      time.Time
	recordFrom time.Time // requests starting earlier are warm-up
	end        time.Time
}

func measure(ctx context.Context, cfg loadConfig) runStats {
	start := time.Now()
	plan := schedule{start: start, recordFrom: start.Add(cfg.warmup), end: start.Add(cfg.warmup + cfg.duration)}
	request := requestBytes(cfg)
	workers := make([]*worker, cfg.connections)
	var wg sync.WaitGroup
	for i := range workers {
		w := &worker{addr: cfg.target.Host, request: request, timeout: cfg.timeout, plan: plan}
		if cfg.rate > 0 {
			// Each connection sends rate/connections per second, offset so
			// the connections together are evenly spaced.
			w.interval = float64(cfg.connections) / cfg.rate * float64(time.Second)
			w.offset = time.Duration(float64(i) / cfg.rate * float64(time.Second))
		}
		workers[i] = w
		wg.Go(func() {
			defer logPanic("load worker")
			w.run(ctx)
		})
	}
	wg.Wait()

	var latency histogram
	var stats runStats
	for _, w := range workers {
		latency.merge(&w.latency)
		stats.Errors += w.errors
		for class, n := range w.status {
			stats.Status[class] += n
		}
		if stats.FirstErr == "" {
			stats.FirstErr = w.firstErr
		}
	}
	stats.Requests = latency.total
	stats.ReqPerSec = float64(stats.Status[2]+stats.Status[3]) / cfg.duration.Seconds()
	stats.ResponsesPerSec = float64(stats.Requests-stats.Errors) / cfg.duration.Seconds()
	stats.P50 = micros(latency.quantile(0.50))
	stats.P90 = micros(latency.quantile(0.90))
	stats.P99 = micros(latency.quantile(0.99))
	stats.P999 = micros(latency.quantile(0.999))
	stats.Max = micros(latency.max)
	stats.Mean = micros(latency.mean())
	return stats
}

func micros(d time.Duration) float64 { return float64(d) / float64(time.Microsecond) }

// worker owns one connection and sends requests on it one at a time.
type worker struct {
	addr     string
	request  []byte
	timeout  time.Duration
	plan     schedule
	interval float64       // nanoseconds between sends in fixed-rate mode; 0 in closed loop
	offset   time.Duration // start offset in fixed-rate mode

	conn   net.Conn
	reader *bufio.Reader
	timer  *time.Timer

	latency  histogram // every request, failed ones at the time they took to fail
	status   [statusClasses]uint64
	errors   uint64
	firstErr string
}

func (w *worker) run(ctx context.Context) {
	defer w.drop()
	for sent := 0; ; sent++ {
		from, ok := w.pace(ctx, sent)
		if !ok {
			return
		}
		// A fixed-rate request already later than the timeout has failed
		// whatever the target does now. Skipping the send lets a worker
		// that fell hopelessly behind finish its schedule instead of
		// extending the run by the whole backlog.
		status, err := 0, errBehindSchedule
		if time.Since(from) <= w.timeout {
			status, err = w.do(ctx)
		}
		elapsed := time.Since(from)
		if err != nil && (ctx.Err() != nil || w.interval == 0 && !w.sleep(ctx, errorBackoff)) {
			return
		}
		if from.Before(w.plan.recordFrom) {
			continue
		}
		w.latency.record(elapsed)
		if err != nil {
			w.errors++
			if w.firstErr == "" {
				w.firstErr = err.Error()
			}
			continue
		}
		class := status / classDivisor
		if class >= statusClasses {
			class = 0
		}
		w.status[class]++
	}
}

// pace waits for the next send and returns the instant its latency is
// measured from. In fixed-rate mode that is always the intended send time,
// so time a request spent queued behind a slow response counts against it.
func (w *worker) pace(ctx context.Context, sent int) (time.Time, bool) {
	now := time.Now()
	if ctx.Err() != nil {
		return now, false
	}
	if w.interval == 0 {
		return now, now.Before(w.plan.end)
	}
	intended := w.plan.start.Add(w.offset + time.Duration(float64(sent)*w.interval))
	if !intended.Before(w.plan.end) {
		return now, false
	}
	if now.Before(intended) && !w.sleep(ctx, intended.Sub(now)) {
		return now, false
	}
	return intended, true
}

func (w *worker) sleep(ctx context.Context, d time.Duration) bool {
	if w.timer == nil {
		w.timer = time.NewTimer(d)
	} else {
		w.timer.Reset(d)
	}
	select {
	case <-ctx.Done():
		w.timer.Stop()
		return false
	case <-w.timer.C:
		return true
	}
}

// do sends one request and reads the whole response.
func (w *worker) do(ctx context.Context) (status int, err error) {
	if w.conn == nil {
		dialer := net.Dialer{Timeout: w.timeout}
		w.conn, err = dialer.DialContext(ctx, "tcp", w.addr)
		if err != nil {
			w.conn = nil
			return 0, err
		}
		if w.reader == nil {
			w.reader = bufio.NewReaderSize(w.conn, readBufferSize)
		}
		w.reader.Reset(w.conn)
	}
	status, closing, err := w.exchange()
	if err != nil || closing {
		w.drop()
	}
	return status, err
}

func (w *worker) exchange() (status int, closing bool, err error) {
	err = w.conn.SetDeadline(time.Now().Add(w.timeout))
	if err != nil {
		return 0, false, err
	}
	_, err = w.conn.Write(w.request)
	if err != nil {
		return 0, false, err
	}
	resp, err := http.ReadResponse(w.reader, nil)
	if err != nil {
		return 0, false, err
	}
	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		return 0, false, err
	}
	err = resp.Body.Close()
	return resp.StatusCode, resp.Close, err
}

func (w *worker) drop() {
	if w.conn == nil {
		return
	}
	err := w.conn.Close()
	if err != nil {
		slog.Debug("close load connection", "error", err)
	}
	w.conn = nil
}

func (r *report) summarise() {
	sorted := slices.Clone(r.Runs)
	slices.SortFunc(sorted, func(a, b runStats) int {
		switch {
		case a.ReqPerSec < b.ReqPerSec:
			return -1
		case a.ReqPerSec > b.ReqPerSec:
			return 1
		}
		return 0
	})
	r.Median = sorted[(len(sorted)-1)/2]
	r.MedianRPS = r.Median.ReqPerSec
	r.MinRPS = sorted[0].ReqPerSec
	r.MaxRPS = sorted[len(sorted)-1].ReqPerSec
}

const reportFileMode = 0o644

func (r *report) save(path string) error {
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	return os.WriteFile(path, append(data, '\n'), reportFileMode)
}

func (r *report) write(out io.Writer, cfg loadConfig) error {
	switch cfg.format {
	case formatJSON:
		return json.NewEncoder(out).Encode(r)
	case formatMarkdown:
		row := r.markdownRow()
		if cfg.header {
			row = markdownHeader + "\n" + row
		}
		_, err := fmt.Fprintln(out, row)
		return err
	}
	_, err := io.WriteString(out, r.text())
	return err
}

// markdownHeader matches markdownRow; -header prints it above the row.
const markdownHeader = `| scenario | offered req/s | ok req/s (median) | ok req/s (min - max) | errors | non-2xx | p50 | p90 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|---|---|---|`

func (r *report) markdownRow() string {
	var errs, other uint64
	for _, run := range r.Runs {
		errs += run.Errors
		other += run.nonSuccess()
	}
	m := r.Median
	offered := "closed loop"
	if r.Rate > 0 {
		offered = fmt.Sprintf("%.0f", r.Rate)
	}
	return fmt.Sprintf("| %s | %s | %.0f | %.0f - %.0f | %d | %d | %s | %s | %s | %s | %s |",
		r.Label, offered, r.MedianRPS, r.MinRPS, r.MaxRPS, errs, other,
		duration(m.P50), duration(m.P90), duration(m.P99), duration(m.P999), duration(m.Max))
}

func (r *report) text() string {
	var b strings.Builder
	if r.Label != "" {
		fmt.Fprintf(&b, "label        %s\n", r.Label)
	}
	fmt.Fprintf(&b, "target       %s\n", r.URL)
	if r.Rate > 0 {
		fmt.Fprintf(&b, "mode         fixed-rate, %.0f req/s over %d connections\n", r.Rate, r.Connections)
	} else {
		fmt.Fprintf(&b, "mode         closed-loop, %d connections\n", r.Connections)
	}
	fmt.Fprintf(&b, "window       %gs measured after %gs warm-up, %d run(s)\n", r.DurationSec, r.WarmupSec, len(r.Runs))
	fmt.Fprintf(&b, "runtime      %s, GOMAXPROCS %d\n", r.GoVersion, r.GOMAXPROCS)
	for i, run := range r.Runs {
		fmt.Fprintf(&b, "\nrun %d\n", i+1)
		run.writeText(&b)
	}
	if len(r.Runs) > 1 {
		fmt.Fprintf(&b, "\nmedian run   %.1f ok req/s (min %.1f, max %.1f)\n", r.MedianRPS, r.MinRPS, r.MaxRPS)
		r.Median.writeText(&b)
	}
	return b.String()
}

func (s runStats) nonSuccess() uint64 {
	var n uint64
	for class, count := range s.Status {
		if class != successClass {
			n += count
		}
	}
	return n
}

func (s runStats) writeText(b *strings.Builder) {
	fmt.Fprintf(b, "  requests   %d\n", s.Requests)
	fmt.Fprintf(b, "  ok req/s   %.1f (status below 400)\n", s.ReqPerSec)
	fmt.Fprintf(b, "  responses  %.1f/s (any status)\n", s.ResponsesPerSec)
	fmt.Fprintf(b, "  errors     %d\n", s.Errors)
	if s.FirstErr != "" {
		fmt.Fprintf(b, "  first err  %s\n", s.FirstErr)
	}
	fmt.Fprintf(b, "  status     2xx=%d 3xx=%d 4xx=%d 5xx=%d other=%d\n", s.Status[2], s.Status[3], s.Status[4], s.Status[5], s.Status[0]+s.Status[1])
	fmt.Fprintf(b, "  latency    (all requests, failed ones included) p50=%s p90=%s p99=%s p99.9=%s max=%s mean=%s\n",
		duration(s.P50), duration(s.P90), duration(s.P99), duration(s.P999), duration(s.Max), duration(s.Mean))
}

// duration formats microseconds with a unit that keeps three or four digits.
func duration(us float64) string {
	const perMilli, perSecond = 1e3, 1e6
	switch {
	case us >= perSecond:
		return fmt.Sprintf("%.2fs", us/perSecond)
	case us >= perMilli:
		return fmt.Sprintf("%.2fms", us/perMilli)
	}
	return fmt.Sprintf("%.0fµs", us)
}
