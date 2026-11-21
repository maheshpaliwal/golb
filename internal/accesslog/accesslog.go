// Package accesslog writes sampled JSON access records without ever blocking
// the request path: a full queue drops the record and counts it.
package accesslog

import (
	"bufio"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

const writeBufferBytes = 64 << 10

// Record is one request. Strings must not alias memory the caller reuses.
type Record struct {
	Time       time.Time `json:"time"`
	Listener   string    `json:"listener"`
	Route      string    `json:"route"`
	Client     string    `json:"client"`
	Method     string    `json:"method"`
	Host       string    `json:"host"`
	Path       string    `json:"path"`
	Proto      string    `json:"proto"`
	Status     int       `json:"status"`
	Bytes      int64     `json:"bytes"`
	DurationMs float64   `json:"duration_ms"`
	Backend    string    `json:"backend"`
	Attempts   int       `json:"attempts"`
}

// Counter is the part of a metrics counter the logger needs.
type Counter interface {
	Inc()
}

type Options struct {
	Out        io.Writer
	BufferSize int     // queued records before dropping
	SampleRate float64 // 0..1
	Dropped    Counter // incremented once per dropped record
	Logger     *slog.Logger
}

// Logger owns one writer goroutine, started by New and stopped by Close.
type Logger struct {
	queue   chan *Record
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
	closed  atomic.Bool
	sample  atomic.Uint64 // math.Float64bits of the rate
	records sync.Pool
	dropped Counter
	logger  *slog.Logger
}

func New(opts Options) *Logger {
	l := &Logger{
		queue:   make(chan *Record, opts.BufferSize),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		records: sync.Pool{New: func() any { return new(Record) }},
		dropped: opts.Dropped,
		logger:  opts.Logger,
	}
	l.SetSampleRate(opts.SampleRate)
	go l.run(opts.Out)
	return l
}

// SetSampleRate changes the fraction of requests logged; safe during traffic.
func (l *Logger) SetSampleRate(rate float64) {
	l.sample.Store(math.Float64bits(rate))
}

// Acquire returns a zeroed record to fill and pass to Submit, or nil when
// this request is not sampled.
func (l *Logger) Acquire() *Record {
	rate := math.Float64frombits(l.sample.Load())
	if rate < 1 && rand.Float64() >= rate {
		return nil
	}
	return l.records.Get().(*Record)
}

// Submit queues the record. It never blocks; the record must not be used afterwards.
func (l *Logger) Submit(rec *Record) {
	if l.closed.Load() {
		l.drop(rec)
		return
	}
	select {
	case l.queue <- rec:
	default:
		l.drop(rec)
	}
}

// Close writes everything already queued, flushes and stops the writer.
func (l *Logger) Close() {
	l.once.Do(func() {
		l.closed.Store(true)
		close(l.stop)
	})
	<-l.done
}

func (l *Logger) drop(rec *Record) {
	l.dropped.Inc()
	l.release(rec)
}

func (l *Logger) release(rec *Record) {
	*rec = Record{}
	l.records.Put(rec)
}

func (l *Logger) run(out io.Writer) {
	defer close(l.done)
	defer func() {
		if r := recover(); r != nil {
			l.logger.Error("access log writer stopped", "panic", r)
		}
	}()
	buf := bufio.NewWriterSize(out, writeBufferBytes)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	failed := false
	write := func(rec *Record) {
		err := enc.Encode(rec)
		l.release(rec)
		if err == nil && len(l.queue) == 0 {
			err = buf.Flush()
		}
		if err != nil && !failed {
			// Logged once: a broken sink would otherwise log per request.
			failed = true
			l.logger.Error("write access log", "error", err)
		}
	}
	for {
		select {
		case rec := <-l.queue:
			write(rec)
		case <-l.stop:
			l.drain(write)
			return
		}
	}
}

func (l *Logger) drain(write func(*Record)) {
	for {
		select {
		case rec := <-l.queue:
			write(rec)
		default:
			return
		}
	}
}
