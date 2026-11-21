package accesslog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countingDrops struct{ n atomic.Int64 }

func (c *countingDrops) Inc() { c.n.Add(1) }

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) lines(t *testing.T) []Record {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []Record
	scanner := bufio.NewScanner(bytes.NewReader(b.buf.Bytes()))
	for scanner.Scan() {
		var rec Record
		err := json.Unmarshal(scanner.Bytes(), &rec)
		if err != nil {
			t.Fatalf("line %q is not JSON: %v", scanner.Text(), err)
		}
		out = append(out, rec)
	}
	return out
}

// blockingWriter holds the writer goroutine inside Write until released.
type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func submit(l *Logger, path string) bool {
	rec := l.Acquire()
	if rec == nil {
		return false
	}
	rec.Path = path
	rec.Status = 200
	l.Submit(rec)
	return true
}

func TestLoggerWritesJSONLines(t *testing.T) {
	out := new(lockedBuffer)
	drops := new(countingDrops)
	l := New(Options{Out: out, BufferSize: 16, SampleRate: 1, Dropped: drops, Logger: quietLogger()})

	rec := l.Acquire()
	rec.Time = time.Unix(1700000000, 0).UTC()
	rec.Listener = "web"
	rec.Method = "GET"
	rec.Path = `/a"b<c>`
	rec.Status = 502
	rec.Bytes = 12
	rec.DurationMs = 1.5
	rec.Backend = "127.0.0.1:9001"
	rec.Attempts = 2
	want := *rec
	l.Submit(rec)
	l.Close()

	got := out.lines(t)
	if len(got) != 1 {
		t.Fatalf("got %d lines, want 1", len(got))
	}
	if got[0] != want {
		t.Errorf("record round trip:\n got %+v\nwant %+v", got[0], want)
	}
	if drops.n.Load() != 0 {
		t.Errorf("dropped = %d, want 0", drops.n.Load())
	}
}

func TestLoggerDropsWhenFullWithoutBlocking(t *testing.T) {
	const (
		buffer = 4
		extra  = 10
	)
	sink := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	drops := new(countingDrops)
	l := New(Options{Out: sink, BufferSize: buffer, SampleRate: 1, Dropped: drops, Logger: quietLogger()})

	// A record larger than the write buffer forces a Write, parking the writer.
	big := l.Acquire()
	big.Path = string(make([]byte, 2*writeBufferBytes))
	l.Submit(big)
	select {
	case <-sink.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("writer never reached the sink")
	}

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for range buffer + extra {
			submit(l, "/x")
		}
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("Submit blocked on a full queue")
	}
	if got := drops.n.Load(); got != extra {
		t.Errorf("dropped = %d, want %d", got, extra)
	}
	close(sink.release)
	l.Close()
}

func TestLoggerSampling(t *testing.T) {
	tests := []struct {
		name     string
		rate     float64
		min, max int
	}{
		{name: "all", rate: 1, min: 2000, max: 2000},
		{name: "none", rate: 0, min: 0, max: 0},
		{name: "about a quarter", rate: 0.25, min: 350, max: 650},
	}
	const requests = 2000
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := new(lockedBuffer)
			l := New(Options{Out: out, BufferSize: requests, SampleRate: tc.rate, Dropped: new(countingDrops), Logger: quietLogger()})
			for range requests {
				submit(l, "/")
			}
			l.Close()
			got := len(out.lines(t))
			if got < tc.min || got > tc.max {
				t.Errorf("logged %d of %d, want between %d and %d", got, requests, tc.min, tc.max)
			}
		})
	}
}

func TestLoggerSubmitAfterCloseDrops(t *testing.T) {
	drops := new(countingDrops)
	l := New(Options{Out: io.Discard, BufferSize: 4, SampleRate: 1, Dropped: drops, Logger: quietLogger()})
	l.Close()
	l.Close()
	submit(l, "/late")
	if got := drops.n.Load(); got != 1 {
		t.Errorf("dropped = %d, want 1", got)
	}
}
