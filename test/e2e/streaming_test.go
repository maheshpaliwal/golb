package e2e

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	streamTotal = 128 << 20
	streamFirst = 64 << 10
	streamChunk = 1 << 20
	// A proxy that buffered the body would hold all of streamTotal.
	streamHeapLimit = streamTotal / 2
)

// heapWatch samples the heap while a transfer runs and reports the highest
// growth over the level it started at.
type heapWatch struct {
	base uint64
	peak atomic.Uint64
	stop chan struct{}
	done chan struct{}
}

func watchHeap() *heapWatch {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	h := &heapWatch{base: stats.HeapAlloc, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(h.done)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-h.stop:
				return
			case <-ticker.C:
			}
			var now runtime.MemStats
			runtime.ReadMemStats(&now)
			if now.HeapAlloc > h.base && now.HeapAlloc-h.base > h.peak.Load() {
				h.peak.Store(now.HeapAlloc - h.base)
			}
		}
	}()
	return h
}

func (h *heapWatch) growth() uint64 {
	close(h.stop)
	<-h.done
	return h.peak.Load()
}

func TestLargeUploadIsStreamed(t *testing.T) {
	gotFirst := make(chan struct{})
	sink := newBackend(t, "sink", func(w http.ResponseWriter, r *http.Request) {
		first := make([]byte, streamFirst)
		_, err := io.ReadFull(r.Body, first)
		if err != nil {
			t.Errorf("backend: read first bytes: %v", err)
			return
		}
		close(gotFirst)
		rest, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Errorf("backend: read body: %v", err)
		}
		writeString(w, strconv.FormatInt(int64(len(first))+rest, 10))
	})
	in := start(t, webConfig(poolOf("round_robin", sink), ""))
	client := newClient(t)
	client.Timeout = time.Minute

	heap := watchHeap()
	reader, writer := io.Pipe()
	go func() {
		chunk := make([]byte, streamChunk)
		_, err := writer.Write(chunk[:streamFirst])
		if err != nil {
			writer.CloseWithError(err)
			return
		}
		// The rest is only written once the backend has the first bytes: a
		// proxy that waits for the whole body before forwarding stalls here.
		select {
		case <-gotFirst:
		case <-time.After(waitTimeout):
			writer.CloseWithError(fmt.Errorf("backend did not receive the first %d bytes while the upload was still open", streamFirst))
			return
		}
		for sent := streamFirst; sent < streamTotal; sent += streamChunk {
			_, err = writer.Write(chunk[:min(streamChunk, streamTotal-sent)])
			if err != nil {
				writer.CloseWithError(err)
				return
			}
		}
		writer.CloseWithError(nil)
	}()

	req, err := http.NewRequest(http.MethodPost, in.url("/upload"), reader)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	r, err := do(client, req)
	growth := heap.growth()
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if r.status != http.StatusOK || r.body != strconv.Itoa(streamTotal) {
		t.Errorf("status %d, backend received %q bytes, want 200 and %d", r.status, r.body, streamTotal)
	}
	t.Logf("heap grew by at most %d MiB during a %d MiB upload", growth>>20, streamTotal>>20)
	if growth > streamHeapLimit {
		t.Errorf("heap grew by %d MiB during a %d MiB upload: the body is being buffered", growth>>20, streamTotal>>20)
	}
}

func TestLargeDownloadIsStreamed(t *testing.T) {
	tests := []struct {
		name    string
		declare bool
	}{
		{name: "chunked"},
		{name: "with Content-Length", declare: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clientGotFirst := make(chan struct{})
			source := newBackend(t, "source", func(w http.ResponseWriter, r *http.Request) {
				if tc.declare {
					w.Header().Set("Content-Length", strconv.Itoa(streamTotal))
				}
				chunk := make([]byte, streamChunk)
				_, err := w.Write(chunk[:streamFirst])
				if err != nil {
					return
				}
				w.(http.Flusher).Flush()
				select {
				case <-clientGotFirst:
				case <-time.After(waitTimeout):
					t.Errorf("client did not receive the first %d bytes while the response was still open", streamFirst)
					return
				}
				for sent := streamFirst; sent < streamTotal; sent += streamChunk {
					_, err = w.Write(chunk[:min(streamChunk, streamTotal-sent)])
					if err != nil {
						return
					}
				}
			})
			in := start(t, webConfig(poolOf("round_robin", source), ""))
			client := newClient(t)
			client.Timeout = time.Minute

			heap := watchHeap()
			began := time.Now()
			res, err := client.Get(in.url("/download"))
			if err != nil {
				t.Fatalf("download: %v", err)
			}
			defer res.Body.Close()
			_, err = io.ReadFull(res.Body, make([]byte, streamFirst))
			if err != nil {
				t.Fatalf("read first bytes: %v", err)
			}
			firstByte := time.Since(began)
			close(clientGotFirst)
			rest, err := io.Copy(io.Discard, res.Body)
			growth := heap.growth()
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if got := streamFirst + rest; got != streamTotal {
				t.Errorf("received %d bytes, want %d", got, streamTotal)
			}
			if tc.declare && res.ContentLength != streamTotal {
				t.Errorf("Content-Length %d, want %d", res.ContentLength, streamTotal)
			}
			t.Logf("first bytes after %v; heap grew by at most %d MiB during a %d MiB download", firstByte, growth>>20, streamTotal>>20)
			if firstByte > time.Second {
				t.Errorf("first bytes arrived after %v", firstByte)
			}
			if growth > streamHeapLimit {
				t.Errorf("heap grew by %d MiB during a %d MiB download: the body is being buffered", growth>>20, streamTotal>>20)
			}
		})
	}
}

func TestStreamingResponseFlushesEachEvent(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
	}{
		{name: "server-sent events", contentType: "text/event-stream"},
		{name: "chunked response of another type", contentType: "application/x-ndjson"},
	}
	const events = 4
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ack := make(chan struct{})
			stream := newBackend(t, "stream", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				for i := range events {
					writeString(w, fmt.Sprintf("data: %d\n\n", i))
					w.(http.Flusher).Flush()
					// The next event is only produced after the client has
					// read this one, so a buffering proxy delivers nothing.
					select {
					case <-ack:
					case <-time.After(2 * time.Second):
						t.Errorf("event %d was not delivered to the client while the stream was open", i)
						return
					}
				}
			})
			in := start(t, webConfig(poolOf("round_robin", stream), ""))
			client := newClient(t)

			res, err := client.Get(in.url("/events"))
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}
			defer res.Body.Close()
			if got := res.Header.Get("Content-Type"); got != tc.contentType {
				t.Errorf("Content-Type %q, want %q", got, tc.contentType)
			}
			lines := bufio.NewReader(res.Body)
			for i := range events {
				began := time.Now()
				line, err := lines.ReadString('\n')
				if err != nil {
					t.Fatalf("read event %d: %v", i, err)
				}
				if want := fmt.Sprintf("data: %d\n", i); line != want {
					t.Fatalf("event %d: got %q, want %q", i, line, want)
				}
				_, err = lines.ReadString('\n')
				if err != nil {
					t.Fatalf("read event %d separator: %v", i, err)
				}
				if waited := time.Since(began); waited > time.Second {
					t.Errorf("event %d arrived after %v", i, waited)
				}
				ack <- struct{}{}
			}
		})
	}
}

func TestResponseTrailersReachTheClient(t *testing.T) {
	source := newBackend(t, "source", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Trailer", "X-Response-Sum")
		writeString(w, "payload")
		w.Header().Set("X-Response-Sum", "abc123")
	})
	in := start(t, webConfig(poolOf("round_robin", source), ""))
	res, err := newClient(t).Get(in.url("/"))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "payload" {
		t.Errorf("body %q, want %q", body, "payload")
	}
	if got := res.Trailer.Get("X-Response-Sum"); got != "abc123" {
		t.Errorf("response trailer %q, want %q", got, "abc123")
	}
}

// PRD 2.1 item 1 and the README list trailers as supported.
func TestRequestTrailersReachTheBackend(t *testing.T) {
	knownBug(t, "BUG: request trailers are dropped: the backend receives the trailer name announced in the Trailer header "+
		"but an empty value after the body (response trailers work)")
	var announced, value atomic.Value
	sink := newBackend(t, "sink", func(w http.ResponseWriter, r *http.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Errorf("backend: read body: %v", err)
		}
		// Request trailers are available once the body has been read.
		_, declared := r.Trailer["X-Request-Sum"]
		announced.Store(declared)
		value.Store(r.Trailer.Get("X-Request-Sum"))
	})
	in := start(t, webConfig(poolOf("round_robin", sink), ""))

	req, err := http.NewRequest(http.MethodPost, in.url("/"), io.NopCloser(strings.NewReader("payload")))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Trailer = http.Header{"X-Request-Sum": {"abc123"}}
	r, err := do(newClient(t), req)
	if err != nil || r.status != http.StatusOK {
		t.Fatalf("request: status %d err %v", r.status, err)
	}
	if got := value.Load(); got != "abc123" {
		t.Errorf("request trailer at the backend: %q (announced: %v), want %q", got, announced.Load(), "abc123")
	}
}

const webSocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

func webSocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + webSocketGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

const (
	opText  = 0x1
	opClose = 0x8
	finBit  = 0x80
	maskBit = 0x80
)

// writeFrame writes one unfragmented WebSocket frame with a payload under
// 126 bytes; clients mask, servers do not.
func writeFrame(w io.Writer, opcode byte, payload []byte, masked bool) error {
	frame := []byte{finBit | opcode, byte(len(payload))}
	if masked {
		key := [4]byte{0x12, 0x34, 0x56, 0x78}
		frame[1] |= maskBit
		frame = append(frame, key[:]...)
		for i, b := range payload {
			frame = append(frame, b^key[i%4])
		}
	} else {
		frame = append(frame, payload...)
	}
	_, err := w.Write(frame)
	return err
}

func readFrame(r io.Reader) (opcode byte, payload []byte, err error) {
	head := make([]byte, 2)
	_, err = io.ReadFull(r, head)
	if err != nil {
		return 0, nil, err
	}
	length := int(head[1] &^ maskBit)
	var key [4]byte
	masked := head[1]&maskBit != 0
	if masked {
		_, err = io.ReadFull(r, key[:])
		if err != nil {
			return 0, nil, err
		}
	}
	payload = make([]byte, length)
	_, err = io.ReadFull(r, payload)
	if err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= key[i%4]
		}
	}
	return head[0] &^ finBit, payload, nil
}

func TestWebSocketEcho(t *testing.T) {
	closed := make(chan struct{})
	var forwardedFor atomic.Value
	ws := newBackend(t, "ws", func(w http.ResponseWriter, r *http.Request) {
		defer close(closed)
		forwardedFor.Store(r.Header.Get("X-Forwarded-For"))
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || !strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
			t.Errorf("backend: upgrade headers did not arrive: Upgrade=%q Connection=%q", r.Header.Get("Upgrade"), r.Header.Get("Connection"))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("backend: hijack: %v", err)
			return
		}
		defer closeConn(conn)
		_, err = fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n",
			webSocketAccept(r.Header.Get("Sec-WebSocket-Key")))
		if err == nil {
			err = rw.Flush()
		}
		if err != nil {
			t.Errorf("backend: write handshake: %v", err)
			return
		}
		for {
			opcode, payload, err := readFrame(rw)
			if err != nil {
				return
			}
			err = writeFrame(conn, opcode, payload, false)
			if err != nil || opcode == opClose {
				return
			}
		}
	})
	in := start(t, webConfig(poolOf("round_robin", ws), ""))

	conn, err := net.DialTimeout("tcp", in.addr(web), waitTimeout)
	if err != nil {
		t.Fatalf("dial golb: %v", err)
	}
	defer closeConn(conn)
	err = conn.SetDeadline(time.Now().Add(waitTimeout))
	if err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	const key = "dGhlIHNhbXBsZSBub25jZQ=="
	_, err = fmt.Fprintf(conn, "GET /chat HTTP/1.1\r\nHost: ws.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", key)
	if err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	reader := bufio.NewReader(conn)
	res, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	if res.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake status %d, want 101", res.StatusCode)
	}
	if got := res.Header.Get("Sec-WebSocket-Accept"); got != webSocketAccept(key) {
		t.Errorf("Sec-WebSocket-Accept %q, want %q", got, webSocketAccept(key))
	}
	if !strings.EqualFold(res.Header.Get("Upgrade"), "websocket") || !strings.EqualFold(res.Header.Get("Connection"), "upgrade") {
		t.Errorf("handshake response lost its upgrade headers: Upgrade=%q Connection=%q", res.Header.Get("Upgrade"), res.Header.Get("Connection"))
	}
	if got := in.backend(ws.addr).Inflight; got != 1 {
		t.Errorf("in-flight during the WebSocket session: %d, want 1", got)
	}

	for _, message := range []string{"hello", "through the proxy", strings.Repeat("z", 120)} {
		err = writeFrame(conn, opText, []byte(message), true)
		if err != nil {
			t.Fatalf("send %q: %v", message, err)
		}
		opcode, payload, err := readFrame(reader)
		if err != nil {
			t.Fatalf("read echo of %q: %v", message, err)
		}
		if opcode != opText || string(payload) != message {
			t.Errorf("echo: opcode %d payload %q, want text %q", opcode, payload, message)
		}
	}
	err = writeFrame(conn, opClose, nil, true)
	if err != nil {
		t.Fatalf("send close: %v", err)
	}
	opcode, _, err := readFrame(reader)
	if err != nil || opcode != opClose {
		t.Errorf("close handshake: opcode %d err %v, want a close frame", opcode, err)
	}
	closeConn(conn)

	select {
	case <-closed:
	case <-time.After(waitTimeout):
		t.Errorf("backend side of the WebSocket was not closed")
	}
	if got := forwardedFor.Load(); got != "127.0.0.1" {
		t.Errorf("X-Forwarded-For on the upgrade request: %v, want 127.0.0.1", got)
	}
	if !eventually(2*time.Second, func() bool { return in.backend(ws.addr).Inflight == 0 }) {
		t.Errorf("in-flight stayed at %d after the WebSocket closed", in.backend(ws.addr).Inflight)
	}
}
