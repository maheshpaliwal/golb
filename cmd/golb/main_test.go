package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const validConfig = `
admin:
  addr: "off"
shutdown_grace: 1s
listeners:
  - name: web
    addr: 127.0.0.1:0
    protocol: http
    routes:
      - pool: app
pools:
  - name: app
    backends:
      - addr: 127.0.0.1:9
`

// syncBuffer lets the test read output while run is still writing it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "golb.yaml")
	err := os.WriteFile(path, []byte(content), 0o600)
	if err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{
			name:       "example config is valid",
			args:       []string{"-check", "-config", filepath.Join("..", "..", "configs", "golb.example.yaml")},
			wantCode:   exitOK,
			wantStdout: "ok",
		},
		{
			name:       "invalid config lists the problem",
			args:       []string{"-check", "-config", writeFile(t, strings.Replace(validConfig, "127.0.0.1:0", ":8080", 1))},
			wantCode:   exitFailure,
			wantStderr: "has no host",
		},
		{
			name:       "missing file",
			args:       []string{"-check", "-config", filepath.Join(t.TempDir(), "absent.yaml")},
			wantCode:   exitFailure,
			wantStderr: "read config",
		},
		{
			name:       "unknown flag",
			args:       []string{"-nope"},
			wantCode:   exitUsage,
			wantStderr: "flag provided but not defined",
		},
		{
			name:       "start with an invalid config fails",
			args:       []string{"-config", writeFile(t, "listeners: []\n")},
			wantCode:   exitFailure,
			wantStderr: "no listeners",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(context.Background(), tc.args, nil, &stdout, &stderr)
			if code != tc.wantCode {
				t.Errorf("exit code = %d, want %d (stderr: %s)", code, tc.wantCode, stderr.String())
			}
			if !strings.Contains(stdout.String(), tc.wantStdout) {
				t.Errorf("stdout %q lacks %q", stdout.String(), tc.wantStdout)
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Errorf("stderr %q lacks %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}

func TestSignals(t *testing.T) {
	path := writeFile(t, validConfig)
	signals := make(chan os.Signal, 2)
	stderr := new(syncBuffer)
	exited := make(chan int, 1)
	go func() {
		exited <- run(context.Background(), []string{"-config", path}, signals, new(syncBuffer), stderr)
	}()

	waitFor := func(text string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !strings.Contains(stderr.String(), text) {
			if time.Now().After(deadline) {
				t.Fatalf("log never showed %q:\n%s", text, stderr.String())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor("golb started")
	signals <- syscall.SIGHUP
	waitFor("configuration reloaded")
	signals <- syscall.SIGTERM

	select {
	case code := <-exited:
		if code != exitOK {
			t.Errorf("exit code after SIGTERM = %d, want %d:\n%s", code, exitOK, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process did not exit after SIGTERM")
	}
	if !strings.Contains(stderr.String(), "golb stopped") {
		t.Errorf("log lacks the stop line:\n%s", stderr.String())
	}
}
