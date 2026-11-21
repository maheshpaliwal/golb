package main

import (
	"io"
	"log/slog"
	"runtime/debug"
	"testing"
)

func TestGCPercent(t *testing.T) {
	tests := []struct {
		name      string
		liveBytes uint64
		want      int
	}{
		{name: "nothing alive yet", liveBytes: 0, want: gcMaxPercent},
		{name: "tiny heap is capped", liveBytes: 1 << 20, want: gcMaxPercent},
		{name: "small heap grows by the floor", liveBytes: 8 << 20, want: 200},
		{name: "just below the floor", liveBytes: gcHeapFloorBytes - 1, want: gcStockPercent},
		{name: "at the floor", liveBytes: gcHeapFloorBytes, want: gcStockPercent},
		{name: "large heap", liveBytes: 4 << 30, want: gcStockPercent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := gcPercent(tc.liveBytes)
			if got != tc.want {
				t.Fatalf("gcPercent(%d) = %d, want %d", tc.liveBytes, got, tc.want)
			}
			// The heap a cycle may reach is never more than the floor above
			// what the stock setting allows.
			growth := tc.liveBytes * uint64(got) / 100
			if growth > max(tc.liveBytes, gcHeapFloorBytes) {
				t.Fatalf("heap may grow by %d with %d live bytes, want at most the floor", growth, tc.liveBytes)
			}
		})
	}
}

// currentGCPercent reads the setting the only way the runtime offers: by
// writing it.
func currentGCPercent() int {
	percent := debug.SetGCPercent(gcStockPercent)
	debug.SetGCPercent(percent)
	return percent
}

func TestStartGCTuning(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	before := currentGCPercent()

	t.Run("raises the setting and restores it", func(t *testing.T) {
		t.Setenv(gcEnv, "")
		stop := startGCTuning(logger)
		during := currentGCPercent()
		stop()
		if during <= gcStockPercent || during > gcMaxPercent {
			t.Errorf("GC percent while tuning = %d, want above %d and at most %d", during, gcStockPercent, gcMaxPercent)
		}
		after := currentGCPercent()
		if after != before {
			t.Errorf("GC percent after stop = %d, want %d", after, before)
		}
	})

	t.Run("leaves an explicit GOGC alone", func(t *testing.T) {
		t.Setenv(gcEnv, "50")
		stop := startGCTuning(logger)
		during := currentGCPercent()
		stop()
		if during != before {
			t.Errorf("GC percent with GOGC set = %d, want %d", during, before)
		}
	})
}
