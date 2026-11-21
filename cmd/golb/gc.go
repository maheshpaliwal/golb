package main

import (
	"log/slog"
	"os"
	"runtime/debug"
	"runtime/metrics"
	"time"
)

// The Go collector starts a cycle when the heap has grown by GOGC percent
// over what survived the last one. A proxy keeps only a few megabytes alive
// and allocates hundreds of megabytes per second under load, so the stock
// setting collects every few megabytes: about 120 cycles per second at
// 40,000 requests per second, which measured as about a sixth of golb's CPU
// time per request (docs/BENCHMARKS.md, section 6).
//
// Unless the operator set GOGC, golb raises the percentage while the live
// heap is small so that a cycle starts only after about gcHeapFloorBytes of
// new allocation. Once that much is alive the stock 100 percent applies, so
// the heap a cycle may reach is never more than the floor above what the
// stock setting allows, however large the heap grows.
const (
	gcEnv            = "GOGC"
	gcHeapFloorBytes = 16 << 20
	gcStockPercent   = 100
	// gcMaxPercent caps the setting, which bounds the overshoot when the
	// live heap grows between two adjustments. With the runtime's 4 MiB
	// minimum heap it still gives the floor when almost nothing is alive.
	gcMaxPercent     = 400
	gcAdjustInterval = time.Second
	liveHeapMetric   = "/gc/heap/live:bytes"
)

// gcPercent is the GOGC value that lets a heap with liveBytes alive grow by
// the floor before the next collection.
func gcPercent(liveBytes uint64) int {
	if liveBytes >= gcHeapFloorBytes {
		return gcStockPercent
	}
	if liveBytes == 0 {
		return gcMaxPercent
	}
	return int(min(gcHeapFloorBytes*100/liveBytes, gcMaxPercent))
}

// startGCTuning applies gcPercent and keeps it in step with the live heap.
// The returned function stops that and restores the previous setting. When
// GOGC is set the operator's choice stands and nothing is started.
func startGCTuning(logger *slog.Logger) (stop func()) {
	if os.Getenv(gcEnv) != "" {
		return func() {}
	}
	sample := []metrics.Sample{{Name: liveHeapMetric}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		logger.Warn("live heap size not available; leaving the collector at its default", "metric", liveHeapMetric)
		return func() {}
	}
	current := gcPercent(sample[0].Value.Uint64())
	previous := debug.SetGCPercent(current)

	quit := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		defer func() {
			if r := recover(); r != nil {
				logger.Error("background task stopped", "task", "gc tuning", "panic", r)
			}
		}()
		ticker := time.NewTicker(gcAdjustInterval)
		defer ticker.Stop()
		for {
			select {
			case <-quit:
				return
			case <-ticker.C:
			}
			metrics.Read(sample)
			wanted := gcPercent(sample[0].Value.Uint64())
			if wanted != current {
				current = wanted
				debug.SetGCPercent(current)
			}
		}
	}()
	return func() {
		close(quit)
		<-stopped
		debug.SetGCPercent(previous)
	}
}
