package balancer

import (
	"testing"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

type benchCase struct {
	name     string
	policy   core.Policy
	backends func() []*core.Backend
}

func benchCases() []benchCase {
	weighted := func() []*core.Backend {
		weights := make([]uint32, 16)
		for i := range weights {
			weights[i] = uint32(i%4 + 1)
		}
		return newBackends(weights...)
	}
	sixteen := func() []*core.Backend { return newBackends(equalWeights(16)...) }
	hundred := func() []*core.Backend { return newBackends(equalWeights(100)...) }
	return []benchCase{
		{name: "round_robin/16", policy: core.RoundRobin, backends: sixteen},
		{name: "round_robin_weighted/16", policy: core.RoundRobin, backends: weighted},
		{name: "least_conn/16", policy: core.LeastConn, backends: sixteen},
		{name: "p2c_ewma/16", policy: core.P2CEWMA, backends: sixteen},
		{name: "ring_hash/100", policy: core.RingHash, backends: hundred},
	}
}

// benchKey gives every iteration a different, unpredictable affinity key, so
// the ring search is not flattered by a branch predictor that has learned a
// small repeating key set.
func benchKey(i uint64) uint64 { return i * goldenGamma }

func prepare(b *testing.B, tc benchCase) core.Balancer {
	b.Helper()
	backends := tc.backends()
	for i, backend := range backends {
		backend.ObserveLatency(time.Duration(i%5+1) * time.Millisecond)
	}
	return mustNew(b, tc.policy, backends)
}

// Pick followed by the in-flight bookkeeping a pool does around a request.
// Using the returned backend keeps the compiler from discarding the pick.
func BenchmarkPickDone(b *testing.B) {
	for _, tc := range benchCases() {
		b.Run(tc.name, func(b *testing.B) {
			bal := prepare(b, tc)
			b.ReportAllocs()
			var i uint64
			for b.Loop() {
				backend := bal.Pick(core.PickInfo{Key: benchKey(i)})
				backend.Acquire()
				backend.Release()
				i++
			}
		})
	}
}

func BenchmarkPickDoneParallel(b *testing.B) {
	for _, tc := range benchCases() {
		b.Run(tc.name, func(b *testing.B) {
			bal := prepare(b, tc)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				var i uint64
				for pb.Next() {
					backend := bal.Pick(core.PickInfo{Key: benchKey(i)})
					backend.Acquire()
					backend.Release()
					i++
				}
			})
		})
	}
}

// A retry: one backend already tried, a quarter of the pool unavailable.
func BenchmarkPickDoneDegraded(b *testing.B) {
	for _, tc := range benchCases() {
		b.Run(tc.name, func(b *testing.B) {
			backends := tc.backends()
			for i, backend := range backends {
				backend.ObserveLatency(time.Millisecond)
				switch i % 8 {
				case 1:
					backend.SetHealthy(false)
				case 5:
					backend.Eject(nowNanos() + int64(time.Hour))
				}
			}
			bal := mustNew(b, tc.policy, backends)
			exclude := backends[:1]
			b.ReportAllocs()
			var i uint64
			for b.Loop() {
				backend := bal.Pick(core.PickInfo{Key: benchKey(i), Exclude: exclude})
				backend.Acquire()
				backend.Release()
				i++
			}
		})
	}
}
