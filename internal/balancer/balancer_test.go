package balancer

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

const picks = 100_000

var policies = []core.Policy{core.RoundRobin, core.LeastConn, core.P2CEWMA, core.RingHash}

func newBackends(weights ...uint32) []*core.Backend {
	backends := make([]*core.Backend, len(weights))
	for i, w := range weights {
		backends[i] = core.NewBackend(fmt.Sprintf("127.0.0.1:%d", 9000+i), w)
	}
	return backends
}

func equalWeights(n int) []uint32 {
	weights := make([]uint32, n)
	for i := range weights {
		weights[i] = 1
	}
	return weights
}

func mustNew(t testing.TB, policy core.Policy, backends []*core.Backend) core.Balancer {
	t.Helper()
	bal, err := New(policy, backends)
	if err != nil {
		t.Fatalf("build %s balancer: %v", policy, err)
	}
	return bal
}

// key returns the affinity hash of the i-th synthetic client.
func key(i int) uint64 {
	return core.Hash(fmt.Sprintf("10.%d.%d.%d", i>>16&255, i>>8&255, i&255))
}

func nowNanos() int64 { return time.Now().UnixNano() }

// assertShares checks each backend's share of the total against its weight
// share, within a relative tolerance.
func assertShares(t *testing.T, backends []*core.Backend, counts map[*core.Backend]int, tolerance float64) {
	t.Helper()
	var total, weights float64
	for _, b := range backends {
		total += float64(counts[b])
		weights += float64(b.Weight)
	}
	for _, b := range backends {
		want := total * float64(b.Weight) / weights
		if got := float64(counts[b]); math.Abs(got-want) > want*tolerance {
			t.Errorf("backend %s weight %d: got %.0f of %.0f picks, want %.0f within %.0f%%", b.Addr, b.Weight, got, total, want, tolerance*100)
		}
	}
}

func TestNewRejectsBadInput(t *testing.T) {
	tests := []struct {
		name     string
		policy   core.Policy
		backends []*core.Backend
		wantErr  error
	}{
		{name: "no backends", policy: core.RoundRobin, wantErr: errNoBackends},
		{name: "unknown policy", policy: "maglev", backends: newBackends(1), wantErr: errUnknownPolicy},
		{name: "zero weight", policy: core.P2CEWMA, backends: newBackends(1, 0)},
		{name: "weight above maximum", policy: core.RingHash, backends: newBackends(core.MaxWeight + 1)},
		{name: "nil backend", policy: core.LeastConn, backends: []*core.Backend{nil}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bal, err := New(tc.policy, tc.backends)
			if err == nil || bal != nil {
				t.Fatalf("got balancer %v and error %v, want an error and no balancer", bal, err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("got error %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestNewCopiesBackends(t *testing.T) {
	backends := newBackends(1, 1)
	kept := backends[0]
	bal := mustNew(t, core.RoundRobin, backends)
	backends[0], backends[1] = nil, nil
	if got := bal.Pick(core.PickInfo{}); got != kept {
		t.Errorf("got %v after the caller changed its slice, want %s", got, kept.Addr)
	}
}

func TestRoundRobinDistribution(t *testing.T) {
	tests := []struct {
		name    string
		weights []uint32
	}{
		{name: "equal", weights: []uint32{1, 1, 1, 1}},
		{name: "weighted", weights: []uint32{1, 2, 5}},
		{name: "common factor", weights: []uint32{64, 128, 256}},
		{name: "single", weights: []uint32{7}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backends := newBackends(tc.weights...)
			bal := mustNew(t, core.RoundRobin, backends)
			counts := make(map[*core.Backend]int)
			for range picks {
				counts[bal.Pick(core.PickInfo{})]++
			}
			assertShares(t, backends, counts, 0.001)
		})
	}
}

func TestRoundRobinScheduleIsSmooth(t *testing.T) {
	backends := newBackends(5, 1, 1)
	schedule := smoothSchedule(backends)
	want := []int{0, 0, 1, 0, 2, 0, 0}
	if len(schedule) != len(want) {
		t.Fatalf("got schedule of %d slots, want %d", len(schedule), len(want))
	}
	for i, b := range schedule {
		if b != backends[want[i]] {
			t.Errorf("slot %d: got %s, want %s", i, b.Addr, backends[want[i]].Addr)
		}
	}

	if got := smoothSchedule(newBackends(64, 128, 256)); len(got) != 7 {
		t.Errorf("weights 64,128,256: got %d slots, want 7 after dividing by the GCD", len(got))
	}
}

func TestRoundRobinKeepsWeightsWhenSkipping(t *testing.T) {
	backends := newBackends(1, 2, 3, 6)
	backends[2].SetHealthy(false)
	bal := mustNew(t, core.RoundRobin, backends)
	counts := make(map[*core.Backend]int)
	for range picks {
		counts[bal.Pick(core.PickInfo{})]++
	}
	if counts[backends[2]] != 0 {
		t.Fatalf("unhealthy backend got %d picks", counts[backends[2]])
	}
	live := []*core.Backend{backends[0], backends[1], backends[3]}
	// Slots of the skipped backend go to its schedule neighbours, so the
	// remaining ratio is close to the weights but not exact.
	assertShares(t, live, counts, 0.35)
}

// Holding every picked slot makes load visible to the policy, which is what
// drives weighted least_conn and p2c towards the weight ratio.
func TestLoadAwarePoliciesHonourWeights(t *testing.T) {
	tests := []struct {
		name      string
		policy    core.Policy
		tolerance float64
	}{
		{name: "least_conn", policy: core.LeastConn, tolerance: 0.01},
		{name: "p2c_ewma", policy: core.P2CEWMA, tolerance: 0.05},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backends := newBackends(1, 2, 3, 4)
			bal := mustNew(t, tc.policy, backends)
			for range picks {
				bal.Pick(core.PickInfo{}).Acquire()
			}
			counts := make(map[*core.Backend]int)
			for _, b := range backends {
				counts[b] = int(b.Inflight())
			}
			assertShares(t, backends, counts, tc.tolerance)
		})
	}
}

func TestIdlePoliciesSpreadEvenly(t *testing.T) {
	tests := []struct {
		name      string
		policy    core.Policy
		tolerance float64
	}{
		{name: "least_conn", policy: core.LeastConn, tolerance: 0.001},
		{name: "p2c_ewma", policy: core.P2CEWMA, tolerance: 0.05},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backends := newBackends(equalWeights(4)...)
			bal := mustNew(t, tc.policy, backends)
			counts := make(map[*core.Backend]int)
			for range picks {
				b := bal.Pick(core.PickInfo{})
				b.Acquire()
				counts[b]++
				b.Release()
			}
			assertShares(t, backends, counts, tc.tolerance)
		})
	}
}

func TestLeastConnPicksLowestLoadPerWeight(t *testing.T) {
	tests := []struct {
		name     string
		weights  []uint32
		inflight []int
		want     int
	}{
		{name: "lowest count", weights: []uint32{1, 1, 1}, inflight: []int{3, 1, 2}, want: 1},
		{name: "weight scales capacity", weights: []uint32{1, 4, 1}, inflight: []int{2, 4, 2}, want: 1},
		{name: "idle beats loaded heavy backend", weights: []uint32{8, 1}, inflight: []int{1, 0}, want: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backends := newBackends(tc.weights...)
			for i, n := range tc.inflight {
				for range n {
					backends[i].Acquire()
				}
			}
			bal := mustNew(t, core.LeastConn, backends)
			for range 50 {
				if got := bal.Pick(core.PickInfo{}); got != backends[tc.want] {
					t.Fatalf("got %s, want %s", got.Addr, backends[tc.want].Addr)
				}
			}
		})
	}
}

func TestP2CPrefersLowerCost(t *testing.T) {
	tests := []struct {
		name  string
		setup func(backends []*core.Backend)
		// wantLeast is the backend expected to receive the fewest picks,
		// wantMost the most.
		wantMost, wantLeast int
	}{
		{
			name: "faster backend",
			setup: func(backends []*core.Backend) {
				for i, b := range backends {
					b.ObserveLatency(time.Duration(i+1) * 10 * time.Millisecond)
				}
			},
			wantMost: 0, wantLeast: 3,
		},
		{
			name: "less loaded backend",
			setup: func(backends []*core.Backend) {
				for i, b := range backends {
					b.ObserveLatency(time.Millisecond)
					for range (len(backends) - i) * 5 {
						b.Acquire()
					}
				}
			},
			wantMost: 3, wantLeast: 0,
		},
		{
			name: "backend without a latency sample is probed first",
			setup: func(backends []*core.Backend) {
				for _, b := range backends[:3] {
					b.ObserveLatency(time.Millisecond)
				}
				backends[0].ObserveLatency(50 * time.Millisecond)
			},
			wantMost: 3, wantLeast: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backends := newBackends(equalWeights(4)...)
			tc.setup(backends)
			bal := mustNew(t, core.P2CEWMA, backends)
			counts := make(map[*core.Backend]int)
			for range picks {
				counts[bal.Pick(core.PickInfo{})]++
			}
			most, least := counts[backends[tc.wantMost]], counts[backends[tc.wantLeast]]
			for i, b := range backends {
				if counts[b] > most || counts[b] < least {
					t.Errorf("backend %d got %d picks, outside [%d, %d] set by backends %d and %d", i, counts[b], least, most, tc.wantLeast, tc.wantMost)
				}
			}
			// With four backends the cheapest wins every pair it appears in
			// (half of all picks) and the costliest wins none.
			if math.Abs(float64(most)-picks/2) > picks*0.02 {
				t.Errorf("cheapest backend got %d picks, want about %d", most, picks/2)
			}
			if least != 0 {
				t.Errorf("costliest backend got %d picks, want 0", least)
			}
		})
	}
}

func TestP2CSingleSlowBackendGetsLessTraffic(t *testing.T) {
	backends := newBackends(equalWeights(4)...)
	for _, b := range backends {
		b.ObserveLatency(time.Millisecond)
	}
	backends[1].ObserveLatency(200 * time.Millisecond)
	backends[1].ObserveLatency(200 * time.Millisecond)
	bal := mustNew(t, core.P2CEWMA, backends)
	counts := make(map[*core.Backend]int)
	for range picks {
		counts[bal.Pick(core.PickInfo{})]++
	}
	if got := counts[backends[1]]; got != 0 {
		t.Errorf("slow idle backend got %d picks, want 0 while faster idle backends exist", got)
	}
}

func TestRingHashDistribution(t *testing.T) {
	tests := []struct {
		name    string
		weights []uint32
		// 160 points per weight unit give a backend's arc share a relative
		// standard deviation of about 1/sqrt(160) = 8%, so single backends
		// land up to about 30% off. The counting policies' 5% does not apply.
		tolerance float64
	}{
		{name: "equal", weights: equalWeights(10), tolerance: 0.35},
		{name: "weighted", weights: []uint32{1, 2, 4, 8}, tolerance: 0.35},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backends := newBackends(tc.weights...)
			bal := mustNew(t, core.RingHash, backends)
			counts := make(map[*core.Backend]int)
			for i := range picks {
				counts[bal.Pick(core.PickInfo{Key: key(i)})]++
			}
			assertShares(t, backends, counts, tc.tolerance)
		})
	}
}

func TestRingHashPointCount(t *testing.T) {
	tests := []struct {
		name    string
		weights []uint32
		want    int
	}{
		{name: "unit weights", weights: []uint32{1, 1, 1}, want: 3 * virtualNodesPerUnit},
		{name: "weights", weights: []uint32{1, 3}, want: 4 * virtualNodesPerUnit},
		{name: "common factor removed", weights: []uint32{100, 200}, want: 3 * virtualNodesPerUnit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ring := newRingHash(newBackends(tc.weights...))
			if len(ring.hashes) != tc.want || len(ring.owners) != tc.want {
				t.Fatalf("got %d hashes and %d owners, want %d", len(ring.hashes), len(ring.owners), tc.want)
			}
			for i := 1; i < len(ring.hashes); i++ {
				if ring.hashes[i-1] > ring.hashes[i] {
					t.Fatalf("hashes not sorted at %d", i)
				}
			}
		})
	}
}

func TestRingHashSearchMatchesFullBinarySearch(t *testing.T) {
	tests := []struct {
		name    string
		weights []uint32
	}{
		{name: "one backend", weights: []uint32{1}},
		{name: "ten backends", weights: equalWeights(10)},
		{name: "point count is a power of two", weights: []uint32{128, 128, 256}},
		{name: "weighted", weights: []uint32{1, 7, 3, 250}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ring := newRingHash(newBackends(tc.weights...))
			check := func(k uint64) {
				t.Helper()
				want := sort.Search(len(ring.hashes), func(i int) bool { return ring.hashes[i] >= k })
				if want == len(ring.hashes) {
					want = 0
				}
				if got := ring.search(k); got != want {
					t.Fatalf("key %#x: got point %d, want %d", k, got, want)
				}
			}
			for _, k := range []uint64{0, 1, math.MaxUint64, math.MaxUint64 - 1, 1 << 63, 1<<63 - 1} {
				check(k)
			}
			// Every point, and its two neighbours in key space, are the
			// boundaries where an off-by-one would show.
			for _, h := range ring.hashes {
				check(h - 1)
				check(h)
				check(h + 1)
			}
			for i := range uint64(50_000) {
				check(mix(i))
			}
		})
	}
}

func TestRingHashSameKeySameBackend(t *testing.T) {
	backends := newBackends(equalWeights(10)...)
	first := mustNew(t, core.RingHash, backends)
	// A second ring over fresh Backend values with the same addresses, in a
	// different order, stands for another golb instance or a restart.
	reordered := make([]*core.Backend, len(backends))
	for i, b := range backends {
		reordered[len(backends)-1-i] = core.NewBackend(b.Addr, b.Weight)
	}
	second := mustNew(t, core.RingHash, reordered)
	for i := range 10_000 {
		info := core.PickInfo{Key: key(i)}
		a := first.Pick(info)
		if again := first.Pick(info); again != a {
			t.Fatalf("key %d moved from %s to %s between picks", i, a.Addr, again.Addr)
		}
		if b := second.Pick(info); b.Addr != a.Addr {
			t.Fatalf("key %d: rings disagree, %s and %s", i, a.Addr, b.Addr)
		}
	}
}

func TestRingHashMinimalRemap(t *testing.T) {
	tests := []struct {
		name string
		// shrink returns the balancer after one backend left, by removal
		// from the set or by becoming unavailable.
		shrink func(t *testing.T, backends []*core.Backend, gone int) core.Balancer
	}{
		{
			name: "backend removed",
			shrink: func(t *testing.T, backends []*core.Backend, gone int) core.Balancer {
				rest := append(append([]*core.Backend{}, backends[:gone]...), backends[gone+1:]...)
				return mustNew(t, core.RingHash, rest)
			},
		},
		{
			name: "backend unhealthy",
			shrink: func(t *testing.T, backends []*core.Backend, gone int) core.Balancer {
				backends[gone].SetHealthy(false)
				return mustNew(t, core.RingHash, backends)
			},
		},
		{
			name: "backend ejected",
			shrink: func(t *testing.T, backends []*core.Backend, gone int) core.Balancer {
				backends[gone].Eject(nowNanos() + int64(time.Hour))
				return mustNew(t, core.RingHash, backends)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const gone = 3
			backends := newBackends(equalWeights(10)...)
			before := mustNew(t, core.RingHash, backends)
			owners := make([]*core.Backend, picks)
			for i := range owners {
				owners[i] = before.Pick(core.PickInfo{Key: key(i)})
			}
			after := tc.shrink(t, backends, gone)

			moved := 0
			for i, owner := range owners {
				got := after.Pick(core.PickInfo{Key: key(i)})
				if got == backends[gone] {
					t.Fatalf("key %d still maps to the backend that left", i)
				}
				if owner == backends[gone] {
					moved++
					continue
				}
				if got != owner {
					t.Fatalf("key %d moved from %s to %s although its backend stayed", i, owner.Addr, got.Addr)
				}
			}
			if share := float64(moved) / picks; share < 0.05 || share > 0.15 {
				t.Errorf("%.1f%% of keys moved, want about 10%%", share*100)
			}
		})
	}
}

func TestRingHashExcludeMovesToNextBackend(t *testing.T) {
	backends := newBackends(equalWeights(5)...)
	bal := mustNew(t, core.RingHash, backends)
	for i := range 1000 {
		info := core.PickInfo{Key: key(i)}
		var tried []*core.Backend
		for range backends {
			b := bal.Pick(core.PickInfo{Key: info.Key, Exclude: tried})
			if b == nil {
				t.Fatalf("key %d: nil with %d of %d backends untried", i, len(backends)-len(tried), len(backends))
			}
			for _, prev := range tried {
				if prev == b {
					t.Fatalf("key %d: %s returned twice", i, b.Addr)
				}
			}
			tried = append(tried, b)
		}
		if b := bal.Pick(core.PickInfo{Key: info.Key, Exclude: tried}); b != nil {
			t.Fatalf("key %d: got %s with every backend excluded, want nil", i, b.Addr)
		}
	}
}

func TestRingHashWalkBeyondBitmap(t *testing.T) {
	backends := newBackends(equalWeights(maxSeenIndex + 6)...)
	for _, b := range backends {
		b.SetHealthy(false)
	}
	bal := mustNew(t, core.RingHash, backends)
	if got := bal.Pick(core.PickInfo{Key: key(1)}); got != nil {
		t.Fatalf("got %s with every backend down, want nil", got.Addr)
	}
	last := backends[len(backends)-1]
	last.SetHealthy(true)
	for i := range 100 {
		if got := bal.Pick(core.PickInfo{Key: key(i)}); got != last {
			t.Fatalf("key %d: got %v, want the only healthy backend", i, got)
		}
	}
}

func TestUnavailableAndExcludedNeverReturned(t *testing.T) {
	type scenario struct {
		name string
		// apply marks some backends unusable and returns the exclude list
		// and the set that may still be returned.
		apply func(backends []*core.Backend) (exclude, allowed []*core.Backend)
	}
	scenarios := []scenario{
		{
			name: "unhealthy",
			apply: func(b []*core.Backend) ([]*core.Backend, []*core.Backend) {
				b[0].SetHealthy(false)
				b[3].SetHealthy(false)
				return nil, []*core.Backend{b[1], b[2], b[4], b[5]}
			},
		},
		{
			name: "ejected",
			apply: func(b []*core.Backend) ([]*core.Backend, []*core.Backend) {
				b[1].Eject(nowNanos() + int64(time.Hour))
				return nil, []*core.Backend{b[0], b[2], b[3], b[4], b[5]}
			},
		},
		{
			name: "expired ejection is available again",
			apply: func(b []*core.Backend) ([]*core.Backend, []*core.Backend) {
				for _, backend := range b {
					backend.Eject(nowNanos() - int64(time.Second))
				}
				return nil, b
			},
		},
		{
			name: "excluded",
			apply: func(b []*core.Backend) ([]*core.Backend, []*core.Backend) {
				return []*core.Backend{b[0], b[1], b[2]}, []*core.Backend{b[3], b[4], b[5]}
			},
		},
		{
			name: "one left after unhealthy, ejected and excluded",
			apply: func(b []*core.Backend) ([]*core.Backend, []*core.Backend) {
				b[0].SetHealthy(false)
				b[1].SetHealthy(false)
				b[2].Eject(nowNanos() + int64(time.Hour))
				return []*core.Backend{b[3], b[5]}, []*core.Backend{b[4]}
			},
		},
		{
			name: "none: all unhealthy",
			apply: func(b []*core.Backend) ([]*core.Backend, []*core.Backend) {
				for _, backend := range b {
					backend.SetHealthy(false)
				}
				return nil, nil
			},
		},
		{
			name: "none: all excluded",
			apply: func(b []*core.Backend) ([]*core.Backend, []*core.Backend) {
				return b, nil
			},
		},
		{
			name: "none: mixed",
			apply: func(b []*core.Backend) ([]*core.Backend, []*core.Backend) {
				b[0].SetHealthy(false)
				b[1].Eject(nowNanos() + int64(time.Hour))
				return b[2:], nil
			},
		},
	}
	for _, policy := range policies {
		for _, sc := range scenarios {
			t.Run(string(policy)+"/"+sc.name, func(t *testing.T) {
				backends := newBackends(1, 2, 1, 3, 1, 2)
				bal := mustNew(t, policy, backends)
				exclude, allowed := sc.apply(backends)
				seen := make(map[*core.Backend]int)
				for i := range 20_000 {
					b := bal.Pick(core.PickInfo{Key: key(i), Exclude: exclude})
					if b == nil {
						if len(allowed) != 0 {
							t.Fatalf("pick %d: got nil with %d usable backends", i, len(allowed))
						}
						continue
					}
					seen[b]++
				}
				for b := range seen {
					found := false
					for _, ok := range allowed {
						found = found || ok == b
					}
					if !found {
						t.Errorf("unusable backend %s returned %d times", b.Addr, seen[b])
					}
				}
				for _, b := range allowed {
					if seen[b] == 0 {
						t.Errorf("usable backend %s never returned", b.Addr)
					}
				}
			})
		}
	}
}

func TestSingleBackend(t *testing.T) {
	for _, policy := range policies {
		t.Run(string(policy), func(t *testing.T) {
			backends := newBackends(3)
			bal := mustNew(t, policy, backends)
			if got := bal.Pick(core.PickInfo{Key: 42}); got != backends[0] {
				t.Fatalf("got %v, want the only backend", got)
			}
			if got := bal.Pick(core.PickInfo{Key: 42, Exclude: backends}); got != nil {
				t.Errorf("got %s with the only backend excluded, want nil", got.Addr)
			}
			backends[0].SetHealthy(false)
			if got := bal.Pick(core.PickInfo{Key: 42}); got != nil {
				t.Errorf("got %s with the only backend unhealthy, want nil", got.Addr)
			}
		})
	}
}

func TestPickDoesNotAllocate(t *testing.T) {
	for _, policy := range policies {
		t.Run(string(policy), func(t *testing.T) {
			backends := newBackends(1, 2, 3, 1, 2, 3, 1, 2)
			backends[1].SetHealthy(false)
			backends[2].Eject(nowNanos() + int64(time.Hour))
			bal := mustNew(t, policy, backends)
			exclude := []*core.Backend{backends[0]}
			i := 0
			allocs := testing.AllocsPerRun(2000, func() {
				i++
				if b := bal.Pick(core.PickInfo{Key: uint64(i) * goldenGamma, Exclude: exclude}); b != nil {
					b.Acquire()
					b.Release()
				}
			})
			if allocs != 0 {
				t.Errorf("got %v allocs per pick, want 0", allocs)
			}
		})
	}
}

// 64 goroutines pick and release while health and ejection state flips. The
// race detector checks the memory model; the assertions check that no
// in-flight slot is lost and that an always-available backend keeps nil away.
func TestConcurrentPickWhileStateFlips(t *testing.T) {
	const goroutines, perGoroutine = 64, 2000
	for _, policy := range policies {
		t.Run(string(policy), func(t *testing.T) {
			backends := newBackends(1, 2, 3, 4, 1, 2, 3, 4)
			bal := mustNew(t, policy, backends)

			var stop atomic.Bool
			var flipper sync.WaitGroup
			flipper.Go(func() {
				for i := 0; !stop.Load(); i++ {
					// The last backend is never touched, so it stays available.
					b := backends[i%(len(backends)-1)]
					b.SetHealthy(i%3 != 0)
					if i%2 == 0 {
						b.Eject(nowNanos() + int64(time.Millisecond))
						continue
					}
					b.Eject(0)
				}
			})

			var nils atomic.Int64
			var workers sync.WaitGroup
			for g := range goroutines {
				workers.Go(func() {
					for i := range perGoroutine {
						b := bal.Pick(core.PickInfo{Key: key(g*perGoroutine + i)})
						if b == nil {
							nils.Add(1)
							continue
						}
						b.Acquire()
						b.Release()
					}
				})
			}
			workers.Wait()
			stop.Store(true)
			flipper.Wait()

			if n := nils.Load(); n != 0 {
				t.Errorf("got %d nil picks although one backend was always available", n)
			}
			for _, b := range backends {
				if b.Inflight() != 0 {
					t.Errorf("backend %s in-flight is %d after all picks were released", b.Addr, b.Inflight())
				}
			}
		})
	}
}
