package balancer

import (
	"cmp"
	"math/bits"
	"slices"

	"github.com/maheshpaliwal/golb/internal/core"
)

const (
	virtualNodesPerUnit = 160

	// goldenGamma and the two multipliers are the SplitMix64 constants.
	goldenGamma uint64 = 0x9e3779b97f4a7c15
	mixFirst    uint64 = 0xbf58476d1ce4e5b9
	mixSecond   uint64 = 0x94d049bb133111eb

	// seenWords bounds the stack bitmap used to stop a ring walk once every
	// backend has been visited.
	seenWords    = 16
	bitsPerWord  = 64
	wordShift    = 6
	maxSeenIndex = seenWords * bitsPerWord
)

// ringHash is a consistent-hash ring. A backend's points depend only on its
// address and weight unit count, so removing one backend moves only the keys
// it owned.
type ringHash struct {
	backends []*core.Backend
	hashes   []uint64 // sorted
	owners   []uint32 // owners[i] indexes backends for hashes[i]
	// starts[p] is the index of the first point whose hash has a top-bits
	// prefix of at least p; one entry more than there are prefixes.
	starts []uint32
	shift  uint // hash >> shift is the prefix
}

type ringPoint struct {
	hash  uint64
	owner uint32
}

func newRingHash(backends []*core.Backend) *ringHash {
	units, total := weightUnits(backends)
	points := make([]ringPoint, 0, total*virtualNodesPerUnit)
	for i, b := range backends {
		seed := core.Hash(b.Addr)
		for node := range uint64(units[i]) * virtualNodesPerUnit {
			points = append(points, ringPoint{hash: mix(seed + (node+1)*goldenGamma), owner: uint32(i)})
		}
	}
	// The address breaks hash ties so the order does not depend on which
	// other backends are in the pool.
	slices.SortFunc(points, func(a, b ringPoint) int {
		if c := cmp.Compare(a.hash, b.hash); c != 0 {
			return c
		}
		return cmp.Compare(backends[a.owner].Addr, backends[b.owner].Addr)
	})
	r := &ringHash{
		backends: backends,
		hashes:   make([]uint64, len(points)),
		owners:   make([]uint32, len(points)),
	}
	for i, p := range points {
		r.hashes[i], r.owners[i] = p.hash, p.owner
	}
	r.indexPrefixes()
	return r
}

// indexPrefixes uses as many prefixes as there are points, rounded up to a
// power of two, so a prefix covers one point on average.
func (r *ringHash) indexPrefixes() {
	prefixBits := uint(bits.Len(uint(len(r.hashes) - 1)))
	r.shift = bitsPerWord - prefixBits
	r.starts = make([]uint32, 1<<prefixBits+1)
	point := 0
	for prefix := range r.starts {
		for point < len(r.hashes) && r.hashes[point]>>r.shift < uint64(prefix) {
			point++
		}
		r.starts[prefix] = uint32(point)
	}
}

func (r *ringHash) Pick(info core.PickInfo) *core.Backend {
	// Keys are FNV-1a of similar strings (addresses, ids) whose high bits
	// correlate; mixing spreads them over the ring.
	first := r.search(mix(info.Key))
	b := r.backends[r.owners[first]]
	if usable(b, info.Exclude) {
		return b
	}
	return r.walk(first, info.Exclude)
}

// search returns the index of the first point at or after key, wrapping to
// 0 past the last one. The key's top bits select a slice of the ring that
// holds about one point, and the binary search runs only inside it. A
// binary search over the whole 16,000-point ring of a 100-backend pool
// measured 73 ns per pick with random keys, nearly all of it cache misses
// and mispredicted branches.
func (r *ringHash) search(key uint64) int {
	prefix := key >> r.shift
	lo, hi := int(r.starts[prefix]), int(r.starts[prefix+1])
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if r.hashes[mid] < key {
			lo = mid + 1
			continue
		}
		hi = mid
	}
	if lo == len(r.hashes) {
		return 0
	}
	return lo
}

// walk continues clockwise from an unusable point to the first usable
// backend. Each backend is tested once, and the walk ends as soon as all of
// them have been seen, so an entirely unavailable pool costs about
// n ln n points rather than the whole ring. Pools larger than the bitmap
// fall back to testing every point.
func (r *ringHash) walk(from int, exclude []*core.Backend) *core.Backend {
	var seen [seenWords]uint64
	track := len(r.backends) <= maxSeenIndex
	remaining := len(r.backends) - 1
	if track {
		first := r.owners[from]
		seen[first>>wordShift] |= 1 << (first % bitsPerWord)
	}
	i := from
	for range len(r.hashes) - 1 {
		if track && remaining == 0 {
			return nil
		}
		i++
		if i == len(r.hashes) {
			i = 0
		}
		owner := r.owners[i]
		if track {
			word, bit := owner>>wordShift, uint64(1)<<(owner%bitsPerWord)
			if seen[word]&bit != 0 {
				continue
			}
			seen[word] |= bit
			remaining--
		}
		b := r.backends[owner]
		if usable(b, exclude) {
			return b
		}
	}
	return nil
}

// mix is the SplitMix64 finaliser: a fixed bijection, so results are stable
// across processes and restarts like core.Hash.
func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= mixFirst
	x ^= x >> 27
	x *= mixSecond
	return x ^ x>>31
}
