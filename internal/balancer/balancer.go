// Package balancer implements the backend selection policies.
package balancer

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/maheshpaliwal/golb/internal/core"
)

var (
	errNoBackends    = errors.New("no backends")
	errUnknownPolicy = errors.New("unknown policy")
)

// New builds the balancer for policy over a fixed backend set. The slice is
// copied; backend state is read live on every Pick.
func New(policy core.Policy, backends []*core.Backend) (core.Balancer, error) {
	if len(backends) == 0 {
		return nil, errNoBackends
	}
	for _, b := range backends {
		if b == nil {
			return nil, errors.New("nil backend")
		}
		if b.Weight < 1 || b.Weight > core.MaxWeight {
			return nil, fmt.Errorf("backend %q weight %d outside 1..%d", b.Addr, b.Weight, core.MaxWeight)
		}
	}
	backends = slices.Clone(backends)
	switch policy {
	case core.RoundRobin:
		return newRoundRobin(backends), nil
	case core.LeastConn:
		return &leastConn{backends: backends}, nil
	case core.P2CEWMA:
		return &p2cEWMA{backends: backends}, nil
	case core.RingHash:
		return newRingHash(backends), nil
	}
	return nil, fmt.Errorf("%w %q", errUnknownPolicy, policy)
}

// usable reports whether b may be returned for this request.
func usable(b *core.Backend, exclude []*core.Backend) bool {
	if !available(b) {
		return false
	}
	for _, tried := range exclude {
		if tried == b {
			return false
		}
	}
	return true
}

// available avoids reading the clock on the common path: Available(0) is
// true exactly when the backend is healthy and carries no ejection deadline.
// Only a backend with a deadline, current or expired, costs a time.Now.
func available(b *core.Backend) bool {
	if b.Available(0) {
		return true
	}
	return b.Healthy() && !b.Ejected(time.Now().UnixNano())
}

// weightUnits returns each backend's weight divided by the GCD of all
// weights, and the sum of the results.
func weightUnits(backends []*core.Backend) (units []uint32, total int) {
	var divisor uint32
	for _, b := range backends {
		divisor = gcd(divisor, b.Weight)
	}
	units = make([]uint32, len(backends))
	for i, b := range backends {
		units[i] = b.Weight / divisor
		total += int(units[i])
	}
	return units, total
}

func gcd(a, b uint32) uint32 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}
