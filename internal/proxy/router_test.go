package proxy

import (
	"testing"

	"github.com/maheshpaliwal/golb/internal/metrics"
)

func TestRouterMatch(t *testing.T) {
	const routes = `
      - pool: app
      - path_prefix: /api
        pool: app
      - path_prefix: /api/v2/
        pool: app
      - host: shop.example.test
        path_prefix: /cart
        pool: app
      - host: "*.example.test"
        pool: app
      - host: "*.eu.example.test"
        pool: app
      - host: Admin.Example.Test
        path_prefix: /
        pool: app
`
	cfg := parseConfig(t, fixtureOptions{backends: []string{"127.0.0.1:1"}, routes: routes})
	reg := metrics.NewRegistry()
	upstream := NewUpstream(UpstreamOptions{Config: cfg.Pools[0], Pool: newRotatingPool("127.0.0.1:1"), Registry: reg})
	router := NewRouter(RouterOptions{
		Listener:  cfg.Listeners[0],
		Upstreams: map[string]*Upstream{"app": upstream},
		Registry:  reg,
	})

	tests := []struct {
		name string
		host string
		path string
		want string // route label
	}{
		{name: "root catches everything", host: "other.test", path: "/anything", want: "/"},
		{name: "prefix", host: "other.test", path: "/api/users", want: "/api"},
		{name: "prefix exact", host: "other.test", path: "/api", want: "/api"},
		{name: "prefix needs a segment boundary", host: "other.test", path: "/apiary", want: "/"},
		{name: "longest prefix wins", host: "other.test", path: "/api/v2/items", want: "/api/v2/"},
		{name: "trailing slash on the rule is not required of the request", host: "other.test", path: "/api/v2", want: "/api/v2/"},
		{name: "double slash is cleaned", host: "other.test", path: "//api//v2/x", want: "/api/v2/"},
		{name: "dot segments cannot dodge a rule", host: "other.test", path: "/public/../api/x", want: "/api"},
		{name: "dot segments cannot fake a rule", host: "other.test", path: "/api/../secret", want: "/"},
		{name: "exact host", host: "shop.example.test", path: "/cart/1", want: "shop.example.test/cart"},
		{name: "exact host with port", host: "shop.example.test:8443", path: "/cart", want: "shop.example.test/cart"},
		{name: "exact host falls through when the path is not covered", host: "shop.example.test", path: "/other", want: "*.example.test/"},
		{name: "wildcard", host: "a.example.test", path: "/", want: "*.example.test/"},
		{name: "longer wildcard suffix wins", host: "a.eu.example.test", path: "/", want: "*.eu.example.test/"},
		{name: "wildcard does not match the bare domain", host: "example.test", path: "/x", want: "/"},
		{name: "host is case-insensitive", host: "ADMIN.example.TEST", path: "/x", want: "admin.example.test/"},
		{name: "trailing dot", host: "admin.example.test.", path: "/x", want: "admin.example.test/"},
		{name: "empty host", host: "", path: "/api", want: "/api"},
		{name: "IPv6 literal host", host: "[::1]:8080", path: "/", want: "/"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			route := router.match(tc.host, tc.path)
			if route == nil {
				t.Fatalf("match(%q, %q) = no route, want %q", tc.host, tc.path, tc.want)
			}
			if route.label != tc.want {
				t.Errorf("match(%q, %q) = %q, want %q", tc.host, tc.path, route.label, tc.want)
			}
		})
	}
}

func TestRouterNoMatch(t *testing.T) {
	cfg := parseConfig(t, fixtureOptions{
		backends: []string{"127.0.0.1:1"},
		routes:   "      - host: only.example.test\n        path_prefix: /api\n        pool: app\n",
	})
	reg := metrics.NewRegistry()
	router := NewRouter(RouterOptions{Listener: cfg.Listeners[0], Upstreams: map[string]*Upstream{}, Registry: reg})
	tests := []struct {
		name string
		host string
		path string
	}{
		{name: "other host", host: "else.example.test", path: "/api"},
		{name: "other path", host: "only.example.test", path: "/"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if route := router.match(tc.host, tc.path); route != nil {
				t.Errorf("match(%q, %q) = %q, want no route", tc.host, tc.path, route.label)
			}
		})
	}
}

func TestRouterKeepsRateLimiterAcrossReload(t *testing.T) {
	routes := func(rate string) string {
		return "      - pool: app\n        rate_limit:\n          requests_per_second: " + rate + "\n          burst: 1\n"
	}
	reg := metrics.NewRegistry()
	build := func(rate string, prev *Router) *Router {
		cfg := parseConfig(t, fixtureOptions{backends: []string{"127.0.0.1:1"}, routes: routes(rate)})
		return NewRouter(RouterOptions{Listener: cfg.Listeners[0], Upstreams: map[string]*Upstream{}, Registry: reg, Prev: prev})
	}
	first := build("5", nil)
	same := build("5", first)
	changed := build("6", same)

	if same.anyHost[0].limiter != first.anyHost[0].limiter {
		t.Error("unchanged rate limit got a new bucket; a reload would hand out a fresh burst")
	}
	if changed.anyHost[0].limiter == same.anyHost[0].limiter {
		t.Error("changed rate limit kept the old bucket")
	}
}
