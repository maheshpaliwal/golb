package proxy

import (
	"cmp"
	"net"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/maheshpaliwal/golb/internal/accesslog"
	"github.com/maheshpaliwal/golb/internal/config"
	"github.com/maheshpaliwal/golb/internal/metrics"
	"github.com/maheshpaliwal/golb/internal/ratelimit"
)

const (
	wildcardMark   = "*"
	unmatchedLabel = "none"
	statusClasses  = 6
)

var classLabels = [statusClasses]string{"", "1xx", "2xx", "3xx", "4xx", "5xx"}

// Route is one routing rule with everything a request needs already resolved.
type Route struct {
	label    string
	prefix   string // without a trailing slash; "" is the root
	upstream *Upstream

	limit          config.RateLimit
	limiter        *ratelimit.Bucket
	requestTimeout time.Duration

	classes  [statusClasses]*metrics.Counter
	duration *metrics.Histogram
	limited  *metrics.Counter
}

// Router is the immutable routing table of one HTTP listener.
type Router struct {
	listener  string
	exact     map[string][]*Route
	wildcards []wildcard
	anyHost   []*Route
	unmatched *Route // metrics for requests no route accepts
	log       *accesslog.Logger
}

type wildcard struct {
	suffix string // ".example.test"
	routes []*Route
}

type RouterOptions struct {
	Listener  config.Listener
	Upstreams map[string]*Upstream
	Registry  *metrics.Registry
	// Log is nil when access logging is off.
	Log *accesslog.Logger
	// Prev is this listener's router from the running configuration, or nil.
	// Rate limiters with unchanged settings carry over, so a reload does not
	// hand out a fresh burst.
	Prev *Router
}

func NewRouter(opts RouterOptions) *Router {
	listener := opts.Listener
	rt := &Router{
		listener:  listener.Name,
		exact:     make(map[string][]*Route),
		unmatched: newRoute(opts.Registry, listener.Name, unmatchedLabel),
		log:       opts.Log,
	}
	limiters := opts.Prev.limiters()
	wild := make(map[string][]*Route)
	for _, cfg := range listener.Routes {
		route := newRoute(opts.Registry, listener.Name, cfg.Label())
		route.prefix = strings.TrimRight(cfg.PathPrefix, "/")
		route.upstream = opts.Upstreams[cfg.Pool]
		route.requestTimeout = listener.Timeouts.Request
		if cfg.RateLimit != nil {
			route.limit = *cfg.RateLimit
			route.limiter = ratelimit.New(cfg.RateLimit.RequestsPerSecond, cfg.RateLimit.Burst)
			old := limiters[route.label]
			if old != nil && old.limit == route.limit {
				route.limiter = old.limiter
			}
		}

		host := strings.ToLower(cfg.Host)
		switch {
		case host == "":
			rt.anyHost = append(rt.anyHost, route)
		case strings.HasPrefix(host, wildcardMark):
			suffix := strings.TrimPrefix(host, wildcardMark)
			wild[suffix] = append(wild[suffix], route)
		default:
			rt.exact[host] = append(rt.exact[host], route)
		}
	}

	for _, routes := range rt.exact {
		sortLongestFirst(routes)
	}
	sortLongestFirst(rt.anyHost)
	for suffix, routes := range wild {
		sortLongestFirst(routes)
		rt.wildcards = append(rt.wildcards, wildcard{suffix: suffix, routes: routes})
	}
	slices.SortFunc(rt.wildcards, func(a, b wildcard) int {
		return cmp.Or(cmp.Compare(len(b.suffix), len(a.suffix)), cmp.Compare(a.suffix, b.suffix))
	})
	return rt
}

func newRoute(reg *metrics.Registry, listener, label string) *Route {
	route := &Route{
		label: label,
		duration: reg.Histogram("golb_request_duration_seconds", "Time from request arrival to the end of the response.",
			metrics.LatencyBuckets, "listener", listener, "route", label),
		limited: reg.Counter("golb_ratelimited_total", "Requests rejected by the route rate limit.", "route", label),
	}
	for class := 1; class < statusClasses; class++ {
		route.classes[class] = reg.Counter("golb_requests_total", "Client requests by status class.",
			"listener", listener, "route", label, "code_class", classLabels[class])
	}
	return route
}

func sortLongestFirst(routes []*Route) {
	slices.SortStableFunc(routes, func(a, b *Route) int {
		return cmp.Compare(len(b.prefix), len(a.prefix))
	})
}

func (rt *Router) limiters() map[string]*Route {
	if rt == nil {
		return nil
	}
	limited := make(map[string]*Route)
	collect := func(routes []*Route) {
		for _, route := range routes {
			if route.limiter != nil {
				limited[route.label] = route
			}
		}
	}
	collect(rt.anyHost)
	for _, routes := range rt.exact {
		collect(routes)
	}
	for _, w := range rt.wildcards {
		collect(w.routes)
	}
	return limited
}

// match picks the route for a request: an exact host beats a wildcard host
// (longest suffix first), which beats a route without a host; within one
// host the longest path prefix wins. A host whose routes do not cover the
// path falls through to the next, less specific host.
//
// The path is matched after cleaning, so "//", "/./" and "/../" cannot dodge
// a more specific route; the request is forwarded unmodified.
func (rt *Router) match(host, urlPath string) *Route {
	host = hostname(host)
	cleaned := path.Clean(urlPath)
	if routes, ok := rt.exact[host]; ok {
		if route := longestPrefix(routes, cleaned); route != nil {
			return route
		}
	}
	for i := range rt.wildcards {
		w := &rt.wildcards[i]
		if len(host) <= len(w.suffix) || !strings.HasSuffix(host, w.suffix) {
			continue
		}
		if route := longestPrefix(w.routes, cleaned); route != nil {
			return route
		}
	}
	return longestPrefix(rt.anyHost, cleaned)
}

// longestPrefix matches whole path segments: "/api" covers "/api" and
// "/api/v1" but not "/apiary".
func longestPrefix(routes []*Route, cleaned string) *Route {
	for _, route := range routes {
		n := len(route.prefix)
		if len(cleaned) < n || cleaned[:n] != route.prefix {
			continue
		}
		if len(cleaned) == n || cleaned[n] == '/' {
			return route
		}
	}
	return nil
}

// hostname reduces a Host header to a lower-case name without port or
// trailing dot. It allocates only when the header has upper-case letters.
func hostname(host string) string {
	if strings.IndexByte(host, ':') >= 0 {
		name, _, err := net.SplitHostPort(host)
		if err == nil {
			host = name
		}
	}
	host = strings.TrimSuffix(host, ".")
	for i := 0; i < len(host); i++ {
		if c := host[i]; c >= 'A' && c <= 'Z' {
			return strings.ToLower(host)
		}
	}
	return host
}

func (r *Route) observe(status int, elapsed time.Duration) {
	class := status / 100
	if class >= 1 && class < statusClasses {
		r.classes[class].Inc()
	}
	r.duration.Observe(elapsed.Seconds())
}
