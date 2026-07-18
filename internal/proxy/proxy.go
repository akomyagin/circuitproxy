// Package proxy implements the L7 reverse proxy: a round-robin balancer over a
// static backend pool (Этап 1), integrated with per-backend circuit breakers
// (Этап 3) and retry-with-backoff (Этап 4). It builds on
// net/http/httputil.ReverseProxy from the standard library.
package proxy

import (
	"net/http"
	"net/url"
	"sync/atomic"

	"github.com/akomyagin/circuitproxy/internal/config"
)

// Backend is a single upstream target and its liveness flag.
type Backend struct {
	URL *url.URL
	// up is toggled by the health checker (Этап 2); balancer skips down backends.
	up atomic.Bool
}

// Balancer selects backends round-robin across a static pool.
type Balancer struct {
	backends []*Backend
	counter  atomic.Uint64
}

// NewBalancer builds a balancer from parsed backend base URLs.
//
// TODO(Этап 1): parse config.Backends into *Backend entries (url.Parse),
// mark all up initially.
func NewBalancer(cfg *config.Config) (*Balancer, error) {
	_ = cfg
	return &Balancer{}, nil
}

// Next returns the next healthy backend round-robin, or nil if none available.
//
// TODO(Этап 1): round-robin via atomic counter mod len(backends).
// TODO(Этап 2): skip backends whose up flag is false.
func (b *Balancer) Next() *Backend {
	return nil
}

// Handler returns the http.Handler that proxies incoming requests.
//
// TODO(Этап 1): wrap httputil.ReverseProxy; pick backend via Balancer.Next,
// set the Director/Rewrite to the chosen backend, proxy the request.
// TODO(Этап 3): consult the per-backend breaker (Allow) before proxying and
// Report the outcome afterwards.
// TODO(Этап 4): retry idempotent requests with exponential backoff, honoring
// breaker state and method idempotency.
func (b *Balancer) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		http.Error(w, "circuitproxy: not implemented (Этап 1)", http.StatusNotImplemented)
	})
}
