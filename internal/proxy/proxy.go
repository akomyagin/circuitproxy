// Package proxy implements the L7 reverse proxy: a round-robin balancer over a
// static backend pool (Этап 1), integrated with per-backend circuit breakers
// (Этап 3) and retry-with-backoff (Этап 4). It builds on
// net/http/httputil.ReverseProxy from the standard library.
package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
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

// NewBalancer builds a balancer from parsed backend base URLs. Every backend
// starts marked up; a balancer with an empty or invalid backend list is an
// error (full config validation arrives in Этап 5, this is the minimum the
// balancer itself cannot live without).
func NewBalancer(cfg *config.Config) (*Balancer, error) {
	if len(cfg.Backends) == 0 {
		return nil, fmt.Errorf("proxy: backend list is empty")
	}
	backends := make([]*Backend, 0, len(cfg.Backends))
	for _, raw := range cfg.Backends {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("proxy: parse backend URL %q: %w", raw, err)
		}
		if u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("proxy: backend URL %q is not absolute (need scheme and host)", raw)
		}
		b := &Backend{URL: u}
		b.up.Store(true)
		backends = append(backends, b)
	}
	return &Balancer{backends: backends}, nil
}

// Next returns the next backend round-robin, or nil if none available.
// It is safe for concurrent use: selection is a single atomic increment,
// no mutexes on this hot path.
//
// TODO(Этап 2): skip backends whose up flag is false; return nil when all
// backends are down.
func (b *Balancer) Next() *Backend {
	n := uint64(len(b.backends))
	if n == 0 {
		return nil
	}
	// Add returns the post-increment value, so subtract 1 to make the very
	// first call land on index 0 and keep the cycle uniform.
	idx := (b.counter.Add(1) - 1) % n
	return b.backends[idx]
}

// backendCtxKey carries the backend chosen by the outer handler into the
// shared ReverseProxy's Rewrite via the request context.
type backendCtxKey struct{}

// Handler returns the http.Handler that proxies incoming requests.
//
// A single *httputil.ReverseProxy is reused across requests (preserving the
// transport connection pool); the per-request backend choice happens in the
// outer handler via Next() and travels to Rewrite through the request context.
//
// TODO(Этап 3): consult the per-backend breaker (Allow) before proxying and
// Report the outcome afterwards.
// TODO(Этап 4): retry idempotent requests with exponential backoff, honoring
// breaker state and method idempotency.
func (b *Balancer) Handler() http.Handler {
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			// The outer handler always sets backendCtxKey before calling
			// rp.ServeHTTP; if that invariant is ever broken, panic loudly
			// here instead of silently proxying to an unset target.
			backend := pr.In.Context().Value(backendCtxKey{}).(*Backend)
			pr.SetURL(backend.URL)
			pr.SetXForwarded()
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			backend, _ := r.Context().Value(backendCtxKey{}).(*Backend)
			var target string
			if backend != nil {
				target = backend.URL.String()
			}
			slog.Error("proxy upstream error",
				"backend", target,
				"method", r.Method,
				"path", r.URL.Path,
				"err", err,
			)
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend := b.Next()
		if backend == nil {
			http.Error(w, "no backends available", http.StatusServiceUnavailable)
			return
		}
		ctx := context.WithValue(r.Context(), backendCtxKey{}, backend)
		rp.ServeHTTP(w, r.WithContext(ctx))
	})
}
