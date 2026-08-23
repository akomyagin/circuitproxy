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

	"github.com/akomyagin/circuitproxy/internal/breaker"
	"github.com/akomyagin/circuitproxy/internal/config"
)

// Backend is a single upstream target and its liveness flag.
type Backend struct {
	URL *url.URL
	// up is toggled by the health checker via SetUp; balancer skips down backends.
	up atomic.Bool
	// cb is this backend's per-backend circuit breaker (Этап 3). Liveness (up)
	// and breaker state are independent mechanisms.
	cb *breaker.Breaker
}

// IsUp reports whether the backend is currently marked live by the health
// checker. Safe for concurrent use.
func (b *Backend) IsUp() bool { return b.up.Load() }

// SetUp marks the backend live (true) or down (false). Called by the health
// checker; safe for concurrent use.
func (b *Backend) SetUp(up bool) { b.up.Store(up) }

// Allow reports whether the circuit breaker permits a request to this
// backend right now. See internal/breaker.
func (b *Backend) Allow() (bool, error) { return b.cb.Allow() }

// Report records the outcome (success/failure) of a request to this backend
// to its circuit breaker.
func (b *Backend) Report(success bool) { b.cb.Report(success) }

// HealthURL builds the absolute health-probe URL for this backend by joining
// its base URL with path. path is the health endpoint from config (e.g. "/health").
// Any query string or fragment on the base URL is dropped: a health probe
// targets exactly path, not whatever query the proxied base URL happened to carry.
func (b *Backend) HealthURL(path string) string {
	u := *b.URL // copy; do not mutate the shared base URL
	u.Path = path
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
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
		b.cb = breaker.New(breaker.Config{
			FailureThreshold: cfg.Breaker.FailureThreshold,
			OpenTimeout:      cfg.Breaker.OpenTimeout(),
			// Now not set — breaker.New defaults to time.Now.
		})
		backends = append(backends, b)
	}
	return &Balancer{backends: backends}, nil
}

// Next returns the next live backend round-robin, or nil if all backends are
// down. Selection stays lock-free: a single atomic increment picks a starting
// slot (Add returns the post-increment value, so subtract 1 to make the very
// first call land on index 0 and keep the cycle uniform), then we scan forward
// at most n slots for a live backend. Worst case is O(n) with n being a
// handful of backends. Liveness may change concurrently during the scan;
// IsUp() is atomic, so the worst outcome is picking a backend that went down
// a moment later (or skipping one that just recovered) — the next call
// self-corrects. Eventually consistent rotation is acceptable here.
func (b *Balancer) Next() *Backend {
	n := uint64(len(b.backends))
	if n == 0 {
		return nil
	}
	start := b.counter.Add(1) - 1
	for i := uint64(0); i < n; i++ {
		idx := (start + i) % n
		be := b.backends[idx]
		if be.IsUp() {
			return be
		}
	}
	return nil
}

// Backends returns the balancer's backend pool for the health checker to probe.
// The slice is the balancer's own backing store; callers must not append to or
// reorder it, only read entries and toggle their liveness via SetUp.
func (b *Balancer) Backends() []*Backend { return b.backends }

// backendCtxKey carries the backend chosen by the outer handler into the
// shared ReverseProxy's Rewrite via the request context.
type backendCtxKey struct{}

// Handler returns the http.Handler that proxies incoming requests.
//
// A single *httputil.ReverseProxy is reused across requests (preserving the
// transport connection pool); the per-request backend choice happens in the
// outer handler via Next() and travels to Rewrite through the request context.
//
// Circuit breaker (Этап 3): the outer handler calls the chosen backend's
// Allow() before proxying; an open circuit fast-fails with 503 without touching
// the backend. The request outcome is reported exactly once — via
// ModifyResponse (classified by status: 5xx = failure, <500 = success) on a
// successful transport, or via ErrorHandler (transport error = failure).
//
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
		ModifyResponse: func(res *http.Response) error {
			// res.Request carries the same context that Rewrite/ErrorHandler use.
			backend, _ := res.Request.Context().Value(backendCtxKey{}).(*Backend)
			if backend != nil {
				// 5xx signals a backend problem (failure); <500 (incl. 4xx,
				// which is the client's fault) counts as the backend answering.
				backend.Report(res.StatusCode < 500)
			}
			// ALWAYS return nil: returning an error would (a) route this response
			// through ErrorHandler (double Report) and (b) replace the backend's
			// response with our 502. A 5xx from the backend must reach the client
			// unchanged; the breaker only records it. See stdlib reverseproxy.go
			// (ModifyResponse error -> ErrorHandler).
			return nil
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
			if backend != nil {
				backend.Report(false) // transport error counts as a failure
			}
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend := b.Next()
		if backend == nil {
			http.Error(w, "no backends available", http.StatusServiceUnavailable)
			return
		}
		// Fast-fail an open circuit before any real backend contact. The error
		// is always ErrBreakerOpen when ok is false; the status is the same 503.
		if ok, _ := backend.Allow(); !ok {
			http.Error(w, "circuit breaker open", http.StatusServiceUnavailable)
			return
		}
		ctx := context.WithValue(r.Context(), backendCtxKey{}, backend)
		rp.ServeHTTP(w, r.WithContext(ctx))
	})
}
