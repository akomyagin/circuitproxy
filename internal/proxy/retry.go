package proxy

import (
	"bytes"
	"errors"
	"expvar"
	"io"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/akomyagin/circuitproxy/internal/config"
)

// Global expvar counters (Этап 5), served on the optional metrics listener
// under /debug/vars. expvar.Int is atomic inside, so increments from
// concurrent request goroutines need no extra synchronization. Registered
// exactly once at package init — expvar panics on duplicate names.
var (
	// requestsTotal — incoming proxied requests (one per RoundTrip).
	requestsTotal = expvar.NewInt("requests_total")
	// errorsTotal — requests that finished as an error for the client: a
	// transport error or a final 5xx after retries were exhausted. Exactly
	// one increment per request, not per attempt.
	errorsTotal = expvar.NewInt("errors_total")
	// fastFailTotal — requests where not a single attempt could start
	// (no live backends / every circuit open) — the synthetic 503 path.
	fastFailTotal = expvar.NewInt("fast_fail_total")
	// retriesTotal — actually performed EXTRA attempts (the first attempt is
	// not a retry).
	retriesTotal = expvar.NewInt("retries_total")
)

// Sentinel errors returned by retryTransport.RoundTrip when not a single
// attempt could be started. Handler's ErrorHandler maps both to 503: no
// backend was contacted, so a synthetic status is appropriate (as opposed to
// 502, which means a real attempt failed in transport).
var (
	// errNoBackends — the pool is empty or every backend is marked down.
	errNoBackends = errors.New("no live backends available")
	// errAllCircuitsOpen — live backends exist, but every circuit is open.
	errAllCircuitsOpen = errors.New("circuit breaker open")
)

// defaultBackoffBase replaces a non-positive backoff_base_ms when retries are
// enabled (MaxRetries > 0). A zero base would degenerate the exponent to
// 0*2^n == 0 — busy-retry with no growth, defeating the purpose of backoff —
// so unlike max_retries=0 (a valid "retries off"), a zero base is normalized.
// 50ms matches config.example.json. Full schema validation arrives in Этап 5;
// this is consumer-side normalization, same as breaker's defaultOpenTimeout.
const defaultBackoffBase = 50 * time.Millisecond

// maxBackoffShift caps the exponent in base*2^n so an absurd max_retries from
// the config cannot overflow time.Duration.
const maxBackoffShift = 16

// retryTransport is the http.RoundTripper behind Handler(). It is the single
// owner of the attempt lifecycle: for every attempt it picks a backend
// (Balancer.Next), asks its circuit breaker (Allow), performs the real
// round-trip and reports the outcome (Report) to the SAME backend the attempt
// ran on — exactly once per (backend, attempt).
//
// Idempotent requests (GET, HEAD, PUT, DELETE, OPTIONS, TRACE) are retried up
// to maxRetries additional times with exponential backoff base*2^n plus
// jitter; POST/PATCH and other non-idempotent methods get exactly one attempt.
//
// Concurrency: all fields are read-only after construction; per-request
// attempt state lives in RoundTrip's local variables. The transport is shared
// by all requests and must stay free of shared mutable state.
type retryTransport struct {
	balancer *Balancer
	// base performs the actual HTTP exchange; http.DefaultTransport in prod.
	base http.RoundTripper
	// maxRetries is the number of ADDITIONAL attempts beyond the first;
	// <= 0 means retries are disabled (a valid config choice, not a mistake).
	maxRetries int
	// backoffBase is the normalized backoff base (see defaultBackoffBase).
	backoffBase time.Duration
	// sleep pauses between attempts; injected so tests never sleep for real.
	sleep func(time.Duration)
	// jitter returns a random addition for the given exponential delay;
	// injected so backoff-growth tests are deterministic. Must be safe for
	// concurrent use — the transport is shared across request goroutines.
	jitter func(d time.Duration) time.Duration
}

// newRetryTransport builds the production transport for bal, normalizing the
// retry config (Решение 4): negative max_retries clamps to 0 ("retries off"),
// and a non-positive backoff base with retries enabled becomes
// defaultBackoffBase.
func newRetryTransport(bal *Balancer, rc config.RetryConfig) *retryTransport {
	maxRetries := rc.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0 // garbage in the config; defensive clamp, not a default
	}
	backoffBase := rc.BackoffBase()
	if maxRetries > 0 && backoffBase <= 0 {
		backoffBase = defaultBackoffBase
	}
	return &retryTransport{
		balancer:    bal,
		base:        http.DefaultTransport,
		maxRetries:  maxRetries,
		backoffBase: backoffBase,
		sleep:       time.Sleep,
		jitter:      defaultJitter,
	}
}

// defaultJitter returns a uniformly random addition in [0, d/2]. The
// top-level math/rand/v2 functions are safe for concurrent use, so the shared
// transport needs no extra synchronization around the randomness source.
func defaultJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return rand.N(d/2 + 1)
}

// isIdempotent reports whether an HTTP method is safe to retry. Methods are
// case-sensitive per HTTP semantics (canonical form is upper-case), so "post"
// would not match http.MethodPost — and would not be retried either, which is
// the safe direction for an unknown method.
func isIdempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut,
		http.MethodDelete, http.MethodOptions, http.MethodTrace:
		return true
	}
	return false
}

// backoffDelay returns the pause before retry number n (n >= 1): base*2^(n-1).
func (t *retryTransport) backoffDelay(n int) time.Duration {
	shift := n - 1
	if shift > maxBackoffShift {
		shift = maxBackoffShift
	}
	return t.backoffBase << uint(shift)
}

// pickBackend selects the backend for one attempt: it walks the round-robin
// rotation at most pool-size times, skipping backends whose circuit refuses
// the request (Allow() == false, no real contact, no Report). It returns the
// first admitted backend, or a sentinel error when there is nowhere to go.
// A backend returned here has WON its Allow() slot (possibly the single
// half-open probe), so the caller MUST perform the attempt and Report the
// outcome to release that slot.
func (t *retryTransport) pickBackend() (*Backend, error) {
	n := len(t.balancer.backends)
	if n == 0 {
		return nil, errNoBackends
	}
	for i := 0; i < n; i++ {
		be := t.balancer.Next()
		if be == nil {
			return nil, errNoBackends // every backend is marked down
		}
		if ok, _ := be.Allow(); ok {
			return be, nil
		}
	}
	return nil, errAllCircuitsOpen // live backends exist, all circuits refuse
}

// RoundTrip implements http.RoundTripper: it counts the request, delegates the
// attempt loop to roundTrip unchanged, and classifies the FINAL outcome for
// the expvar counters in one place — a sentinel (nothing was contacted) bumps
// fast_fail_total, any other error or a final 5xx bumps errors_total, so each
// counter moves exactly once per request no matter how many attempts ran.
func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	requestsTotal.Add(1)
	resp, err := t.roundTrip(req)
	switch {
	case errors.Is(err, errNoBackends) || errors.Is(err, errAllCircuitsOpen):
		fastFailTotal.Add(1)
	case err != nil || resp.StatusCode >= 500:
		errorsTotal.Add(1)
	}
	return resp, err
}

// roundTrip runs up to 1+maxRetries attempts for idempotent requests (exactly
// one otherwise), each on a freshly picked backend, with exponential backoff
// between attempts and per-attempt breaker accounting. On exhaustion it
// returns the LAST real result — the final backend response verbatim (even
// 5xx) or the final transport error (Решение 5); if no attempt could even
// start, a sentinel error mapping to 503.
func (t *retryTransport) roundTrip(req *http.Request) (*http.Response, error) {
	attempts := 1
	if t.maxRetries > 0 && isIdempotent(req.Method) {
		attempts = 1 + t.maxRetries
	}

	// Body replay (Решение 2): when a retry is possible and the request has a
	// body, arrange a fresh reader per attempt. Prefer the client-provided
	// GetBody (the same mechanism stdlib uses for its own redirects/retries);
	// otherwise buffer the whole body in memory ONCE, before the first
	// attempt. The full body is buffered without a size cap — a limit /
	// streaming replay is post-MVP. bufLen >= 0 only on the buffering path,
	// where the clone's ContentLength must be pinned to the buffer size.
	var freshBody func() (io.ReadCloser, error)
	bufLen := int64(-1)
	if attempts > 1 && req.Body != nil && req.Body != http.NoBody {
		if req.GetBody != nil {
			freshBody = req.GetBody
		} else {
			buf, err := io.ReadAll(req.Body)
			req.Body.Close()
			if err != nil {
				return nil, err
			}
			bufLen = int64(len(buf))
			freshBody = func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(buf)), nil
			}
		}
	}

	// Exactly one of lastResp/lastErr is set after each failed attempt.
	var lastResp *http.Response
	var lastErr error

	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			retriesTotal.Add(1) // every extra attempt beyond the first
			d := t.backoffDelay(attempt)
			t.sleep(d + t.jitter(d))
		}

		var body io.ReadCloser
		if freshBody != nil {
			b, err := freshBody()
			if err != nil {
				if lastResp != nil || lastErr != nil {
					return lastResp, lastErr
				}
				return nil, err
			}
			body = b
		}

		backend, pickErr := t.pickBackend()
		if backend == nil {
			if body != nil {
				body.Close()
			}
			// Mid-retry exhaustion of backends: fall back to the last real
			// result. Only when NO attempt ever started does the sentinel
			// surface (fast-fail 503 via ErrorHandler).
			if lastResp != nil || lastErr != nil {
				return lastResp, lastErr
			}
			return nil, pickErr
		}

		// A new real attempt starts: the previous failed response (kept in
		// case no further attempt could start) is now superseded — release
		// its body/connection before it leaks.
		if lastResp != nil {
			lastResp.Body.Close()
			lastResp = nil
		}

		// Clone per attempt: the shared ReverseProxy hands us one outgoing
		// request, but each attempt needs its own target URL and body.
		// Backend base URLs are scheme://host (see NewBalancer/HealthURL), so
		// retargeting is scheme+host; the client's path/query pass through.
		out := req.Clone(req.Context())
		out.URL.Scheme = backend.URL.Scheme
		out.URL.Host = backend.URL.Host
		// Blank Host so the transport derives the Host header from URL.Host —
		// same behavior as ProxyRequest.SetURL on Этапы 1-3.
		out.Host = ""
		if freshBody != nil {
			out.Body = body
			out.GetBody = freshBody
			if bufLen >= 0 {
				out.ContentLength = bufLen
			}
		}

		resp, err := t.base.RoundTrip(out)
		// Same classification as Этап 3: transport error or 5xx = failure;
		// any response < 500 (incl. 4xx — the client's fault) = success.
		success := err == nil && resp.StatusCode < 500
		backend.Report(success) // exactly once, to the backend of THIS attempt
		if success {
			return resp, nil
		}
		if err != nil {
			lastResp, lastErr = nil, err
		} else {
			lastResp, lastErr = resp, nil
		}
	}
	return lastResp, lastErr
}
