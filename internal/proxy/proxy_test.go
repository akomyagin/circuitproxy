package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akomyagin/circuitproxy/internal/breaker"
	"github.com/akomyagin/circuitproxy/internal/config"
)

// testConfig builds a minimal config for the balancer under test.
func testConfig(backends ...string) *config.Config {
	return &config.Config{Listen: ":0", Backends: backends}
}

// mustBalancer builds a balancer or fails the test.
func mustBalancer(t *testing.T, backends ...string) *Balancer {
	t.Helper()
	b, err := NewBalancer(testConfig(backends...))
	if err != nil {
		t.Fatalf("NewBalancer(%v): unexpected error: %v", backends, err)
	}
	return b
}

// --- NewBalancer validation -------------------------------------------------

func TestNewBalancer_EmptyBackends(t *testing.T) {
	if _, err := NewBalancer(testConfig()); err == nil {
		t.Fatal("NewBalancer with empty backend list: want error, got nil")
	}
}

func TestNewBalancer_InvalidURL(t *testing.T) {
	cases := []string{
		"://bad",             // unparsable
		"/relative/path",     // no scheme, no host
		"localhost:9001/foo", // scheme-less form parses but has no host part we accept
	}
	for _, raw := range cases {
		if _, err := NewBalancer(testConfig(raw)); err == nil {
			t.Errorf("NewBalancer(%q): want error, got nil", raw)
		}
	}
}

func TestNewBalancer_Valid(t *testing.T) {
	urls := []string{"http://localhost:9001", "http://localhost:9002", "https://example.com"}
	b := mustBalancer(t, urls...)
	if got := len(b.backends); got != len(urls) {
		t.Fatalf("len(backends) = %d, want %d", got, len(urls))
	}
	for i, backend := range b.backends {
		if backend.URL.String() != urls[i] {
			t.Errorf("backends[%d].URL = %q, want %q", i, backend.URL, urls[i])
		}
		if !backend.up.Load() {
			t.Errorf("backends[%d].up = false, want true after NewBalancer", i)
		}
	}
}

// --- Next(): round-robin unit ----------------------------------------------

func TestNext_UniformCyclicOrder(t *testing.T) {
	const n, m = 3, 4
	b := mustBalancer(t,
		"http://localhost:9001", "http://localhost:9002", "http://localhost:9003")

	counts := make(map[*Backend]int, n)
	for i := 0; i < m*n; i++ {
		got := b.Next()
		if got == nil {
			t.Fatalf("Next() call %d returned nil on non-empty pool", i)
		}
		if want := b.backends[i%n]; got != want {
			t.Fatalf("Next() call %d = %v, want %v (cyclic order broken)", i, got.URL, want.URL)
		}
		counts[got]++
	}
	for i, backend := range b.backends {
		if counts[backend] != m {
			t.Errorf("backend %d picked %d times, want %d", i, counts[backend], m)
		}
	}
}

func TestNext_EmptyPoolReturnsNil(t *testing.T) {
	var b Balancer
	if got := b.Next(); got != nil {
		t.Fatalf("Next() on empty pool = %v, want nil", got.URL)
	}
}

// TestNext_Concurrent hammers Next() from many goroutines while other
// goroutines toggle liveness of backends 1 and 2. Backend 0 stays pinned up,
// so Next() must never return nil: at least one backend is always live and
// the forward scan is guaranteed to find it. Rotation under toggling is
// eventually consistent, so beyond the nil-freedom invariant the test only
// asserts that the race detector stays silent.
func TestNext_Concurrent(t *testing.T) {
	const goroutines = 50
	const callsPerGoroutine = 200
	const togglers = 4
	b := mustBalancer(t,
		"http://localhost:9001", "http://localhost:9002", "http://localhost:9003")

	var picks atomic.Int64
	var nils atomic.Int64
	start := make(chan struct{})
	stopToggling := make(chan struct{})
	var wg, toggleWg sync.WaitGroup

	// Togglers flip liveness of backends 1 and 2 concurrently with Next().
	// Backend 0 is never touched: it stays up for the whole run.
	for i := 0; i < togglers; i++ {
		i := i
		toggleWg.Add(1)
		go func() {
			defer toggleWg.Done()
			<-start
			target := b.backends[1+i%2]
			flip := false
			for {
				select {
				case <-stopToggling:
					return
				default:
					target.SetUp(flip)
					flip = !flip
				}
			}
		}()
	}

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // maximize simultaneity
			for j := 0; j < callsPerGoroutine; j++ {
				if b.Next() != nil {
					picks.Add(1)
				} else {
					nils.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(stopToggling)
	toggleWg.Wait()

	if nils.Load() != 0 {
		t.Errorf("Next() returned nil %d times while backend 0 was always up", nils.Load())
	}
	if got, want := picks.Load(), int64(goroutines*callsPerGoroutine); got != want {
		t.Errorf("total picks = %d, want %d", got, want)
	}
}

// --- Next(): liveness (Этап 2) ----------------------------------------------

func TestNext_SkipsDown(t *testing.T) {
	b := mustBalancer(t,
		"http://localhost:9001", "http://localhost:9002", "http://localhost:9003")
	down := b.backends[1]
	down.SetUp(false)

	counts := make(map[*Backend]int, 2)
	for i := 0; i < 30; i++ {
		got := b.Next()
		if got == nil {
			t.Fatalf("Next() call %d returned nil with two live backends", i)
		}
		if got == down {
			t.Fatalf("Next() call %d returned a down backend %v", i, got.URL)
		}
		counts[got]++
	}
	for _, idx := range []int{0, 2} {
		if counts[b.backends[idx]] == 0 {
			t.Errorf("live backend %d never returned by Next()", idx)
		}
	}
}

func TestNext_AllDown_ReturnsNil(t *testing.T) {
	b := mustBalancer(t,
		"http://localhost:9001", "http://localhost:9002", "http://localhost:9003")
	for _, backend := range b.backends {
		backend.SetUp(false)
	}
	for i := 0; i < 10; i++ {
		if got := b.Next(); got != nil {
			t.Fatalf("Next() call %d = %v, want nil with all backends down", i, got.URL)
		}
	}
}

func TestNext_RecoveryReturnsToRotation(t *testing.T) {
	b := mustBalancer(t,
		"http://localhost:9001", "http://localhost:9002", "http://localhost:9003")
	b.backends[0].SetUp(false)
	b.backends[1].SetUp(false)

	for i := 0; i < 10; i++ {
		if got := b.Next(); got != b.backends[2] {
			t.Fatalf("Next() call %d = %v, want the only live backend %v", i, got, b.backends[2].URL)
		}
	}

	b.backends[1].SetUp(true)
	counts := make(map[*Backend]int, 2)
	for i := 0; i < 20; i++ {
		got := b.Next()
		if got == nil || got == b.backends[0] {
			t.Fatalf("Next() call %d = %v, want one of the two live backends", i, got)
		}
		counts[got]++
	}
	if counts[b.backends[1]] == 0 {
		t.Error("recovered backend never returned to rotation")
	}
	if counts[b.backends[2]] == 0 {
		t.Error("always-live backend dropped out of rotation")
	}
}

// --- Handler(): proxying ----------------------------------------------------

// startBackends spins up n httptest backends, each tagging responses with its
// index in the X-Backend-Index header, and returns them with their URLs.
func startBackends(t *testing.T, n int) []*httptest.Server {
	t.Helper()
	servers := make([]*httptest.Server, n)
	for i := 0; i < n; i++ {
		i := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Backend-Index", strconv.Itoa(i))
		}))
		t.Cleanup(servers[i].Close)
	}
	return servers
}

func TestHandler_RoundRobinDistribution(t *testing.T) {
	const n, k = 3, 5
	servers := startBackends(t, n)
	urls := make([]string, n)
	for i, s := range servers {
		urls[i] = s.URL
	}
	h := mustBalancer(t, urls...).Handler()

	seq := make([]int, 0, k*n)
	for i := 0; i < k*n; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, rec.Code)
		}
		idx, err := strconv.Atoi(rec.Header().Get("X-Backend-Index"))
		if err != nil {
			t.Fatalf("request %d: bad X-Backend-Index %q", i, rec.Header().Get("X-Backend-Index"))
		}
		seq = append(seq, idx)
	}

	counts := make([]int, n)
	for _, idx := range seq {
		counts[idx]++
	}
	for i, c := range counts {
		if c != k {
			t.Errorf("backend %d received %d requests, want exactly %d (seq=%v)", i, c, k, seq)
		}
	}
	for i, idx := range seq {
		if want := (seq[0] + i) % n; idx != want {
			t.Fatalf("request %d hit backend %d, want %d: order not cyclic (seq=%v)", i, idx, want, seq)
		}
	}
}

func TestHandler_RequestBodyTransparent(t *testing.T) {
	const payload = "hello backend, bytes must survive \x00\x01\x02"
	var gotBody atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("backend read body: %v", err)
		}
		gotBody.Store(string(body))
	}))
	t.Cleanup(srv.Close)

	h := mustBalancer(t, srv.URL).Handler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/submit", strings.NewReader(payload)))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := gotBody.Load(); got != payload {
		t.Fatalf("backend saw body %q, want %q", got, payload)
	}
}

func TestHandler_RequestHeadersTransparent(t *testing.T) {
	var gotCustom, gotXFF atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCustom.Store(r.Header.Get("X-Client-Token"))
		gotXFF.Store(r.Header.Get("X-Forwarded-For"))
	}))
	t.Cleanup(srv.Close)

	h := mustBalancer(t, srv.URL).Handler()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Client-Token", "secret-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := gotCustom.Load(); got != "secret-123" {
		t.Errorf("backend saw X-Client-Token %q, want %q", got, "secret-123")
	}
	// httptest.NewRequest sets RemoteAddr to 192.0.2.1:1234; SetXForwarded
	// must surface the client IP to the backend.
	if got := gotXFF.Load(); got != "192.0.2.1" {
		t.Errorf("backend saw X-Forwarded-For %q, want %q", got, "192.0.2.1")
	}
}

func TestHandler_ResponseTransparent(t *testing.T) {
	const body = `{"created":true}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend-Version", "v42")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)

	h := mustBalancer(t, srv.URL).Handler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/things", nil))

	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusCreated)
	}
	if got := rec.Header().Get("X-Backend-Version"); got != "v42" {
		t.Errorf("X-Backend-Version = %q, want %q", got, "v42")
	}
	if got := rec.Body.String(); got != body {
		t.Errorf("body = %q, want %q", got, body)
	}
}

func TestHandler_UnreachableBackendReturns502(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	target := srv.URL
	srv.Close() // backend is now down: connection refused

	h := mustBalancer(t, target).Handler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadGateway)
	}
}

func TestHandler_NoBackendsReturns503(t *testing.T) {
	// Zero-value balancer models "no backend to pick" (Next() returns nil);
	// on Этап 2 this becomes reachable when every backend is down.
	var b Balancer
	rec := httptest.NewRecorder()
	b.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

// --- Handler(): circuit breaker (Этап 3) ------------------------------------

// clock is a test-controllable time source (nanoseconds) for injecting time
// into a backend's breaker without real sleeps.
type clock struct{ nanos atomic.Int64 }

func newClock() *clock {
	c := &clock{}
	c.nanos.Store(time.Now().UnixNano())
	return c
}

func (c *clock) now() time.Time          { return time.Unix(0, c.nanos.Load()) }
func (c *clock) advance(d time.Duration) { c.nanos.Add(int64(d)) }

func TestHandler_BreakerOpensAndFastFails(t *testing.T) {
	const threshold = 3
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	clk := newClock()
	bal, _, err := newBalancerWithBreaker(srv.URL, breaker.Config{
		FailureThreshold: threshold,
		OpenTimeout:      time.Hour,
		Now:              clk.now,
	})
	if err != nil {
		t.Fatalf("newBalancerWithBreaker: %v", err)
	}
	h := bal.Handler()

	// First `threshold` requests reach the backend and get its 500 verbatim.
	for i := 0; i < threshold; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("request %d: status = %d, want 500 (backend response proxied verbatim)", i, rec.Code)
		}
	}
	if got := calls.Load(); got != threshold {
		t.Fatalf("backend calls = %d, want %d before circuit opens", got, threshold)
	}

	// Circuit is now open: the next request must fast-fail 503 without hitting
	// the backend.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("open circuit: status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "circuit breaker open") {
		t.Errorf("open circuit body = %q, want to contain %q", rec.Body.String(), "circuit breaker open")
	}
	if got := calls.Load(); got != threshold {
		t.Fatalf("backend calls = %d, want %d (fast-fail must not reach backend)", got, threshold)
	}
}

func TestHandler_BreakerHalfOpenRecovers(t *testing.T) {
	const threshold = 3
	var calls atomic.Int64
	var healthy atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if healthy.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	clk := newClock()
	const openTimeout = time.Minute
	bal, _, err := newBalancerWithBreaker(srv.URL, breaker.Config{
		FailureThreshold: threshold,
		OpenTimeout:      openTimeout,
		Now:              clk.now,
	})
	if err != nil {
		t.Fatalf("newBalancerWithBreaker: %v", err)
	}
	h := bal.Handler()

	// Drive the circuit open with 5xx responses.
	for i := 0; i < threshold; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	}
	callsAfterOpen := calls.Load()

	// Advance past OpenTimeout and let the backend recover.
	clk.advance(openTimeout + time.Second)
	healthy.Store(true)

	// One probe request goes through, hits the backend, gets 200 -> closes.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("probe request: status = %d, want 200", rec.Code)
	}
	if got := calls.Load(); got != callsAfterOpen+1 {
		t.Fatalf("backend calls = %d, want %d (exactly one probe reached backend)", got, callsAfterOpen+1)
	}

	// Circuit closed: subsequent requests flow to the backend again.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("post-recovery request: status = %d, want 200", rec.Code)
	}
	if got := calls.Load(); got != callsAfterOpen+2 {
		t.Fatalf("backend calls = %d, want %d after recovery", got, callsAfterOpen+2)
	}
}

func TestHandler_BreakerCounts5xxAsFailure(t *testing.T) {
	const threshold = 3

	t.Run("5xx opens the circuit", func(t *testing.T) {
		var calls atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable) // 503, a 5xx
		}))
		t.Cleanup(srv.Close)

		bal, _, err := newBalancerWithBreaker(srv.URL, breaker.Config{
			FailureThreshold: threshold,
			OpenTimeout:      time.Hour,
			Now:              newClock().now,
		})
		if err != nil {
			t.Fatalf("newBalancerWithBreaker: %v", err)
		}
		h := bal.Handler()

		for i := 0; i < threshold; i++ {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		}
		// Circuit should now be open: fast-fail without reaching backend.
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
		if got := calls.Load(); got != threshold {
			t.Fatalf("backend calls = %d, want %d (5xx counted as failure, circuit opened)", got, threshold)
		}
	})

	t.Run("4xx keeps the circuit closed", func(t *testing.T) {
		var calls atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusNotFound) // 404, a 4xx = success for breaker
		}))
		t.Cleanup(srv.Close)

		bal, _, err := newBalancerWithBreaker(srv.URL, breaker.Config{
			FailureThreshold: threshold,
			OpenTimeout:      time.Hour,
			Now:              newClock().now,
		})
		if err != nil {
			t.Fatalf("newBalancerWithBreaker: %v", err)
		}
		h := bal.Handler()

		const n = threshold + 5
		for i := 0; i < n; i++ {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
			if rec.Code != http.StatusNotFound {
				t.Fatalf("request %d: status = %d, want 404 (proxied verbatim, never fast-failed)", i, rec.Code)
			}
		}
		if got := calls.Load(); got != n {
			t.Fatalf("backend calls = %d, want %d (4xx must not open the circuit)", got, n)
		}
	})
}

func TestHandler_BreakerCountsTransportErrorAsFailure(t *testing.T) {
	const threshold = 3
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	target := srv.URL
	srv.Close() // backend fully down -> connection refused -> transport error

	bal, _, err := newBalancerWithBreaker(target, breaker.Config{
		FailureThreshold: threshold,
		OpenTimeout:      time.Hour,
		Now:              newClock().now,
	})
	if err != nil {
		t.Fatalf("newBalancerWithBreaker: %v", err)
	}
	h := bal.Handler()

	// First `threshold` requests actually attempt the backend -> 502 each.
	for i := 0; i < threshold; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("request %d: status = %d, want 502 (transport error, real attempt)", i, rec.Code)
		}
	}

	// Circuit now open from transport failures: next request fast-fails 503,
	// not 502 -> proves ErrorHandler reported the failures.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("open circuit: status = %d, want 503 (not 502)", rec.Code)
	}
}
