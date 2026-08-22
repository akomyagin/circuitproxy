package healthcheck

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akomyagin/circuitproxy/internal/config"
	"github.com/akomyagin/circuitproxy/internal/proxy"
)

const healthPath = "/health"

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before deadline")
}

// newFlakyBackend starts an httptest server whose health endpoint answers 200
// while healthy is true and 500 otherwise (SKILL.md pattern). Non-health paths
// answer 200 with an X-Backend-Index header for routing assertions.
func newFlakyBackend(t *testing.T, index int) (*httptest.Server, *atomic.Bool) {
	t.Helper()
	healthy := &atomic.Bool{}
	healthy.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == healthPath {
			if healthy.Load() {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("X-Backend-Index", strconv.Itoa(index))
	}))
	t.Cleanup(srv.Close)
	return srv, healthy
}

// mustBalancer builds a proxy.Balancer over the given URLs or fails the test.
func mustBalancer(t *testing.T, urls ...string) *proxy.Balancer {
	t.Helper()
	b, err := proxy.NewBalancer(&config.Config{Backends: urls})
	if err != nil {
		t.Fatalf("NewBalancer(%v): unexpected error: %v", urls, err)
	}
	return b
}

// newChecker builds a Checker over the given URLs and returns it with its
// backend pool. The interval is the schema minimum of 1s (whole seconds only);
// state-transition tests avoid waiting on the ticker by calling probeAll
// directly (same package), per the plan's testing note.
func newChecker(t *testing.T, urls ...string) (*Checker, []*proxy.Backend) {
	t.Helper()
	backends := mustBalancer(t, urls...).Backends()
	cfg := config.HealthCheckConfig{
		Path:            healthPath,
		IntervalSeconds: 1,
		TimeoutSeconds:  1,
	}
	return New(cfg, backends), backends
}

// --- B1-B4: probe transitions via direct probeAll ---------------------------

func TestChecker_ProbeMarksDown(t *testing.T) {
	srv, healthy := newFlakyBackend(t, 0)
	healthy.Store(false)
	c, backends := newChecker(t, srv.URL)

	c.probeAll(context.Background())

	if backends[0].IsUp() {
		t.Fatal("backend answering 500 on health path: IsUp() = true, want false")
	}
}

func TestChecker_ProbeMarksUp(t *testing.T) {
	srv, _ := newFlakyBackend(t, 0)
	c, backends := newChecker(t, srv.URL)
	backends[0].SetUp(false)

	c.probeAll(context.Background())

	if !backends[0].IsUp() {
		t.Fatal("backend answering 200 on health path: IsUp() = false, want true")
	}
}

func TestChecker_TransportErrorMarksDown(t *testing.T) {
	srv, _ := newFlakyBackend(t, 0)
	c, backends := newChecker(t, srv.URL)
	srv.Close() // connection refused from now on

	c.probeAll(context.Background())

	if backends[0].IsUp() {
		t.Fatal("unreachable backend: IsUp() = true, want false")
	}
}

func TestChecker_DownThenRecover(t *testing.T) {
	srv, healthy := newFlakyBackend(t, 0)
	c, backends := newChecker(t, srv.URL)
	ctx := context.Background()

	c.probeAll(ctx)
	if !backends[0].IsUp() {
		t.Fatal("healthy backend after first probe: IsUp() = false, want true")
	}

	healthy.Store(false)
	c.probeAll(ctx)
	if backends[0].IsUp() {
		t.Fatal("backend gone unhealthy: IsUp() = true after probe, want false")
	}

	healthy.Store(true)
	c.probeAll(ctx)
	if !backends[0].IsUp() {
		t.Fatal("backend recovered: IsUp() = false after probe, want true")
	}
}

// --- B5-B7: Run loop --------------------------------------------------------

func TestChecker_RunImmediateProbe(t *testing.T) {
	srv, healthy := newFlakyBackend(t, 0)
	healthy.Store(false)
	// Huge interval: the only way the flag can flip within the waitFor window
	// is the immediate probe Run performs before its first tick.
	backends := mustBalancer(t, srv.URL).Backends()
	c := New(config.HealthCheckConfig{
		Path:            healthPath,
		IntervalSeconds: 3600,
		TimeoutSeconds:  1,
	}, backends)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	waitFor(t, func() bool { return !backends[0].IsUp() })
}

func TestChecker_RunStopsOnCtxCancel(t *testing.T) {
	srv, _ := newFlakyBackend(t, 0)
	c, _ := newChecker(t, srv.URL)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s after ctx cancellation")
	}
}

func TestChecker_RunTicksRepeatedly(t *testing.T) {
	// The config schema only allows whole-second intervals, so this test uses
	// the minimum 1s tick; waitFor's 2s deadline covers at least one tick.
	srv, healthy := newFlakyBackend(t, 0)
	c, backends := newChecker(t, srv.URL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	// Immediate probe sees a healthy backend.
	waitFor(t, func() bool { return backends[0].IsUp() })

	// Flip health between ticks: only a subsequent tick (no manual probeAll)
	// can observe it.
	healthy.Store(false)
	waitFor(t, func() bool { return !backends[0].IsUp() })
}

// --- defaults: guard against a zero/omitted cfg.HealthCheck ------------------

func TestChecker_RunZeroIntervalUsesDefault(t *testing.T) {
	// IntervalSeconds: 0 models a config that omits health_check entirely;
	// full schema validation arrives in Этап 5. Run must not panic
	// (time.NewTicker(0) would) and must still complete the immediate probe.
	srv, healthy := newFlakyBackend(t, 0)
	healthy.Store(false)
	backends := mustBalancer(t, srv.URL).Backends()
	c := New(config.HealthCheckConfig{Path: healthPath}, backends)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	waitFor(t, func() bool { return !backends[0].IsUp() })
}

func TestChecker_ZeroTimeoutUsesDefault(t *testing.T) {
	// TimeoutSeconds: 0 must not leave the http.Client with no timeout
	// (Timeout: 0 means unbounded) — a backend that never responds must
	// still fail the probe instead of hanging it forever.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done() // never respond; only the client timeout ends this
	}))
	t.Cleanup(srv.Close)
	backends := mustBalancer(t, srv.URL).Backends()
	c := New(config.HealthCheckConfig{Path: healthPath}, backends)

	done := make(chan struct{})
	go func() {
		c.probeAll(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("probeAll did not return within 5s: zero timeout left the client unbounded")
	}
	if backends[0].IsUp() {
		t.Fatal("backend that never responds: IsUp() = true, want false")
	}
}

// --- B8: concurrency --------------------------------------------------------

func TestChecker_ConcurrentProbe(t *testing.T) {
	srv1, healthy1 := newFlakyBackend(t, 0)
	srv2, _ := newFlakyBackend(t, 1)
	c, backends := newChecker(t, srv1.URL, srv2.URL)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	const readers = 8
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for _, be := range backends {
						be.IsUp() // concurrent read against Run's SetUp writes
					}
				}
			}
		}()
	}

	// Force at least one liveness write while readers are spinning.
	healthy1.Store(false)
	waitFor(t, func() bool { return !backends[0].IsUp() })

	close(stop)
	wg.Wait()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

// --- 9: integration — down backend leaves and re-enters routing -------------

func TestIntegration_DownBackendExcludedFromRouting(t *testing.T) {
	srv0, _ := newFlakyBackend(t, 0)
	srv1, healthy1 := newFlakyBackend(t, 1)

	balancer := mustBalancer(t, srv0.URL, srv1.URL)
	backends := balancer.Backends()
	c := New(config.HealthCheckConfig{
		Path:            healthPath,
		IntervalSeconds: 1,
		TimeoutSeconds:  1,
	}, backends)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	waitFor(t, func() bool { return backends[0].IsUp() && backends[1].IsUp() })

	// Down phase: backend 1 fails its health check and must leave rotation.
	healthy1.Store(false)
	waitFor(t, func() bool { return !backends[1].IsUp() })

	h := balancer.Handler()
	for i := 0; i < 10; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d while backend 1 down: status = %d, want 200", i, rec.Code)
		}
		if got := rec.Header().Get("X-Backend-Index"); got != "0" {
			t.Fatalf("request %d routed to backend %q, want only live backend 0", i, got)
		}
	}

	// Recovery phase: backend 1 heals and must re-enter rotation.
	healthy1.Store(true)
	waitFor(t, func() bool { return backends[1].IsUp() })

	seen := map[string]bool{}
	for i := 0; i < 10; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d after recovery: status = %d, want 200", i, rec.Code)
		}
		seen[rec.Header().Get("X-Backend-Index")] = true
	}
	if !seen["0"] || !seen["1"] {
		t.Fatalf("after recovery traffic hit backends %v, want both 0 and 1", seen)
	}
}
