package breaker

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// forceOpenedAt rewinds the breaker's open timestamp so the next Allow treats
// OpenTimeout as elapsed, without a real sleep. Test-only.
func (b *Breaker) forceOpenedAt(t time.Time) {
	b.openedAtNanos.Store(t.UnixNano())
}

// --- Single-threaded --------------------------------------------------------

func TestBreaker_ClosedToOpen_OnThreshold(t *testing.T) {
	b := New(Config{FailureThreshold: 3, OpenTimeout: time.Second})

	b.Report(false)
	b.Report(false)
	if got := b.State(); got != StateClosed {
		t.Fatalf("after 2/3 failures: state = %v, want StateClosed", got)
	}
	if ok, err := b.Allow(); !ok || err != nil {
		t.Fatalf("after 2/3 failures: Allow() = (%v, %v), want (true, nil)", ok, err)
	}

	b.Report(false) // third failure -> open
	if got := b.State(); got != StateOpen {
		t.Fatalf("after 3/3 failures: state = %v, want StateOpen", got)
	}
	ok, err := b.Allow()
	if ok || !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("open: Allow() = (%v, %v), want (false, ErrBreakerOpen)", ok, err)
	}
}

func TestBreaker_ClosedSuccessResetsFailures(t *testing.T) {
	b := New(Config{FailureThreshold: 3, OpenTimeout: time.Second})

	b.Report(false)
	b.Report(false)
	b.Report(true) // resets the streak
	b.Report(false)
	b.Report(false) // only 2 consecutive since reset -> still below threshold 3

	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want StateClosed (streak was reset, threshold not reached)", got)
	}
}

func TestBreaker_OpenFastFailsUntilTimeout(t *testing.T) {
	b := New(Config{FailureThreshold: 1, OpenTimeout: time.Hour})
	b.Report(false) // open
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want StateOpen", got)
	}

	ok, err := b.Allow()
	if ok || !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("Allow() = (%v, %v), want (false, ErrBreakerOpen)", ok, err)
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("after fast-fail: state = %v, want StateOpen (timeout not elapsed)", got)
	}
}

func TestBreaker_OpenToHalfOpen_AfterTimeout(t *testing.T) {
	b := New(Config{FailureThreshold: 1, OpenTimeout: time.Second})
	b.Report(false) // open
	b.forceOpenedAt(time.Now().Add(-time.Hour))

	ok, err := b.Allow()
	if !ok || err != nil {
		t.Fatalf("first Allow() after timeout = (%v, %v), want (true, nil)", ok, err)
	}
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("state = %v, want StateHalfOpen", got)
	}

	ok, err = b.Allow() // probe already in flight
	if ok || !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("second Allow() = (%v, %v), want (false, ErrBreakerOpen)", ok, err)
	}
}

func TestBreaker_HalfOpenToClosed_OnSuccess(t *testing.T) {
	b := New(Config{FailureThreshold: 1, OpenTimeout: time.Second})
	b.Report(false)
	b.forceOpenedAt(time.Now().Add(-time.Hour))
	if ok, _ := b.Allow(); !ok {
		t.Fatal("expected probe to be admitted in half-open")
	}

	b.Report(true) // success closes the circuit
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want StateClosed", got)
	}
	// failures reset and trialInFlight released -> next Allow allowed.
	if ok, err := b.Allow(); !ok || err != nil {
		t.Fatalf("Allow() after recovery = (%v, %v), want (true, nil)", ok, err)
	}
	if b.failures.Load() != 0 {
		t.Fatalf("failures = %d, want 0 after recovery", b.failures.Load())
	}
}

func TestBreaker_HalfOpenToOpen_OnFailure(t *testing.T) {
	b := New(Config{FailureThreshold: 1, OpenTimeout: time.Second})
	b.Report(false)
	b.forceOpenedAt(time.Now().Add(-time.Hour))
	if ok, _ := b.Allow(); !ok {
		t.Fatal("expected probe to be admitted in half-open")
	}

	b.Report(false) // probe failed -> reopen, timeout restarted
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want StateOpen", got)
	}
	// Timeout restarted (openedAtNanos = now) -> immediate fast-fail.
	if ok, err := b.Allow(); ok || !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("Allow() right after reopen = (%v, %v), want (false, ErrBreakerOpen)", ok, err)
	}

	// Rewind again: a fresh probe must be admittable (half-open->open correctly
	// restored the ability to probe again).
	b.forceOpenedAt(time.Now().Add(-time.Hour))
	if ok, err := b.Allow(); !ok || err != nil {
		t.Fatalf("Allow() after second timeout = (%v, %v), want (true, nil)", ok, err)
	}
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("state = %v, want StateHalfOpen after re-probe", got)
	}
}

func TestBreaker_ZeroThresholdOpensOnFirstFailure(t *testing.T) {
	for _, threshold := range []int32{0, -5} {
		b := New(Config{FailureThreshold: threshold, OpenTimeout: time.Second})
		b.Report(false)
		if got := b.State(); got != StateOpen {
			t.Fatalf("threshold %d: state = %v, want StateOpen (normalized to 1)", threshold, got)
		}
	}
}

func TestBreaker_ReportFailureWhileOpenIsNoop(t *testing.T) {
	b := New(Config{FailureThreshold: 1, OpenTimeout: time.Second})
	b.Report(false) // open
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want StateOpen", got)
	}

	b.Report(false) // stray Report while open: defensive no-op
	if got := b.State(); got != StateOpen {
		t.Fatalf("after stray Report: state = %v, want StateOpen", got)
	}
	// openedAtNanos untouched: rewinding still admits exactly one probe.
	b.forceOpenedAt(time.Now().Add(-time.Hour))
	if ok, err := b.Allow(); !ok || err != nil {
		t.Fatalf("Allow() after timeout = (%v, %v), want (true, nil)", ok, err)
	}
}

func TestBreaker_ZeroOpenTimeoutUsesDefault(t *testing.T) {
	// OpenTimeout: 0 models a config that omits open_timeout_seconds; without a
	// floor, Allow's "elapsed < OpenTimeout" check degenerates to "elapsed < 0",
	// which is false for any non-negative elapsed -- every request right after
	// opening would immediately win the half-open probe instead of fast-failing.
	b := New(Config{FailureThreshold: 1})
	b.Report(false) // open
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want StateOpen", got)
	}

	ok, err := b.Allow()
	if ok || !errors.Is(err, ErrBreakerOpen) {
		t.Fatalf("Allow() immediately after opening = (%v, %v), want (false, ErrBreakerOpen) -- defaultOpenTimeout not applied", ok, err)
	}
	if got := b.State(); got != StateOpen {
		t.Fatalf("state = %v, want StateOpen (defaultOpenTimeout not elapsed)", got)
	}
}

// --- Concurrent (must run under -race) --------------------------------------

func TestBreaker_HalfOpen_ExactlyOneTrial(t *testing.T) {
	b := New(Config{FailureThreshold: 1, OpenTimeout: 10 * time.Millisecond})
	b.Report(false) // open
	b.forceOpenedAt(time.Now().Add(-time.Second))

	const N = 100
	var granted atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // maximize simultaneity
			if ok, _ := b.Allow(); ok {
				granted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := granted.Load(); got != 1 {
		t.Fatalf("half-open admitted %d probes, want exactly 1", got)
	}
	if got := b.State(); got != StateHalfOpen {
		t.Fatalf("state = %v, want StateHalfOpen after probe grant", got)
	}
}

// --- Stress (must run under -race) ------------------------------------------

func TestBreaker_StressAllowReport(t *testing.T) {
	b := New(Config{FailureThreshold: 3, OpenTimeout: 5 * time.Millisecond})

	const (
		goroutines = 50
		iterations = 200
	)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(g)))
			<-start
			for i := 0; i < iterations; i++ {
				switch rng.Intn(3) {
				case 0:
					b.Allow()
				case 1:
					b.Report(rng.Intn(2) == 0)
				case 2:
					// churn transitions: rewind the open timestamp so open->half-open fires.
					b.forceOpenedAt(time.Now().Add(-time.Hour))
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	// No state assertion: the goal is that the race detector stays silent.
}
