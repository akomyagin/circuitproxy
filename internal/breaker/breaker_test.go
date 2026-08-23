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

// --- OnTransition callback (Этап 5) ------------------------------------------

// transition is one recorded OnTransition invocation.
type transition struct{ from, to State }

// recordTransitions wires a recording OnTransition into cfg and returns the
// slice of recorded calls. Only for single-goroutine tests (no locking).
func recordTransitions(cfg Config) (*Breaker, *[]transition) {
	calls := &[]transition{}
	cfg.OnTransition = func(from, to State) {
		*calls = append(*calls, transition{from, to})
	}
	return New(cfg), calls
}

// Кейс 16: closed→open по порогу — ровно один вызов; дальнейшие Report(false)
// в open (no-op ветка) колбэк не зовут.
func TestBreaker_OnTransition_ClosedToOpen(t *testing.T) {
	b, calls := recordTransitions(Config{FailureThreshold: 3, OpenTimeout: time.Hour})

	b.Report(false)
	b.Report(false)
	if got := len(*calls); got != 0 {
		t.Fatalf("below threshold: %d callback calls, want 0", got)
	}
	b.Report(false) // threshold reached -> the one real transition
	if want := []transition{{StateClosed, StateOpen}}; len(*calls) != 1 || (*calls)[0] != want[0] {
		t.Fatalf("calls = %v, want %v", *calls, want)
	}

	b.Report(false) // stray Report in open: no-op, no transition
	b.Report(false)
	if got := len(*calls); got != 1 {
		t.Fatalf("after stray open Reports: %d callback calls, want still 1", got)
	}
}

// Кейс 17: open→half-open — победитель CAS триггерит колбэк ровно один раз;
// второй Allow (проигравший слот пробника) не триггерит.
func TestBreaker_OnTransition_OpenToHalfOpen(t *testing.T) {
	b, calls := recordTransitions(Config{FailureThreshold: 1, OpenTimeout: time.Second})
	b.Report(false) // closed->open (call #1)
	b.forceOpenedAt(time.Now().Add(-time.Hour))

	if ok, _ := b.Allow(); !ok {
		t.Fatal("first Allow after timeout must admit the probe")
	}
	want := []transition{{StateClosed, StateOpen}, {StateOpen, StateHalfOpen}}
	if len(*calls) != 2 || (*calls)[1] != want[1] {
		t.Fatalf("calls = %v, want %v", *calls, want)
	}

	if ok, _ := b.Allow(); ok {
		t.Fatal("second Allow must fast-fail while the probe is in flight")
	}
	if got := len(*calls); got != 2 {
		t.Fatalf("losing Allow triggered a callback: %d calls, want 2", got)
	}
}

// Кейс 18: half-open→closed на успехе пробника — ровно один вызов.
func TestBreaker_OnTransition_HalfOpenToClosed(t *testing.T) {
	b, calls := recordTransitions(Config{FailureThreshold: 1, OpenTimeout: time.Second})
	b.Report(false)
	b.forceOpenedAt(time.Now().Add(-time.Hour))
	if ok, _ := b.Allow(); !ok {
		t.Fatal("probe must be admitted")
	}

	before := len(*calls)
	b.Report(true)
	if got := (*calls)[len(*calls)-1]; len(*calls) != before+1 || got != (transition{StateHalfOpen, StateClosed}) {
		t.Fatalf("calls after probe success = %v, want one new (half-open, closed)", *calls)
	}
}

// Кейс 19: half-open→open на провале пробника — ровно один вызов.
func TestBreaker_OnTransition_HalfOpenToOpen(t *testing.T) {
	b, calls := recordTransitions(Config{FailureThreshold: 1, OpenTimeout: time.Second})
	b.Report(false)
	b.forceOpenedAt(time.Now().Add(-time.Hour))
	if ok, _ := b.Allow(); !ok {
		t.Fatal("probe must be admitted")
	}

	before := len(*calls)
	b.Report(false)
	if got := (*calls)[len(*calls)-1]; len(*calls) != before+1 || got != (transition{StateHalfOpen, StateOpen}) {
		t.Fatalf("calls after probe failure = %v, want one new (half-open, open)", *calls)
	}
}

// Кейс 20: горячий closed-путь без переходов колбэк не зовёт вовсе: успехи и
// падения ниже порога — ноль вызовов (успех в closed — никогда не переход).
func TestBreaker_OnTransition_ClosedHotPathSilent(t *testing.T) {
	b, calls := recordTransitions(Config{FailureThreshold: 5, OpenTimeout: time.Hour})

	for i := 0; i < 10; i++ {
		b.Allow()
		b.Report(true)
	}
	b.Report(false)
	b.Report(false)
	b.Report(true) // resets the streak — still no transition
	b.Report(false)

	if got := len(*calls); got != 0 {
		t.Fatalf("closed hot path made %d callback calls, want 0: %v", got, *calls)
	}
}

// Кейс 21: nil-колбэк безопасен — полный цикл переходов работает как раньше.
func TestBreaker_OnTransition_NilSafe(t *testing.T) {
	b := New(Config{FailureThreshold: 1, OpenTimeout: time.Second}) // OnTransition nil
	b.Report(false)                                                 // closed->open
	b.forceOpenedAt(time.Now().Add(-time.Hour))
	if ok, _ := b.Allow(); !ok { // open->half-open + probe
		t.Fatal("probe must be admitted with nil callback")
	}
	b.Report(true) // half-open->closed
	if got := b.State(); got != StateClosed {
		t.Fatalf("state = %v, want StateClosed (nil callback must not change behavior)", got)
	}
}

// Кейс 22: State.String() — человекочитаемые имена, без паники на мусоре.
func TestState_String(t *testing.T) {
	cases := []struct {
		s    State
		want string
	}{
		{StateClosed, "closed"},
		{StateOpen, "open"},
		{StateHalfOpen, "half-open"},
	}
	for _, c := range cases {
		if got := c.s.String(); got != c.want {
			t.Errorf("State(%d).String() = %q, want %q", int32(c.s), got, c.want)
		}
	}
	if got := State(99).String(); got == "" {
		t.Error("State(99).String() = empty, want a non-empty diagnostic name")
	}
}

// --- OnTransition under concurrency (Этап 5, must run under -race) -----------

// Кейс 24: инвариант «ровно один пробник» сохраняется при активном колбэке, и
// переход open→half-open репортится ровно один раз (только победителем CAS),
// а не N раз.
func TestBreaker_HalfOpen_ExactlyOneTrial_WithCallback(t *testing.T) {
	var openToHalfOpen, other atomic.Int32
	b := New(Config{
		FailureThreshold: 1,
		OpenTimeout:      10 * time.Millisecond,
		OnTransition: func(from, to State) {
			if from == StateOpen && to == StateHalfOpen {
				openToHalfOpen.Add(1)
			} else {
				other.Add(1)
			}
		},
	})
	b.Report(false) // open (records one closed->open in "other")
	b.forceOpenedAt(time.Now().Add(-time.Second))
	otherBefore := other.Load()

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
		t.Fatalf("half-open admitted %d probes, want exactly 1 (callback must not break the invariant)", got)
	}
	if got := openToHalfOpen.Load(); got != 1 {
		t.Fatalf("open->half-open reported %d times, want exactly 1 (CAS winner only)", got)
	}
	if got := other.Load(); got != otherBefore {
		t.Fatalf("unexpected extra transitions during concurrent Allow: %d, want %d", got, otherBefore)
	}
}

// Кейс 25: stress под -race с активным колбэком — детектор гонок молчит,
// колбэк не вносит data race ни на своих from/to, ни на состоянии breaker'а.
func TestBreaker_StressAllowReport_WithCallback(t *testing.T) {
	var transitions atomic.Int64
	b := New(Config{
		FailureThreshold: 3,
		OpenTimeout:      5 * time.Millisecond,
		OnTransition: func(from, to State) {
			if from == to {
				t.Errorf("callback got from == to (%v): not a real transition", from)
			}
			transitions.Add(1)
		},
	})

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
	// No count assertion: the goal is a silent race detector with the hook active.
}
