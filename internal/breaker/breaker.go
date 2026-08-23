// Package breaker implements a per-backend circuit breaker state machine
// (closed / open / half-open) with a concurrency-correct half-open transition:
// when the circuit moves from open to half-open, exactly ONE probe request is
// admitted regardless of parallel load, using atomic CAS on independent fields.
//
// This is the core of the project — see docs/TECHNICAL_PLAN.md
// ("Конкурентная модель circuit breaker").
package breaker

import (
	"errors"
	"sync/atomic"
	"time"
)

// State encodes the circuit-breaker state.
type State int32

const (
	// StateClosed — requests flow normally; consecutive failures are counted.
	StateClosed State = iota
	// StateOpen — all requests fast-fail until OpenTimeout elapses.
	StateOpen
	// StateHalfOpen — exactly one probe request is admitted.
	StateHalfOpen
)

// ErrBreakerOpen is returned by Allow when the request must not reach the backend.
var ErrBreakerOpen = errors.New("circuit breaker open")

// defaultOpenTimeout guards against a zero/omitted cfg.OpenTimeout: full config
// validation arrives in Этап 5, but a zero timeout collapses the open state to
// zero dwell time (Allow's elapsed-since-open check treats "elapsed < 0" as
// false for any non-negative elapsed), so every request would immediately win
// the half-open probe instead of fast-failing while the backend recovers.
const defaultOpenTimeout = 30 * time.Second

// Config holds breaker thresholds.
type Config struct {
	// FailureThreshold — consecutive failures in closed state before opening.
	FailureThreshold int32
	// OpenTimeout — time in open state before allowing a half-open probe.
	OpenTimeout time.Duration
	// Now is injected for deterministic testing; defaults to time.Now.
	Now func() time.Time
}

// Breaker is a per-backend circuit breaker. The zero value is not usable; use New.
type Breaker struct {
	state         atomic.Int32 // State
	failures      atomic.Int32 // consecutive failures in closed
	openedAtNanos atomic.Int64 // wall-clock nanos of last open transition
	trialInFlight atomic.Bool  // true while the single half-open probe is running

	failureThreshold int32
	openTimeout      time.Duration
	now              func() time.Time
}

// New constructs a Breaker in the closed state.
func New(cfg Config) *Breaker {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	openTimeout := cfg.OpenTimeout
	if openTimeout <= 0 {
		openTimeout = defaultOpenTimeout
	}
	b := &Breaker{
		failureThreshold: cfg.FailureThreshold,
		openTimeout:      openTimeout,
		now:              now,
	}
	// Normalize a non-positive threshold to 1 ("open on the first failure").
	// The config is not validated until Этап 5 (same nature as interval=0 on
	// Этап 2); normalize here so Report never has to re-check per call.
	if b.failureThreshold <= 0 {
		b.failureThreshold = 1
	}
	b.state.Store(int32(StateClosed))
	return b
}

// Allow reports whether a request may proceed to the backend. In half-open,
// exactly one caller receives true; the rest fast-fail with ErrBreakerOpen.
//
// State machine:
//   - closed: always allow.
//   - open: fast-fail until OpenTimeout elapses; then CAS open→half-open and
//     fall through to the half-open admission below.
//   - half-open: admit exactly one probe via trialInFlight.CompareAndSwap; the
//     rest fast-fail. This is the concurrency-correct heart of the breaker.
func (b *Breaker) Allow() (bool, error) {
	switch State(b.state.Load()) {
	case StateClosed:
		return true, nil
	case StateOpen:
		openedAt := b.openedAtNanos.Load()
		if b.now().UnixNano()-openedAt < int64(b.openTimeout) {
			return false, ErrBreakerOpen // still too early — fast-fail
		}
		// Time to probe: try to win the open→half-open transition. Both the
		// winner and the losers (who see it is already half-open) then race for
		// the single probe slot in admitHalfOpenProbe. Losers of this CAS do
		// NOT retry/spin: they just fall through to the trialInFlight CAS, where
		// they almost certainly lose and fast-fail — the desired behavior.
		b.state.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen))
		return b.admitHalfOpenProbe()
	case StateHalfOpen:
		return b.admitHalfOpenProbe()
	}
	return false, ErrBreakerOpen // unreachable; defensive default
}

// admitHalfOpenProbe grants the single half-open probe to exactly one caller via
// a CAS on trialInFlight; everyone else fast-fails. Report releases the flag.
func (b *Breaker) admitHalfOpenProbe() (bool, error) {
	if b.trialInFlight.CompareAndSwap(false, true) {
		return true, nil // the one and only probe
	}
	return false, ErrBreakerOpen
}

// Report records the outcome of an allowed request and drives state transitions.
// In half-open, success closes the circuit (failures reset) and failure reopens
// it (openedAtNanos refreshed); either way the probe slot is released last. In
// closed, consecutive failures are counted against the threshold and open the
// circuit when reached.
func (b *Breaker) Report(success bool) {
	switch State(b.state.Load()) {
	case StateHalfOpen:
		if success {
			b.failures.Store(0)
			b.state.Store(int32(StateClosed))
		} else {
			b.openedAtNanos.Store(b.now().UnixNano())
			b.state.Store(int32(StateOpen))
		}
		// Release the probe slot LAST, after the target state is set, so no
		// other goroutine can observe half-open with trialInFlight=false and
		// grab a "second" probe in the same window.
		b.trialInFlight.Store(false)
	case StateClosed:
		if success {
			// Reset the streak only if there is one (avoid a redundant write).
			if b.failures.Load() > 0 {
				b.failures.Store(0)
			}
			return
		}
		if b.failures.Add(1) >= b.failureThreshold {
			// CAS so we do not clobber a concurrent transition made by another
			// goroutine (e.g. one that already moved us to open/half-open).
			if b.state.CompareAndSwap(int32(StateClosed), int32(StateOpen)) {
				b.openedAtNanos.Store(b.now().UnixNano())
			}
		}
	case StateOpen:
		// Defensive no-op: with correct integration this is unreachable (open
		// requests fast-fail in Allow and never reach the backend, so Report is
		// not called). Deliberately do NOT touch openedAtNanos — a stray call
		// must not extend the open window indefinitely.
	}
}

// State returns the current breaker state (primarily for observability/tests).
func (b *Breaker) State() State {
	return State(b.state.Load())
}
