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
	b := &Breaker{
		failureThreshold: cfg.FailureThreshold,
		openTimeout:      cfg.OpenTimeout,
		now:              now,
	}
	b.state.Store(int32(StateClosed))
	return b
}

// Allow reports whether a request may proceed to the backend. In half-open,
// exactly one caller receives true; the rest fast-fail with ErrBreakerOpen.
//
// TODO(Этап 3): implement the full state machine per docs/TECHNICAL_PLAN.md:
//   - closed: allow; open→half-open via CAS on state when OpenTimeout elapsed;
//   - half-open: admit exactly one probe via trialInFlight.CompareAndSwap.
func (b *Breaker) Allow() (bool, error) {
	// Skeleton: always allow until Этап 3 lands the real logic.
	return true, nil
}

// Report records the outcome of an allowed request and drives state transitions.
//
// TODO(Этап 3): success in half-open → closed (reset failures); failure → open
// (refresh openedAtNanos). In closed, count consecutive failures against the
// threshold. Always clear trialInFlight when concluding a half-open probe.
func (b *Breaker) Report(success bool) {
	_ = success
}

// State returns the current breaker state (primarily for observability/tests).
func (b *Breaker) State() State {
	return State(b.state.Load())
}
