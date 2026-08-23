package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akomyagin/circuitproxy/internal/breaker"
	"github.com/akomyagin/circuitproxy/internal/config"
)

// --- Handler(): retry with backoff (Этап 4) ---------------------------------

// failNTimesBackend starts a server whose first failN requests get a 500 and
// every later request a 200. calls counts real requests reaching the server.
func failNTimesBackend(t *testing.T, failN int64) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= failN {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, calls
}

// alwaysFail is a failN value no test can reach: the backend never recovers.
const alwaysFail = int64(1 << 30)

// retryBalancer builds a real balancer over urls with the given retry policy
// and a breaker that will not interfere (huge failure threshold): retry tests
// drive many consecutive failures and must not trip the circuit unless the
// test is explicitly about the breaker.
func retryBalancer(t *testing.T, rcfg config.RetryConfig, urls ...string) *Balancer {
	t.Helper()
	bal, err := NewBalancer(&config.Config{
		Listen:   ":0",
		Backends: urls,
		Breaker:  config.BreakerConfig{FailureThreshold: 1000},
		Retry:    rcfg,
	})
	if err != nil {
		t.Fatalf("NewBalancer(%v): unexpected error: %v", urls, err)
	}
	return bal
}

// recordingHandler wires bal's handler around a retryTransport whose sleep
// records requested delays instead of sleeping and whose jitter is zero, so
// backoff assertions are deterministic and tests spend no real time. The
// returned slice pointer is only safe for sequential requests.
func recordingHandler(bal *Balancer, rcfg config.RetryConfig) (http.Handler, *[]time.Duration) {
	rt := newRetryTransport(bal, rcfg)
	sleeps := &[]time.Duration{}
	rt.sleep = func(d time.Duration) { *sleeps = append(*sleeps, d) }
	rt.jitter = func(time.Duration) time.Duration { return 0 }
	return bal.handler(rt), sleeps
}

// Кейс 1: идемпотентный ретрай до успеха.
func TestRetry_IdempotentRetriesUntilSuccess(t *testing.T) {
	const failN = 2
	srv, calls := failNTimesBackend(t, failN)
	rcfg := config.RetryConfig{MaxRetries: failN, BackoffBaseMs: 1}
	h, _ := recordingHandler(retryBalancer(t, rcfg, srv.URL), rcfg)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 after %d retries", rec.Code, failN)
	}
	if got := calls.Load(); got != failN+1 {
		t.Fatalf("backend calls = %d, want %d (failN + final success)", got, failN+1)
	}
}

// Кейс 2: ретрай исчерпан — клиент видит последний реальный результат.
func TestRetry_ExhaustedReturnsLastResult(t *testing.T) {
	const maxRetries = 2
	rcfg := config.RetryConfig{MaxRetries: maxRetries, BackoffBaseMs: 1}

	t.Run("always 5xx: last 5xx verbatim", func(t *testing.T) {
		srv, calls := failNTimesBackend(t, alwaysFail)
		h, sleeps := recordingHandler(retryBalancer(t, rcfg, srv.URL), rcfg)

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500 (backend's last response verbatim)", rec.Code)
		}
		if got := calls.Load(); got != maxRetries+1 {
			t.Fatalf("backend calls = %d, want exactly %d", got, maxRetries+1)
		}
		if got := len(*sleeps); got != maxRetries {
			t.Fatalf("pauses = %d, want %d (only BETWEEN attempts)", got, maxRetries)
		}
	})

	t.Run("transport error: 502", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		target := srv.URL
		srv.Close() // connection refused on every attempt

		h, sleeps := recordingHandler(retryBalancer(t, rcfg, target), rcfg)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502 (last transport error)", rec.Code)
		}
		if got := len(*sleeps); got != maxRetries {
			t.Fatalf("pauses = %d, want %d (transport errors are retried too)", got, maxRetries)
		}
	})
}

// Кейс 3: POST/PATCH не ретраятся даже при доступных ретраях.
func TestRetry_NonIdempotentSingleAttempt(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPatch} {
		t.Run(method, func(t *testing.T) {
			srv, calls := failNTimesBackend(t, 2)
			rcfg := config.RetryConfig{MaxRetries: 3, BackoffBaseMs: 1}
			h, sleeps := recordingHandler(retryBalancer(t, rcfg, srv.URL), rcfg)

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(method, "/", strings.NewReader("payload")))

			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500 (the FIRST error, not retried)", rec.Code)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("backend calls = %d, want 1 (%s must not be retried)", got, method)
			}
			if got := len(*sleeps); got != 0 {
				t.Fatalf("pauses = %d, want 0 (single attempt, no backoff)", got)
			}
		})
	}
}

// Кейс 4: тело идемпотентного запроса переигрывается между попытками
// неповреждённым и доходит до успешной попытки.
func TestRetry_BodyReplayedAcrossAttempts(t *testing.T) {
	const payload = "idempotent body, bytes must survive \x00\x01\x02"
	const failN = 2
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			var mu sync.Mutex
			var bodies []string
			var calls atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("backend read body: %v", err)
				}
				mu.Lock()
				bodies = append(bodies, string(body))
				mu.Unlock()
				if calls.Add(1) <= failN {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(srv.Close)

			rcfg := config.RetryConfig{MaxRetries: failN, BackoffBaseMs: 1}
			h, _ := recordingHandler(retryBalancer(t, rcfg, srv.URL), rcfg)

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(method, "/", strings.NewReader(payload)))

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 after retries", rec.Code)
			}
			mu.Lock()
			defer mu.Unlock()
			if got := len(bodies); got != failN+1 {
				t.Fatalf("backend saw %d requests, want %d", got, failN+1)
			}
			for i, body := range bodies {
				if body != payload {
					t.Errorf("attempt %d saw body %q, want %q (replay corrupted)", i, body, payload)
				}
			}
		})
	}
}

// Кейс 5: backoff растёт экспоненциально, пауз ровно MaxRetries.
func TestRetry_BackoffGrowsExponentially(t *testing.T) {
	const maxRetries = 3
	const baseMs = 10
	srv, _ := failNTimesBackend(t, alwaysFail)
	rcfg := config.RetryConfig{MaxRetries: maxRetries, BackoffBaseMs: baseMs}
	h, sleeps := recordingHandler(retryBalancer(t, rcfg, srv.URL), rcfg)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if got := len(*sleeps); got != maxRetries {
		t.Fatalf("pauses = %d, want %d (between attempts only, none after the last)", got, maxRetries)
	}
	base := baseMs * time.Millisecond
	for n, got := range *sleeps {
		if want := base << uint(n); got != want {
			t.Errorf("pause %d = %v, want %v (base * 2^%d, jitter zeroed)", n, got, want, n)
		}
	}
}

// Кейс 6: backoff_base_ms — это миллисекунды, а не секунды.
func TestRetry_BackoffBaseIsMilliseconds(t *testing.T) {
	srv, _ := failNTimesBackend(t, alwaysFail)
	rcfg := config.RetryConfig{MaxRetries: 1, BackoffBaseMs: 50}
	h, sleeps := recordingHandler(retryBalancer(t, rcfg, srv.URL), rcfg)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if got := len(*sleeps); got != 1 {
		t.Fatalf("pauses = %d, want 1", got)
	}
	if got, want := (*sleeps)[0], 50*time.Millisecond; got != want {
		t.Fatalf("first pause = %v, want %v (ms, not seconds)", got, want)
	}
}

// Кейс 7: попытка не выполняется на backend'е с open circuit — он молча
// пропускается, запрос уходит на здоровый backend.
func TestRetry_SkipsOpenCircuitBackend(t *testing.T) {
	var callsA, callsB atomic.Int64
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callsA.Add(1)
	}))
	t.Cleanup(srvA.Close)
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callsB.Add(1)
	}))
	t.Cleanup(srvB.Close)

	clk := newClock()
	rcfg := config.RetryConfig{MaxRetries: 2, BackoffBaseMs: 1}
	bal, err := newBalancerWithBreakers(
		[]string{srvA.URL, srvB.URL},
		breaker.Config{FailureThreshold: 1, OpenTimeout: time.Hour, Now: clk.now},
		rcfg,
	)
	if err != nil {
		t.Fatalf("newBalancerWithBreakers: %v", err)
	}
	// Drive A's circuit open (threshold 1: a single failure opens it); the
	// hour-long OpenTimeout on an injected clock keeps it open for the test.
	bal.backends[0].Report(false)

	h, _ := recordingHandler(bal, rcfg)
	const n = 4
	for i := 0; i < n; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200 via healthy backend B", i, rec.Code)
		}
	}
	if got := callsA.Load(); got != 0 {
		t.Fatalf("open-circuit backend A got %d real requests, want 0", got)
	}
	if got := callsB.Load(); got != n {
		t.Fatalf("healthy backend B got %d real requests, want %d", got, n)
	}
}

// Кейс 12 (найдено на ревью): при обычном 5xx-сбое (не open circuit) повтор
// уходит на СЛЕДУЮЩИЙ backend по round-robin, а не повторяет тот же.
func TestRetry_RetriesOnDifferentBackend(t *testing.T) {
	var callsA, callsB atomic.Int64
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callsA.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srvA.Close)
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callsB.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srvB.Close)

	rcfg := config.RetryConfig{MaxRetries: 1, BackoffBaseMs: 1}
	// Backend order follows the urls given here: A first, B second, so the
	// round-robin's first pick is A and the retry's is B.
	h, _ := recordingHandler(retryBalancer(t, rcfg, srvA.URL, srvB.URL), rcfg)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (retry reached healthy backend B)", rec.Code)
	}
	if got := callsA.Load(); got != 1 {
		t.Fatalf("backend A calls = %d, want 1 (first attempt only)", got)
	}
	if got := callsB.Load(); got != 1 {
		t.Fatalf("backend B calls = %d, want 1 (retry landed here)", got)
	}
}

// Кейс 8: все backend'ы недоступны или с open circuit → 503 fast-fail,
// без паники и зависания.
func TestRetry_NoAvailableBackends503(t *testing.T) {
	rcfg := config.RetryConfig{MaxRetries: 2, BackoffBaseMs: 1}

	t.Run("all down", func(t *testing.T) {
		srvA, callsA := failNTimesBackend(t, 0)
		srvB, callsB := failNTimesBackend(t, 0)
		bal := retryBalancer(t, rcfg, srvA.URL, srvB.URL)
		for _, be := range bal.Backends() {
			be.SetUp(false)
		}
		h, _ := recordingHandler(bal, rcfg)

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503 (not 502: nothing was contacted)", rec.Code)
		}
		if got := callsA.Load() + callsB.Load(); got != 0 {
			t.Fatalf("backends got %d real requests, want 0", got)
		}
	})

	t.Run("all circuits open", func(t *testing.T) {
		srvA, callsA := failNTimesBackend(t, 0)
		srvB, callsB := failNTimesBackend(t, 0)
		bal, err := newBalancerWithBreakers(
			[]string{srvA.URL, srvB.URL},
			breaker.Config{FailureThreshold: 1, OpenTimeout: time.Hour, Now: newClock().now},
			rcfg,
		)
		if err != nil {
			t.Fatalf("newBalancerWithBreakers: %v", err)
		}
		for _, be := range bal.backends {
			be.Report(false) // threshold 1: opens each circuit
		}
		h, _ := recordingHandler(bal, rcfg)

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "circuit breaker open") {
			t.Errorf("body = %q, want to contain %q", rec.Body.String(), "circuit breaker open")
		}
		if got := callsA.Load() + callsB.Load(); got != 0 {
			t.Fatalf("backends got %d real requests, want 0 (fast-fail)", got)
		}
	})
}

// Кейс 9: max_retries=0 — «ретраи выключены», ровно одна попытка;
// отрицательное значение нормализуется к 0, а не к скрытому дефолту.
func TestRetry_MaxRetriesZeroOrNegativeSingleAttempt(t *testing.T) {
	for _, maxRetries := range []int{0, -1} {
		t.Run(map[int]string{0: "zero", -1: "negative"}[maxRetries], func(t *testing.T) {
			srv, calls := failNTimesBackend(t, 1) // would succeed on a 2nd attempt
			rcfg := config.RetryConfig{MaxRetries: maxRetries, BackoffBaseMs: 50}
			h, sleeps := recordingHandler(retryBalancer(t, rcfg, srv.URL), rcfg)

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status = %d, want 500 (retries are OFF, first error returned)", rec.Code)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("backend calls = %d, want exactly 1", got)
			}
			if got := len(*sleeps); got != 0 {
				t.Fatalf("pauses = %d, want 0", got)
			}
		})
	}
}

// Кейс 10: backoff_base_ms=0 при включённых ретраях нормализуется к дефолту —
// паузы ненулевые и растут, а не 0,0.
func TestRetry_ZeroBackoffBaseNormalized(t *testing.T) {
	const maxRetries = 2
	srv, _ := failNTimesBackend(t, alwaysFail)
	rcfg := config.RetryConfig{MaxRetries: maxRetries, BackoffBaseMs: 0}
	h, sleeps := recordingHandler(retryBalancer(t, rcfg, srv.URL), rcfg)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if got := len(*sleeps); got != maxRetries {
		t.Fatalf("pauses = %d, want %d", got, maxRetries)
	}
	for n, d := range *sleeps {
		if d <= 0 {
			t.Fatalf("pause %d = %v, want > 0 (zero base must be normalized)", n, d)
		}
	}
	if got, want := (*sleeps)[0], defaultBackoffBase; got != want {
		t.Errorf("first pause = %v, want normalized default %v", got, want)
	}
	if got, want := (*sleeps)[1], 2*(*sleeps)[0]; got != want {
		t.Errorf("second pause = %v, want %v (backoff must still grow)", got, want)
	}
}

// Конкурентный smoke: общий retryTransport (с продовым потокобезопасным
// jitter) обслуживает параллельные ретраящиеся запросы под -race. sleep
// подменён на no-op без разделяемого состояния.
func TestRetry_ConcurrentRequests(t *testing.T) {
	const goroutines = 16
	const perGoroutine = 5
	const maxRetries = 2
	srv, calls := failNTimesBackend(t, alwaysFail)
	rcfg := config.RetryConfig{MaxRetries: maxRetries, BackoffBaseMs: 1}
	bal := retryBalancer(t, rcfg, srv.URL)
	rt := newRetryTransport(bal, rcfg)
	rt.sleep = func(time.Duration) {} // no real sleeping; keep default jitter
	h := bal.handler(rt)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < perGoroutine; j++ {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
				if rec.Code != http.StatusInternalServerError {
					t.Errorf("status = %d, want 500", rec.Code)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if got, want := calls.Load(), int64(goroutines*perGoroutine*(maxRetries+1)); got != want {
		t.Fatalf("backend calls = %d, want %d (every request exhausts its attempts)", got, want)
	}
}
