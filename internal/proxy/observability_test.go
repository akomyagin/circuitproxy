package proxy

import (
	"bytes"
	"encoding/json"
	"expvar"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/akomyagin/circuitproxy/internal/config"
)

// --- Кейс 23: сквозной slog-тест «переход breaker'а → лог с backend-меткой» --

// captureSlog redirects the default slog logger into a buffer (JSON, one
// record per line) for the duration of the test.
func captureSlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

func TestBreakerTransitionLoggedWithBackendLabel(t *testing.T) {
	buf := captureSlog(t)

	srv, _ := failNTimesBackend(t, alwaysFail)
	const threshold = 2
	bal, err := NewBalancer(&config.Config{
		Listen:   ":0",
		Backends: []string{srv.URL},
		// A huge open timeout keeps the circuit open after the transition —
		// exactly one closed->open must be logged.
		Breaker: config.BreakerConfig{FailureThreshold: threshold, OpenTimeoutSeconds: 3600},
	})
	if err != nil {
		t.Fatalf("NewBalancer: %v", err)
	}

	h, _ := recordingHandler(bal, config.RetryConfig{})
	for i := 0; i < threshold; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	}

	var transitions int
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec struct {
			Msg     string `json:"msg"`
			Backend string `json:"backend"`
			From    string `json:"from"`
			To      string `json:"to"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", line, err)
		}
		if rec.Msg != "circuit breaker transition" {
			continue
		}
		transitions++
		if rec.Backend != srv.URL {
			t.Errorf("transition log backend = %q, want %q", rec.Backend, srv.URL)
		}
		if rec.From != "closed" || rec.To != "open" {
			t.Errorf("transition log from/to = %q/%q, want closed/open", rec.From, rec.To)
		}
	}
	if transitions != 1 {
		t.Fatalf("transition log records = %d, want exactly 1 (log output:\n%s)", transitions, buf.String())
	}
}

// --- Кейсы 26-30: expvar-счётчики ---------------------------------------------

// counters snapshots all four expvar counters. The counters are process-global,
// so tests assert DELTAS between snapshots, never absolute values.
type counters struct {
	requests, errors, fastFail, retries int64
}

func snapshotCounters(t *testing.T) counters {
	t.Helper()
	get := func(name string) int64 {
		v, ok := expvar.Get(name).(*expvar.Int)
		if !ok {
			t.Fatalf("expvar %q is not registered as *expvar.Int", name)
		}
		return v.Value()
	}
	return counters{
		requests: get("requests_total"),
		errors:   get("errors_total"),
		fastFail: get("fast_fail_total"),
		retries:  get("retries_total"),
	}
}

// assertDeltas runs one request through h and checks the counter deltas.
func assertDeltas(t *testing.T, h http.Handler, wantStatus int, want counters) {
	t.Helper()
	before := snapshotCounters(t)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d", rec.Code, wantStatus)
	}
	after := snapshotCounters(t)
	got := counters{
		requests: after.requests - before.requests,
		errors:   after.errors - before.errors,
		fastFail: after.fastFail - before.fastFail,
		retries:  after.retries - before.retries,
	}
	if got != want {
		t.Fatalf("counter deltas {requests errors fastFail retries} = %+v, want %+v", got, want)
	}
}

// Кейсы 26 и 30: успешный запрос — только requests_total растёт; errors_total
// и fast_fail_total не срабатывают ложно.
func TestMetrics_SuccessOnlyCountsRequest(t *testing.T) {
	srv, _ := failNTimesBackend(t, 0) // always 200
	rcfg := config.RetryConfig{MaxRetries: 2, BackoffBaseMs: 1}
	h, _ := recordingHandler(retryBalancer(t, rcfg, srv.URL), rcfg)

	assertDeltas(t, h, http.StatusOK, counters{requests: 1})
}

// Кейс 27: исчерпанные ретраи на вечном 5xx — errors_total +1 (а не по числу
// попыток), retries_total — по числу реальных повторов.
func TestMetrics_ExhaustedRetriesCountOneError(t *testing.T) {
	const maxRetries = 2
	srv, _ := failNTimesBackend(t, alwaysFail)
	rcfg := config.RetryConfig{MaxRetries: maxRetries, BackoffBaseMs: 1}
	h, _ := recordingHandler(retryBalancer(t, rcfg, srv.URL), rcfg)

	assertDeltas(t, h, http.StatusInternalServerError,
		counters{requests: 1, errors: 1, retries: maxRetries})
}

// Кейс 28: все backend'ы down — fast_fail_total +1, errors_total не растёт.
func TestMetrics_FastFailWhenNoBackends(t *testing.T) {
	srv, _ := failNTimesBackend(t, 0)
	rcfg := config.RetryConfig{MaxRetries: 2, BackoffBaseMs: 1}
	bal := retryBalancer(t, rcfg, srv.URL)
	for _, be := range bal.Backends() {
		be.SetUp(false)
	}
	h, _ := recordingHandler(bal, rcfg)

	assertDeltas(t, h, http.StatusServiceUnavailable, counters{requests: 1, fastFail: 1})
}

// Кейс 29: N ошибок, затем успех — retries_total += N (первая попытка — не
// retry), errors_total не растёт (финал успешен).
func TestMetrics_RetriesCountedPerExtraAttempt(t *testing.T) {
	const failN = 2
	srv, _ := failNTimesBackend(t, failN)
	rcfg := config.RetryConfig{MaxRetries: failN, BackoffBaseMs: 1}
	h, _ := recordingHandler(retryBalancer(t, rcfg, srv.URL), rcfg)

	assertDeltas(t, h, http.StatusOK, counters{requests: 1, retries: failN})
}
