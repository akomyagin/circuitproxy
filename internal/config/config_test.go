package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// BackoffBase must interpret backoff_base_ms as MILLISECONDS: the sibling
// accessors (Interval/Timeout/OpenTimeout) multiply by time.Second, and a
// blindly copied unit here would inflate every pause 1000x (50ms -> 50s).
func TestRetryConfig_BackoffBase_Milliseconds(t *testing.T) {
	cases := []struct {
		ms   duration
		want time.Duration
	}{
		{0, 0},
		{1, time.Millisecond},
		{50, 50 * time.Millisecond},
		{1500, 1500 * time.Millisecond},
	}
	for _, c := range cases {
		if got := (RetryConfig{BackoffBaseMs: c.ms}).BackoffBase(); got != c.want {
			t.Errorf("BackoffBase() with BackoffBaseMs=%d = %v, want %v", c.ms, got, c.want)
		}
	}
}

// --- Validate (Этап 5) -------------------------------------------------------

// validConfig returns a full valid config equivalent to config.example.json;
// table cases mutate one field at a time.
func validConfig() Config {
	return Config{
		Listen:        ":8080",
		MetricsListen: "127.0.0.1:9090",
		Backends: []string{
			"http://127.0.0.1:9001",
			"http://127.0.0.1:9002",
		},
		HealthCheck: HealthCheckConfig{Path: "/healthz", IntervalSeconds: 2, TimeoutSeconds: 1},
		Breaker:     BreakerConfig{FailureThreshold: 5, OpenTimeoutSeconds: 10},
		Retry:       RetryConfig{MaxRetries: 2, BackoffBaseMs: 50},
	}
}

func TestConfig_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string // substring of the error; "" = must be valid
	}{
		// Кейс 1: полный валидный конфиг.
		{"valid full config", func(c *Config) {}, ""},

		// Кейсы 2-4: listen.
		{"empty listen", func(c *Config) { c.Listen = "" }, "listen"},
		{"unparsable listen", func(c *Config) { c.Listen = "::::" }, "::::"},
		{"non-numeric port", func(c *Config) { c.Listen = "host:notaport" }, "notaport"},
		{"listen without host is valid", func(c *Config) { c.Listen = ":8080" }, ""},

		// Кейсы 5-7: backends.
		{"empty backends", func(c *Config) { c.Backends = nil }, "no backends"},
		{"relative backend URL", func(c *Config) { c.Backends = []string{"/foo"} }, "/foo"},
		{"unparsable backend URL", func(c *Config) { c.Backends = []string{"://x"} }, "://x"},
		{"scheme-less backend URL", func(c *Config) { c.Backends = []string{"not a url"} }, "not a url"},
		{
			"bad backend among good names its index",
			func(c *Config) { c.Backends = []string{"http://ok:1", "/bad"} },
			"backends[1]",
		},
		{"all backends absolute", func(c *Config) {
			c.Backends = []string{"http://a:1", "https://b.example.com"}
		}, ""},

		// Кейсы 8-10: временные поля — отрицательное запрещено, ноль разрешён.
		{
			"negative health interval",
			func(c *Config) { c.HealthCheck.IntervalSeconds = -1 },
			"health_check.interval_seconds",
		},
		{"zero health interval is valid", func(c *Config) { c.HealthCheck.IntervalSeconds = 0 }, ""},
		{
			"negative health timeout",
			func(c *Config) { c.HealthCheck.TimeoutSeconds = -1 },
			"health_check.timeout_seconds",
		},
		{"zero health timeout is valid", func(c *Config) { c.HealthCheck.TimeoutSeconds = 0 }, ""},
		{
			"negative breaker open timeout",
			func(c *Config) { c.Breaker.OpenTimeoutSeconds = -1 },
			"breaker.open_timeout_seconds",
		},
		{"zero breaker open timeout is valid", func(c *Config) { c.Breaker.OpenTimeoutSeconds = 0 }, ""},

		// Кейсы 11-13: retry и failure_threshold сознательно НЕ валидируются
		// (Решение 4) — их ноль/отрицательное осмысленно нормализуется
		// потребителями (Этапы 3-4).
		{"negative max_retries is valid", func(c *Config) { c.Retry.MaxRetries = -1 }, ""},
		{"zero backoff base is valid", func(c *Config) { c.Retry.BackoffBaseMs = 0 }, ""},
		{"negative backoff base is valid", func(c *Config) { c.Retry.BackoffBaseMs = -10 }, ""},
		{"zero failure threshold is valid", func(c *Config) { c.Breaker.FailureThreshold = 0 }, ""},
		{"negative failure threshold is valid", func(c *Config) { c.Breaker.FailureThreshold = -5 }, ""},

		// Кейс 15: metrics_listen опционален, пустое значение валидно.
		{"omitted metrics_listen is valid", func(c *Config) { c.MetricsListen = "" }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want error mentioning %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() = %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

// --- Load (Этап 5): end-to-end through a file on disk ------------------------

// writeConfigFile writes content to a temp file and returns its path.
func writeConfigFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

func TestLoad_ValidFile(t *testing.T) {
	path := writeConfigFile(t, `{
		"listen": ":8080",
		"metrics_listen": "127.0.0.1:9090",
		"backends": ["http://127.0.0.1:9001"],
		"health_check": {"path": "/healthz", "interval_seconds": 2, "timeout_seconds": 1},
		"breaker": {"failure_threshold": 5, "open_timeout_seconds": 10},
		"retry": {"max_retries": 2, "backoff_base_ms": 50}
	}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}
	if cfg.MetricsListen != "127.0.0.1:9090" {
		t.Errorf("MetricsListen = %q, want %q", cfg.MetricsListen, "127.0.0.1:9090")
	}
}

// Кейс 15: metrics_listen omitted — метрики выключены (пустая строка).
func TestLoad_OmittedMetricsListenDisablesMetrics(t *testing.T) {
	path := writeConfigFile(t, `{"listen": ":8080", "backends": ["http://127.0.0.1:9001"]}`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}
	if cfg.MetricsListen != "" {
		t.Errorf("MetricsListen = %q, want empty (metrics off by default)", cfg.MetricsListen)
	}
}

// Load must reject an invalid schema (validation is wired in, not just Validate alone).
func TestLoad_InvalidSchemaRejected(t *testing.T) {
	path := writeConfigFile(t, `{"listen": ":8080", "backends": []}`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("Load() with empty backends = nil error, want validation error")
	}
	if !strings.Contains(err.Error(), "no backends") {
		t.Fatalf("Load() error = %q, want it to mention %q", err, "no backends")
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("Load() error = %q, want it to mention the file path %q", err, path)
	}
}

// Кейс 14: несуществующий файл и битый JSON — существующее поведение сохранено.
func TestLoad_MissingFileOrBrokenJSON(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("Load() on a missing file = nil error, want error")
	}
	if _, err := Load(writeConfigFile(t, `{not json`)); err == nil {
		t.Fatal("Load() on broken JSON = nil error, want error")
	}
}

// Кейс 32: config.example.json в корне репозитория загружается и проходит валидацию.
func TestLoad_ExampleConfig(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "config.example.json"))
	if err != nil {
		t.Fatalf("Load(config.example.json) = %v, want nil", err)
	}
	if len(cfg.Backends) == 0 {
		t.Fatal("example config has no backends")
	}
}
