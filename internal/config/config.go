// Package config parses and validates the CircuitProxy JSON configuration:
// listen address, backend list, health-check parameters, circuit-breaker
// thresholds and retry policy.
package config

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Config is the top-level proxy configuration decoded from a JSON file.
type Config struct {
	// Listen is the address the proxy binds to, e.g. ":8080".
	Listen string `json:"listen"`
	// MetricsListen is the OPTIONAL address of a separate expvar metrics
	// server (/debug/vars). Empty (the default) means metrics are disabled —
	// only breaker-transition logs remain. The metrics endpoint deliberately
	// lives on its OWN listener: the main listener proxies EVERY path to the
	// backends, so mounting /debug/vars there would silently hijack client
	// traffic to that path.
	MetricsListen string `json:"metrics_listen"`
	// Backends is the static list of upstream backend base URLs.
	Backends []string `json:"backends"`

	// HealthCheck controls active backend probing (Этап 2).
	HealthCheck HealthCheckConfig `json:"health_check"`
	// Breaker controls circuit-breaker thresholds (Этап 3).
	Breaker BreakerConfig `json:"breaker"`
	// Retry controls retry-with-backoff policy (Этап 4).
	Retry RetryConfig `json:"retry"`
}

// HealthCheckConfig configures active health checks.
type HealthCheckConfig struct {
	Path            string   `json:"path"`
	IntervalSeconds duration `json:"interval_seconds"`
	TimeoutSeconds  duration `json:"timeout_seconds"`
}

// BreakerConfig configures the per-backend circuit breaker.
type BreakerConfig struct {
	FailureThreshold   int32    `json:"failure_threshold"`
	OpenTimeoutSeconds duration `json:"open_timeout_seconds"`
}

// RetryConfig configures retry with backoff.
type RetryConfig struct {
	MaxRetries    int      `json:"max_retries"`
	BackoffBaseMs duration `json:"backoff_base_ms"`
}

// duration is a helper allowing plain numbers in JSON to decode into a value we
// can later interpret; kept minimal for the Этап 0 skeleton.
type duration int64

// Load reads, decodes and validates a config file: schema errors are reported
// with actionable messages BEFORE the server starts (see Config.Validate).
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config %q: %w", path, err)
	}
	return &cfg, nil
}

// Validate checks the decoded config schema and returns the FIRST problem
// found, naming the offending field and value so the operator can fix it. The
// check order is deterministic (listen, then backends, then time fields) so
// error messages are predictable.
//
// Deliberate validation boundary (Этап 5, Решение 4): only fields whose bad
// values would otherwise start a broken server are rejected here.
//   - listen: non-empty host:port with a NUMERIC port (empty host ":8080" ok).
//   - backends: non-empty list of absolute URLs (shared with proxy.NewBalancer
//     via ParseBackendURL, so both layers give one and the same verdict).
//   - health_check.interval_seconds / timeout_seconds and
//     breaker.open_timeout_seconds: must not be NEGATIVE. Zero/omitted stays
//     VALID — consumers (healthcheck.New, breaker.New) turn zero into their
//     working defaults, and that fallback path is regression-tested.
//   - retry.max_retries, retry.backoff_base_ms, breaker.failure_threshold are
//     NOT validated at all: their zero/negative semantics ("feature off" /
//     silent consumer-side normalization) were fixed on Этапы 3-4 and are
//     covered by tests; rejecting them here would break that contract.
//   - metrics_listen: not validated; empty means "metrics off", a bad
//     non-empty address surfaces at ListenAndServe (kept simple for MVP).
func (c *Config) Validate() error {
	if c.Listen == "" {
		return fmt.Errorf("listen address is empty")
	}
	_, port, err := net.SplitHostPort(c.Listen)
	if err != nil {
		return fmt.Errorf("listen address %q is not a valid host:port: %w", c.Listen, err)
	}
	if _, err := strconv.Atoi(port); err != nil {
		return fmt.Errorf("listen address %q: port %q is not a number", c.Listen, port)
	}
	if len(c.Backends) == 0 {
		return fmt.Errorf("no backends configured (backends must be a non-empty list)")
	}
	for i, raw := range c.Backends {
		if _, err := ParseBackendURL(raw); err != nil {
			return fmt.Errorf("backends[%d]: %w", i, err)
		}
	}
	if c.HealthCheck.IntervalSeconds < 0 {
		return fmt.Errorf("health_check.interval_seconds must not be negative, got %d", c.HealthCheck.IntervalSeconds)
	}
	if c.HealthCheck.TimeoutSeconds < 0 {
		return fmt.Errorf("health_check.timeout_seconds must not be negative, got %d", c.HealthCheck.TimeoutSeconds)
	}
	if c.Breaker.OpenTimeoutSeconds < 0 {
		return fmt.Errorf("breaker.open_timeout_seconds must not be negative, got %d", c.Breaker.OpenTimeoutSeconds)
	}
	return nil
}

// ParseBackendURL parses and validates a single backend base URL: it must
// parse and be absolute (scheme and host present). Shared by Config.Validate
// and proxy.NewBalancer so both validation layers produce one and the same
// error message instead of two drifting copies (Этап 5, Решение 4; config
// cannot import proxy, so the shared helper lives here).
func ParseBackendURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse backend URL %q: %w", raw, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("backend URL %q is not absolute (need scheme and host)", raw)
	}
	return u, nil
}

// Interval returns the health-check interval as a time.Duration.
func (h HealthCheckConfig) Interval() time.Duration {
	return time.Duration(h.IntervalSeconds) * time.Second
}

// Timeout returns the per-probe HTTP timeout as a time.Duration.
func (h HealthCheckConfig) Timeout() time.Duration {
	return time.Duration(h.TimeoutSeconds) * time.Second
}

// OpenTimeout returns the breaker open-state timeout as a time.Duration.
func (b BreakerConfig) OpenTimeout() time.Duration {
	return time.Duration(b.OpenTimeoutSeconds) * time.Second
}

// BackoffBase returns the retry backoff base as a time.Duration. Unlike the
// *_seconds fields above, backoff_base_ms is in MILLISECONDS.
func (r RetryConfig) BackoffBase() time.Duration {
	return time.Duration(r.BackoffBaseMs) * time.Millisecond
}
