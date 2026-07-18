// Package config parses and validates the CircuitProxy JSON configuration:
// listen address, backend list, health-check parameters, circuit-breaker
// thresholds and retry policy.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Config is the top-level proxy configuration decoded from a JSON file.
type Config struct {
	// Listen is the address the proxy binds to, e.g. ":8080".
	Listen string `json:"listen"`
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

// Load reads and decodes a config file. Validation is added in Этап 5.
//
// TODO(Этап 5): add full schema validation with actionable error messages
// (non-empty backends, valid listen addr, positive thresholds/timeouts).
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	return &cfg, nil
}

// Interval returns the health-check interval as a time.Duration.
func (h HealthCheckConfig) Interval() time.Duration {
	return time.Duration(h.IntervalSeconds) * time.Second
}
