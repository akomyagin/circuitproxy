// Package healthcheck actively probes backends on an interval and marks them
// up/down so the balancer can exclude unavailable backends from rotation (Этап 2).
package healthcheck

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/akomyagin/circuitproxy/internal/config"
	"github.com/akomyagin/circuitproxy/internal/proxy"
)

// defaultInterval and defaultTimeout guard against a zero/omitted
// cfg.HealthCheck value: full config validation arrives in Этап 5, but a
// degenerate interval must not panic time.NewTicker and a degenerate timeout
// must not leave the http.Client to hang forever on a stalled backend.
const (
	defaultInterval = 10 * time.Second
	defaultTimeout  = 2 * time.Second
)

// Checker periodically probes backends and updates their liveness flags.
type Checker struct {
	cfg      config.HealthCheckConfig
	backends []*proxy.Backend
	client   *http.Client
}

// New constructs a health Checker for the given backends. The per-probe timeout
// comes from cfg.Timeout(), falling back to defaultTimeout when non-positive.
func New(cfg config.HealthCheckConfig, backends []*proxy.Backend) *Checker {
	timeout := cfg.Timeout()
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return &Checker{
		cfg:      cfg,
		backends: backends,
		client:   &http.Client{Timeout: timeout},
	}
}

// Run probes all backends once immediately, then on every cfg.Interval() tick
// (falling back to defaultInterval when non-positive), until ctx is
// cancelled. It blocks; callers run it in a goroutine.
func (c *Checker) Run(ctx context.Context) {
	c.probeAll(ctx)

	interval := c.cfg.Interval()
	if interval <= 0 {
		interval = defaultInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.probeAll(ctx)
		}
	}
}

// probeAll probes every backend sequentially and updates its liveness.
func (c *Checker) probeAll(ctx context.Context) {
	for _, be := range c.backends {
		c.probe(ctx, be)
	}
}

// probe sends one GET to the backend's health URL and toggles its liveness.
// Any transport error or non-2xx status marks the backend down; a 2xx marks it up.
func (c *Checker) probe(ctx context.Context, be *proxy.Backend) {
	healthURL := be.HealthURL(c.cfg.Path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		c.mark(be, false, healthURL, "build request", err)
		return
	}
	resp, err := c.client.Do(req)
	if err != nil {
		c.mark(be, false, healthURL, "request failed", err)
		return
	}
	defer resp.Body.Close()
	// Drain the body so the underlying connection can be reused by the
	// client's transport pool instead of being closed on every probe.
	_, _ = io.Copy(io.Discard, resp.Body)

	up := resp.StatusCode >= 200 && resp.StatusCode < 300
	if up {
		c.mark(be, true, healthURL, "", nil)
	} else {
		c.mark(be, false, healthURL, "non-2xx status", nil)
	}
}

// mark applies liveness and logs a transition only when it changes, to avoid
// per-tick log spam.
func (c *Checker) mark(be *proxy.Backend, up bool, healthURL, reason string, err error) {
	prev := be.IsUp()
	be.SetUp(up)
	if prev == up {
		return
	}
	if up {
		slog.Info("backend recovered", "backend", healthURL)
	} else {
		slog.Warn("backend marked down", "backend", healthURL, "reason", reason, "err", err)
	}
}
