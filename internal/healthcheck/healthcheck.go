// Package healthcheck actively probes backends on an interval and marks them
// up/down so the balancer can exclude unavailable backends from rotation (Этап 2).
package healthcheck

import (
	"context"

	"github.com/akomyagin/circuitproxy/internal/config"
)

// Checker periodically probes backends and updates their liveness.
type Checker struct {
	cfg config.HealthCheckConfig
}

// New constructs a health Checker from config.
func New(cfg config.HealthCheckConfig) *Checker {
	return &Checker{cfg: cfg}
}

// Run starts the health-check loop until ctx is cancelled.
//
// TODO(Этап 2): time.Ticker loop; for each backend send GET cfg.Path with
// cfg.Timeout; mark backend up/down (atomic flag consumed by the balancer);
// stop cleanly on ctx.Done().
func (c *Checker) Run(ctx context.Context) {
	<-ctx.Done()
}
