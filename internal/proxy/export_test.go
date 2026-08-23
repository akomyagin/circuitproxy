package proxy

import (
	"net/url"

	"github.com/akomyagin/circuitproxy/internal/breaker"
)

// newBalancerWithBreaker builds a single-backend balancer whose backend uses the
// given breaker config (allowing injected time via cfg.Now). Test-only accessor,
// not part of the package API.
func newBalancerWithBreaker(rawURL string, cfg breaker.Config) (*Balancer, *Backend, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	be := &Backend{URL: u}
	be.up.Store(true)
	be.cb = breaker.New(cfg)
	bal := &Balancer{backends: []*Backend{be}}
	return bal, be, nil
}
