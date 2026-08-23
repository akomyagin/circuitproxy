package proxy

import (
	"net/url"

	"github.com/akomyagin/circuitproxy/internal/breaker"
	"github.com/akomyagin/circuitproxy/internal/config"
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

// newBalancerWithBreakers builds a multi-backend balancer where every backend
// uses the given breaker config (allowing injected time via bcfg.Now) and the
// balancer carries the given retry policy. Test-only accessor for Этап 4
// retry+breaker scenarios; backend order follows rawURLs.
func newBalancerWithBreakers(rawURLs []string, bcfg breaker.Config, rcfg config.RetryConfig) (*Balancer, error) {
	backends := make([]*Backend, 0, len(rawURLs))
	for _, raw := range rawURLs {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, err
		}
		be := &Backend{URL: u}
		be.up.Store(true)
		be.cb = breaker.New(bcfg)
		backends = append(backends, be)
	}
	return &Balancer{backends: backends, retry: rcfg}, nil
}
