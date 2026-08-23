package config

import (
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
