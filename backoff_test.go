package stomp

import (
	"math"
	"testing"
	"time"
)

func TestBackoffDelayCeilingIsMinOfMaxAndExponential(t *testing.T) {
	for _, tt := range []struct {
		n         int
		base, max time.Duration
		want      time.Duration
	}{
		{0, 100 * time.Millisecond, 30 * time.Second, 100 * time.Millisecond},
		{3, 100 * time.Millisecond, 30 * time.Second, 800 * time.Millisecond},
		{20, 100 * time.Millisecond, 30 * time.Second, 30 * time.Second},
		{62, 1, math.MaxInt64, 1 << 62},
		{63, 1, math.MaxInt64, math.MaxInt64},
		{200, 1, math.MaxInt64, math.MaxInt64},
	} {
		if got := backoffDelay(tt.n, tt.base, tt.max, 1); got != tt.want {
			t.Errorf("backoffDelay(%d, %v, %v, 1) = %d; want %d", tt.n, tt.base, tt.max, got, tt.want)
		}
	}
}
