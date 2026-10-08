package backoff

import (
	"testing"
	"time"
)

func TestDelayBounds(t *testing.T) {
	p := Policy{Min: time.Second, Max: 8 * time.Second}
	for attempt, max := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second, 8 * time.Second} {
		for range 50 {
			d := p.Delay(attempt)
			if d < max/2 || d > max {
				t.Fatalf("attempt %d: %v вне [%v, %v]", attempt, d, max/2, max)
			}
		}
	}
}
