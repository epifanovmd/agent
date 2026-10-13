// Package backoff — экспоненциальная задержка повторов с джиттером.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Policy — границы задержки: Min·2^attempt, не больше Max, джиттер [50%, 100%].
type Policy struct {
	Min time.Duration
	Max time.Duration
}

// Default — переподключение: от 1 с до 60 с.
var Default = Policy{Min: time.Second, Max: time.Minute}

// Delay — задержка перед попыткой номер attempt (с 0).
func (p Policy) Delay(attempt int) time.Duration {
	d := p.Min
	for i := 0; i < attempt && d < p.Max; i++ {
		d *= 2
	}
	if d > p.Max {
		d = p.Max
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
