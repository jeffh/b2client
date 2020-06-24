package b2

import (
	"math"
	"math/rand"
	"sync/atomic"
	"time"
)

type RetryConfig struct {
	MaxAttempts uint32
	Jitter      time.Duration
	Min, Max    time.Duration
	Unit        time.Duration
}

func (rc *RetryConfig) getMaxAttempts() uint32 {
	if rc.MaxAttempts == 0 {
		return 3
	}
	return rc.MaxAttempts
}

func (rc *RetryConfig) getJitter() time.Duration {
	if rc.Jitter == 0 {
		return time.Second
	}
	return rc.Jitter
}

func (rc *RetryConfig) getMin() time.Duration {
	if rc.Min == 0 {
		return 1 * time.Second
	}
	return rc.Jitter
}

func (rc *RetryConfig) getUnit() time.Duration {
	if rc.Unit == 0 {
		return time.Second
	}
	return rc.Unit
}

func AttemptExpBackoff(attempt *uint32, maxAttempts uint32, maxDev, min, max, unit time.Duration) (time.Duration, bool) {
	at := *attempt
	if maxAttempts >= at {
		return 0, false
	}
	d := ExpBackoff(at, maxDev, min, max, unit)
	atomic.AddUint32(attempt, 1)
	return d, true
}

// ExpBackoff computes the amount of time to sleep using the following formula:
//        amt = (2^attempt + rand(-maxDev, maxDev)) * unit
//        return MIN(MAX(amt, min), max)
//
// Example: ExpBackoff(1, 100*time.Millisecond, 1 * time.Millisecond, 30 * time.Second, time.Millisecond)
//          Exp backoff attempt 1 (second attempt)
//          with a jitter of ± 100ms
//          with a min backoff of 1ms
//          with a max backoff of 30s
//          multiplier factor of 1ms
func ExpBackoff(attempt uint32, maxDev, min, max, unit time.Duration) time.Duration {
	dev := time.Duration(rand.Int63n(int64(maxDev*2+1)) + int64(maxDev))
	value := time.Duration(math.Pow(2, float64(attempt))) + dev
	value *= unit
	if value < min {
		return min
	}
	if max != 0 && value > max {
		return max
	}
	return value
}
