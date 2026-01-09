package worker

import (
	"math"
	"math/rand"
	"time"
)

// RetryDelayFunc calculates the delay before the next retry.
// n is the retry count (1 for first retry, 2 for second, etc.)
type RetryDelayFunc func(n int, err error, task *Task) time.Duration

// Note: DefaultRetryDelayFunc is defined in config.go

// FixedRetryDelay returns a retry delay function with a fixed delay.
func FixedRetryDelay(delay time.Duration) RetryDelayFunc {
	return func(n int, err error, task *Task) time.Duration {
		return delay
	}
}

// LinearRetryDelay returns a retry delay function with linear backoff.
// Formula: base * n
func LinearRetryDelay(base time.Duration) RetryDelayFunc {
	return func(n int, err error, task *Task) time.Duration {
		return base * time.Duration(n)
	}
}

// ExponentialRetryDelay returns a retry delay function with configurable exponential backoff.
func ExponentialRetryDelay(base, maxDelay time.Duration) RetryDelayFunc {
	return func(n int, err error, task *Task) time.Duration {
		delay := float64(base) * math.Pow(2, float64(n-1))
		if delay > float64(maxDelay) {
			delay = float64(maxDelay)
		}
		// Add jitter (±10%)
		jitter := delay * 0.1 * (rand.Float64()*2 - 1)
		return time.Duration(delay + jitter)
	}
}
