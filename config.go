package worker

import (
	"crypto/tls"
	"time"
)

// Config holds the configuration for a Worker.
type Config struct {
	// Redis connection settings
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RedisTLS      *tls.Config // nil = no TLS

	// Worker settings
	Concurrency int            // Max parallel tasks (default: 1)
	Queues      map[string]int // Queue name -> priority weight (higher = more priority)

	// Retry settings
	MaxRetry       int                                           // Max retry attempts (default: 25)
	RetryDelayFunc func(n int, err error, t *Task) time.Duration // Custom backoff (optional)

	// Timeouts
	ShutdownTimeout time.Duration // Time to wait for graceful shutdown (default: 8s)

	// Logger (optional, defaults to standard logger)
	Logger Logger

	// Queue handler configuration (for config-driven workers)
	QueueHandlers map[string]*HandlerConfig // queue name -> handler config
	Defaults      DefaultsConfig
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		RedisAddr:       "localhost:6379",
		RedisDB:         0,
		Concurrency:     1,
		Queues:          map[string]int{"default": 1},
		MaxRetry:        25,
		ShutdownTimeout: 8 * time.Second,
		Logger:          NewStdLogger(),
	}
}

// DefaultRetryDelayFunc returns exponential backoff delay.
// Delay = min(retry^4 * 10s, 24h)
func DefaultRetryDelayFunc(n int, err error, t *Task) time.Duration {
	r := time.Duration(n)
	delay := r * r * r * r * 10 * time.Second
	if delay > 24*time.Hour {
		delay = 24 * time.Hour
	}
	return delay
}
