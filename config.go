package worker

import (
	"crypto/tls"
	"time"
)

// Config holds the configuration for a Worker.
type Config struct {
	// Server Bootstrap (required for dynamic configuration)
	ServerURL  string   // Required: runqy-server URL (e.g., "https://server.example.com")
	APIKey     string   // Required: Authentication key for server
	Queue      string   // DEPRECATED: Single queue name (backward compat, set to first queue)
	QueueNames []string // List of queue names to bootstrap and listen on
	Version    string   // Worker version (for registration metadata)

	// Bootstrap Retry
	BootstrapRetries    int           // Number of retries for server contact (default: 3)
	BootstrapRetryDelay time.Duration // Delay between retries (default: 5s)

	// Git Authentication
	GitSSHKey string // Optional: Path to SSH private key file
	GitToken  string // Optional: Personal access token or password

	// Deployment
	DeploymentDir         string // Directory for code deployment (default: "./deployment")
	UseSystemSitePackages bool   // Use --system-site-packages for venv (default: true)

	// Redis connection settings (populated by bootstrap, not required in config)
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	RedisTLS      *tls.Config // nil = no TLS

	// Worker settings
	Concurrency int            // Max parallel tasks (default: 1)
	Queues      map[string]int // Queue name -> priority weight (populated by bootstrap)

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
		// Bootstrap defaults
		BootstrapRetries:    3,
		BootstrapRetryDelay: 5 * time.Second,
		DeploymentDir:       "./deployment",

		// Deployment defaults
		UseSystemSitePackages: true, // Default: inherit system packages (e.g., PyTorch)

		// Redis defaults (will be overwritten by bootstrap)
		RedisAddr: "localhost:6379",
		RedisDB:   0,

		// Worker defaults
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
