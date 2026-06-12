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

	// Task lifecycle fallback defaults. The per-task values stamped by the server (read from
	// the task hash at dequeue) take priority; these apply only when a task lacks the field
	// (old tasks, or tasks enqueued outside the server). 0 = disabled / no expiry.
	TTLCompleted   time.Duration // TTL for successfully completed task keys (default: 24h)
	TTLArchived    time.Duration // TTL for failed/archived task keys (default: 72h)
	PendingTimeout time.Duration // Max age in :pending before archive-on-dequeue (default: 0 = disabled)
	ActiveTimeout  time.Duration // Max execution time before a retriable timeout (default: 0 = disabled)

	// Lease settings
	// A task's lease is set at dequeue and periodically renewed by the worker while it
	// is being processed. If the worker dies, the lease expires and the lease recoverer
	// reclaims the task. LeaseDuration must be comfortably larger than LeaseExtendInterval.
	LeaseDuration       time.Duration // How long a lease is valid before it must be renewed (default: 2m)
	LeaseExtendInterval time.Duration // How often in-flight leases are renewed (default: LeaseDuration/2)

	// Logger (optional, defaults to standard logger)
	Logger Logger

	// Process recovery
	Recovery RecoveryConfig // Auto-recovery settings for crashed processes

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
		TTLCompleted:    24 * time.Hour,
		TTLArchived:     72 * time.Hour,
		PendingTimeout:  0,
		ActiveTimeout:   0,

		// Lease defaults
		LeaseDuration:       2 * time.Minute,
		LeaseExtendInterval: 1 * time.Minute,

		Logger:          NewStdLogger(),
		Recovery:        DefaultRecoveryConfig(),
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
