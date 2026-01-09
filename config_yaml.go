package worker

import (
	"crypto/tls"
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// YAMLConfig represents the YAML configuration file structure.
type YAMLConfig struct {
	Redis    RedisConfig              `yaml:"redis"`
	Worker   WorkerConfig             `yaml:"worker"`
	Queues   map[string]*QueueConfig  `yaml:"queues"`
	Retry    RetryConfig              `yaml:"retry"`
	Defaults DefaultsConfig           `yaml:"defaults"`
}

// QueueConfig defines a queue with its priority and handler.
type QueueConfig struct {
	Priority int            `yaml:"priority"` // Weight for queue processing (higher = more priority)
	Handler  *HandlerConfig `yaml:"handler"`  // How to handle tasks from this queue
}

// DefaultsConfig provides default values for handlers.
type DefaultsConfig struct {
	HTTP HTTPDefaultsConfig `yaml:"http"`
}

// HTTPDefaultsConfig provides default values for HTTP handlers.
type HTTPDefaultsConfig struct {
	Timeout string            `yaml:"timeout"`
	RetryOn []int             `yaml:"retry_on"`
	FailOn  []int             `yaml:"fail_on"`
	Headers map[string]string `yaml:"headers"`
}

// HandlerConfig defines how to handle a specific task type.
type HandlerConfig struct {
	Type    string            `yaml:"type"`    // http (default), log
	URL     string            `yaml:"url"`     // HTTP endpoint URL
	Method  string            `yaml:"method"`  // HTTP method (default: POST)
	Timeout string            `yaml:"timeout"` // Request timeout
	Headers map[string]string `yaml:"headers"` // Custom headers
	Auth    AuthConfig        `yaml:"auth"`    // Authentication config
	RetryOn []int             `yaml:"retry_on"` // HTTP status codes to retry
	FailOn  []int             `yaml:"fail_on"`  // HTTP status codes for permanent failure
}

// AuthConfig for HTTP authentication.
type AuthConfig struct {
	Type     string `yaml:"type"`     // basic, bearer, api_key
	Username string `yaml:"username"` // For basic auth
	Password string `yaml:"password"` // For basic auth
	Token    string `yaml:"token"`    // For bearer auth
	Header   string `yaml:"header"`   // For api_key auth (header name)
	Key      string `yaml:"key"`      // For api_key auth (key value)
}

// RedisConfig holds Redis connection settings from YAML.
type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	TLS      bool   `yaml:"tls"`
}

// WorkerConfig holds worker settings from YAML.
type WorkerConfig struct {
	Concurrency     int    `yaml:"concurrency"`
	ShutdownTimeout string `yaml:"shutdown_timeout"`
}

// RetryConfig holds retry settings from YAML.
type RetryConfig struct {
	MaxRetry int `yaml:"max_retry"`
}

// LoadConfig loads configuration from a YAML file.
// If path is empty, it defaults to "config.yml" in the current directory.
func LoadConfig(path string) (*Config, error) {
	if path == "" {
		path = "config.yml"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Expand environment variables
	data = expandEnvVars(data)

	var yc YAMLConfig
	if err := yaml.Unmarshal(data, &yc); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	cfg := toWorkerConfig(&yc)
	return &cfg, nil
}

// expandEnvVars replaces ${VAR} and ${VAR:-default} patterns with environment variable values.
func expandEnvVars(data []byte) []byte {
	// Pattern matches ${VAR} or ${VAR:-default}
	re := regexp.MustCompile(`\$\{([^}:\s]+)(?::-([^}]*))?\}`)

	return re.ReplaceAllFunc(data, func(match []byte) []byte {
		submatch := re.FindSubmatch(match)
		if len(submatch) < 2 {
			return match
		}

		varName := string(submatch[1])
		value := os.Getenv(varName)

		// If env var is not set and we have a default, use it
		if value == "" && len(submatch) >= 3 && len(submatch[2]) > 0 {
			value = string(submatch[2])
		}

		return []byte(value)
	})
}

// toWorkerConfig converts YAMLConfig to the internal Config struct.
func toWorkerConfig(yc *YAMLConfig) Config {
	cfg := DefaultConfig()

	// Redis settings
	if yc.Redis.Addr != "" {
		cfg.RedisAddr = yc.Redis.Addr
	}
	cfg.RedisPassword = yc.Redis.Password
	cfg.RedisDB = yc.Redis.DB

	// TLS is not fully configurable via YAML (just enable/disable)
	// For custom TLS config, use programmatic configuration
	if yc.Redis.TLS {
		cfg.RedisTLS = &defaultTLSConfig
	}

	// Worker settings
	if yc.Worker.Concurrency > 0 {
		cfg.Concurrency = yc.Worker.Concurrency
	}
	if yc.Worker.ShutdownTimeout != "" {
		if d, err := time.ParseDuration(yc.Worker.ShutdownTimeout); err == nil {
			cfg.ShutdownTimeout = d
		}
	}

	// Queues - extract priorities and handlers
	if len(yc.Queues) > 0 {
		cfg.Queues = make(map[string]int)
		cfg.QueueHandlers = make(map[string]*HandlerConfig)
		for name, qc := range yc.Queues {
			if qc == nil {
				continue
			}
			priority := qc.Priority
			if priority <= 0 {
				priority = 1
			}
			cfg.Queues[name] = priority
			if qc.Handler != nil {
				cfg.QueueHandlers[name] = qc.Handler
			}
		}
	}

	// Retry settings
	if yc.Retry.MaxRetry > 0 {
		cfg.MaxRetry = yc.Retry.MaxRetry
	}

	// Defaults
	cfg.Defaults = yc.Defaults

	return cfg
}

// defaultTLSConfig is a minimal TLS configuration for Redis connections.
var defaultTLSConfig = tls.Config{}
