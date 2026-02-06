package worker

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// YAMLConfig represents the YAML configuration file structure.
type YAMLConfig struct {
	Server     ServerConfig   `yaml:"server"`
	Worker     WorkerConfig   `yaml:"worker"`
	Bootstrap  BootstrapConfig `yaml:"bootstrap"`
	Git        GitConfig      `yaml:"git"`
	Deployment DeploymentYAML `yaml:"deployment"`
	Retry      RetryConfig    `yaml:"retry"`
}

// ServerConfig holds runqy-server connection settings.
type ServerConfig struct {
	URL    string `yaml:"url"`     // Required: runqy-server URL
	APIKey string `yaml:"api_key"` // Required: Authentication key
}

// BootstrapConfig holds bootstrap retry settings.
type BootstrapConfig struct {
	Retries    int    `yaml:"retries"`     // Number of retries (default: 3)
	RetryDelay string `yaml:"retry_delay"` // Delay between retries (default: 5s)
}

// GitConfig holds git authentication settings.
type GitConfig struct {
	SSHKey string `yaml:"ssh_key"` // Path to SSH private key file
	Token  string `yaml:"token"`   // Personal access token or password
}

// DeploymentYAML holds deployment directory settings.
type DeploymentYAML struct {
	Dir                   string `yaml:"dir"`                               // Directory for code deployment (default: "./deployment")
	UseSystemSitePackages *bool  `yaml:"use_system_site_packages,omitempty"` // Use --system-site-packages for venv (default: true)
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

// WorkerConfig holds worker settings from YAML.
type WorkerConfig struct {
	Queue           string   `yaml:"queue"`            // DEPRECATED: Single queue name (backward compat)
	Queues          []string `yaml:"queues"`           // List of queue names to listen on
	Concurrency     int      `yaml:"concurrency"`
	ShutdownTimeout string   `yaml:"shutdown_timeout"`
}

// RetryConfig holds retry settings from YAML.
type RetryConfig struct {
	MaxRetry int `yaml:"max_retry"`
}

// LoadConfig loads configuration from a YAML file or environment variables.
// If path is empty, it defaults to "config.yml" in the current directory.
// If the config file does not exist, it falls back to environment variables.
func LoadConfig(path string) (*Config, error) {
	if path == "" {
		path = "config.yml"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Fall back to environment variables
			cfg := loadConfigFromEnv()
			return &cfg, nil
		}
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

// loadConfigFromEnv builds configuration purely from environment variables.
// Environment variables:
//   - RUNQY_SERVER_URL: runqy-server URL (required)
//   - RUNQY_API_KEY: Authentication key (required)
//   - RUNQY_QUEUES: Comma-separated list of queue names (required)
//   - RUNQY_CONCURRENCY: Number of concurrent workers (default: 1)
//   - RUNQY_SHUTDOWN_TIMEOUT: Graceful shutdown timeout (default: 8s)
//   - RUNQY_BOOTSTRAP_RETRIES: Number of bootstrap retries (default: 3)
//   - RUNQY_BOOTSTRAP_RETRY_DELAY: Delay between retries (default: 5s)
//   - RUNQY_GIT_SSH_KEY: Path to SSH private key
//   - RUNQY_GIT_TOKEN: Git personal access token
//   - RUNQY_DEPLOYMENT_DIR: Code deployment directory (default: ./deployment)
//   - RUNQY_MAX_RETRY: Max task retry attempts (default: 25)
func loadConfigFromEnv() Config {
	cfg := DefaultConfig()

	// Server settings (required)
	cfg.ServerURL = os.Getenv("RUNQY_SERVER_URL")
	cfg.APIKey = os.Getenv("RUNQY_API_KEY")

	// Queue settings
	if queues := os.Getenv("RUNQY_QUEUES"); queues != "" {
		cfg.QueueNames = strings.Split(queues, ",")
		for i := range cfg.QueueNames {
			cfg.QueueNames[i] = strings.TrimSpace(cfg.QueueNames[i])
		}
		if len(cfg.QueueNames) > 0 {
			cfg.Queue = cfg.QueueNames[0]
		}
	}

	// Worker settings
	if concurrency := os.Getenv("RUNQY_CONCURRENCY"); concurrency != "" {
		if n, err := strconv.Atoi(concurrency); err == nil && n > 0 {
			cfg.Concurrency = n
		}
	}
	if timeout := os.Getenv("RUNQY_SHUTDOWN_TIMEOUT"); timeout != "" {
		if d, err := time.ParseDuration(timeout); err == nil {
			cfg.ShutdownTimeout = d
		}
	}

	// Bootstrap settings
	if retries := os.Getenv("RUNQY_BOOTSTRAP_RETRIES"); retries != "" {
		if n, err := strconv.Atoi(retries); err == nil && n > 0 {
			cfg.BootstrapRetries = n
		}
	}
	if delay := os.Getenv("RUNQY_BOOTSTRAP_RETRY_DELAY"); delay != "" {
		if d, err := time.ParseDuration(delay); err == nil {
			cfg.BootstrapRetryDelay = d
		}
	}

	// Git authentication
	cfg.GitSSHKey = os.Getenv("RUNQY_GIT_SSH_KEY")
	cfg.GitToken = os.Getenv("RUNQY_GIT_TOKEN")

	// Deployment settings
	if dir := os.Getenv("RUNQY_DEPLOYMENT_DIR"); dir != "" {
		cfg.DeploymentDir = dir
	}
	if useSysSite := os.Getenv("RUNQY_USE_SYSTEM_SITE_PACKAGES"); useSysSite != "" {
		cfg.UseSystemSitePackages = strings.ToLower(useSysSite) == "true" || useSysSite == "1"
	}

	// Retry settings
	if maxRetry := os.Getenv("RUNQY_MAX_RETRY"); maxRetry != "" {
		if n, err := strconv.Atoi(maxRetry); err == nil && n > 0 {
			cfg.MaxRetry = n
		}
	}

	return cfg
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

	// Server settings (required)
	cfg.ServerURL = yc.Server.URL
	cfg.APIKey = yc.Server.APIKey

	// Worker settings - handle both queue (single) and queues (list) for backward compatibility
	if len(yc.Worker.Queues) > 0 {
		cfg.QueueNames = yc.Worker.Queues
	} else if yc.Worker.Queue != "" {
		cfg.QueueNames = []string{yc.Worker.Queue}
	}
	// Keep Queue field for backward compat (use first queue if available)
	if len(cfg.QueueNames) > 0 {
		cfg.Queue = cfg.QueueNames[0]
	}
	if yc.Worker.Concurrency > 0 {
		cfg.Concurrency = yc.Worker.Concurrency
	}
	if yc.Worker.ShutdownTimeout != "" {
		if d, err := time.ParseDuration(yc.Worker.ShutdownTimeout); err == nil {
			cfg.ShutdownTimeout = d
		}
	}

	// Bootstrap settings
	if yc.Bootstrap.Retries > 0 {
		cfg.BootstrapRetries = yc.Bootstrap.Retries
	}
	if yc.Bootstrap.RetryDelay != "" {
		if d, err := time.ParseDuration(yc.Bootstrap.RetryDelay); err == nil {
			cfg.BootstrapRetryDelay = d
		}
	}

	// Git authentication
	cfg.GitSSHKey = yc.Git.SSHKey
	cfg.GitToken = yc.Git.Token

	// Deployment settings
	if yc.Deployment.Dir != "" {
		cfg.DeploymentDir = yc.Deployment.Dir
	}
	// use_system_site_packages (default true if not specified)
	if yc.Deployment.UseSystemSitePackages != nil {
		cfg.UseSystemSitePackages = *yc.Deployment.UseSystemSitePackages
	}

	// Retry settings
	if yc.Retry.MaxRetry > 0 {
		cfg.MaxRetry = yc.Retry.MaxRetry
	}

	return cfg
}
