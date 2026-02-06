package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"time"
)

// Logger interface for bootstrap package logging.
type Logger interface {
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	Debug(format string, args ...interface{})
}

// Config holds the configuration needed for bootstrap operations.
type Config struct {
	ServerURL             string
	APIKey                string
	Queue                 string
	Version               string
	BootstrapRetries      int
	BootstrapRetryDelay   time.Duration
	DeploymentDir         string
	GitSSHKey             string
	GitToken              string
	UseSystemSitePackages bool // Use --system-site-packages for venv (default: true)
}

// Response contains the configuration received from runqy-server.
type Response struct {
	Redis      RedisConfig       `json:"redis"`
	Queue      QueueConfig       `json:"queue"`
	SubQueues  []SubQueueConfig  `json:"sub_queues"`
	Deployment DeploymentConfig  `json:"deployment"`
	Vaults     map[string]string `json:"vaults,omitempty"`    // Decrypted vault key-value pairs for env injection
	GitToken   string            `json:"git_token,omitempty"` // Resolved git token for authentication
}

// RedisConfig holds Redis connection settings from server.
type RedisConfig struct {
	Addr     string `json:"addr"` // "host:port"
	Password string `json:"password"`
	DB       int    `json:"db"`
	UseTLS   bool   `json:"use_tls"` // Simple boolean for TLS
}

// QueueConfig holds queue configuration from server.
type QueueConfig struct {
	Name     string `json:"name"`
	Priority int    `json:"priority"` // Weight for this queue
}

// SubQueueConfig represents a sub-queue with its full name and priority.
type SubQueueConfig struct {
	Name     string `json:"name"`     // Full name: "inference:high"
	Priority int    `json:"priority"` // Weight for this sub-queue
}

// DeploymentConfig holds code deployment settings from server.
type DeploymentConfig struct {
	GitURL             string `json:"git_url"`              // Repository URL
	Branch             string `json:"branch"`               // Git branch/tag
	CodePath           string `json:"code_path"`            // Subdirectory where task code lives (enables sparse checkout)
	StartupCmd         string `json:"startup_cmd"`          // Command to start process
	RedisStorage       bool   `json:"redis_storage"`        // Write the result on redis or not
	StartupTimeoutSecs int    `json:"startup_timeout_secs"` // Timeout for ready signal
	Mode               string `json:"mode"`                 // "long_running" (default) or "one_shot"
}

// Request contains worker metadata sent to server during registration.
type Request struct {
	Queue    string `json:"queue"`
	Hostname string `json:"hostname"`
	OS       string `json:"os"`
	Version  string `json:"version"`
}

// Bootstrap contacts the runqy-server and retrieves worker configuration.
// It retries according to config.BootstrapRetries with config.BootstrapRetryDelay between attempts.
func Bootstrap(ctx context.Context, config Config, logger Logger) (*Response, error) {
	// Build request body
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	version := config.Version
	if version == "" {
		version = "dev"
	}

	reqBody := Request{
		Queue:    config.Queue,
		Hostname: hostname,
		OS:       runtime.GOOS,
		Version:  version,
	}

	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bootstrap request: %w", err)
	}

	// Build endpoint URL
	endpoint := config.ServerURL + "/worker/register"

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	var lastErr error
	maxAttempts := config.BootstrapRetries
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		logger.Info("Bootstrap attempt %d/%d: contacting %s", attempt, maxAttempts, endpoint)

		resp, err := doBootstrapRequest(ctx, client, endpoint, config.APIKey, reqJSON)
		if err != nil {
			lastErr = err
			logger.Warn("Bootstrap attempt %d failed: %v", attempt, err)

			if attempt < maxAttempts {
				logger.Info("Retrying in %v...", config.BootstrapRetryDelay)
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(config.BootstrapRetryDelay):
					continue
				}
			}
			continue
		}

		// Validate response
		if err := ValidateResponse(resp); err != nil {
			return nil, fmt.Errorf("invalid bootstrap response: %w", err)
		}

		logger.Info("Bootstrap successful: redis=%s, queue=%s",
			resp.Redis.Addr, resp.Queue.Name)
		return resp, nil
	}

	return nil, fmt.Errorf("bootstrap failed after %d attempts: %w", maxAttempts, lastErr)
}

// doBootstrapRequest performs a single HTTP request to the server.
func doBootstrapRequest(ctx context.Context, client *http.Client, endpoint, apiKey string, body []byte) (*Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(respBody))
	}

	var result Response
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &result, nil
}

// ValidateResponse checks that required fields are present.
func ValidateResponse(resp *Response) error {
	if resp.Redis.Addr == "" {
		return fmt.Errorf("redis.addr is required")
	}
	if resp.Queue.Name == "" {
		return fmt.Errorf("queue.name is required")
	}
	if resp.Deployment.GitURL == "" {
		return fmt.Errorf("deployment.git_url is required")
	}
	if resp.Deployment.Branch == "" {
		return fmt.Errorf("deployment.branch is required")
	}
	if resp.Deployment.StartupCmd == "" {
		return fmt.Errorf("deployment.startup_cmd is required")
	}
	return nil
}
