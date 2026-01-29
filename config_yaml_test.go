package worker

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	// Create temp config file
	tempDir, err := os.MkdirTemp("", "config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	configContent := `
server:
  url: "https://server.example.com"
  api_key: "test-key-123"

worker:
  queue: "inference"
  concurrency: 8
  shutdown_timeout: "60s"

bootstrap:
  retries: 5
  retry_delay: "10s"

git:
  ssh_key: "/path/to/key"
  token: "git-token"

deployment:
  dir: "/custom/deployment"

retry:
  max_retry: 10
`

	configPath := filepath.Join(tempDir, "config.yml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Verify server settings
	if cfg.ServerURL != "https://server.example.com" {
		t.Errorf("ServerURL = %q, want %q", cfg.ServerURL, "https://server.example.com")
	}
	if cfg.APIKey != "test-key-123" {
		t.Errorf("APIKey = %q, want %q", cfg.APIKey, "test-key-123")
	}

	// Verify worker settings
	if cfg.Queue != "inference" {
		t.Errorf("Queue = %q, want %q", cfg.Queue, "inference")
	}
	if cfg.Concurrency != 8 {
		t.Errorf("Concurrency = %d, want 8", cfg.Concurrency)
	}
	if cfg.ShutdownTimeout != 60*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 60s", cfg.ShutdownTimeout)
	}

	// Verify bootstrap settings
	if cfg.BootstrapRetries != 5 {
		t.Errorf("BootstrapRetries = %d, want 5", cfg.BootstrapRetries)
	}
	if cfg.BootstrapRetryDelay != 10*time.Second {
		t.Errorf("BootstrapRetryDelay = %v, want 10s", cfg.BootstrapRetryDelay)
	}

	// Verify git settings
	if cfg.GitSSHKey != "/path/to/key" {
		t.Errorf("GitSSHKey = %q, want %q", cfg.GitSSHKey, "/path/to/key")
	}
	if cfg.GitToken != "git-token" {
		t.Errorf("GitToken = %q, want %q", cfg.GitToken, "git-token")
	}

	// Verify deployment settings
	if cfg.DeploymentDir != "/custom/deployment" {
		t.Errorf("DeploymentDir = %q, want %q", cfg.DeploymentDir, "/custom/deployment")
	}

	// Verify retry settings
	if cfg.MaxRetry != 10 {
		t.Errorf("MaxRetry = %d, want 10", cfg.MaxRetry)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Minimal config
	configContent := `
server:
  url: "https://server.example.com"
  api_key: "key"

worker:
  queue: "default"
`

	configPath := filepath.Join(tempDir, "config.yml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	// Check defaults are applied
	if cfg.Concurrency != 1 {
		t.Errorf("default Concurrency = %d, want 1", cfg.Concurrency)
	}
	if cfg.MaxRetry != 25 {
		t.Errorf("default MaxRetry = %d, want 25", cfg.MaxRetry)
	}
	if cfg.BootstrapRetries != 3 {
		t.Errorf("default BootstrapRetries = %d, want 3", cfg.BootstrapRetries)
	}
	if cfg.BootstrapRetryDelay != 5*time.Second {
		t.Errorf("default BootstrapRetryDelay = %v, want 5s", cfg.BootstrapRetryDelay)
	}
	if cfg.DeploymentDir != "./deployment" {
		t.Errorf("default DeploymentDir = %q, want './deployment'", cfg.DeploymentDir)
	}
}

func TestLoadConfigEnvVars(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Set environment variable
	os.Setenv("TEST_SERVER_URL", "https://env.example.com")
	os.Setenv("TEST_API_KEY", "env-key-456")
	defer os.Unsetenv("TEST_SERVER_URL")
	defer os.Unsetenv("TEST_API_KEY")

	configContent := `
server:
  url: "${TEST_SERVER_URL}"
  api_key: "${TEST_API_KEY}"

worker:
  queue: "test"
`

	configPath := filepath.Join(tempDir, "config.yml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.ServerURL != "https://env.example.com" {
		t.Errorf("ServerURL = %q, want %q", cfg.ServerURL, "https://env.example.com")
	}
	if cfg.APIKey != "env-key-456" {
		t.Errorf("APIKey = %q, want %q", cfg.APIKey, "env-key-456")
	}
}

func TestLoadConfigEnvVarsWithDefaults(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Unset to test default
	os.Unsetenv("NONEXISTENT_VAR")

	configContent := `
server:
  url: "${NONEXISTENT_VAR:-https://default.example.com}"
  api_key: "key"

worker:
  queue: "test"
`

	configPath := filepath.Join(tempDir, "config.yml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.ServerURL != "https://default.example.com" {
		t.Errorf("ServerURL = %q, want default %q", cfg.ServerURL, "https://default.example.com")
	}
}

func TestExpandEnvVars(t *testing.T) {
	os.Setenv("TEST_VAR", "test-value")
	defer os.Unsetenv("TEST_VAR")

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple variable",
			input:    "${TEST_VAR}",
			expected: "test-value",
		},
		{
			name:     "variable with default used",
			input:    "${NONEXISTENT:-default}",
			expected: "default",
		},
		{
			name:     "variable with default not used",
			input:    "${TEST_VAR:-default}",
			expected: "test-value",
		},
		{
			name:     "no variables",
			input:    "plain text",
			expected: "plain text",
		},
		{
			name:     "multiple variables",
			input:    "${TEST_VAR} and ${TEST_VAR}",
			expected: "test-value and test-value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := string(expandEnvVars([]byte(tt.input)))
			if result != tt.expected {
				t.Errorf("expandEnvVars(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestLoadConfigFileNotFound(t *testing.T) {
	// When config file is not found, LoadConfig falls back to environment variables
	// and returns a valid config (no error)
	cfg, err := LoadConfig("/nonexistent/path/config.yml")
	if err != nil {
		t.Errorf("unexpected error: %v (LoadConfig should fall back to env vars)", err)
	}
	if cfg == nil {
		t.Error("expected config from env vars fallback, got nil")
	}
}

func TestLoadConfigInvalidYAML(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	configContent := `
server:
  url: "test
  invalid yaml here
`

	configPath := filepath.Join(tempDir, "config.yml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	_, err = LoadConfig(configPath)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}
