package bootstrap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// testLogger is a simple logger for tests.
type testLogger struct{}

func (l *testLogger) Info(format string, args ...interface{})  {}
func (l *testLogger) Warn(format string, args ...interface{})  {}
func (l *testLogger) Error(format string, args ...interface{}) {}
func (l *testLogger) Debug(format string, args ...interface{}) {}

func newTestLogger() Logger {
	return &testLogger{}
}

func TestBootstrapRequest(t *testing.T) {
	// Create a test server
	var receivedReq Request
	var receivedAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check method
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}

		// Check path
		if r.URL.Path != "/worker/register" {
			t.Errorf("expected /worker/register, got %s", r.URL.Path)
		}

		// Check auth header
		receivedAuth = r.Header.Get("Authorization")

		// Parse request body
		if err := json.NewDecoder(r.Body).Decode(&receivedReq); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}

		// Send response
		resp := Response{
			Redis: RedisConfig{
				Addr:     "localhost:6379",
				Password: "secret",
				DB:       0,
				UseTLS:   false,
			},
			Queue: QueueConfig{
				Name:     "test-queue",
				Priority: 5,
			},
			Deployment: DeploymentConfig{
				GitURL:             "https://github.com/test/repo.git",
				Branch:             "main",
				StartupCmd:         "uvicorn app:app",
				EnvVars:            map[string]string{"KEY": "value"},
				StartupTimeoutSecs: 60,
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	// Create config
	cfg := Config{
		ServerURL:           server.URL,
		APIKey:              "test-api-key",
		Queue:               "test-queue",
		Version:             "1.0.0",
		BootstrapRetries:    1,
		BootstrapRetryDelay: time.Millisecond,
	}

	// Call bootstrap
	logger := newTestLogger()
	resp, err := Bootstrap(context.Background(), cfg, logger)
	if err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}

	// Verify request
	if receivedAuth != "Bearer test-api-key" {
		t.Errorf("expected 'Bearer test-api-key', got %q", receivedAuth)
	}
	if receivedReq.Queue != "test-queue" {
		t.Errorf("expected queue 'test-queue', got %q", receivedReq.Queue)
	}
	if receivedReq.Version != "1.0.0" {
		t.Errorf("expected version '1.0.0', got %q", receivedReq.Version)
	}

	// Verify response
	if resp.Redis.Addr != "localhost:6379" {
		t.Errorf("expected redis addr 'localhost:6379', got %q", resp.Redis.Addr)
	}
	if resp.Queue.Name != "test-queue" {
		t.Errorf("expected queue name 'test-queue', got %q", resp.Queue.Name)
	}
}

func TestBootstrapRetry(t *testing.T) {
	attempts := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		resp := Response{
			Redis:      RedisConfig{Addr: "localhost:6379"},
			Queue:      QueueConfig{Name: "q", Priority: 1},
			Deployment: DeploymentConfig{GitURL: "https://github.com/t/r.git", Branch: "main", StartupCmd: "cmd"},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	cfg := Config{
		ServerURL:           server.URL,
		APIKey:              "key",
		Queue:               "q",
		BootstrapRetries:    3,
		BootstrapRetryDelay: time.Millisecond,
	}

	logger := newTestLogger()
	_, err := Bootstrap(context.Background(), cfg, logger)
	if err != nil {
		t.Fatalf("bootstrap should have succeeded after retries: %v", err)
	}

	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestBootstrapValidation(t *testing.T) {
	tests := []struct {
		name    string
		resp    Response
		wantErr bool
	}{
		{
			name: "valid response",
			resp: Response{
				Redis:      RedisConfig{Addr: "localhost:6379"},
				Queue:      QueueConfig{Name: "q", Priority: 1},
				Deployment: DeploymentConfig{GitURL: "https://github.com/t/r.git", Branch: "main", StartupCmd: "cmd"},
			},
			wantErr: false,
		},
		{
			name: "missing redis addr",
			resp: Response{
				Redis:      RedisConfig{Addr: ""},
				Queue:      QueueConfig{Name: "q", Priority: 1},
				Deployment: DeploymentConfig{GitURL: "https://github.com/t/r.git", Branch: "main", StartupCmd: "cmd"},
			},
			wantErr: true,
		},
		{
			name: "missing queue name",
			resp: Response{
				Redis:      RedisConfig{Addr: "localhost:6379"},
				Queue:      QueueConfig{Name: "", Priority: 1},
				Deployment: DeploymentConfig{GitURL: "https://github.com/t/r.git", Branch: "main", StartupCmd: "cmd"},
			},
			wantErr: true,
		},
		{
			name: "missing git url",
			resp: Response{
				Redis:      RedisConfig{Addr: "localhost:6379"},
				Queue:      QueueConfig{Name: "q", Priority: 1},
				Deployment: DeploymentConfig{GitURL: "", Branch: "main", StartupCmd: "cmd"},
			},
			wantErr: true,
		},
		{
			name: "missing branch",
			resp: Response{
				Redis:      RedisConfig{Addr: "localhost:6379"},
				Queue:      QueueConfig{Name: "q", Priority: 1},
				Deployment: DeploymentConfig{GitURL: "https://github.com/t/r.git", Branch: "", StartupCmd: "cmd"},
			},
			wantErr: true,
		},
		{
			name: "missing startup cmd",
			resp: Response{
				Redis:      RedisConfig{Addr: "localhost:6379"},
				Queue:      QueueConfig{Name: "q", Priority: 1},
				Deployment: DeploymentConfig{GitURL: "https://github.com/t/r.git", Branch: "main", StartupCmd: ""},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateResponse(&tt.resp)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateResponse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBootstrapContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Second) // Slow response
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := Config{
		ServerURL:           server.URL,
		APIKey:              "key",
		Queue:               "q",
		BootstrapRetries:    1,
		BootstrapRetryDelay: time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	logger := newTestLogger()
	_, err := Bootstrap(ctx, cfg, logger)
	if err == nil {
		t.Error("expected error due to cancelled context")
	}
}
