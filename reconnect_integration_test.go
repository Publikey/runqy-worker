package worker

import (
	"testing"
	"errors"
)

func TestIsConnectionError_ClientClosed(t *testing.T) {
	tests := []struct {
		err      string
		expected bool
	}{
		{"redis: client is closed", true},
		{"brpop failed: redis: client is closed", true},
		{"i/o timeout", true},
		{"connection refused", true},
		{"some random error", false},
		{"WRONGPASS invalid username-password", false},
	}

	for _, tt := range tests {
		result := isConnectionError(errors.New(tt.err))
		if result != tt.expected {
			t.Errorf("isConnectionError(%q) = %v, want %v", tt.err, result, tt.expected)
		}
	}
}

func TestHandleError_ClientClosed_TriggersReconnect(t *testing.T) {
	// Create a reconnector with a fake config (will fail to connect)
	config := RedisConnConfig{
		Addr:     "localhost:59999", // nothing listening
		Password: "",
		DB:       0,
	}

	logger := &testLogger{t: t}
	r := newRedisReconnector(config, nil, logger)

	// "client is closed" should now trigger reconnection attempt
	err := errors.New("brpop failed: redis: client is closed")
	attempted := r.handleError(err)

	// It should have attempted reconnection (even though it fails because no Redis)
	// The key thing: it doesn't skip with "not a connection error"
	if r.consecutiveErrs == 0 && !attempted {
		t.Error("Expected reconnection attempt for 'client is closed' error")
	}

	// reconnectFails should have incremented (since fake Redis is unreachable)
	if r.reconnectFails != 1 {
		t.Errorf("Expected reconnectFails=1, got %d", r.reconnectFails)
	}
}

type testLogger struct {
	t *testing.T
}

func (l *testLogger) Debug(format string, args ...interface{}) { l.t.Logf("[DEBUG] "+format, args...) }
func (l *testLogger) Info(format string, args ...interface{})  { l.t.Logf("[INFO] "+format, args...) }
func (l *testLogger) Warn(format string, args ...interface{})  { l.t.Logf("[WARN] "+format, args...) }
func (l *testLogger) Fatal(format string, args ...interface{}) { l.t.Logf("[FATAL] "+format, args...) }
func (l *testLogger) Error(format string, args ...interface{}) { l.t.Logf("[ERROR] "+format, args...) }
