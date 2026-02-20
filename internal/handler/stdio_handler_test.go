package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"
)

type testLogger struct{}

func (l *testLogger) Info(format string, args ...interface{})  {}
func (l *testLogger) Warn(format string, args ...interface{})  {}
func (l *testLogger) Error(format string, args ...interface{}) {}
func (l *testLogger) Debug(format string, args ...interface{}) {}

// mockTask implements the Task interface for testing.
type mockTask struct {
	id       string
	typeName string
	payload  []byte
	queue    string
	result   bytes.Buffer
}

func (t *mockTask) ID() string           { return t.id }
func (t *mockTask) Type() string          { return t.typeName }
func (t *mockTask) Payload() []byte       { return t.payload }
func (t *mockTask) RetryCount() int       { return 0 }
func (t *mockTask) MaxRetry() int         { return 3 }
func (t *mockTask) Queue() string         { return t.queue }
func (t *mockTask) ResultWriter() io.Writer { return &t.result }

func TestStdioHandler_Reconnect(t *testing.T) {
	logger := &testLogger{}

	// Phase 1: Create handler with initial pipes
	oldStdin := &bytes.Buffer{}
	oldStdoutR, oldStdoutW := io.Pipe()

	h := NewStdioHandler(oldStdin, oldStdoutR, logger, false)
	h.Start()

	// Send a response on old stdout to verify it's working
	go func() {
		resp := StdioTaskResponse{TaskID: "task-1", Result: json.RawMessage(`{"ok":true}`)}
		data, _ := json.Marshal(resp)
		oldStdoutW.Write(append(data, '\n'))
	}()

	// Process a task on the old pipes
	task := &mockTask{id: "task-1", typeName: "test", payload: []byte(`{}`), queue: "q"}
	err := h.ProcessTask(context.Background(), task)
	if err != nil {
		t.Fatalf("ProcessTask on old pipes failed: %v", err)
	}

	// Phase 2: Close old stdout (simulates process crash)
	oldStdoutW.Close()

	// Phase 3: Reconnect with new pipes
	newStdin := &bytes.Buffer{}
	newStdoutR, newStdoutW := io.Pipe()

	h.Reconnect(newStdin, newStdoutR)

	// Phase 4: Process a task on the new pipes
	go func() {
		resp := StdioTaskResponse{TaskID: "task-2", Result: json.RawMessage(`{"new":true}`)}
		data, _ := json.Marshal(resp)
		newStdoutW.Write(append(data, '\n'))
	}()

	task2 := &mockTask{id: "task-2", typeName: "test", payload: []byte(`{}`), queue: "q"}
	err = h.ProcessTask(context.Background(), task2)
	if err != nil {
		t.Fatalf("ProcessTask on new pipes failed: %v", err)
	}

	// Verify the request was written to new stdin
	if !strings.Contains(newStdin.String(), "task-2") {
		t.Errorf("expected task-2 request in new stdin, got: %s", newStdin.String())
	}

	// Clean up
	newStdoutW.Close()
	h.Stop()
}

func TestStdioHandler_ReconnectFailsPendingTasks(t *testing.T) {
	logger := &testLogger{}

	stdinBuf := &bytes.Buffer{}
	stdoutR, stdoutW := io.Pipe()

	h := NewStdioHandler(stdinBuf, stdoutR, logger, false)
	h.Start()

	// Start a task that will be pending (no response sent)
	taskDone := make(chan error, 1)
	go func() {
		task := &mockTask{id: "pending-task", typeName: "test", payload: []byte(`{}`), queue: "q"}
		taskDone <- h.ProcessTask(context.Background(), task)
	}()

	// Give the task time to register as pending
	time.Sleep(100 * time.Millisecond)

	// Close stdout to simulate crash (this makes readResponses exit)
	stdoutW.Close()

	// Reconnect — this should fail the pending task
	newStdin := &bytes.Buffer{}
	newStdoutR, newStdoutW := io.Pipe()
	h.Reconnect(newStdin, newStdoutR)

	// The pending task should have failed
	select {
	case err := <-taskDone:
		if err == nil {
			t.Error("expected error for pending task after reconnect, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for pending task to fail")
	}

	newStdoutW.Close()
	h.Stop()
}

func TestStdioHandler_ReconnectMultipleTimes(t *testing.T) {
	logger := &testLogger{}

	stdin1 := &bytes.Buffer{}
	stdoutR1, stdoutW1 := io.Pipe()

	h := NewStdioHandler(stdin1, stdoutR1, logger, false)
	h.Start()

	// Reconnect 3 times, verify each time works
	for i := 0; i < 3; i++ {
		stdoutW1.Close() // Close current stdout

		newStdin := &bytes.Buffer{}
		newStdoutR, newStdoutW := io.Pipe()
		h.Reconnect(newStdin, newStdoutR)

		// Process a task on new pipes
		taskID := "task-" + string(rune('A'+i))
		go func() {
			resp := StdioTaskResponse{TaskID: taskID}
			data, _ := json.Marshal(resp)
			newStdoutW.Write(append(data, '\n'))
		}()

		task := &mockTask{id: taskID, typeName: "test", payload: []byte(`{}`), queue: "q"}
		if err := h.ProcessTask(context.Background(), task); err != nil {
			t.Fatalf("reconnect #%d: ProcessTask failed: %v", i+1, err)
		}

		// Update references for next iteration
		stdoutW1 = newStdoutW
	}

	stdoutW1.Close()
	h.Stop()
}
