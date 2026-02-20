package handler

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// StdioTaskRequest is the JSON payload sent to the child process via stdin.
type StdioTaskRequest struct {
	TaskID     string          `json:"task_id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	RetryCount int             `json:"retry_count"`
	MaxRetry   int             `json:"max_retry"`
	Queue      string          `json:"queue"`
}

// StdioTaskResponse is the JSON response received from the child process via stdout.
type StdioTaskResponse struct {
	TaskID string          `json:"task_id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
	Retry  bool            `json:"retry,omitempty"`
}

// StdioHandler communicates with a child process via stdin/stdout JSON lines.
type StdioHandler struct {
	stdin   io.Writer
	stdout  io.Reader
	pending map[string]chan *StdioTaskResponse
	mu      sync.RWMutex
	stdinMu sync.Mutex // serializes writes to stdin to prevent interleaving with concurrency > 1
	logger  Logger

	redisStorage bool
	logBuffer    LogAppender // optional log buffer for non-JSON stdout

	// For graceful shutdown
	done   chan struct{}
	wg     sync.WaitGroup
	closed bool
}

// NewStdioHandler creates a new StdioHandler.
// stdin is used to write task requests, stdout is used to read responses.
// The caller must call Start() to begin reading responses.
func NewStdioHandler(stdin io.Writer, stdout io.Reader, logger Logger, redisStorage bool) *StdioHandler {
	return &StdioHandler{
		stdin:   stdin,
		stdout:  stdout,
		pending:      make(map[string]chan *StdioTaskResponse),
		logger:       logger,
		redisStorage: redisStorage,
		done:         make(chan struct{}),
	}
}

// SetLogBuffer sets the log buffer for capturing non-JSON stdout lines.
func (h *StdioHandler) SetLogBuffer(buf LogAppender) {
	h.logBuffer = buf
}

// Start begins reading responses from stdout in a background goroutine.
// Must be called before processing tasks.
func (h *StdioHandler) Start() {
	h.wg.Add(1)
	go h.readResponses()
}

// Stop signals the handler to stop and waits for cleanup.
func (h *StdioHandler) Stop() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	close(h.done)
	h.mu.Unlock()

	h.wg.Wait()

	// Cancel all pending tasks
	h.mu.Lock()
	for taskID, ch := range h.pending {
		close(ch)
		delete(h.pending, taskID)
	}
	h.mu.Unlock()
}

// ProcessTask sends a task to the child process and waits for the response.
func (h *StdioHandler) ProcessTask(ctx context.Context, task Task) error {
	// Check if handler is closed
	h.mu.RLock()
	if h.closed {
		h.mu.RUnlock()
		return fmt.Errorf("stdio handler is closed")
	}
	h.mu.RUnlock()

	taskID := task.ID()

	// Create response channel
	responseCh := make(chan *StdioTaskResponse, 1)

	// Register in pending map
	h.mu.Lock()
	h.pending[taskID] = responseCh
	h.mu.Unlock()

	// Ensure cleanup
	defer func() {
		h.mu.Lock()
		delete(h.pending, taskID)
		h.mu.Unlock()
	}()

	// Build request
	payload := task.Payload()
	var jsonPayload json.RawMessage
	if json.Valid(payload) {
		jsonPayload = payload
	}

	req := StdioTaskRequest{
		TaskID:     taskID,
		Type:       task.Type(),
		Payload:    jsonPayload,
		RetryCount: task.RetryCount(),
		MaxRetry:   task.MaxRetry(),
		Queue:      task.Queue(),
	}

	// Marshal request
	reqBytes, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal task request: %w", err)
	}

	// Write request line (with newline), guarded against closed pipe panic
	h.logger.Debug("Sending task %s to child process", taskID)
	reqLine := append(reqBytes, '\n')

	h.mu.RLock()
	closed := h.closed
	doneCh := h.done // capture under lock to avoid race with Reconnect
	h.mu.RUnlock()
	if closed {
		return fmt.Errorf("stdio handler is closed")
	}

	var writeErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				writeErr = fmt.Errorf("panic writing to stdin: %v", r)
			}
		}()
		h.stdinMu.Lock()
		_, writeErr = h.stdin.Write(reqLine)
		h.stdinMu.Unlock()
	}()
	if writeErr != nil {
		return fmt.Errorf("failed to write to stdin: %w", writeErr)
	}

	// Wait for response
	select {
	case resp, ok := <-responseCh:
		if !ok {
			// Channel closed (handler stopped or process exited)
			return fmt.Errorf("handler closed while waiting for response")
		}
		return h.handleResponse(resp, task)

	case <-ctx.Done():
		return ctx.Err()

	case <-doneCh:
		return fmt.Errorf("handler stopped while waiting for response")
	}
}

// handleResponse processes the response from the child process.
func (h *StdioHandler) handleResponse(resp *StdioTaskResponse, task Task) error {
	h.logger.Debug("Received response for task %s", resp.TaskID)

	// Check for error
	if resp.Error != "" {
		if resp.Retry {
			// Retryable error
			return fmt.Errorf("%s (will retry)", resp.Error)
		}
		// Permanent failure
		return NewSkipRetryError(resp.Error)
	}

	// Success - write result only if redis storage is enabled
	if h.redisStorage && len(resp.Result) > 0 {
		if _, err := task.ResultWriter().Write(resp.Result); err != nil {
			h.logger.Error("Failed to write task result: %v", err)
		}
	}

	return nil
}

// Reconnect swaps the stdin/stdout pipes after a process restart.
// It waits for the old readResponses goroutine to finish, fails all pending tasks,
// then starts a new readResponses goroutine with the new pipes.
func (h *StdioHandler) Reconnect(stdin io.Writer, stdout io.Reader) {
	// Mark as closed first so ProcessTask() rejects new tasks during reconnect
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()

	// Wait for old readResponses to finish (it exits when stdout closes)
	h.wg.Wait()

	// Fail all pending tasks and swap pipes atomically
	h.stdinMu.Lock()
	h.mu.Lock()
	// Close old done channel to wake up any ProcessTask selects
	select {
	case <-h.done:
		// already closed
	default:
		close(h.done)
	}
	for taskID, ch := range h.pending {
		close(ch)
		delete(h.pending, taskID)
	}
	// Swap pipes and reset state — stdinMu held to sync with ProcessTask
	h.stdin = stdin
	h.stdout = stdout
	h.closed = false
	h.done = make(chan struct{})
	h.mu.Unlock()
	h.stdinMu.Unlock()

	// Start new readResponses goroutine
	h.wg.Add(1)
	go h.readResponses()
}

// readResponses reads JSON lines from stdout and dispatches to pending tasks.
func (h *StdioHandler) readResponses() {
	defer h.wg.Done()

	scanner := bufio.NewScanner(h.stdout)
	// Increase buffer for large responses
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024) // 10MB max line

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var resp StdioTaskResponse
		if err := json.Unmarshal(line, &resp); err != nil {
			h.logger.Warn("Failed to parse response: %v (line: %s)", err, string(line))
			continue
		}

		// Skip status messages (used for startup detection)
		if resp.TaskID == "" {
			h.logger.Debug("Received status message: %s", string(line))
			if h.logBuffer != nil {
				h.logBuffer.Append("stdout", string(line))
			}
			continue
		}

		// Find pending task
		h.mu.RLock()
		ch, ok := h.pending[resp.TaskID]
		h.mu.RUnlock()

		if !ok {
			h.logger.Warn("Received response for unknown task: %s", resp.TaskID)
			continue
		}

		// Send response (non-blocking)
		select {
		case ch <- &resp:
		default:
			h.logger.Warn("Response channel full for task: %s", resp.TaskID)
		}
	}

	if err := scanner.Err(); err != nil {
		h.logger.Error("Error reading stdout: %v", err)
	}

	h.logger.Info("Stdout reader stopped")
}

// ReadySignal is sent by the child process to indicate it's ready or has failed.
type ReadySignal struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// WaitForReady waits for the child process to send a ready signal.
// Returns when the child sends {"status":"ready"} or the context is cancelled.
func WaitForReady(ctx context.Context, stdout io.Reader, logger Logger) error {
	scanner := bufio.NewScanner(stdout)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	// Create a channel to signal ready
	readyCh := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			logger.Debug("Child output: %s", string(line))

			var signal ReadySignal
			if err := json.Unmarshal(line, &signal); err != nil {
				// Not a JSON line, might be regular log output
				continue
			}

			if signal.Status == "ready" {
				close(readyCh)
				return
			}
			if signal.Status == "error" {
				errCh <- fmt.Errorf("process startup failed: %s", signal.Error)
				return
			}
		}

		if err := scanner.Err(); err != nil {
			errCh <- fmt.Errorf("error reading stdout: %w", err)
		} else {
			errCh <- fmt.Errorf("stdout closed before ready signal")
		}
	}()

	select {
	case <-readyCh:
		logger.Info("Child process ready")
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
