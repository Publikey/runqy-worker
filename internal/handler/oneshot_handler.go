package handler

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// OneShotHandler spawns a new process for each task.
// The process handles one task and exits. Suitable for lightweight tasks
// that don't benefit from staying loaded in memory.
type OneShotHandler struct {
	repoPath     string
	venvPath     string
	venvPython   string
	startupCmd   string
	vaultVars    map[string]string // Vault entries to inject as environment variables
	timeout      time.Duration
	redisStorage bool
	logger       Logger
}

// NewOneShotHandler creates a new OneShotHandler.
func NewOneShotHandler(
	repoPath string,
	venvPath string,
	startupCmd string,
	vaultVars map[string]string,
	timeout time.Duration,
	redisStorage bool,
	logger Logger,
) *OneShotHandler {
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	// Calculate venv python path
	var venvPython string
	if runtime.GOOS == "windows" {
		venvPython = filepath.Join(venvPath, "Scripts", "python.exe")
	} else {
		venvPython = filepath.Join(venvPath, "bin", "python")
	}

	return &OneShotHandler{
		repoPath:     repoPath,
		venvPath:     venvPath,
		venvPython:   venvPython,
		startupCmd:   startupCmd,
		vaultVars:    vaultVars,
		timeout:      timeout,
		redisStorage: redisStorage,
		logger:       logger,
	}
}

// ProcessTask spawns a new process, sends the task, and waits for the response.
func (h *OneShotHandler) ProcessTask(ctx context.Context, task Task) error {
	taskID := task.ID()
	h.logger.Debug("OneShot: spawning process for task %s", taskID)

	// Parse startup command
	args := parseCommand(h.startupCmd)
	if len(args) == 0 {
		return fmt.Errorf("empty startup command")
	}

	// Replace python/python3 with the venv python to avoid Windows PATH issues
	if len(args) > 0 && (args[0] == "python" || args[0] == "python3" || args[0] == "python.exe" || args[0] == "python3.exe") {
		args[0] = h.venvPython
		h.logger.Debug("OneShot: using venv python: %s", args[0])
	}

	// Create process with timeout context
	procCtx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	cmd := exec.CommandContext(procCtx, args[0], args[1:]...)
	cmd.Dir = h.repoPath
	cmd.Env = h.buildEnvironment()

	// Create pipes
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the process
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start process: %w", err)
	}

	h.logger.Debug("OneShot: process started with PID %d for task %s", cmd.Process.Pid, taskID)

	// Stream stderr to logs in background
	go h.streamStderr(stderr, taskID)

	// Create buffered reader for stdout
	stdoutReader := bufio.NewReaderSize(stdout, 64*1024)

	// Wait for ready signal
	if err := h.waitForReady(procCtx, stdoutReader, taskID); err != nil {
		cmd.Process.Kill()
		return fmt.Errorf("failed to get ready signal: %w", err)
	}

	// Build and send task request
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

	reqBytes, err := json.Marshal(req)
	if err != nil {
		cmd.Process.Kill()
		return fmt.Errorf("failed to marshal task request: %w", err)
	}

	// Write request
	h.logger.Debug("OneShot: sending task %s to process", taskID)
	if _, err := stdin.Write(append(reqBytes, '\n')); err != nil {
		cmd.Process.Kill()
		return fmt.Errorf("failed to write to stdin: %w", err)
	}
	stdin.Close() // Signal EOF to process

	// Read response
	resp, err := h.readResponse(procCtx, stdoutReader, taskID)
	if err != nil {
		cmd.Process.Kill()
		return err
	}

	// Wait for process to exit (it should exit after handling the task)
	cmd.Wait()

	h.logger.Debug("OneShot: process exited for task %s", taskID)

	// Handle response
	return h.handleResponse(resp, task)
}

// waitForReady waits for the {"status":"ready"} signal.
func (h *OneShotHandler) waitForReady(ctx context.Context, reader *bufio.Reader, taskID string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("stdout closed before ready signal")
			}
			return fmt.Errorf("error reading stdout: %w", err)
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		h.logger.Debug("OneShot[%s]: stdout: %s", taskID, line)

		var status struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal([]byte(line), &status); err == nil {
			if status.Status == "ready" {
				h.logger.Debug("OneShot[%s]: ready signal received", taskID)
				return nil
			}
		}
	}
}

// readResponse reads the task response from stdout.
func (h *OneShotHandler) readResponse(ctx context.Context, reader *bufio.Reader, taskID string) (*StdioTaskResponse, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("stdout closed before response received")
			}
			return nil, fmt.Errorf("error reading response: %w", err)
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		h.logger.Debug("OneShot[%s]: response line: %s", taskID, line)

		var resp StdioTaskResponse
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			h.logger.Warn("OneShot[%s]: failed to parse response: %v", taskID, err)
			continue
		}

		if resp.TaskID == taskID {
			return &resp, nil
		}

		// Skip status messages or other non-matching responses
		h.logger.Debug("OneShot[%s]: ignoring response for different task: %s", taskID, resp.TaskID)
	}
}

// handleResponse processes the response from the child process.
func (h *OneShotHandler) handleResponse(resp *StdioTaskResponse, task Task) error {
	h.logger.Debug("OneShot: handling response for task %s", resp.TaskID)

	if resp.Error != "" {
		if resp.Retry {
			return fmt.Errorf("%s (will retry)", resp.Error)
		}
		return NewSkipRetryError(resp.Error)
	}

	// Success - write result only if redis storage is enabled
	if h.redisStorage && len(resp.Result) > 0 {
		if _, err := task.ResultWriter().Write(resp.Result); err != nil {
			h.logger.Error("OneShot: failed to write task result: %v", err)
		}
	}

	return nil
}

// streamStderr reads stderr and logs output.
func (h *OneShotHandler) streamStderr(stderr io.ReadCloser, taskID string) {
	scanner := bufio.NewScanner(stderr)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		h.logger.Info("OneShot[%s][STDERR] %s", taskID, line)
	}
}

// buildEnvironment builds the environment variables for the process.
func (h *OneShotHandler) buildEnvironment() []string {
	env := os.Environ()

	// Prepend virtualenv bin to PATH
	var binDir string
	if runtime.GOOS == "windows" {
		binDir = filepath.Join(h.venvPath, "Scripts")
	} else {
		binDir = filepath.Join(h.venvPath, "bin")
	}

	// Update PATH
	pathUpdated := false
	for i, e := range env {
		if strings.HasPrefix(strings.ToUpper(e), "PATH=") {
			currentPath := strings.SplitN(e, "=", 2)[1]
			env[i] = "PATH=" + binDir + string(os.PathListSeparator) + currentPath
			pathUpdated = true
			break
		}
	}
	if !pathUpdated {
		env = append(env, "PATH="+binDir)
	}

	// Add VIRTUAL_ENV
	env = append(env, "VIRTUAL_ENV="+h.venvPath)

	// Add vault environment variables
	for k, v := range h.vaultVars {
		env = append(env, k+"="+v)
	}

	return env
}

// parseCommand splits a command string into executable and arguments.
func parseCommand(cmd string) []string {
	var args []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for _, r := range cmd {
		switch {
		case r == '"' || r == '\'':
			if inQuote && r == quoteChar {
				inQuote = false
			} else if !inQuote {
				inQuote = true
				quoteChar = r
			} else {
				current.WriteRune(r)
			}
		case r == ' ' && !inQuote:
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}
