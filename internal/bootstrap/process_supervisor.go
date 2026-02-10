package bootstrap

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
	"sync"
	"time"
)

// ProcessSupervisor manages a supervised child process.
type ProcessSupervisor struct {
	cmd        *exec.Cmd
	deployment *DeploymentResult
	startupCmd string
	vaultVars  map[string]string // Vault entries to inject as environment variables
	timeoutSec int
	logger     Logger

	// Pipes for stdio communication
	stdin        io.WriteCloser
	stdout       io.ReadCloser
	stdoutReader *bufio.Reader // Buffered reader for stdout (preserves buffered data)

	logBuffer LogAppender // optional log buffer for capturing stderr/stdout

	mu        sync.RWMutex
	healthy   bool
	crashed   bool
	crashedAt time.Time
	exitCode  int

	done   chan struct{}
	cancel context.CancelFunc
}

// NewProcessSupervisor creates a new ProcessSupervisor.
func NewProcessSupervisor(deployment *DeploymentResult, spec DeploymentConfig, vaultVars map[string]string, logger Logger) *ProcessSupervisor {
	timeout := spec.StartupTimeoutSecs
	if timeout <= 0 {
		timeout = 60 // Default 60 seconds
	}

	return &ProcessSupervisor{
		deployment: deployment,
		startupCmd: spec.StartupCmd,
		vaultVars:  vaultVars,
		timeoutSec: timeout,
		logger:     logger,
		done:       make(chan struct{}),
	}
}

// Start starts the supervised process and waits for it to become healthy.
// The process signals readiness by writing {"status":"ready"} to stdout.
func (s *ProcessSupervisor) Start(ctx context.Context) error {
	// Parse startup command
	args := ParseCommand(s.startupCmd)
	if len(args) == 0 {
		return fmt.Errorf("empty startup command")
	}

	// Replace python/python3 with the venv python to avoid Windows PATH issues
	if len(args) > 0 && (args[0] == "python" || args[0] == "python3" || args[0] == "python.exe" || args[0] == "python3.exe") {
		args[0] = s.deployment.VenvPython
		s.logger.Debug("Using venv python: %s", args[0])
	}

	// Create a cancellable context for the process
	procCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	// Build the command
	s.cmd = exec.CommandContext(procCtx, args[0], args[1:]...)
	s.cmd.Dir = s.deployment.CodePath

	// Build environment
	s.cmd.Env = s.buildEnvironment()

	// Create pipes for stdin/stdout/stderr
	stdin, err := s.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}
	s.stdin = stdin

	stdout, err := s.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}
	s.stdout = stdout
	// Create buffered reader - any data buffered during startup remains available
	s.stdoutReader = bufio.NewReaderSize(stdout, 64*1024)

	stderr, err := s.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the process
	s.logger.Info("Starting process: %s", s.startupCmd)
	s.logger.Info("Working directory: %s", s.deployment.CodePath)

	if err := s.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start process: %w", err)
	}

	s.logger.Info("Process started with PID %d", s.cmd.Process.Pid)

	// Stream stderr to logs (stderr is for logging, stdout is for communication)
	go s.streamStderr(stderr)

	// Start process monitor goroutine
	go s.monitorProcess()

	// Wait for startup with timeout - look for {"status":"ready"} on stdout
	startupTimeout := time.Duration(s.timeoutSec) * time.Second
	s.logger.Info("Waiting for startup (timeout: %v)...", startupTimeout)

	if err := s.waitForReady(ctx, startupTimeout); err != nil {
		s.Stop()
		return err
	}

	s.mu.Lock()
	s.healthy = true
	s.mu.Unlock()
	s.logger.Info("Process startup detected - service is ready")
	return nil
}

// waitForReady waits for the child to send {"status":"ready"} on stdout.
// Uses the buffered reader so any extra data remains available for StdioHandler.
func (s *ProcessSupervisor) waitForReady(ctx context.Context, timeout time.Duration) error {
	readyCh := make(chan struct{})
	errCh := make(chan error, 1)

	// Read stdout looking for ready signal using the buffered reader
	go func() {
		for {
			line, err := s.stdoutReader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					errCh <- fmt.Errorf("stdout closed before ready signal received")
				} else {
					errCh <- fmt.Errorf("error reading stdout: %w", err)
				}
				return
			}

			// Trim newline
			line = strings.TrimSuffix(line, "\n")
			line = strings.TrimSuffix(line, "\r")

			s.logger.Debug("[STDOUT] %s", line)

			// Try to parse as JSON status message
			var status struct {
				Status string `json:"status"`
			}
			if err := json.Unmarshal([]byte(line), &status); err == nil {
				if status.Status == "ready" {
					close(readyCh)
					return
				}
			}
		}
	}()

	select {
	case <-readyCh:
		return nil

	case err := <-errCh:
		return err

	case <-time.After(timeout):
		return fmt.Errorf("startup timeout exceeded after %v", timeout)

	case <-s.done:
		s.mu.RLock()
		exitCode := s.exitCode
		s.mu.RUnlock()
		return fmt.Errorf("process exited during startup with code %d", exitCode)

	case <-ctx.Done():
		return ctx.Err()
	}
}

// SetLogBuffer sets the log buffer for capturing stderr and stdout output.
func (s *ProcessSupervisor) SetLogBuffer(buf LogAppender) {
	s.logBuffer = buf
}

// streamStderr reads stderr and logs output.
func (s *ProcessSupervisor) streamStderr(stderr io.ReadCloser) {
	scanner := bufio.NewScanner(stderr)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		s.logger.Info("[STDERR] %s", line)
		if s.logBuffer != nil {
			s.logBuffer.Append("stderr", line)
		}
	}

	if err := scanner.Err(); err != nil {
		s.logger.Warn("Error reading stderr: %v", err)
	}
}

// buildEnvironment builds the environment variables for the process.
func (s *ProcessSupervisor) buildEnvironment() []string {
	env := os.Environ()

	// Prepend virtualenv bin to PATH
	var binDir string
	if runtime.GOOS == "windows" {
		binDir = filepath.Join(s.deployment.VenvPath, "Scripts")
	} else {
		binDir = filepath.Join(s.deployment.VenvPath, "bin")
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
	env = append(env, "VIRTUAL_ENV="+s.deployment.VenvPath)

	// Add vault environment variables
	for k, v := range s.vaultVars {
		env = append(env, k+"="+v)
	}

	return env
}

// Stdin returns the stdin pipe for writing to the child process.
func (s *ProcessSupervisor) Stdin() io.Writer {
	return s.stdin
}

// Stdout returns the buffered stdout reader for reading from the child process.
// This is a buffered reader that preserves any data read during startup detection.
func (s *ProcessSupervisor) Stdout() io.Reader {
	return s.stdoutReader
}

// monitorProcess waits for the process to exit and updates state.
func (s *ProcessSupervisor) monitorProcess() {
	err := s.cmd.Wait()

	s.mu.Lock()
	s.healthy = false
	s.crashed = true
	s.crashedAt = time.Now()

	if s.cmd.ProcessState != nil {
		s.exitCode = s.cmd.ProcessState.ExitCode()
	}
	s.mu.Unlock()

	if err != nil {
		s.logger.Error("Process exited with error: %v (exit code: %d)", err, s.exitCode)
	} else {
		s.logger.Warn("Process exited normally (exit code: %d)", s.exitCode)
	}

	close(s.done)
}

// IsHealthy returns true if the supervised process is running and healthy.
func (s *ProcessSupervisor) IsHealthy() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.healthy && !s.crashed
}

// HasCrashed returns true if the process has crashed.
func (s *ProcessSupervisor) HasCrashed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.crashed
}

// CrashedAt returns when the process crashed.
func (s *ProcessSupervisor) CrashedAt() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.crashedAt
}

// Stop stops the supervised process gracefully.
func (s *ProcessSupervisor) Stop() error {
	if s.cmd == nil || s.cmd.Process == nil {
		return nil
	}

	s.logger.Info("Stopping supervised process (PID %d)...", s.cmd.Process.Pid)

	// Cancel the context to signal shutdown
	if s.cancel != nil {
		s.cancel()
	}

	// On Windows, we can only kill (no SIGTERM)
	// On Unix, the context cancellation sends SIGKILL by default
	// We'll give it a grace period then force kill
	gracePeriod := 10 * time.Second

	select {
	case <-s.done:
		s.logger.Info("Process stopped gracefully")
		return nil

	case <-time.After(gracePeriod):
		s.logger.Warn("Grace period exceeded, forcing kill...")
		if err := s.cmd.Process.Kill(); err != nil {
			s.logger.Error("Failed to kill process: %v", err)
			return err
		}
	}

	// Wait for done signal after kill
	select {
	case <-s.done:
		s.logger.Info("Process killed")
	case <-time.After(5 * time.Second):
		s.logger.Error("Process did not terminate after kill")
	}

	return nil
}

// Done returns a channel that's closed when the process exits.
func (s *ProcessSupervisor) Done() <-chan struct{} {
	return s.done
}

// ParseCommand splits a command string into executable and arguments.
// Handles quoted strings for arguments with spaces.
func ParseCommand(cmd string) []string {
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
