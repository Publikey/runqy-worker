package bootstrap

import (
	"bufio"
	"context"
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

// ProcessSupervisor manages a supervised Python FastAPI process.
type ProcessSupervisor struct {
	cmd        *exec.Cmd
	deployment *DeploymentResult
	startupCmd string
	envVars    map[string]string
	timeoutSec int
	logger     Logger

	mu        sync.RWMutex
	healthy   bool
	crashed   bool
	crashedAt time.Time
	exitCode  int

	done   chan struct{}
	cancel context.CancelFunc
}

// NewProcessSupervisor creates a new ProcessSupervisor.
func NewProcessSupervisor(deployment *DeploymentResult, spec DeploymentConfig, logger Logger) *ProcessSupervisor {
	timeout := spec.StartupTimeoutSecs
	if timeout <= 0 {
		timeout = 60 // Default 60 seconds
	}

	return &ProcessSupervisor{
		deployment: deployment,
		startupCmd: spec.StartupCmd,
		envVars:    spec.EnvVars,
		timeoutSec: timeout,
		logger:     logger,
		done:       make(chan struct{}),
	}
}

// Start starts the supervised process and waits for it to become healthy.
func (s *ProcessSupervisor) Start(ctx context.Context) error {
	// Parse startup command
	args := ParseCommand(s.startupCmd)
	if len(args) == 0 {
		return fmt.Errorf("empty startup command")
	}

	// Create a cancellable context for the process
	procCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel

	// Build the command
	s.cmd = exec.CommandContext(procCtx, args[0], args[1:]...)
	s.cmd.Dir = s.deployment.RepoPath

	// Build environment
	s.cmd.Env = s.buildEnvironment()

	// Create pipes for stdout/stderr
	stdout, err := s.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := s.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the process
	s.logger.Info("Starting process: %s", s.startupCmd)
	s.logger.Info("Working directory: %s", s.deployment.RepoPath)

	if err := s.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start process: %w", err)
	}

	s.logger.Info("Process started with PID %d", s.cmd.Process.Pid)

	// Channel to signal when startup phrase is detected
	startupDetected := make(chan struct{})
	startupOnce := sync.Once{}

	// Start log streaming goroutines
	go s.streamLogs(stdout, "STDOUT", startupDetected, &startupOnce)
	go s.streamLogs(stderr, "STDERR", startupDetected, &startupOnce)

	// Start process monitor goroutine
	go s.monitorProcess()

	// Wait for startup with timeout
	startupTimeout := time.Duration(s.timeoutSec) * time.Second
	s.logger.Info("Waiting for startup (timeout: %v)...", startupTimeout)

	select {
	case <-startupDetected:
		s.mu.Lock()
		s.healthy = true
		s.mu.Unlock()
		s.logger.Info("Process startup detected - service is healthy")
		return nil

	case <-time.After(startupTimeout):
		s.logger.Error("Startup timeout exceeded (%v)", startupTimeout)
		s.Stop()
		return fmt.Errorf("startup timeout exceeded after %v", startupTimeout)

	case <-s.done:
		s.mu.RLock()
		exitCode := s.exitCode
		s.mu.RUnlock()
		return fmt.Errorf("process exited during startup with code %d", exitCode)

	case <-ctx.Done():
		s.Stop()
		return ctx.Err()
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

	// Add custom environment variables from spec
	for k, v := range s.envVars {
		env = append(env, k+"="+v)
	}

	return env
}

// streamLogs reads from a pipe and logs output, detecting startup phrases.
func (s *ProcessSupervisor) streamLogs(pipe io.ReadCloser, prefix string, startupDetected chan struct{}, once *sync.Once) {
	scanner := bufio.NewScanner(pipe)
	// Increase buffer size for long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		s.logger.Info("[%s] %s", prefix, line)

		// Check for startup indicators
		if s.isStartupIndicator(line) {
			once.Do(func() {
				close(startupDetected)
			})
		}
	}

	if err := scanner.Err(); err != nil {
		s.logger.Warn("Error reading %s: %v", prefix, err)
	}
}

// isStartupIndicator checks if a log line indicates successful startup.
func (s *ProcessSupervisor) isStartupIndicator(line string) bool {
	indicators := []string{
		"Uvicorn running",
		"Application startup complete",
		"Started server process",
		"Waiting for application startup",
	}

	lineLower := strings.ToLower(line)
	for _, indicator := range indicators {
		if strings.Contains(lineLower, strings.ToLower(indicator)) {
			return true
		}
	}
	return false
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
