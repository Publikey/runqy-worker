package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestProcessSupervisor_Restart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tempDir, err := os.MkdirTemp("", "restart-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	pythonBin := "python3"
	if runtime.GOOS == "windows" {
		pythonBin = "python"
	}

	// Script that sends ready, then reads from stdin
	script := `import sys, json
sys.stdout.write(json.dumps({"status": "ready"}) + "\n")
sys.stdout.flush()
for line in sys.stdin:
    pass
`
	scriptPath := filepath.Join(tempDir, "test_task.py")
	if err := os.WriteFile(scriptPath, []byte(script), 0644); err != nil {
		t.Fatalf("failed to write script: %v", err)
	}

	deployment := &DeploymentResult{
		RepoPath:   tempDir,
		CodePath:   tempDir,
		VenvPath:   tempDir,
		VenvPython: pythonBin,
	}
	spec := DeploymentConfig{
		StartupCmd:         pythonBin + " " + scriptPath,
		StartupTimeoutSecs: 10,
	}

	logger := newTestLogger()
	supervisor := NewProcessSupervisor(deployment, spec, nil, logger)

	ctx := context.Background()

	// Start
	if err := supervisor.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if !supervisor.IsHealthy() {
		t.Fatal("expected healthy after Start")
	}
	if supervisor.RestartCount() != 0 {
		t.Errorf("RestartCount = %d, want 0", supervisor.RestartCount())
	}

	// Restart
	if err := supervisor.Restart(ctx); err != nil {
		t.Fatalf("Restart failed: %v", err)
	}
	if !supervisor.IsHealthy() {
		t.Fatal("expected healthy after Restart")
	}
	if supervisor.RestartCount() != 1 {
		t.Errorf("RestartCount = %d, want 1", supervisor.RestartCount())
	}
	if supervisor.LastRestart().IsZero() {
		t.Error("LastRestart should not be zero after restart")
	}

	// New Done() channel should be open (not closed)
	select {
	case <-supervisor.Done():
		t.Error("Done() channel should not be closed after successful restart")
	default:
		// Good
	}

	// New pipes should work (non-nil)
	if supervisor.Stdin() == nil {
		t.Error("Stdin should not be nil after restart")
	}
	if supervisor.Stdout() == nil {
		t.Error("Stdout should not be nil after restart")
	}

	// Stop
	if err := supervisor.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

func TestProcessSupervisor_RestartAfterCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tempDir, err := os.MkdirTemp("", "restart-crash-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	pythonBin := "python3"
	if runtime.GOOS == "windows" {
		pythonBin = "python"
	}

	// Script that sends ready then exits immediately (simulates crash)
	crashScript := `import sys, json
sys.stdout.write(json.dumps({"status": "ready"}) + "\n")
sys.stdout.flush()
sys.exit(1)
`
	crashPath := filepath.Join(tempDir, "crash.py")
	if err := os.WriteFile(crashPath, []byte(crashScript), 0644); err != nil {
		t.Fatalf("failed to write crash script: %v", err)
	}

	// Script that stays alive (used after restart)
	stableScript := `import sys, json
sys.stdout.write(json.dumps({"status": "ready"}) + "\n")
sys.stdout.flush()
for line in sys.stdin:
    pass
`
	stablePath := filepath.Join(tempDir, "stable.py")
	if err := os.WriteFile(stablePath, []byte(stableScript), 0644); err != nil {
		t.Fatalf("failed to write stable script: %v", err)
	}

	deployment := &DeploymentResult{
		RepoPath:   tempDir,
		CodePath:   tempDir,
		VenvPath:   tempDir,
		VenvPython: pythonBin,
	}
	spec := DeploymentConfig{
		StartupCmd:         pythonBin + " " + crashPath,
		StartupTimeoutSecs: 10,
	}

	logger := newTestLogger()
	supervisor := NewProcessSupervisor(deployment, spec, nil, logger)

	ctx := context.Background()

	// Start with crash script
	if err := supervisor.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for crash
	select {
	case <-supervisor.Done():
		// Process crashed
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for process to crash")
	}

	if !supervisor.HasCrashed() {
		t.Error("expected HasCrashed=true")
	}
	if supervisor.IsHealthy() {
		t.Error("expected IsHealthy=false after crash")
	}

	// Change startup command to stable script for restart
	supervisor.startupCmd = pythonBin + " " + stablePath

	// Restart should work even after crash
	if err := supervisor.Restart(ctx); err != nil {
		t.Fatalf("Restart after crash failed: %v", err)
	}

	if !supervisor.IsHealthy() {
		t.Error("expected IsHealthy=true after restart")
	}
	if supervisor.HasCrashed() {
		t.Error("expected HasCrashed=false after successful restart")
	}
	if supervisor.RestartCount() != 1 {
		t.Errorf("RestartCount = %d, want 1", supervisor.RestartCount())
	}

	supervisor.Stop()
}

func TestProcessSupervisor_TimeoutSec(t *testing.T) {
	deployment := &DeploymentResult{}
	spec := DeploymentConfig{
		StartupTimeoutSecs: 120,
	}
	supervisor := NewProcessSupervisor(deployment, spec, nil, newTestLogger())
	if supervisor.TimeoutSec() != 120 {
		t.Errorf("TimeoutSec() = %d, want 120", supervisor.TimeoutSec())
	}
}

func TestProcessSupervisor_MultipleRestarts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	tempDir, err := os.MkdirTemp("", "multi-restart-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	pythonBin := "python3"
	if runtime.GOOS == "windows" {
		pythonBin = "python"
	}

	script := `import sys, json
sys.stdout.write(json.dumps({"status": "ready"}) + "\n")
sys.stdout.flush()
for line in sys.stdin:
    pass
`
	scriptPath := filepath.Join(tempDir, "task.py")
	if err := os.WriteFile(scriptPath, []byte(script), 0644); err != nil {
		t.Fatalf("failed to write script: %v", err)
	}

	deployment := &DeploymentResult{
		RepoPath:   tempDir,
		CodePath:   tempDir,
		VenvPath:   tempDir,
		VenvPython: pythonBin,
	}
	spec := DeploymentConfig{
		StartupCmd:         pythonBin + " " + scriptPath,
		StartupTimeoutSecs: 10,
	}

	logger := newTestLogger()
	supervisor := NewProcessSupervisor(deployment, spec, nil, logger)
	ctx := context.Background()

	if err := supervisor.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Restart 3 times
	for i := 1; i <= 3; i++ {
		if err := supervisor.Restart(ctx); err != nil {
			t.Fatalf("Restart #%d failed: %v", i, err)
		}
		if supervisor.RestartCount() != i {
			t.Errorf("after restart #%d: RestartCount = %d, want %d", i, supervisor.RestartCount(), i)
		}
		if !supervisor.IsHealthy() {
			t.Errorf("after restart #%d: expected healthy", i)
		}
	}

	supervisor.Stop()
}
