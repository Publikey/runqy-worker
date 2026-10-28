package worker

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// sequentialSleeperScript mimics runqy-python's loop: one task at a time, a task whose
// payload has "sleep" blocks that long before answering.
func sequentialSleeperScript(t *testing.T, dir string) (string, string) {
	t.Helper()
	script := `import sys, time, json
sys.stdout.write(json.dumps({"status": "ready"}) + "\n")
sys.stdout.flush()
for line in sys.stdin:
    obj = json.loads(line)
    time.sleep((obj.get("payload") or {}).get("sleep", 0))
    sys.stdout.write(json.dumps({"task_id": obj["task_id"], "result": {"ok": True}}) + "\n")
    sys.stdout.flush()
`
	path := filepath.Join(dir, "sleeper.py")
	if err := os.WriteFile(path, []byte(script), 0644); err != nil {
		t.Fatal(err)
	}
	py := "python3"
	if runtime.GOOS == "windows" {
		py = "python"
	}
	return path, py
}

func newLongRunningState(t *testing.T, withRecovery bool) *QueueState {
	t.Helper()
	dir := t.TempDir()
	script, py := sequentialSleeperScript(t, dir)
	logger := NewStdLogger()
	sup := newProcessSupervisor(
		&DeploymentResult{RepoPath: dir, CodePath: dir, VenvPath: dir, VenvPython: py},
		BootstrapDeploymentConfig{StartupCmd: py + " " + script, StartupTimeoutSecs: 10},
		nil, logger)
	if err := sup.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	state := &QueueState{Name: "video", Supervisor: sup, Mode: "long_running"}
	state.StdioHandler = NewStdioHandler(sup.Stdin(), sup.Stdout(), logger, false)
	state.StdioHandler.Start()
	// Cleanups run LIFO: recovery (registered below) stops first, then the process, then the
	// stdout reader (which only ends once the process exits).
	t.Cleanup(func() {
		_ = state.Supervisor.Stop() // first: the stdout reader only ends once the process exits
		state.StdioHandler.Stop()
	})
	if withRecovery {
		rec := newProcessRecovery("video", sup, RecoveryConfig{
			Enabled: true, MaxRestarts: 3, InitialDelay: 100 * time.Millisecond,
			MaxDelay: time.Second, BackoffFactor: 2, CooldownPeriod: time.Minute,
		}, logger)
		rec.SetOnRestart(func(s *ProcessSupervisor) { state.StdioHandler.Reconnect(s.Stdin(), s.Stdout()) })
		rec.Start()
		state.Recovery = rec
		t.Cleanup(rec.Stop)
	}
	return state
}

// After an active timeout the abandoned computation must not block the next task.
func testTimeoutRestartsProcess(t *testing.T, withRecovery bool) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	state := newLongRunningState(t, withRecovery)
	handle := longRunningHandler(state, NewStdLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	err := handle(ctx, &Task{id: "slow", payload: []byte(`{"sleep": 30}`), queue: "video.default"})
	cancel()
	if err == nil || !strings.Contains(err.Error(), "process restarted") {
		t.Fatalf("slow task: err = %v, want a timeout with restart", err)
	}

	start := time.Now()
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := handle(ctx, &Task{id: "fast", payload: []byte(`{}`), queue: "video.default"}); err != nil {
		t.Fatalf("fast task after restart: %v", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("fast task took %v: still queued behind the abandoned one", d)
	}
	if state.Supervisor.RestartCount() != 1 {
		t.Fatalf("RestartCount = %d, want 1", state.Supervisor.RestartCount())
	}
	if withRecovery {
		if s := state.Recovery.State(); s != recoveryIdle {
			t.Fatalf("recovery state = %s, want idle", s)
		}
		if n := state.Recovery.ConsecutiveFails(); n != 0 {
			t.Fatalf("planned restart counted as a crash (ConsecutiveFails = %d)", n)
		}
	}
}

func TestTimeoutRestartsProcess_WithRecovery(t *testing.T)    { testTimeoutRestartsProcess(t, true) }
func TestTimeoutRestartsProcess_WithoutRecovery(t *testing.T) { testTimeoutRestartsProcess(t, false) }

// A task cancelled by a worker shutdown (not its deadline) must not restart the process.
func TestShutdownCancelDoesNotRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	state := newLongRunningState(t, true)
	handle := longRunningHandler(state, NewStdLogger())

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(300*time.Millisecond, cancel)
	if err := handle(ctx, &Task{id: "slow", payload: []byte(`{"sleep": 2}`), queue: "video.default"}); err == nil {
		t.Fatal("expected a cancellation error")
	}
	if n := state.Supervisor.RestartCount(); n != 0 {
		t.Fatalf("RestartCount = %d, want 0", n)
	}
}
