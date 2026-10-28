package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// processRestarter restarts a long_running process whose task hit its active timeout.
//
// The Python runtime handles tasks one at a time and cannot be interrupted, so after a
// timeout it would keep computing the abandoned task while the next ones wait behind it
// on stdin (their own timeout ticking). Restarting the process stops that computation;
// the worker slot stays busy until the new process is ready.
type processRestarter struct {
	mu  sync.Mutex
	gen uint64 // incremented on every restart done here
}

// generation returns the current restart generation (taken before sending a task).
func (r *processRestarter) generation() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gen
}

// restartAfterTimeout restarts the process unless it was already restarted since gen
// (concurrent timeouts on the same process restart it once).
func (r *processRestarter) restartAfterTimeout(gen uint64, state *QueueState, taskID string, logger Logger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gen != gen {
		return
	}
	r.gen++

	logger.Warn("[%s] Task %s timed out — restarting the process to stop it", state.Name, taskID)
	start := time.Now()
	var err error
	if state.Recovery != nil {
		err = state.Recovery.RestartNow()
	} else {
		timeout := time.Duration(state.Supervisor.TimeoutSec())*time.Second + 30*time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err = state.Supervisor.Restart(ctx)
		cancel()
		if err == nil {
			state.StdioHandler.Reconnect(state.Supervisor.Stdin(), state.Supervisor.Stdout())
		}
	}
	if err != nil {
		logger.Error("[%s] Restart after timeout of task %s failed: %v", state.Name, taskID, err)
		return
	}
	logger.Info("[%s] Process restarted after timeout of task %s (%v)", state.Name, taskID, time.Since(start).Truncate(time.Millisecond))
}

// longRunningHandler sends tasks to the supervised process and restarts it when a task
// exceeds its active timeout.
func longRunningHandler(state *QueueState, logger Logger) HandlerFunc {
	return func(ctx context.Context, task *Task) error {
		gen := state.restarter.generation()
		err := state.StdioHandler.ProcessTask(ctx, task)
		// DeadlineExceeded is the task's own active timeout; a worker shutdown cancels instead.
		if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			state.restarter.restartAfterTimeout(gen, state, task.ID(), logger)
			return fmt.Errorf("%w (process restarted)", err)
		}
		return err
	}
}
