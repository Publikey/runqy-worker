package worker

import (
	"context"
	"sync"
	"time"
)

// RecoveryConfig configures the process auto-recovery behavior.
type RecoveryConfig struct {
	Enabled        bool          // Enable auto-recovery (default: true)
	MaxRestarts    int           // Max consecutive restarts before circuit breaker trips (default: 5)
	InitialDelay   time.Duration // Initial delay before first restart attempt (default: 1s)
	MaxDelay       time.Duration // Maximum delay between restart attempts (default: 5m)
	BackoffFactor  float64       // Multiplier for exponential backoff (default: 2.0)
	CooldownPeriod time.Duration // Time without crash to reset consecutive failures (default: 10m)
}

// DefaultRecoveryConfig returns a RecoveryConfig with sensible defaults.
func DefaultRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{
		Enabled:        true,
		MaxRestarts:    5,
		InitialDelay:   1 * time.Second,
		MaxDelay:       5 * time.Minute,
		BackoffFactor:  2.0,
		CooldownPeriod: 10 * time.Minute,
	}
}

// recoveryState represents the current state of the recovery component.
type recoveryState string

const (
	recoveryIdle       recoveryState = "idle"       // Normal operation, watching for crashes
	recoveryRecovering recoveryState = "recovering" // Restart in progress
	recoveryDegraded   recoveryState = "degraded"   // Circuit breaker tripped, no more restarts
)

// processRecovery watches a ProcessSupervisor and automatically restarts
// the process on crash with exponential backoff and circuit breaker.
type processRecovery struct {
	queueName  string
	supervisor *ProcessSupervisor
	config     RecoveryConfig
	logger     Logger

	mu               sync.RWMutex
	state            recoveryState
	consecutiveFails int
	totalRestarts    int
	currentDelay     time.Duration // current backoff delay, reset by cooldown

	onRestart func(sup *ProcessSupervisor) // callback after successful restart
	done      chan struct{}
	wg        sync.WaitGroup
}

// newProcessRecovery creates a new processRecovery watcher.
func newProcessRecovery(queueName string, supervisor *ProcessSupervisor, config RecoveryConfig, logger Logger) *processRecovery {
	return &processRecovery{
		queueName:  queueName,
		supervisor: supervisor,
		config:     config,
		logger:     logger,
		state:      recoveryIdle,
		done:       make(chan struct{}),
	}
}

// SetOnRestart sets the callback invoked after a successful process restart.
// The callback receives the supervisor so it can access the new stdin/stdout pipes.
func (r *processRecovery) SetOnRestart(fn func(sup *ProcessSupervisor)) {
	r.onRestart = fn
}

// Start begins watching the supervisor for crashes.
func (r *processRecovery) Start() {
	r.wg.Add(1)
	go r.watchLoop()
}

// Stop stops the recovery watcher.
func (r *processRecovery) Stop() {
	select {
	case <-r.done:
		// Already stopped
	default:
		close(r.done)
	}
	r.wg.Wait()
}

// State returns the current recovery state.
func (r *processRecovery) State() recoveryState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// ConsecutiveFails returns the current consecutive failure count.
func (r *processRecovery) ConsecutiveFails() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.consecutiveFails
}

// TotalRestarts returns the total number of successful restarts.
func (r *processRecovery) TotalRestarts() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.totalRestarts
}

// watchLoop monitors the supervisor and triggers recovery on crash.
func (r *processRecovery) watchLoop() {
	defer r.wg.Done()

	r.mu.Lock()
	r.currentDelay = r.config.InitialDelay
	r.mu.Unlock()

	for {
		select {
		case <-r.done:
			return
		case <-r.supervisor.Done():
			// Process crashed
			if !r.supervisor.HasCrashed() {
				// Normal exit (e.g., during shutdown), don't recover
				r.logger.Info("[%s] Process exited normally, recovery not needed", r.queueName)
				return
			}

			// Increment consecutive crash counter
			r.mu.Lock()
			r.consecutiveFails++
			fails := r.consecutiveFails
			r.state = recoveryRecovering
			delay := r.currentDelay
			r.mu.Unlock()

			r.logger.Warn("[%s] Process crash detected (consecutive failures: %d/%d)",
				r.queueName, fails, r.config.MaxRestarts)

			// Circuit breaker: check if we've exhausted all restarts
			if fails >= r.config.MaxRestarts {
				r.mu.Lock()
				r.state = recoveryDegraded
				r.mu.Unlock()
				r.logger.Error("[%s] Max restarts reached (%d) — entering degraded state (manual restart required)",
					r.queueName, r.config.MaxRestarts)
				return
			}

			if r.attemptRestart(delay) {
				// Restart succeeded, launch cooldown and continue watching
				r.wg.Add(1)
				go r.cooldownTimer()
				// Increase delay for next potential crash
				r.mu.Lock()
				r.currentDelay = time.Duration(float64(r.currentDelay) * r.config.BackoffFactor)
				if r.currentDelay > r.config.MaxDelay {
					r.currentDelay = r.config.MaxDelay
				}
				r.mu.Unlock()
			} else {
				// Restart failed (either Restart() error or shutdown)
				r.mu.RLock()
				state := r.state
				r.mu.RUnlock()
				if state != recoveryDegraded {
					// Shutdown interrupted, not degraded
					return
				}
				r.logger.Error("[%s] Recovery failed — entering degraded state (manual restart required)", r.queueName)
				return
			}
		}
	}
}

// attemptRestart tries to restart the process with the given delay.
// On failure, it retries with exponential backoff up to MaxRestarts total attempts.
// Returns true if restart succeeded, false if all attempts failed or shutdown.
func (r *processRecovery) attemptRestart(initialDelay time.Duration) bool {
	delay := initialDelay

	// Try up to (MaxRestarts - consecutiveFails) more times for Restart() errors
	r.mu.RLock()
	remaining := r.config.MaxRestarts - r.consecutiveFails + 1
	r.mu.RUnlock()
	if remaining <= 0 {
		remaining = 1
	}

	for attempt := 1; attempt <= remaining; attempt++ {
		r.logger.Info("[%s] Recovery attempt %d (delay: %v)",
			r.queueName, attempt, delay)

		// Wait for delay (interruptible by shutdown)
		select {
		case <-r.done:
			r.logger.Info("[%s] Recovery interrupted by shutdown", r.queueName)
			return false
		case <-time.After(delay):
		}

		// Attempt restart with a generous timeout
		timeout := time.Duration(r.supervisor.TimeoutSec())*time.Second + 30*time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := r.supervisor.Restart(ctx)
		cancel()

		if err != nil {
			r.logger.Error("[%s] Recovery attempt %d failed: %v", r.queueName, attempt, err)

			r.mu.Lock()
			r.consecutiveFails++
			fails := r.consecutiveFails
			r.mu.Unlock()

			// Check circuit breaker after failed restart attempt
			if fails >= r.config.MaxRestarts {
				r.mu.Lock()
				r.state = recoveryDegraded
				r.mu.Unlock()
				return false
			}

			// Increase delay with exponential backoff
			delay = time.Duration(float64(delay) * r.config.BackoffFactor)
			if delay > r.config.MaxDelay {
				delay = r.config.MaxDelay
			}
			continue
		}

		// Restart succeeded
		r.mu.Lock()
		r.totalRestarts++
		r.state = recoveryIdle
		r.mu.Unlock()

		r.logger.Info("[%s] Recovery successful (total restarts: %d)", r.queueName, r.totalRestarts)

		// Notify callback to reconnect StdioHandler
		if r.onRestart != nil {
			r.onRestart(r.supervisor)
		}

		return true
	}

	return false
}

// cooldownTimer resets consecutiveFails to 0 if the process runs
// without crashing for the CooldownPeriod.
func (r *processRecovery) cooldownTimer() {
	defer r.wg.Done()

	r.logger.Debug("[%s] Cooldown timer started (%v)", r.queueName, r.config.CooldownPeriod)

	select {
	case <-r.done:
		return
	case <-r.supervisor.Done():
		// Process crashed again during cooldown, don't reset
		r.logger.Debug("[%s] Process crashed during cooldown, counter not reset", r.queueName)
		return
	case <-time.After(r.config.CooldownPeriod):
		r.mu.Lock()
		old := r.consecutiveFails
		r.consecutiveFails = 0
		r.currentDelay = r.config.InitialDelay
		r.mu.Unlock()
		r.logger.Info("[%s] Cooldown complete, consecutive failures reset (%d → 0)", r.queueName, old)
	}
}
