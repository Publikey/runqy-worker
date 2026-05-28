package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// processor handles dequeuing and executing tasks.
type processor struct {
	redis            *redisClient
	redisMu          sync.RWMutex           // protects redis client during reconnection
	handler          Handler                // fallback handler (ServeMux for type-based routing)
	queueHandlers    map[string]HandlerFunc // queue name -> handler (for queue-based routing)
	config           Config
	queues           []string               // Queue names in priority order
	queueStates      map[string]*QueueState // supervised processes per queue (may be nil)
	subQueueToState  map[string]*QueueState // sub-queue name -> parent queue state
	reconnector      *redisReconnector      // for connection recovery

	done      chan struct{}
	stopping  chan struct{} // closed to signal "stop dequeuing" (graceful drain)
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	logger    Logger

	// In-flight tasks owned by this processor (taskID -> sub-queue name).
	// Used to renew leases while processing and to requeue leftovers on shutdown.
	inflight   map[string]string
	inflightMu sync.Mutex

	// Lease extender lifecycle.
	leaseDone chan struct{}
	leaseWg   sync.WaitGroup
}

// newProcessor creates a new processor.
func newProcessor(rdb *redisClient, handler Handler, cfg Config) *processor {
	// Build queue list based on weights (higher weight = appears more in list)
	queues := buildQueueList(cfg.Queues)

	return &processor{
		redis:         rdb,
		handler:       handler,
		queueHandlers: make(map[string]HandlerFunc),
		config:        cfg,
		queues:        queues,
		done:          make(chan struct{}),
		stopping:      make(chan struct{}),
		logger:        cfg.Logger,
		inflight:      make(map[string]string),
		leaseDone:     make(chan struct{}),
	}
}

// trackInflight records a task as in-flight (owned by this processor).
func (p *processor) trackInflight(taskID, queueName string) {
	p.inflightMu.Lock()
	p.inflight[taskID] = queueName
	p.inflightMu.Unlock()
}

// untrackInflight removes a task from the in-flight set.
func (p *processor) untrackInflight(taskID string) {
	p.inflightMu.Lock()
	delete(p.inflight, taskID)
	p.inflightMu.Unlock()
}

// snapshotInflight returns a copy of the current in-flight tasks (taskID -> queue).
func (p *processor) snapshotInflight() map[string]string {
	p.inflightMu.Lock()
	defer p.inflightMu.Unlock()
	out := make(map[string]string, len(p.inflight))
	for k, v := range p.inflight {
		out[k] = v
	}
	return out
}

// SetQueueHandler sets the handler for a specific queue.
func (p *processor) SetQueueHandler(queue string, handler HandlerFunc) {
	p.queueHandlers[queue] = handler
}

// SetQueueStates sets the queue states for per-queue health checks.
func (p *processor) SetQueueStates(queueStates map[string]*QueueState) {
	p.queueStates = queueStates
	// Build reverse lookup from sub-queue names to parent queue state
	p.subQueueToState = make(map[string]*QueueState)
	for _, state := range queueStates {
		for _, sq := range state.SubQueues {
			p.subQueueToState[sq.Name] = state
		}
	}
}

// SetReconnector sets the reconnection manager for the processor.
func (p *processor) SetReconnector(r *redisReconnector) {
	p.reconnector = r
}

// updateClient updates the Redis client after reconnection.
func (p *processor) updateClient(client *redis.Client, logger Logger) {
	p.redisMu.Lock()
	defer p.redisMu.Unlock()
	p.redis = newRedisClient(client, logger)
}

// buildQueueList creates a weighted list of queues for round-robin selection.
func buildQueueList(queueWeights map[string]int) []string {
	var result []string
	for queue, weight := range queueWeights {
		for i := 0; i < weight; i++ {
			result = append(result, queue)
		}
	}
	return result
}

// Note: parentQueueName is defined in worker.go and shared by both files.

// start begins processing tasks with the configured concurrency.
func (p *processor) start() {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	for i := 0; i < p.config.Concurrency; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
	// Renew leases of in-flight tasks so a live worker keeps ownership while a
	// dead worker's leases expire and become recoverable.
	p.leaseWg.Add(1)
	go p.leaseExtenderLoop()
}

// leaseExtenderLoop periodically renews the leases of all in-flight tasks.
func (p *processor) leaseExtenderLoop() {
	defer p.leaseWg.Done()

	interval := p.config.LeaseExtendInterval
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.leaseDone:
			return
		case <-ticker.C:
			p.extendInflightLeases()
		}
	}
}

// extendInflightLeases renews the lease for every currently in-flight task.
func (p *processor) extendInflightLeases() {
	snapshot := p.snapshotInflight()
	if len(snapshot) == 0 {
		return
	}

	p.redisMu.RLock()
	rdb := p.redis
	p.redisMu.RUnlock()

	leaseDuration := p.config.LeaseDuration
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for taskID, queueName := range snapshot {
		if err := rdb.extendLease(ctx, taskID, queueName, leaseDuration); err != nil {
			p.logger.Warn("Failed to extend lease for task %s (queue=%s): %v", taskID, queueName, err)
		}
	}
}

// stop gracefully stops all workers.
// First stops dequeuing, then waits for in-flight tasks up to shutdown_timeout,
// then cancels any remaining tasks.
func (p *processor) stop() {
	// Phase 1: Signal workers to stop dequeuing new tasks
	close(p.stopping)

	// Phase 2: Wait for in-flight tasks to finish (up to shutdown_timeout)
	timeout := p.config.ShutdownTimeout
	if timeout <= 0 {
		timeout = 8 * time.Second
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All workers finished gracefully
	case <-time.After(timeout):
		// Phase 3: Force cancel remaining tasks
		p.logger.Warn("Shutdown timeout reached, cancelling remaining tasks")
		p.cancel()
		p.wg.Wait()
	}

	// Phase 4: Stop the lease extender now that no task goroutines remain.
	// Any task left in-flight here was force-cancelled; the caller requeues it
	// via snapshotInflight(), and its lease will no longer be renewed.
	close(p.leaseDone)
	p.leaseWg.Wait()
}

// worker is a single worker goroutine.
func (p *processor) worker(id int) {
	defer p.wg.Done()

	p.logger.Info(fmt.Sprintf("Worker %d started", id))
	for {
		// Check if we should stop dequeuing (graceful drain)
		select {
		case <-p.stopping:
			p.logger.Info(fmt.Sprintf("Worker %d stopping (graceful drain)", id))
			return
		default:
		}

		p.processOne(p.ctx)
		if p.ctx.Err() != nil {
			p.logger.Info(fmt.Sprintf("Worker %d stopping", id))
			return
		}
	}
}

// processOne attempts to dequeue and process a single task.
func (p *processor) processOne(ctx context.Context) {
	// Get redis client with read lock
	p.redisMu.RLock()
	redisClient := p.redis
	p.redisMu.RUnlock()

	// Dequeue a task
	task, err := redisClient.dequeue(ctx, p.queues, p.config.LeaseDuration)
	if err != nil {
		p.logger.Error("Dequeue error: %v", err)

		// Check if this is a connection error and handle reconnection
		if p.reconnector != nil {
			if p.reconnector.handleError(err) {
				// Reconnection was attempted, back off longer
				time.Sleep(5 * time.Second)
			} else {
				time.Sleep(time.Second) // Back off on error
			}
		} else {
			p.logger.Debug("No reconnector configured, cannot auto-recover")
			time.Sleep(time.Second) // Back off on error
		}
		return
	}

	if task == nil {
		// No task available, will retry after BRPOP timeout
		// Note: Don't reset error count here - a successful BRPOP timeout
		// doesn't mean the connection is healthy for write operations
		return
	}

	// Reset consecutive errors only when we actually dequeue a task
	if p.reconnector != nil {
		p.reconnector.resetErrors()
	}

	queueName := task.queue

	// Track as in-flight: the lease extender renews its lease while it runs, and
	// graceful shutdown can requeue it if it doesn't finish. Untracked by the
	// terminal handlers (handleSuccess/handleError) below.
	p.trackInflight(task.id, queueName)

	// Extract parent queue for supervisor health check (sub-queue -> parent)
	parentQueue := parentQueueName(queueName)

	// Check if parent queue's supervisor is healthy
	if state, ok := p.queueStates[parentQueue]; ok {
		if state.Supervisor != nil && !state.Supervisor.IsHealthy() {
			if state.Recovery != nil {
				switch state.Recovery.State() {
				case recoveryRecovering:
					p.logger.Info("Queue %s recovering — restart attempt in progress, task will retry", parentQueue)
				case recoveryDegraded:
					p.logger.Error("Queue %s in degraded state — all recovery attempts exhausted, manual restart required", parentQueue)
				default:
					p.logger.Error("Queue %s is unhealthy — supervised process has crashed", parentQueue)
				}
			} else {
				p.logger.Error("Queue %s is in degraded state — supervised process has crashed (no auto-recovery)", parentQueue)
			}
			// Fail the task so it can be retried later
			p.handleError(ctx, task, queueName, fmt.Errorf("queue %s supervisor crashed", parentQueue))
			return
		}
	}

	p.logger.Info(fmt.Sprintf("Processing task %s (queue=%s, type=%s, retry=%d)", task.id, queueName, task.typename, task.retry))

	// Process the task - try queue handler first, then fall back to type-based handler
	if handler, ok := p.queueHandlers[queueName]; ok {
		err = handler(ctx, task)
	} else if p.handler != nil {
		err = p.handler.ProcessTask(ctx, task)
	} else {
		err = fmt.Errorf("no handler configured for queue %q", queueName)
	}

	if err != nil {
		// If the processor is shutting down (context cancelled), the error is just
		// cancellation, not a real task failure. Leave the task in-flight (still in
		// active/lease) so the graceful-shutdown requeue moves it back to pending
		// without counting a retry.
		if ctx.Err() != nil {
			return
		}
		p.handleError(ctx, task, queueName, err)
	} else {
		p.handleSuccess(ctx, task, queueName)
	}
}

// redisRetry retries an operation up to 3 times with backoff.
func (p *processor) redisRetry(operation string, taskID string, fn func() error) error {
	delays := []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, 1 * time.Second}
	var lastErr error
	for attempt, delay := range delays {
		if err := fn(); err != nil {
			lastErr = err
			p.logger.Warn("Redis %s failed for task %s (attempt %d/3): %v", operation, taskID, attempt+1, err)
			time.Sleep(delay)
			continue
		}
		return nil
	}
	p.logger.Error("Redis %s permanently failed for task %s after 3 attempts: %v — manual investigation required", operation, taskID, lastErr)
	return lastErr
}

// handleSuccess handles successful task completion.
func (p *processor) handleSuccess(ctx context.Context, task *Task, queueName string) {
	defer p.untrackInflight(task.id)

	// Use a detached context so completion bookkeeping persists even if the
	// processor context was cancelled during shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Check if redis storage is enabled for this queue
	storeInRedis := true
	if state, ok := p.subQueueToState[queueName]; ok {
		storeInRedis = state.RedisStorage
	}

	if storeInRedis {
		p.redisRetry("complete", task.id, func() error {
			return p.redis.complete(ctx, task, queueName, p.config.CompletedTaskTTL)
		})
	} else {
		// Just clean up the active queue without storing completion
		p.redisRetry("cleanupActive", task.id, func() error {
			return p.redis.cleanupActive(ctx, task, queueName)
		})
	}
	if err := p.redis.incrementProcessed(ctx, queueName); err != nil {
		p.logger.Error("Failed to increment processed counter: %v", err)
	}
	p.logger.Info(fmt.Sprintf("Task %s completed successfully", task.id))
}

// handleError handles task processing errors.
func (p *processor) handleError(ctx context.Context, task *Task, queueName string, err error) {
	defer p.untrackInflight(task.id)

	p.logger.Error(fmt.Sprintf("Task %s failed: %v", task.id, err))

	// Use a detached context so retry/fail bookkeeping persists even if the
	// processor context was cancelled during shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Check if redis storage is enabled for this queue
	storeInRedis := true
	if state, ok := p.subQueueToState[queueName]; ok {
		storeInRedis = state.RedisStorage
	}

	// Check for permanent failure (skip retry)
	if IsSkipRetry(err) {
		p.logger.Warn(fmt.Sprintf("Task %s marked as permanent failure, skipping retry", task.id))
		if storeInRedis {
			p.redisRetry("fail", task.id, func() error {
				return p.redis.fail(ctx, task, queueName, err.Error(), p.config.CompletedTaskTTL)
			})
		} else {
			p.redisRetry("cleanupActive", task.id, func() error {
				return p.redis.cleanupActive(ctx, task, queueName)
			})
		}
		if incrErr := p.redis.incrementFailed(ctx, queueName); incrErr != nil {
			p.logger.Error("Failed to increment failed counter: %v", incrErr)
		}
		return
	}

	// Check if we should retry
	if task.retry < task.maxRetry {
		// Calculate retry delay
		var delay time.Duration
		if p.config.RetryDelayFunc != nil {
			delay = p.config.RetryDelayFunc(task.retry+1, err, task)
		} else {
			delay = DefaultRetryDelayFunc(task.retry+1, err, task)
		}

		p.logger.Info(fmt.Sprintf("Task %s will retry in %v (attempt %d/%d)", task.id, delay, task.retry+1, task.maxRetry))

		p.redisRetry("retry", task.id, func() error {
			return p.redis.retry(ctx, task, queueName, delay)
		})
	} else {
		p.logger.Warn(fmt.Sprintf("Task %s exceeded max retries (%d), marking as failed", task.id, task.maxRetry))

		if storeInRedis {
			p.redisRetry("fail", task.id, func() error {
				return p.redis.fail(ctx, task, queueName, err.Error(), p.config.CompletedTaskTTL)
			})
		} else {
			p.redisRetry("cleanupActive", task.id, func() error {
				return p.redis.cleanupActive(ctx, task, queueName)
			})
		}
		if incrErr := p.redis.incrementFailed(ctx, queueName); incrErr != nil {
			p.logger.Error("Failed to increment failed counter: %v", incrErr)
		}
	}
}
