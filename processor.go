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
	}
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
	task, err := redisClient.dequeue(ctx, p.queues)
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
	// Check if redis storage is enabled for this queue
	storeInRedis := true
	if state, ok := p.subQueueToState[queueName]; ok {
		storeInRedis = state.RedisStorage
	}

	if storeInRedis {
		p.redisRetry("complete", task.id, func() error {
			return p.redis.complete(ctx, task, queueName)
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
	p.logger.Error(fmt.Sprintf("Task %s failed: %v", task.id, err))

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
				return p.redis.fail(ctx, task, queueName, err.Error())
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
				return p.redis.fail(ctx, task, queueName, err.Error())
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
