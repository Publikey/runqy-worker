package worker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// processor handles dequeuing and executing tasks.
type processor struct {
	redis            *redisClient
	handler          Handler                // fallback handler (ServeMux for type-based routing)
	queueHandlers    map[string]HandlerFunc // queue name -> handler (for queue-based routing)
	config           Config
	queues           []string               // Queue names in priority order
	queueStates      map[string]*QueueState // supervised processes per queue (may be nil)
	subQueueToState  map[string]*QueueState // sub-queue name -> parent queue state

	done   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger Logger
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

// parentQueueName extracts the parent queue name from a sub-queue name.
// For example: "inference:high" -> "inference", "simple" -> "simple"
func parentQueueName(subQueueName string) string {
	if idx := strings.Index(subQueueName, ":"); idx > 0 {
		return subQueueName[:idx]
	}
	return subQueueName
}

// start begins processing tasks with the configured concurrency.
func (p *processor) start() {
	p.ctx, p.cancel = context.WithCancel(context.Background())
	for i := 0; i < p.config.Concurrency; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// stop gracefully stops all workers.
func (p *processor) stop() {
	p.cancel()
	p.wg.Wait()
}

// worker is a single worker goroutine.
func (p *processor) worker(id int) {
	defer p.wg.Done()

	p.logger.Info(fmt.Sprintf("Worker %d started", id))
	for {
		p.processOne(p.ctx)
		if p.ctx.Err() != nil {
			p.logger.Info(fmt.Sprintf("Worker %d stopping", id))
			return
		}
	}
}

// processOne attempts to dequeue and process a single task.
func (p *processor) processOne(ctx context.Context) {
	// ctx := context.Background()
	// Dequeue a task
	task, err := p.redis.dequeue(ctx, p.queues)
	if err != nil {
		p.logger.Error("Dequeue error: %v", err)
		time.Sleep(time.Second) // Back off on error
		return
	}

	if task == nil {
		// No task available, will retry after BRPOP timeout
		return
	}

	queueName := task.queue

	// Extract parent queue for supervisor health check (sub-queue -> parent)
	parentQueue := parentQueueName(queueName)

	// Check if parent queue's supervisor is healthy
	if state, ok := p.queueStates[parentQueue]; ok {
		if state.Supervisor != nil && !state.Supervisor.IsHealthy() {
			p.logger.Error("Queue %s is in degraded state - supervised process has crashed", parentQueue)
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

// handleSuccess handles successful task completion.
func (p *processor) handleSuccess(ctx context.Context, task *Task, queueName string) {
	// Check if redis storage is enabled for this queue
	storeInRedis := true
	if state, ok := p.subQueueToState[queueName]; ok {
		storeInRedis = state.RedisStorage
	}

	if storeInRedis {
		if err := p.redis.complete(ctx, task, queueName); err != nil {
			p.logger.Error("Failed to mark task complete: %v", err)
		}
	} else {
		// Just clean up the active queue without storing completion
		if err := p.redis.cleanupActive(ctx, task, queueName); err != nil {
			p.logger.Error("Failed to cleanup active task: %v", err)
		}
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
			if failErr := p.redis.fail(ctx, task, queueName, err.Error()); failErr != nil {
				p.logger.Error("Failed to mark task as failed: %v", failErr)
			}
		} else {
			if cleanupErr := p.redis.cleanupActive(ctx, task, queueName); cleanupErr != nil {
				p.logger.Error("Failed to cleanup active task: %v", cleanupErr)
			}
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

		if retryErr := p.redis.retry(ctx, task, queueName, delay); retryErr != nil {
			p.logger.Error("Failed to schedule retry: %v", retryErr)
		}
	} else {
		p.logger.Warn(fmt.Sprintf("Task %s exceeded max retries (%d), marking as failed", task.id, task.maxRetry))

		if storeInRedis {
			if failErr := p.redis.fail(ctx, task, queueName, err.Error()); failErr != nil {
				p.logger.Error("Failed to mark task as failed: %v", failErr)
			}
		} else {
			if cleanupErr := p.redis.cleanupActive(ctx, task, queueName); cleanupErr != nil {
				p.logger.Error("Failed to cleanup active task: %v", cleanupErr)
			}
		}
		if incrErr := p.redis.incrementFailed(ctx, queueName); incrErr != nil {
			p.logger.Error("Failed to increment failed counter: %v", incrErr)
		}
	}
}
