package worker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// processor handles dequeuing and executing tasks.
type processor struct {
	redis         *redisClient
	handler       Handler                    // fallback handler (ServeMux for type-based routing)
	queueHandlers map[string]HandlerFunc     // queue name -> handler (for queue-based routing)
	config        Config
	queues        []string // Queue names in priority order
	supervisor    *ProcessSupervisor         // supervised process (may be nil)

	done   chan struct{}
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

// SetSupervisor sets the process supervisor for health checks.
func (p *processor) SetSupervisor(supervisor *ProcessSupervisor) {
	p.supervisor = supervisor
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

// start begins processing tasks with the configured concurrency.
func (p *processor) start() {
	for i := 0; i < p.config.Concurrency; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// stop gracefully stops all workers.
func (p *processor) stop() {
	close(p.done)
	p.wg.Wait()
}

// worker is a single worker goroutine.
func (p *processor) worker(id int) {
	defer p.wg.Done()

	p.logger.Info(fmt.Sprintf("Worker %d started", id))

	for {
		select {
		case <-p.done:
			p.logger.Info(fmt.Sprintf("Worker %d stopping", id))
			return
		default:
			p.processOne()
		}
	}
}

// processOne attempts to dequeue and process a single task.
func (p *processor) processOne() {
	// Check if supervised process is healthy
	if p.supervisor != nil && !p.supervisor.IsHealthy() {
		p.logger.Error("Worker in degraded state - supervised process has crashed")
		time.Sleep(5 * time.Second)
		return
	}

	ctx := context.Background()

	// Dequeue a task
	task, err := p.redis.dequeue(ctx, p.queues)
	if err != nil {
		p.logger.Error("Dequeue error:", err)
		time.Sleep(time.Second) // Back off on error
		return
	}

	if task == nil {
		// No task available, will retry after BRPOP timeout
		return
	}

	queueName := task.queue

	// Context is set by the task during dequeue

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
	if err := p.redis.complete(ctx, task, queueName); err != nil {
		p.logger.Error("Failed to mark task complete:", err)
	}
	p.logger.Info(fmt.Sprintf("Task %s completed successfully", task.id))
}

// handleError handles task processing errors.
func (p *processor) handleError(ctx context.Context, task *Task, queueName string, err error) {
	p.logger.Error(fmt.Sprintf("Task %s failed: %v", task.id, err))

	// Check for permanent failure (skip retry)
	if IsSkipRetry(err) {
		p.logger.Warn(fmt.Sprintf("Task %s marked as permanent failure, skipping retry", task.id))
		if failErr := p.redis.fail(ctx, task, queueName, err.Error()); failErr != nil {
			p.logger.Error("Failed to mark task as failed:", failErr)
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
			p.logger.Error("Failed to schedule retry:", retryErr)
		}
	} else {
		p.logger.Warn(fmt.Sprintf("Task %s exceeded max retries (%d), marking as failed", task.id, task.maxRetry))

		if failErr := p.redis.fail(ctx, task, queueName, err.Error()); failErr != nil {
			p.logger.Error("Failed to mark task as failed:", failErr)
		}
	}
}
