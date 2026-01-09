package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis key prefixes (asynq-compatible)
const (
	keyPrefix       = "asynq:"
	keyPending      = keyPrefix + "%s:pending"      // List of pending task IDs
	keyActive       = keyPrefix + "%s:active"       // List of active task IDs
	keyTask         = keyPrefix + "t:%s"            // Task data hash
	keyResult       = keyPrefix + "result:%s"       // Task result
	keyWorkers      = keyPrefix + "workers"         // Set of worker IDs
	keyWorkerData   = keyPrefix + "workers:%s"      // Worker data hash
)

// Task data fields in Redis hash (asynq-compatible)
const (
	fieldType       = "type"
	fieldPayload    = "payload"
	fieldState      = "state"
	fieldRetry      = "retry"
	fieldMaxRetry   = "max_retry"
	fieldQueue      = "queue"
	fieldTimeout    = "timeout"
	fieldDeadline   = "deadline"
	fieldPendingSince = "pending_since"
)

// Task states
const (
	stateActive    = "active"
	statePending   = "pending"
	stateCompleted = "completed"
	stateRetry     = "retry"
	stateFailed    = "archived" // asynq uses "archived" for failed
)

// taskData represents the task data stored in Redis.
type taskData struct {
	Type     string `json:"type"`
	Payload  []byte `json:"payload"`
	Retry    int    `json:"retry"`
	MaxRetry int    `json:"max_retry"`
	Queue    string `json:"queue"`
}

// redisClient wraps Redis operations.
type redisClient struct {
	rdb    *redis.Client
	logger Logger
}

// newRedisClient creates a new Redis client wrapper.
func newRedisClient(rdb *redis.Client, logger Logger) *redisClient {
	return &redisClient{rdb: rdb, logger: logger}
}

// dequeue attempts to dequeue a task from the given queues.
// Returns nil if no task is available.
func (r *redisClient) dequeue(ctx context.Context, queues []string) (*Task, error) {
	// Build list of pending queue keys
	keys := make([]string, len(queues))
	for i, q := range queues {
		keys[i] = fmt.Sprintf(keyPending, q)
	}

	// BRPOP with timeout - blocks until a task is available
	result, err := r.rdb.BRPop(ctx, 2*time.Second, keys...).Result()
	if err == redis.Nil {
		return nil, nil // No task available
	}
	if err != nil {
		return nil, fmt.Errorf("brpop failed: %w", err)
	}

	// result[0] = key name, result[1] = task ID
	queueKey := result[0]
	taskID := result[1]

	// Extract queue name from key
	var queueName string
	fmt.Sscanf(queueKey, keyPrefix+"%s:pending", &queueName)

	// Get task data from hash
	taskKey := fmt.Sprintf(keyTask, taskID)
	data, err := r.rdb.HGetAll(ctx, taskKey).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall failed for task %s: %w", taskID, err)
	}

	if len(data) == 0 {
		r.logger.Warn("Task data not found for ID:", taskID)
		return nil, nil
	}

	// Parse task data
	var retry, maxRetry int
	fmt.Sscanf(data[fieldRetry], "%d", &retry)
	fmt.Sscanf(data[fieldMaxRetry], "%d", &maxRetry)

	// Move task to active queue
	activeKey := fmt.Sprintf(keyActive, queueName)
	r.rdb.LPush(ctx, activeKey, taskID)

	// Update task state to active
	r.rdb.HSet(ctx, taskKey, fieldState, stateActive)

	// Decode payload (stored as JSON-encoded bytes in asynq)
	var payload []byte
	if p, ok := data[fieldPayload]; ok {
		// Try to unmarshal as JSON string first (asynq stores base64 or raw)
		if err := json.Unmarshal([]byte(p), &payload); err != nil {
			payload = []byte(p) // Use raw value
		}
	}

	task := &Task{
		id:        taskID,
		typename:  data[fieldType],
		payload:   payload,
		retry:     retry,
		maxRetry:  maxRetry,
		queue:     queueName,
		rdb:       r.rdb,
		resultKey: fmt.Sprintf(keyResult, taskID),
	}

	return task, nil
}

// complete marks a task as completed and removes it from active queue.
func (r *redisClient) complete(ctx context.Context, task *Task, queueName string) error {
	taskKey := fmt.Sprintf(keyTask, task.id)
	activeKey := fmt.Sprintf(keyActive, queueName)

	// Remove from active queue
	r.rdb.LRem(ctx, activeKey, 1, task.id)

	// Update state to completed
	r.rdb.HSet(ctx, taskKey, fieldState, stateCompleted)

	return nil
}

// retry re-queues a task for retry.
func (r *redisClient) retry(ctx context.Context, task *Task, queueName string, delay time.Duration) error {
	taskKey := fmt.Sprintf(keyTask, task.id)
	activeKey := fmt.Sprintf(keyActive, queueName)
	pendingKey := fmt.Sprintf(keyPending, queueName)

	// Remove from active queue
	r.rdb.LRem(ctx, activeKey, 1, task.id)

	// Increment retry count
	r.rdb.HIncrBy(ctx, taskKey, fieldRetry, 1)
	r.rdb.HSet(ctx, taskKey, fieldState, stateRetry)

	// Re-queue after delay (simple approach: sleep then push)
	// For production, you'd want a scheduled queue
	if delay > 0 {
		go func() {
			time.Sleep(delay)
			r.rdb.LPush(context.Background(), pendingKey, task.id)
			r.rdb.HSet(context.Background(), taskKey, fieldState, statePending)
		}()
	} else {
		r.rdb.LPush(ctx, pendingKey, task.id)
		r.rdb.HSet(ctx, taskKey, fieldState, statePending)
	}

	return nil
}

// fail marks a task as permanently failed.
func (r *redisClient) fail(ctx context.Context, task *Task, queueName string, errMsg string) error {
	taskKey := fmt.Sprintf(keyTask, task.id)
	activeKey := fmt.Sprintf(keyActive, queueName)

	// Remove from active queue
	r.rdb.LRem(ctx, activeKey, 1, task.id)

	// Update state to failed (archived in asynq terms)
	r.rdb.HSet(ctx, taskKey, fieldState, stateFailed)
	r.rdb.HSet(ctx, taskKey, "error", errMsg)

	return nil
}

// ping checks Redis connectivity.
func (r *redisClient) ping(ctx context.Context) error {
	return r.rdb.Ping(ctx).Err()
}
