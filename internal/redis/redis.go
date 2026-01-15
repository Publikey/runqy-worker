package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Logger interface for redis package logging.
type Logger interface {
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	Debug(format string, args ...interface{})
}

// Redis key prefixes (asynq-compatible)
const (
	KeyPrefix     = "asynq:"
	KeyPending    = KeyPrefix + "%s:pending"    // List of pending task IDs
	KeyActive     = KeyPrefix + "%s:active"     // List of active task IDs
	KeyTask       = KeyPrefix + "t:%s"          // Task data hash
	KeyResult     = KeyPrefix + "result:%s"     // Task result
	KeyWorkers    = KeyPrefix + "workers"       // Set of worker IDs
	KeyWorkerData = KeyPrefix + "workers:%s"    // Worker data hash
)

// Task data fields in Redis hash (asynq-compatible)
const (
	FieldType         = "type"
	FieldPayload      = "payload"
	FieldState        = "state"
	FieldRetry        = "retry"
	FieldMaxRetry     = "max_retry"
	FieldQueue        = "queue"
	FieldTimeout      = "timeout"
	FieldDeadline     = "deadline"
	FieldPendingSince = "pending_since"
)

// Task states
const (
	StateActive    = "active"
	StatePending   = "pending"
	StateCompleted = "completed"
	StateRetry     = "retry"
	StateFailed    = "archived" // asynq uses "archived" for failed
)

// TaskData represents the task data from Redis.
type TaskData struct {
	ID        string
	Type      string
	Payload   []byte
	Retry     int
	MaxRetry  int
	Queue     string
	ResultKey string
}

// Client wraps Redis operations.
type Client struct {
	Rdb    *redis.Client
	Logger Logger
}

// NewClient creates a new Redis client wrapper.
func NewClient(rdb *redis.Client, logger Logger) *Client {
	return &Client{Rdb: rdb, Logger: logger}
}

// Dequeue attempts to dequeue a task from the given queues.
// Returns nil if no task is available.
func (r *Client) Dequeue(ctx context.Context, queues []string) (*TaskData, error) {
	// Build list of pending queue keys
	keys := make([]string, len(queues))
	for i, q := range queues {
		keys[i] = fmt.Sprintf(KeyPending, q)
	}

	// BRPOP with timeout - blocks until a task is available
	result, err := r.Rdb.BRPop(ctx, 2*time.Second, keys...).Result()
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
	fmt.Sscanf(queueKey, KeyPrefix+"%s:pending", &queueName)

	// Get task data from hash
	taskKey := fmt.Sprintf(KeyTask, taskID)
	data, err := r.Rdb.HGetAll(ctx, taskKey).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall failed for task %s: %w", taskID, err)
	}

	if len(data) == 0 {
		r.Logger.Warn("Task data not found for ID:", taskID)
		return nil, nil
	}

	// Parse task data
	var retry, maxRetry int
	fmt.Sscanf(data[FieldRetry], "%d", &retry)
	fmt.Sscanf(data[FieldMaxRetry], "%d", &maxRetry)

	// Move task to active queue
	activeKey := fmt.Sprintf(KeyActive, queueName)
	r.Rdb.LPush(ctx, activeKey, taskID)

	// Update task state to active
	r.Rdb.HSet(ctx, taskKey, FieldState, StateActive)

	// Decode payload (stored as JSON-encoded bytes in asynq)
	var payload []byte
	if p, ok := data[FieldPayload]; ok {
		// Try to unmarshal as JSON string first (asynq stores base64 or raw)
		if err := json.Unmarshal([]byte(p), &payload); err != nil {
			payload = []byte(p) // Use raw value
		}
	}

	taskData := &TaskData{
		ID:        taskID,
		Type:      data[FieldType],
		Payload:   payload,
		Retry:     retry,
		MaxRetry:  maxRetry,
		Queue:     queueName,
		ResultKey: fmt.Sprintf(KeyResult, taskID),
	}

	return taskData, nil
}

// Complete marks a task as completed and removes it from active queue.
func (r *Client) Complete(ctx context.Context, taskID string, queueName string) error {
	taskKey := fmt.Sprintf(KeyTask, taskID)
	activeKey := fmt.Sprintf(KeyActive, queueName)

	// Remove from active queue
	r.Rdb.LRem(ctx, activeKey, 1, taskID)

	// Update state to completed
	r.Rdb.HSet(ctx, taskKey, FieldState, StateCompleted)

	return nil
}

// Retry re-queues a task for retry.
func (r *Client) Retry(ctx context.Context, taskID string, queueName string, delay time.Duration) error {
	taskKey := fmt.Sprintf(KeyTask, taskID)
	activeKey := fmt.Sprintf(KeyActive, queueName)
	pendingKey := fmt.Sprintf(KeyPending, queueName)

	// Remove from active queue
	r.Rdb.LRem(ctx, activeKey, 1, taskID)

	// Increment retry count
	r.Rdb.HIncrBy(ctx, taskKey, FieldRetry, 1)
	r.Rdb.HSet(ctx, taskKey, FieldState, StateRetry)

	// Re-queue after delay (simple approach: sleep then push)
	// For production, you'd want a scheduled queue
	if delay > 0 {
		go func() {
			time.Sleep(delay)
			r.Rdb.LPush(context.Background(), pendingKey, taskID)
			r.Rdb.HSet(context.Background(), taskKey, FieldState, StatePending)
		}()
	} else {
		r.Rdb.LPush(ctx, pendingKey, taskID)
		r.Rdb.HSet(ctx, taskKey, FieldState, StatePending)
	}

	return nil
}

// Fail marks a task as permanently failed.
func (r *Client) Fail(ctx context.Context, taskID string, queueName string, errMsg string) error {
	taskKey := fmt.Sprintf(KeyTask, taskID)
	activeKey := fmt.Sprintf(KeyActive, queueName)

	// Remove from active queue
	r.Rdb.LRem(ctx, activeKey, 1, taskID)

	// Update state to failed (archived in asynq terms)
	r.Rdb.HSet(ctx, taskKey, FieldState, StateFailed)
	r.Rdb.HSet(ctx, taskKey, "error", errMsg)

	return nil
}

// Ping checks Redis connectivity.
func (r *Client) Ping(ctx context.Context) error {
	return r.Rdb.Ping(ctx).Err()
}

// WriteResult writes a task result to Redis.
func (r *Client) WriteResult(ctx context.Context, resultKey string, data []byte) error {
	return r.Rdb.Set(ctx, resultKey, data, 0).Err()
}

// GetRedisClient returns the underlying redis client for advanced operations.
func (r *Client) GetRedisClient() *redis.Client {
	return r.Rdb
}
