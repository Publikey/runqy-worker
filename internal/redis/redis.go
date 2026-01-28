package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Logger interface for redis package logging.
type Logger interface {
	Info(format string, args ...any)
	Warn(format string, args ...any)
	Error(format string, args ...any)
	Debug(format string, args ...any)
}

// Redis key prefixes (asynq-compatible)
// Queue names are wrapped in {} for Redis Cluster hash slot compatibility
const (
	KeyPrefix     = "asynq:"
	KeyPending    = KeyPrefix + "{%s}:pending"   // List of pending task IDs
	KeyActive     = KeyPrefix + "{%s}:active"    // List of active task IDs
	KeyLease      = KeyPrefix + "{%s}:lease"     // Sorted set for lease expiration tracking (score = expiration time)
	KeyScheduled  = KeyPrefix + "{%s}:scheduled" // Sorted set of scheduled task IDs
	KeyRetry      = KeyPrefix + "{%s}:retry"     // Sorted set of retry task IDs (score = retry time)
	KeyCompleted  = KeyPrefix + "{%s}:completed" // Sorted set of completed task IDs
	KeyArchived   = KeyPrefix + "{%s}:archived"  // Sorted set of failed/archived task IDs
	KeyTask       = KeyPrefix + "{%s}:t:%s"      // Task data hash: asynq:{queue}:t:{taskID}
	KeyWorkers    = KeyPrefix + "workers"        // Set of worker IDs
	KeyWorkerData = KeyPrefix + "workers:%s"     // Worker data hash

	// Stats keys
	KeyProcessed      = KeyPrefix + "{%s}:processed"    // Total processed count by queue
	KeyFailed         = KeyPrefix + "{%s}:failed"       // Total failed count by queue
	KeyProcessedDaily = KeyPrefix + "{%s}:processed:%s" // Daily processed count: asynq:{queue}:processed:YYYY-MM-DD
	KeyFailedDaily    = KeyPrefix + "{%s}:failed:%s"    // Daily failed count: asynq:{queue}:failed:YYYY-MM-DD
)

// Task data fields in Redis hash (asynq-compatible)
const (
	FieldMsg          = "msg"           // Protobuf-encoded task message (asynq format)
	FieldState        = "state"         // Task state
	FieldResult       = "result"        // Task result
	FieldPendingSince = "pending_since" // Timestamp when task entered pending state
	FieldCompletedAt  = "completed_at"  // Timestamp when task completed
	FieldRetried      = "retried"       // Number of times task has been retried
	FieldErrorMsg     = "error_msg"     // Error message for failed tasks
	FieldLastFailedAt = "last_failed_at"
	// Legacy field names (for non-asynq format)
	FieldType     = "type"
	FieldPayload  = "payload"
	FieldRetry    = "retry"
	FieldMaxRetry = "max_retry"
	FieldQueue    = "queue"
	FieldTimeout  = "timeout"
	FieldDeadline = "deadline"
)

// Task states (asynq-compatible)
const (
	StateActive    = "active"
	StatePending   = "pending"
	StateScheduled = "scheduled"
	StateRetry     = "retry"
	StateCompleted = "completed"
	StateArchived  = "archived" // asynq uses "archived" for failed tasks
)

// TaskData represents the task data from Redis.
type TaskData struct {
	ID       string
	Type     string
	Payload  []byte
	Retry    int    // Current retry count
	MaxRetry int    // Maximum retry attempts
	Queue    string
	TaskKey  string // The task hash key: asynq:{queue}:t:{taskID}
}

// TypedResponse is the canonical structure used to store task results.
type TypedResponse map[string]any

// Client wraps Redis operations.
type Client struct {
	Rdb    *redis.Client
	Logger Logger
}

// NewClient creates a new Redis client wrapper.
func NewClient(rdb *redis.Client, logger Logger) *Client {
	return &Client{
		Rdb:    rdb,
		Logger: logger,
	}
}

// asynqTaskMessage represents the decoded protobuf message from asynq.
type asynqTaskMessage struct {
	Type     string
	Payload  []byte
	ID       string
	Queue    string
	MaxRetry int
	Retried  int // Current retry count from protobuf field 6
}

// parseAsynqMessage parses the protobuf-encoded msg field from asynq.
// Asynq uses protobuf with the following fields:
// - Field 1 (string): type
// - Field 2 (bytes): payload
// - Field 3 (string): id
// - Field 4 (string): queue
// - Field 5 (int32): retry (max_retry)
// - Field 6 (int32): retried (current retry count)
func parseAsynqMessage(data []byte) (*asynqTaskMessage, error) {
	msg := &asynqTaskMessage{}
	pos := 0

	for pos < len(data) {
		if pos >= len(data) {
			break
		}

		// Read field tag (varint)
		tag := int(data[pos])
		pos++

		fieldNum := tag >> 3
		wireType := tag & 0x07

		switch wireType {
		case 0: // Varint
			val, n := readVarint(data[pos:])
			pos += n
			switch fieldNum {
			case 5:
				msg.MaxRetry = int(val)
			case 6:
				msg.Retried = int(val)
			}
		case 2: // Length-delimited (string, bytes)
			length, n := readVarint(data[pos:])
			pos += n
			if pos+int(length) > len(data) {
				return nil, fmt.Errorf("invalid protobuf: length exceeds data")
			}
			value := data[pos : pos+int(length)]
			pos += int(length)

			switch fieldNum {
			case 1:
				msg.Type = string(value)
			case 2:
				msg.Payload = value
			case 3:
				msg.ID = string(value)
			case 4:
				msg.Queue = string(value)
			}
		default:
			// Skip unknown wire types
			return nil, fmt.Errorf("unsupported wire type: %d", wireType)
		}
	}

	return msg, nil
}

// readVarint reads a varint from the byte slice and returns the value and bytes consumed.
func readVarint(data []byte) (uint64, int) {
	var val uint64
	var shift uint
	for i, b := range data {
		val |= uint64(b&0x7F) << shift
		if b < 0x80 {
			return val, i + 1
		}
		shift += 7
		if shift >= 64 {
			return 0, i + 1
		}
	}
	return val, len(data)
}

// encodeAsynqMessage encodes an asynqTaskMessage back to protobuf format.
func encodeAsynqMessage(msg *asynqTaskMessage) []byte {
	var buf []byte

	// Field 1 (string): type
	if msg.Type != "" {
		buf = append(buf, (1<<3)|2) // field 1, wire type 2 (length-delimited)
		buf = appendVarint(buf, uint64(len(msg.Type)))
		buf = append(buf, msg.Type...)
	}

	// Field 2 (bytes): payload
	if len(msg.Payload) > 0 {
		buf = append(buf, (2<<3)|2) // field 2, wire type 2
		buf = appendVarint(buf, uint64(len(msg.Payload)))
		buf = append(buf, msg.Payload...)
	}

	// Field 3 (string): id
	if msg.ID != "" {
		buf = append(buf, (3<<3)|2) // field 3, wire type 2
		buf = appendVarint(buf, uint64(len(msg.ID)))
		buf = append(buf, msg.ID...)
	}

	// Field 4 (string): queue
	if msg.Queue != "" {
		buf = append(buf, (4<<3)|2) // field 4, wire type 2
		buf = appendVarint(buf, uint64(len(msg.Queue)))
		buf = append(buf, msg.Queue...)
	}

	// Field 5 (int32): retry (max_retry)
	if msg.MaxRetry > 0 {
		buf = append(buf, (5<<3)|0) // field 5, wire type 0 (varint)
		buf = appendVarint(buf, uint64(msg.MaxRetry))
	}

	// Field 6 (int32): retried (current retry count)
	if msg.Retried > 0 {
		buf = append(buf, (6<<3)|0) // field 6, wire type 0 (varint)
		buf = appendVarint(buf, uint64(msg.Retried))
	}

	return buf
}

// appendVarint appends a varint-encoded uint64 to the buffer.
func appendVarint(buf []byte, val uint64) []byte {
	for val >= 0x80 {
		buf = append(buf, byte(val)|0x80)
		val >>= 7
	}
	buf = append(buf, byte(val))
	return buf
}

// Dequeue attempts to dequeue a task from the given queues using BRPOP.
// This is compatible with asynq's list-based pending queue.
// Returns nil if no task is available.
func (r *Client) Dequeue(ctx context.Context, queues []string) (*TaskData, error) {
	// Build list of pending queue keys
	keys := make([]string, len(queues))
	for i, q := range queues {
		keys[i] = fmt.Sprintf(KeyPending, q)
	}

	// BRPOP with timeout - blocks until a task is available
	result, err := r.Rdb.BRPop(ctx, 2*time.Second, keys...).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // No task available
	}
	if err != nil {
		return nil, fmt.Errorf("brpop failed: %w", err)
	}

	// result[0] = key name, result[1] = task ID
	queueKey := result[0]
	taskID := result[1]

	// Extract queue name from key (e.g., "asynq:{inference}:pending" -> "inference")
	queueName := strings.TrimPrefix(queueKey, KeyPrefix)
	queueName = strings.TrimSuffix(queueName, ":pending")
	queueName = strings.Trim(queueName, "{}")

	// Get task data from hash
	taskKey := fmt.Sprintf(KeyTask, queueName, taskID)
	data, err := r.Rdb.HGetAll(ctx, taskKey).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall failed for task %s: %w", taskID, err)
	}

	if len(data) == 0 {
		r.Logger.Warn("Task data not found for ID: %s", taskID)
		return nil, nil
	}

	// Parse task data - try asynq protobuf format first, then legacy format
	var taskType string
	var payload []byte
	var maxRetry int
	var retried int

	if msgData, ok := data[FieldMsg]; ok {
		// Asynq format: parse protobuf-encoded msg field
		asynqMsg, err := parseAsynqMessage([]byte(msgData))
		if err != nil {
			r.Logger.Warn("Failed to parse asynq message for task %s: %v", taskID, err)
			return nil, nil
		}
		taskType = asynqMsg.Type
		payload = asynqMsg.Payload
		maxRetry = asynqMsg.MaxRetry
		retried = asynqMsg.Retried
		// Check the hash field for updated retry count (incremented by Retry())
		// since the protobuf message is not updated on retries
		if retriedStr, ok := data[FieldRetried]; ok {
			var hashRetried int
			if _, err := fmt.Sscanf(retriedStr, "%d", &hashRetried); err == nil && hashRetried > retried {
				retried = hashRetried
			}
		}
	} else {
		// Legacy format: read individual fields
		taskType = data[FieldType]
		fmt.Sscanf(data[FieldMaxRetry], "%d", &maxRetry)
		fmt.Sscanf(data[FieldRetried], "%d", &retried)
		if p, ok := data[FieldPayload]; ok {
			if err := json.Unmarshal([]byte(p), &payload); err != nil {
				payload = []byte(p)
			}
		}
	}

	// Move task to active queue (List) and lease tracking (Sorted Set)
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)
	leaseExpiration := float64(time.Now().Add(30 * time.Minute).Unix())
	r.Rdb.LPush(ctx, activeKey, taskID)
	r.Rdb.ZAdd(ctx, leaseKey, redis.Z{Score: leaseExpiration, Member: taskID})

	// Update task state to active and remove pending_since (asynq-compatible)
	r.Rdb.HSet(ctx, taskKey, FieldState, StateActive)
	r.Rdb.HDel(ctx, taskKey, FieldPendingSince)

	taskData := &TaskData{
		ID:       taskID,
		Type:     taskType,
		Payload:  payload,
		Retry:    retried,
		MaxRetry: maxRetry,
		Queue:    queueName,
		TaskKey:  taskKey,
	}

	return taskData, nil
}

// Complete marks a task as completed and removes it from active queue.
// Results are already written to the task hash by TaskResultWriter.
func (r *Client) Complete(ctx context.Context, taskID string, queueName string) error {
	taskKey := fmt.Sprintf(KeyTask, queueName, taskID)
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)
	completedKey := fmt.Sprintf(KeyCompleted, queueName)

	now := time.Now().Unix()

	// Remove from active queue (List) and lease tracking (Sorted Set)
	r.Rdb.LRem(ctx, activeKey, 1, taskID)
	r.Rdb.ZRem(ctx, leaseKey, taskID)

	// Update task hash: state and completed_at
	r.Rdb.HSet(ctx, taskKey,
		FieldState, StateCompleted,
		FieldCompletedAt, now,
	)

	// Add to completed sorted set with Unix timestamp as score
	r.Rdb.ZAdd(ctx, completedKey, redis.Z{Score: float64(now), Member: taskID})

	return nil
}

// Retry re-queues a task for retry using the retry sorted set.
func (r *Client) Retry(ctx context.Context, taskID string, queueName string, delay time.Duration) error {
	taskKey := fmt.Sprintf(KeyTask, queueName, taskID)
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)
	retryKey := fmt.Sprintf(KeyRetry, queueName)
	pendingKey := fmt.Sprintf(KeyPending, queueName)

	now := time.Now()

	// Remove from active queue (List) and lease tracking (Sorted Set)
	r.Rdb.LRem(ctx, activeKey, 1, taskID)
	r.Rdb.ZRem(ctx, leaseKey, taskID)

	// Update the protobuf msg field with incremented retry count (for asynqmon compatibility)
	if msgData, err := r.Rdb.HGet(ctx, taskKey, FieldMsg).Result(); err == nil {
		if asynqMsg, err := parseAsynqMessage([]byte(msgData)); err == nil {
			asynqMsg.Retried++
			updatedMsg := encodeAsynqMessage(asynqMsg)
			r.Rdb.HSet(ctx, taskKey, FieldMsg, string(updatedMsg))
		}
	}

	// Increment retry count in hash field and update state
	r.Rdb.HIncrBy(ctx, taskKey, FieldRetried, 1)
	r.Rdb.HSet(ctx, taskKey,
		FieldState, StateRetry,
		FieldLastFailedAt, now.Unix(),
	)

	if delay > 0 {
		// Add to retry sorted set with scheduled time as score
		retryAt := float64(now.Add(delay).Unix())
		r.Rdb.ZAdd(ctx, retryKey, redis.Z{Score: retryAt, Member: taskID})

		// Start a goroutine to move the task from retry to pending when ready
		go func() {
			time.Sleep(delay)
			// Move from retry to pending
			r.Rdb.ZRem(context.Background(), retryKey, taskID)
			r.Rdb.LPush(context.Background(), pendingKey, taskID)
			r.Rdb.HSet(context.Background(), taskKey, FieldState, StatePending)
		}()
	} else {
		// No delay, push directly to pending
		r.Rdb.LPush(ctx, pendingKey, taskID)
		r.Rdb.HSet(ctx, taskKey, FieldState, StatePending)
	}

	return nil
}

// Fail marks a task as permanently failed (archived).
func (r *Client) Fail(ctx context.Context, taskID string, queueName string, errMsg string) error {
	taskKey := fmt.Sprintf(KeyTask, queueName, taskID)
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)
	archivedKey := fmt.Sprintf(KeyArchived, queueName)

	now := time.Now().Unix()

	// Remove from active queue (List) and lease tracking (Sorted Set)
	r.Rdb.LRem(ctx, activeKey, 1, taskID)
	r.Rdb.ZRem(ctx, leaseKey, taskID)

	// Update task hash: state, error message, and last failed timestamp
	r.Rdb.HSet(ctx, taskKey,
		FieldState, StateArchived,
		FieldErrorMsg, errMsg,
		FieldLastFailedAt, now,
	)

	// Add to archived sorted set with Unix timestamp as score
	r.Rdb.ZAdd(ctx, archivedKey, redis.Z{Score: float64(now), Member: taskID})

	return nil
}

// Ping checks Redis connectivity.
func (r *Client) Ping(ctx context.Context) error {
	return r.Rdb.Ping(ctx).Err()
}

// GetRedisClient returns the underlying redis client for advanced operations.
func (r *Client) GetRedisClient() *redis.Client {
	return r.Rdb
}

// CleanupActive removes a task from active queue without storing completion data.
// Used when redis_storage is disabled.
func (r *Client) CleanupActive(ctx context.Context, taskID string, queueName string) error {
	taskKey := fmt.Sprintf(KeyTask, queueName, taskID)
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)

	// Remove from active queue (List) and lease tracking (Sorted Set)
	r.Rdb.LRem(ctx, activeKey, 1, taskID)
	r.Rdb.ZRem(ctx, leaseKey, taskID)

	// Delete the task hash entirely
	r.Rdb.Del(ctx, taskKey)

	return nil
}

// IncrementProcessed increments both total and daily processed counters for a queue.
func (r *Client) IncrementProcessed(ctx context.Context, queueName string) error {
	totalKey := fmt.Sprintf(KeyProcessed, queueName)
	dailyKey := fmt.Sprintf(KeyProcessedDaily, queueName, time.Now().UTC().Format("2006-01-02"))

	pipe := r.Rdb.Pipeline()
	pipe.Incr(ctx, totalKey)
	pipe.Incr(ctx, dailyKey)
	_, err := pipe.Exec(ctx)
	return err
}

// IncrementFailed increments both total and daily failed counters for a queue.
func (r *Client) IncrementFailed(ctx context.Context, queueName string) error {
	totalKey := fmt.Sprintf(KeyFailed, queueName)
	dailyKey := fmt.Sprintf(KeyFailedDaily, queueName, time.Now().UTC().Format("2006-01-02"))

	pipe := r.Rdb.Pipeline()
	pipe.Incr(ctx, totalKey)
	pipe.Incr(ctx, dailyKey)
	_, err := pipe.Exec(ctx)
	return err
}
