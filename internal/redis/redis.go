package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// parseSecondsField reads an integer-seconds hash field into a Duration.
// Returns -1 when the field is absent so callers can apply their own fallback;
// a present "0" yields 0 (explicitly disabled / no expiry).
func parseSecondsField(data map[string]string, field string) time.Duration {
	if v, ok := data[field]; ok {
		if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.Duration(secs) * time.Second
		}
	}
	return -1
}

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
	KeyTaskLookup = KeyPrefix + "t:%s"           // Reverse-lookup hash (task_id -> queue), written by the server; NOT queue-scoped/hash-tagged
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
	// Per-task lifecycle fields stamped by the server at enqueue (values in seconds).
	// Absent → the worker applies its config fallback; "0" → explicitly disabled / no expiry.
	FieldTTLCompleted   = "ttl_completed"   // TTL for successful completion
	FieldTTLArchived    = "ttl_archived"    // TTL for failure/archival
	FieldPendingTimeout = "pending_timeout" // Max age in pending before archive
	FieldActiveTimeout  = "active_timeout"  // Max execution time before retriable timeout
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

	// Per-task lifecycle values stamped by the server. A negative Duration means the field
	// was absent (caller applies its config fallback); 0 means explicitly disabled/no expiry.
	PendingSince   int64         // unix seconds when the task entered pending (0 = unknown)
	TTLCompleted   time.Duration // -1 = absent
	TTLArchived    time.Duration // -1 = absent
	PendingTimeout time.Duration // -1 = absent
	ActiveTimeout  time.Duration // -1 = absent
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
		case 1: // Fixed 64-bit
			if pos+8 > len(data) {
				return nil, fmt.Errorf("invalid protobuf: fixed64 exceeds data")
			}
			pos += 8
		case 5: // Fixed 32-bit
			if pos+4 > len(data) {
				return nil, fmt.Errorf("invalid protobuf: fixed32 exceeds data")
			}
			pos += 4
		default:
			return nil, fmt.Errorf("invalid wire type: %d", wireType)
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
// leaseDuration sets how long the task's lease is valid before it must be
// renewed (see ExtendLease); the worker renews it periodically while processing.
// Returns nil if no task is available.
func (r *Client) Dequeue(ctx context.Context, queues []string, leaseDuration time.Duration) (*TaskData, error) {
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

	// Atomically: move task to active queue, add to lease tracking, update state
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)
	if leaseDuration <= 0 {
		leaseDuration = 2 * time.Minute
	}
	leaseExpiration := float64(time.Now().Add(leaseDuration).Unix())

	activateScript := redis.NewScript(`
		redis.call("LPUSH", KEYS[1], ARGV[1])
		redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
		redis.call("HSET", KEYS[3], "state", ARGV[3])
		redis.call("HDEL", KEYS[3], "pending_since")
		return 1
	`)
	if err := activateScript.Run(ctx, r.Rdb,
		[]string{activeKey, leaseKey, taskKey},
		taskID, leaseExpiration, StateActive,
	).Err(); err != nil {
		r.Logger.Error("Lua activate script failed for task %s, returning to pending: %v", taskID, err)
		// Push back to pending so another worker (or this one) can pick it up.
		// Do NOT push to active — that would risk duplicates.
		pendingKey := fmt.Sprintf(KeyPending, queueName)
		r.Rdb.RPush(ctx, pendingKey, taskID)
		return nil, fmt.Errorf("activate script failed for task %s: %w", taskID, err)
	}

	// Capture pending_since BEFORE the activate script HDELs it, plus the per-task
	// lifecycle values stamped by the server (absent fields → -1, caller falls back to config).
	var pendingSince int64
	if v, ok := data[FieldPendingSince]; ok {
		pendingSince, _ = strconv.ParseInt(v, 10, 64)
	}

	taskData := &TaskData{
		ID:             taskID,
		Type:           taskType,
		Payload:        payload,
		Retry:          retried,
		MaxRetry:       maxRetry,
		Queue:          queueName,
		TaskKey:        taskKey,
		PendingSince:   pendingSince,
		TTLCompleted:   parseSecondsField(data, FieldTTLCompleted),
		TTLArchived:    parseSecondsField(data, FieldTTLArchived),
		PendingTimeout: parseSecondsField(data, FieldPendingTimeout),
		ActiveTimeout:  parseSecondsField(data, FieldActiveTimeout),
	}

	return taskData, nil
}

// neverExpireSeconds is the :completed/:archived ZSet score offset used when a task has no
// TTL (ttl <= 0). The score encodes the task's expiry time so the janitor can purge uniformly
// ("score < now"); a ~100-year offset keeps no-expiry entries effectively permanent.
const neverExpireSeconds = int64(100 * 365 * 24 * 3600)

// completeCmd atomically removes a task from active/lease and marks it as completed.
// KEYS[1] = activeKey, KEYS[2] = leaseKey, KEYS[3] = completedKey, KEYS[4] = taskKey
// ARGV[1] = taskID, ARGV[2] = now (unix), ARGV[3] = state, ARGV[4] = ttl (seconds, 0 = no expiry),
// ARGV[5] = expiry score (unix seconds the entry becomes purgeable by the janitor)
var completeCmd = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[1])
redis.call("HSET", KEYS[4], "state", ARGV[3], "completed_at", ARGV[2])
if tonumber(ARGV[4]) > 0 then
    redis.call("EXPIRE", KEYS[4], ARGV[4])
end
return 1
`)

// expiryScore returns the ZSet score (unix seconds) at which a finished task becomes purgeable:
// now+ttl when ttl>0, else a far-future sentinel so no-expiry entries are never purged.
func expiryScore(now int64, ttl time.Duration) int64 {
	if ttl > 0 {
		return now + int64(ttl.Seconds())
	}
	return now + neverExpireSeconds
}

// Complete marks a task as completed and removes it from active queue.
// Results are already written to the task hash by TaskResultWriter.
// If ttl > 0, sets an expiry on the task hash key so it is automatically cleaned up.
func (r *Client) Complete(ctx context.Context, taskID string, queueName string, ttl time.Duration) error {
	taskKey := fmt.Sprintf(KeyTask, queueName, taskID)
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)
	completedKey := fmt.Sprintf(KeyCompleted, queueName)

	now := time.Now().Unix()

	// Best-effort: update protobuf msg field with completed_at timestamp (for asynq inspector compatibility).
	// This is non-critical; the hash fields are authoritative.
	if msgData, err := r.Rdb.HGet(ctx, taskKey, FieldMsg).Result(); err == nil {
		var extra []byte
		extra = append(extra, (13<<3)|0) // field 13, wire type 0 (varint)
		extra = appendVarint(extra, uint64(now))
		r.Rdb.HSet(ctx, taskKey, FieldMsg, string(append([]byte(msgData), extra...)))
	}

	// Atomic: remove from active + lease, add to completed, update state
	ttlSecs := int64(0)
	if ttl > 0 {
		ttlSecs = int64(ttl.Seconds())
	}
	if err := completeCmd.Run(ctx, r.Rdb,
		[]string{activeKey, leaseKey, completedKey, taskKey},
		taskID, now, StateCompleted, ttlSecs, expiryScore(now, ttl),
	).Err(); err != nil {
		return fmt.Errorf("complete script failed for task %s: %w", taskID, err)
	}

	// Realign the server-written reverse-lookup key (asynq:t:<id>) with the task's TTL so
	// it doesn't outlive the task hash. Best-effort, separate call: it lives outside the
	// queue hash-tag and so can't be part of the multi-key Lua script above.
	if ttl > 0 {
		r.Rdb.Expire(ctx, fmt.Sprintf(KeyTaskLookup, taskID), ttl)
	}

	return nil
}

// retryWithDelayCmd atomically removes from active/lease, adds to retry sorted set.
// KEYS[1] = activeKey, KEYS[2] = leaseKey, KEYS[3] = retryKey, KEYS[4] = taskKey
// ARGV[1] = taskID, ARGV[2] = retryAt (unix), ARGV[3] = now (unix)
var retryWithDelayCmd = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("ZADD", KEYS[3], ARGV[2], ARGV[1])
redis.call("HINCRBY", KEYS[4], "retried", 1)
redis.call("HSET", KEYS[4], "state", "retry", "last_failed_at", ARGV[3])
return 1
`)

// retryImmediateCmd atomically removes from active/lease, pushes directly to pending.
// KEYS[1] = activeKey, KEYS[2] = leaseKey, KEYS[3] = pendingKey, KEYS[4] = taskKey
// ARGV[1] = taskID, ARGV[2] = now (unix)
var retryImmediateCmd = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("LPUSH", KEYS[3], ARGV[1])
redis.call("HINCRBY", KEYS[4], "retried", 1)
redis.call("HSET", KEYS[4], "state", "pending", "last_failed_at", ARGV[2])
return 1
`)

// Retry re-queues a task for retry using the retry sorted set.
func (r *Client) Retry(ctx context.Context, task *TaskData, queueName string, delay time.Duration) error {
	taskKey := fmt.Sprintf(KeyTask, queueName, task.ID)
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)
	retryKey := fmt.Sprintf(KeyRetry, queueName)
	pendingKey := fmt.Sprintf(KeyPending, queueName)

	now := time.Now()

	// Best-effort: update protobuf msg field with incremented retry count (for asynqmon compatibility).
	if msgData, err := r.Rdb.HGet(ctx, taskKey, FieldMsg).Result(); err == nil {
		if asynqMsg, err := parseAsynqMessage([]byte(msgData)); err == nil {
			asynqMsg.Retried++
			updatedMsg := encodeAsynqMessage(asynqMsg)
			r.Rdb.HSet(ctx, taskKey, FieldMsg, string(updatedMsg))
		}
	} else {
		msg := encodeAsynqMessage(&asynqTaskMessage{
			Type:     task.Type,
			ID:       task.ID,
			Queue:    queueName,
			MaxRetry: task.MaxRetry,
			Retried:  task.Retry + 1,
		})
		r.Rdb.HSet(ctx, taskKey, FieldMsg, string(msg))
	}

	// Atomic: remove from active + lease, move to retry or pending
	if delay > 0 {
		retryAt := float64(now.Add(delay).Unix())
		if err := retryWithDelayCmd.Run(ctx, r.Rdb,
			[]string{activeKey, leaseKey, retryKey, taskKey},
			task.ID, retryAt, now.Unix(),
		).Err(); err != nil {
			return fmt.Errorf("retry script failed for task %s: %w", task.ID, err)
		}
	} else {
		if err := retryImmediateCmd.Run(ctx, r.Rdb,
			[]string{activeKey, leaseKey, pendingKey, taskKey},
			task.ID, now.Unix(),
		).Err(); err != nil {
			return fmt.Errorf("retry-immediate script failed for task %s: %w", task.ID, err)
		}
	}

	return nil
}

// ForwardReady moves tasks from the retry sorted set to pending for each queue.
// It scans for tasks whose retry time (score) has passed and atomically moves them.
// Returns the total number of tasks forwarded.
func (r *Client) ForwardReady(ctx context.Context, queues []string) (int, error) {
	now := float64(time.Now().Unix())
	total := 0

	// Deduplicate queue names (buildQueueList may repeat queues for weighting)
	seen := make(map[string]bool, len(queues))
	for _, q := range queues {
		if seen[q] {
			continue
		}
		seen[q] = true

		retryKey := fmt.Sprintf(KeyRetry, q)
		pendingKey := fmt.Sprintf(KeyPending, q)

		// Get all tasks whose score (retry time) <= now
		tasks, err := r.Rdb.ZRangeByScore(ctx, retryKey, &redis.ZRangeBy{
			Min: "-inf",
			Max: fmt.Sprintf("%v", now),
		}).Result()
		if err != nil {
			r.Logger.Error("ForwardReady: failed to scan retry set for queue %s: %v", q, err)
			continue
		}

		for _, taskID := range tasks {
			taskKey := fmt.Sprintf(KeyTask, q, taskID)

			// Atomically: remove from retry, push to pending, update state
			forwardScript := redis.NewScript(`
				local removed = redis.call("ZREM", KEYS[1], ARGV[1])
				if removed == 1 then
					redis.call("LPUSH", KEYS[2], ARGV[1])
					redis.call("HSET", KEYS[3], "state", ARGV[2], "pending_since", ARGV[3])
					return 1
				end
				return 0
			`)
			result, err := forwardScript.Run(ctx, r.Rdb,
				[]string{retryKey, pendingKey, taskKey},
				taskID, StatePending, time.Now().Unix(),
			).Int()
			if err != nil {
				r.Logger.Error("ForwardReady: failed to forward task %s: %v", taskID, err)
				continue
			}
			if result == 1 {
				total++
				r.Logger.Info("ForwardReady: moved task %s from retry to pending (queue=%s)", taskID, q)
			}
		}
	}

	return total, nil
}

// failCmd atomically removes from active/lease and archives a task.
// KEYS[1] = activeKey, KEYS[2] = leaseKey, KEYS[3] = archivedKey, KEYS[4] = taskKey
// ARGV[1] = taskID, ARGV[2] = now (unix), ARGV[3] = errMsg, ARGV[4] = ttl (seconds, 0 = no expiry),
// ARGV[5] = expiry score (unix seconds the entry becomes purgeable by the janitor)
var failCmd = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("ZADD", KEYS[3], ARGV[5], ARGV[1])
redis.call("HSET", KEYS[4], "state", "archived", "error_msg", ARGV[3], "last_failed_at", ARGV[2])
if tonumber(ARGV[4]) > 0 then
    redis.call("EXPIRE", KEYS[4], ARGV[4])
end
return 1
`)

// Fail marks a task as permanently failed (archived).
// If ttl > 0, sets an expiry on the task hash key so it is automatically cleaned up.
func (r *Client) Fail(ctx context.Context, task *TaskData, queueName string, errMsg string, ttl time.Duration) error {
	taskKey := fmt.Sprintf(KeyTask, queueName, task.ID)
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)
	archivedKey := fmt.Sprintf(KeyArchived, queueName)

	now := time.Now().Unix()

	// Best-effort: ensure protobuf msg field exists for asynq inspector compatibility.
	if exists, _ := r.Rdb.HExists(ctx, taskKey, FieldMsg).Result(); !exists {
		msg := encodeAsynqMessage(&asynqTaskMessage{
			Type:     task.Type,
			ID:       task.ID,
			Queue:    queueName,
			MaxRetry: task.MaxRetry,
			Retried:  task.Retry,
		})
		r.Rdb.HSet(ctx, taskKey, FieldMsg, string(msg))
	}

	// Atomic: remove from active + lease, add to archived, update state
	ttlSecs := int64(0)
	if ttl > 0 {
		ttlSecs = int64(ttl.Seconds())
	}
	if err := failCmd.Run(ctx, r.Rdb,
		[]string{activeKey, leaseKey, archivedKey, taskKey},
		task.ID, now, errMsg, ttlSecs, expiryScore(now, ttl),
	).Err(); err != nil {
		return fmt.Errorf("fail script failed for task %s: %w", task.ID, err)
	}

	// Realign the server-written reverse-lookup key (asynq:t:<id>) with the task's TTL
	// (see Complete). Best-effort, outside the queue hash-tag.
	if ttl > 0 {
		r.Rdb.Expire(ctx, fmt.Sprintf(KeyTaskLookup, task.ID), ttl)
	}

	return nil
}

// cleanFinishedBatchSize bounds how many expired entries are removed per Lua call.
const cleanFinishedBatchSize = 100

// cleanFinishedCmd removes expired members from a finished-task sorted set (completed or
// archived) and deletes their queue-scoped task hashes. All keys share the {queue} hash
// tag, so this is Redis-Cluster safe (mirrors upstream asynq's janitor).
// KEYS[1] = finishedKey (the :completed or :archived ZSet)
// ARGV[1] = cutoff score (unix seconds), ARGV[2] = task hash prefix "asynq:{queue}:t:",
// ARGV[3] = batch limit. Returns the list of removed task IDs.
var cleanFinishedCmd = redis.NewScript(`
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[3]))
for _, id in ipairs(ids) do
    redis.call("DEL", ARGV[2] .. id)
    redis.call("ZREM", KEYS[1], id)
end
return ids
`)

// CleanFinished trims the :completed and :archived sorted sets, removing entries whose expiry
// score (set by Complete/Fail to now+ttl) has passed. It also deletes the queue-scoped task hash
// and the server-written reverse-lookup key (asynq:t:<id>) for each removed entry. This is the
// worker-side replacement for asynq's janitor, which never runs (the server uses asynq only as a
// Client/Inspector). Per-task TTLs are honoured because the score encodes each task's own expiry.
// Returns the total number of entries removed.
func (r *Client) CleanFinished(ctx context.Context, queues []string) (int, error) {
	cutoff := time.Now().Unix()
	total := 0
	for _, q := range queues {
		taskPrefix := fmt.Sprintf(KeyTask, q, "") // "asynq:{queue}:t:"
		for _, zkey := range []string{fmt.Sprintf(KeyCompleted, q), fmt.Sprintf(KeyArchived, q)} {
			// Drain in batches so a large backlog clears over successive calls without one
			// giant blocking script. Cap iterations to keep a single cycle bounded.
			for i := 0; i < 100; i++ {
				ids, err := cleanFinishedCmd.Run(ctx, r.Rdb,
					[]string{zkey}, cutoff, taskPrefix, cleanFinishedBatchSize,
				).StringSlice()
				if err != nil {
					return total, fmt.Errorf("clean finished failed for %s: %w", zkey, err)
				}
				for _, id := range ids {
					// Reverse-lookup key is not queue-scoped, so it must be deleted outside
					// the script; per-id DEL is Cluster-safe (each routed to its own slot).
					r.Rdb.Del(ctx, fmt.Sprintf(KeyTaskLookup, id))
				}
				total += len(ids)
				if len(ids) < cleanFinishedBatchSize {
					break
				}
			}
		}
	}
	return total, nil
}

// Ping checks Redis connectivity.
func (r *Client) Ping(ctx context.Context) error {
	return r.Rdb.Ping(ctx).Err()
}

// GetRedisClient returns the underlying redis client for advanced operations.
func (r *Client) GetRedisClient() *redis.Client {
	return r.Rdb
}

// cleanupActiveCmd atomically removes from active/lease and deletes the task hash.
// KEYS[1] = activeKey, KEYS[2] = leaseKey, KEYS[3] = taskKey
// ARGV[1] = taskID
var cleanupActiveCmd = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("DEL", KEYS[3])
return 1
`)

// CleanupActive removes a task from active queue without storing completion data.
// Used when redis_storage is disabled.
func (r *Client) CleanupActive(ctx context.Context, taskID string, queueName string) error {
	taskKey := fmt.Sprintf(KeyTask, queueName, taskID)
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)

	if err := cleanupActiveCmd.Run(ctx, r.Rdb,
		[]string{activeKey, leaseKey, taskKey},
		taskID,
	).Err(); err != nil {
		return fmt.Errorf("cleanup-active script failed for task %s: %w", taskID, err)
	}

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

// --- Lease recovery methods ---

// ListLeaseExpired returns task IDs with expired leases for a given queue.
// cutoff is the time threshold: tasks with lease score <= cutoff are considered expired.
func (r *Client) ListLeaseExpired(ctx context.Context, queueName string, cutoff time.Time) ([]*TaskData, error) {
	leaseKey := fmt.Sprintf(KeyLease, queueName)

	ids, err := r.Rdb.ZRangeByScore(ctx, leaseKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", cutoff.Unix()),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("zrangebyscore failed for lease key %s: %w", leaseKey, err)
	}

	var tasks []*TaskData
	for _, taskID := range ids {
		taskKey := fmt.Sprintf(KeyTask, queueName, taskID)
		data, err := r.Rdb.HGetAll(ctx, taskKey).Result()
		if err != nil || len(data) == 0 {
			// Orphaned lease entry — task hash deleted. Return with zero MaxRetry so recoverer archives it.
			tasks = append(tasks, &TaskData{
				ID:      taskID,
				Queue:   queueName,
				TaskKey: taskKey,
			})
			continue
		}

		// Parse task data
		var maxRetry, retried int
		var taskType string
		if msgData, ok := data[FieldMsg]; ok {
			if asynqMsg, err := parseAsynqMessage([]byte(msgData)); err == nil {
				taskType = asynqMsg.Type
				maxRetry = asynqMsg.MaxRetry
				retried = asynqMsg.Retried
			}
		}
		// Hash field overrides protobuf for retried count
		if retriedStr, ok := data[FieldRetried]; ok {
			var hashRetried int
			if _, err := fmt.Sscanf(retriedStr, "%d", &hashRetried); err == nil && hashRetried > retried {
				retried = hashRetried
			}
		}

		tasks = append(tasks, &TaskData{
			ID:       taskID,
			Type:     taskType,
			Retry:    retried,
			MaxRetry: maxRetry,
			Queue:    queueName,
			TaskKey:  taskKey,
		})
	}

	return tasks, nil
}

// recoverToRetryCmd atomically removes from active/lease and adds to retry sorted set.
var recoverToRetryCmd = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("ZADD", KEYS[3], ARGV[2], ARGV[1])
redis.call("HINCRBY", KEYS[4], "retried", 1)
redis.call("HSET", KEYS[4], "state", "retry", "error_msg", "lease expired", "last_failed_at", ARGV[3])
return 1
`)

// RecoverToRetry moves an expired-lease task from active to retry with a delay.
func (r *Client) RecoverToRetry(ctx context.Context, task *TaskData, queueName string, delay time.Duration) error {
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)
	retryKey := fmt.Sprintf(KeyRetry, queueName)
	taskKey := fmt.Sprintf(KeyTask, queueName, task.ID)

	now := time.Now()
	retryAt := float64(now.Add(delay).Unix())

	if err := recoverToRetryCmd.Run(ctx, r.Rdb,
		[]string{activeKey, leaseKey, retryKey, taskKey},
		task.ID, retryAt, now.Unix(),
	).Err(); err != nil {
		return fmt.Errorf("recover-to-retry script failed for task %s: %w", task.ID, err)
	}
	return nil
}

// recoverToArchiveCmd atomically removes from active/lease and archives a task.
var recoverToArchiveCmd = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("ZADD", KEYS[3], ARGV[2], ARGV[1])
redis.call("HSET", KEYS[4], "state", "archived", "error_msg", "lease expired: max retries exceeded", "last_failed_at", ARGV[2])
return 1
`)

// RecoverToArchive moves an expired-lease task from active to archived.
func (r *Client) RecoverToArchive(ctx context.Context, task *TaskData, queueName string) error {
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)
	archivedKey := fmt.Sprintf(KeyArchived, queueName)
	taskKey := fmt.Sprintf(KeyTask, queueName, task.ID)

	now := time.Now().Unix()

	if err := recoverToArchiveCmd.Run(ctx, r.Rdb,
		[]string{activeKey, leaseKey, archivedKey, taskKey},
		task.ID, now,
	).Err(); err != nil {
		return fmt.Errorf("recover-to-archive script failed for task %s: %w", task.ID, err)
	}
	return nil
}

// recoverOrphanedCmd removes orphaned entries from active list and lease set (no task hash to update).
var recoverOrphanedCmd = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
return 1
`)

// RecoverOrphaned removes an orphaned task (no hash data) from active and lease.
func (r *Client) RecoverOrphaned(ctx context.Context, taskID string, queueName string) error {
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)

	if err := recoverOrphanedCmd.Run(ctx, r.Rdb,
		[]string{activeKey, leaseKey},
		taskID,
	).Err(); err != nil {
		return fmt.Errorf("recover-orphaned script failed for task %s: %w", taskID, err)
	}
	return nil
}

// ExtendLease renews the lease of an in-flight task, pushing its expiration to
// now+leaseDuration. It uses ZADD with XX so a task whose lease was already
// removed (e.g. it just completed) is NOT re-added — this avoids resurrecting a
// finished task's lease entry. Safe to call on the shared per-sub-queue lease key
// from multiple workers, since each only extends its own task IDs.
func (r *Client) ExtendLease(ctx context.Context, taskID, queueName string, leaseDuration time.Duration) error {
	if leaseDuration <= 0 {
		leaseDuration = 2 * time.Minute
	}
	leaseKey := fmt.Sprintf(KeyLease, queueName)
	expiration := float64(time.Now().Add(leaseDuration).Unix())
	if err := r.Rdb.ZAddArgs(ctx, leaseKey, redis.ZAddArgs{
		XX:      true,
		Members: []redis.Z{{Score: expiration, Member: taskID}},
	}).Err(); err != nil {
		return fmt.Errorf("extend-lease failed for task %s: %w", taskID, err)
	}
	return nil
}

// requeueActiveCmd moves a SINGLE task from active back to pending, but only if it
// is actually still in the active list. It never touches other tasks, so it is safe
// on the shared per-sub-queue active/lease keys when multiple workers run.
// Returns the number of entries removed from active (0 if the task was no longer active).
var requeueActiveCmd = redis.NewScript(`
local removed = redis.call("LREM", KEYS[1], 0, ARGV[1])
if removed > 0 then
    redis.call("ZREM", KEYS[2], ARGV[1])
    redis.call("LPUSH", KEYS[3], ARGV[1])
    redis.call("HSET", KEYS[4], "state", "pending")
end
return removed
`)

// RequeueActive moves one task from the active list back to pending (and clears its
// lease) if it is still active. Used on graceful shutdown to requeue this worker's
// own in-flight tasks without disturbing tasks owned by other workers.
func (r *Client) RequeueActive(ctx context.Context, taskID, queueName string) (int, error) {
	activeKey := fmt.Sprintf(KeyActive, queueName)
	leaseKey := fmt.Sprintf(KeyLease, queueName)
	pendingKey := fmt.Sprintf(KeyPending, queueName)
	taskKey := fmt.Sprintf(KeyTask, queueName, taskID)

	removed, err := requeueActiveCmd.Run(ctx, r.Rdb,
		[]string{activeKey, leaseKey, pendingKey, taskKey},
		taskID,
	).Int()
	if err != nil {
		return 0, fmt.Errorf("requeue-active script failed for task %s: %w", taskID, err)
	}
	return removed, nil
}
