package worker

import (
	"context"
	"io"

	"github.com/redis/go-redis/v9"
)

// Task represents a unit of work to be processed.
type Task struct {
	id       string
	typename string
	payload  []byte
	retry    int
	maxRetry int
	queue    string // The queue this task was dequeued from

	// For result writing
	rdb       *redis.Client
	resultKey string
}

// ID returns the task ID.
func (t *Task) ID() string {
	return t.id
}

// Type returns the task type name.
func (t *Task) Type() string {
	return t.typename
}

// Payload returns the task payload data.
func (t *Task) Payload() []byte {
	return t.payload
}

// RetryCount returns the current retry count.
func (t *Task) RetryCount() int {
	return t.retry
}

// MaxRetry returns the maximum retry attempts.
func (t *Task) MaxRetry() int {
	return t.maxRetry
}

// Queue returns the queue name this task was dequeued from.
func (t *Task) Queue() string {
	return t.queue
}

// ResultWriter returns a writer that stores the result in Redis.
func (t *Task) ResultWriter() io.Writer {
	return &TaskResultWriter{
		task: t,
	}
}

// TaskResultWriter writes task results to Redis.
type TaskResultWriter struct {
	task   *Task
	ctx    context.Context
}

// TaskID returns the task ID.
func (w *TaskResultWriter) TaskID() string {
	return w.task.id
}

// Write writes data to the task result in Redis.
func (w *TaskResultWriter) Write(data []byte) (int, error) {
	if w.task.rdb == nil {
		return len(data), nil // No-op if no Redis client
	}
	ctx := w.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	err := w.task.rdb.Set(ctx, w.task.resultKey, data, 0).Err()
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// SetContext sets the context for Redis operations.
func (w *TaskResultWriter) SetContext(ctx context.Context) {
	w.ctx = ctx
}
