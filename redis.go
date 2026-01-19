package worker

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/publikey/runqy-worker/internal/redis"
)

// Re-export key constants from internal/redis
const (
	keyPrefix     = redis.KeyPrefix
	keyPending    = redis.KeyPending
	keyActive     = redis.KeyActive
	keyTask       = redis.KeyTask
	keyWorkers    = redis.KeyWorkers
	keyWorkerData = redis.KeyWorkerData
)

// redisClient wraps the internal redis.Client and provides Task creation.
type redisClient struct {
	internal *redis.Client
	rdb      *goredis.Client
}

// newRedisClient creates a new Redis client wrapper.
func newRedisClient(rdb *goredis.Client, logger Logger) *redisClient {
	return &redisClient{
		internal: redis.NewClient(rdb, &loggerAdapter{logger}),
		rdb:      rdb,
	}
}

// dequeue attempts to dequeue a task from the given queues.
// Returns nil if no task is available.
func (r *redisClient) dequeue(ctx context.Context, queues []string) (*Task, error) {
	taskData, err := r.internal.Dequeue(ctx, queues)
	if err != nil {
		return nil, err
	}

	if taskData == nil {
		return nil, nil
	}

	// Convert TaskData to Task
	task := &Task{
		id:       taskData.ID,
		typename: taskData.Type,
		payload:  taskData.Payload,
		retry:    taskData.Retry,
		maxRetry: taskData.MaxRetry,
		queue:    taskData.Queue,
		rdb:      r.rdb,
		taskKey:  taskData.TaskKey,
	}

	return task, nil
}

// complete marks a task as completed and removes it from active queue.
func (r *redisClient) complete(ctx context.Context, task *Task, queueName string) error {
	return r.internal.Complete(ctx, task.id, queueName)
}

// retry re-queues a task for retry.
func (r *redisClient) retry(ctx context.Context, task *Task, queueName string, delay time.Duration) error {
	return r.internal.Retry(ctx, task.id, queueName, delay)
}

// fail marks a task as permanently failed.
func (r *redisClient) fail(ctx context.Context, task *Task, queueName string, errMsg string) error {
	return r.internal.Fail(ctx, task.id, queueName, errMsg)
}

// ping checks Redis connectivity.
func (r *redisClient) ping(ctx context.Context) error {
	return r.internal.Ping(ctx)
}
