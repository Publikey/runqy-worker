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
// leaseDuration sets the initial lease validity for the dequeued task.
// Returns nil if no task is available.
func (r *redisClient) dequeue(ctx context.Context, queues []string, leaseDuration time.Duration) (*Task, error) {
	taskData, err := r.internal.Dequeue(ctx, queues, leaseDuration)
	if err != nil {
		return nil, err
	}

	if taskData == nil {
		return nil, nil
	}

	// Convert TaskData to Task
	task := &Task{
		id:             taskData.ID,
		typename:       taskData.Type,
		payload:        taskData.Payload,
		retry:          taskData.Retry,
		maxRetry:       taskData.MaxRetry,
		queue:          taskData.Queue,
		pendingSince:   taskData.PendingSince,
		ttlCompleted:   taskData.TTLCompleted,
		ttlArchived:    taskData.TTLArchived,
		pendingTimeout: taskData.PendingTimeout,
		activeTimeout:  taskData.ActiveTimeout,
		rdb:            r.rdb,
		taskKey:        taskData.TaskKey,
	}

	return task, nil
}

// complete marks a task as completed and removes it from active queue.
// If ttl > 0, sets an expiry on the task hash key.
func (r *redisClient) complete(ctx context.Context, task *Task, queueName string, ttl time.Duration) error {
	return r.internal.Complete(ctx, task.id, queueName, ttl)
}

// retry re-queues a task for retry.
func (r *redisClient) retry(ctx context.Context, task *Task, queueName string, delay time.Duration) error {
	return r.internal.Retry(ctx, &redis.TaskData{
		ID:       task.id,
		Type:     task.typename,
		Queue:    task.queue,
		MaxRetry: task.maxRetry,
		Retry:    task.retry,
	}, queueName, delay)
}

// fail marks a task as permanently failed.
// If ttl > 0, sets an expiry on the task hash key.
func (r *redisClient) fail(ctx context.Context, task *Task, queueName string, errMsg string, ttl time.Duration) error {
	return r.internal.Fail(ctx, &redis.TaskData{
		ID:       task.id,
		Type:     task.typename,
		Queue:    task.queue,
		MaxRetry: task.maxRetry,
		Retry:    task.retry,
	}, queueName, errMsg, ttl)
}

// forwardReady moves tasks from retry sorted sets to pending for the given queues.
func (r *redisClient) forwardReady(ctx context.Context, queues []string) (int, error) {
	return r.internal.ForwardReady(ctx, queues)
}

// cleanFinished trims the :completed and :archived sorted sets, removing entries whose per-task
// expiry has passed, along with their task hashes and reverse-lookup keys.
func (r *redisClient) cleanFinished(ctx context.Context, queues []string) (int, error) {
	return r.internal.CleanFinished(ctx, queues)
}

// ping checks Redis connectivity.
func (r *redisClient) ping(ctx context.Context) error {
	return r.internal.Ping(ctx)
}

// cleanupActive removes a task from active queue without storing completion data.
func (r *redisClient) cleanupActive(ctx context.Context, task *Task, queueName string) error {
	return r.internal.CleanupActive(ctx, task.id, queueName)
}

// incrementProcessed increments the processed counters for a queue.
func (r *redisClient) incrementProcessed(ctx context.Context, queueName string) error {
	return r.internal.IncrementProcessed(ctx, queueName)
}

// incrementFailed increments the failed counters for a queue.
func (r *redisClient) incrementFailed(ctx context.Context, queueName string) error {
	return r.internal.IncrementFailed(ctx, queueName)
}

// listLeaseExpired returns tasks with expired leases for a given queue.
func (r *redisClient) listLeaseExpired(ctx context.Context, queueName string, cutoff time.Time) ([]*redis.TaskData, error) {
	return r.internal.ListLeaseExpired(ctx, queueName, cutoff)
}

// recoverToRetry moves an expired-lease task to retry with delay.
func (r *redisClient) recoverToRetry(ctx context.Context, task *redis.TaskData, queueName string, delay time.Duration) error {
	return r.internal.RecoverToRetry(ctx, task, queueName, delay)
}

// recoverToArchive moves an expired-lease task to archived.
func (r *redisClient) recoverToArchive(ctx context.Context, task *redis.TaskData, queueName string) error {
	return r.internal.RecoverToArchive(ctx, task, queueName)
}

// recoverOrphaned removes an orphaned task from active and lease.
func (r *redisClient) recoverOrphaned(ctx context.Context, taskID string, queueName string) error {
	return r.internal.RecoverOrphaned(ctx, taskID, queueName)
}

// extendLease renews the lease of an in-flight task.
func (r *redisClient) extendLease(ctx context.Context, taskID, queueName string, leaseDuration time.Duration) error {
	return r.internal.ExtendLease(ctx, taskID, queueName, leaseDuration)
}

// requeueActive moves a single in-flight task from active back to pending (if still active).
func (r *redisClient) requeueActive(ctx context.Context, taskID, queueName string) (int, error) {
	return r.internal.RequeueActive(ctx, taskID, queueName)
}
