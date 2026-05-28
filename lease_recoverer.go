package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	iredis "github.com/publikey/runqy-worker/internal/redis"
)

// leaseRecoverer periodically scans the lease sorted sets for tasks with expired leases
// and recovers them (retry or archive). This handles cases where a worker crashes or
// is killed without properly cleaning up its active tasks.
type leaseRecoverer struct {
	redis    *redisClient
	redisMu  *sync.RWMutex // shared lock from processor for redis client swaps
	queues   []string
	interval time.Duration
	logger   Logger

	done chan struct{}
	wg   sync.WaitGroup
}

// newLeaseRecoverer creates a new lease recoverer.
// interval controls how often the lease sorted sets are scanned (default 60s).
func newLeaseRecoverer(rdb *redisClient, redisMu *sync.RWMutex, queues []string, interval time.Duration, logger Logger) *leaseRecoverer {
	if interval <= 0 {
		interval = 60 * time.Second
	}
	return &leaseRecoverer{
		redis:    rdb,
		redisMu:  redisMu,
		queues:   queues,
		interval: interval,
		logger:   logger,
		done:     make(chan struct{}),
	}
}

// start begins the recovery loop.
func (lr *leaseRecoverer) start() {
	lr.wg.Add(1)
	go lr.loop()
}

// stop signals the recoverer to stop and waits for it to finish.
func (lr *leaseRecoverer) stop() {
	close(lr.done)
	lr.wg.Wait()
}

// updateClient updates the Redis client after reconnection.
func (lr *leaseRecoverer) updateClient(rdb *redisClient) {
	lr.redis = rdb
}

func (lr *leaseRecoverer) loop() {
	defer lr.wg.Done()

	ticker := time.NewTicker(lr.interval)
	defer ticker.Stop()

	for {
		select {
		case <-lr.done:
			return
		case <-ticker.C:
			lr.recover()
		}
	}
}

func (lr *leaseRecoverer) recover() {
	lr.redisMu.RLock()
	rdb := lr.redis
	lr.redisMu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 30-second buffer to accommodate clock skew (same as asynq's recoverer)
	cutoff := time.Now().Add(-30 * time.Second)

	// Deduplicate queues (buildQueueList may repeat queues for weighting)
	seen := make(map[string]bool, len(lr.queues))
	for _, q := range lr.queues {
		if seen[q] {
			continue
		}
		seen[q] = true

		lr.recoverQueue(ctx, rdb, q, cutoff)
	}
}

func (lr *leaseRecoverer) recoverQueue(ctx context.Context, rdb *redisClient, queue string, cutoff time.Time) {
	tasks, err := rdb.listLeaseExpired(ctx, queue, cutoff)
	if err != nil {
		lr.logger.Error("Lease recoverer: failed to list expired for queue %s: %v", queue, err)
		return
	}

	for _, task := range tasks {
		lr.recoverTask(ctx, rdb, task, queue)
	}
}

func (lr *leaseRecoverer) recoverTask(ctx context.Context, rdb *redisClient, task *iredis.TaskData, queue string) {
	// Orphaned task (hash deleted) — just clean up active + lease
	if task.Type == "" && task.MaxRetry == 0 && task.Retry == 0 {
		if err := rdb.recoverOrphaned(ctx, task.ID, queue); err != nil {
			lr.logger.Error("Lease recoverer: failed to clean orphaned task %s: %v", task.ID, err)
		} else {
			lr.logger.Warn("Lease recoverer: cleaned orphaned task %s (queue=%s)", task.ID, queue)
		}
		return
	}

	if task.Retry < task.MaxRetry {
		// Retry with exponential backoff based on retry count
		delay := defaultRecoverRetryDelay(task.Retry + 1)
		if err := rdb.recoverToRetry(ctx, task, queue, delay); err != nil {
			lr.logger.Error("Lease recoverer: failed to retry task %s: %v", task.ID, err)
		} else {
			lr.logger.Warn("Lease recoverer: recovered task %s to retry (queue=%s, attempt=%d/%d)",
				task.ID, queue, task.Retry+1, task.MaxRetry)
		}
	} else {
		if err := rdb.recoverToArchive(ctx, task, queue); err != nil {
			lr.logger.Error("Lease recoverer: failed to archive task %s: %v", task.ID, err)
		} else {
			lr.logger.Warn("Lease recoverer: archived expired task %s (queue=%s, max retries exceeded)",
				task.ID, queue)
		}
		// Increment failed counter
		if err := rdb.incrementFailed(ctx, queue); err != nil {
			lr.logger.Error("Lease recoverer: failed to increment failed counter for queue %s: %v", queue, err)
		}
	}
}

// defaultRecoverRetryDelay returns a simple exponential backoff delay for recovered tasks.
func defaultRecoverRetryDelay(n int) time.Duration {
	// 15s, 30s, 60s, 120s, ... capped at 10 minutes
	delay := time.Duration(15<<uint(n-1)) * time.Second
	if delay > 10*time.Minute {
		delay = 10 * time.Minute
	}
	return delay
}

// recoverActiveOnShutdown requeues this worker's own remaining in-flight tasks back
// to pending. It operates strictly on the given taskID -> queue map, so it never
// disturbs tasks owned by OTHER workers sharing the same per-sub-queue active/lease
// keys. Should be called during graceful shutdown, after the processor has stopped.
func recoverActiveOnShutdown(ctx context.Context, rdb *redisClient, inflight map[string]string, logger Logger) {
	recovered := 0
	for taskID, queueName := range inflight {
		n, err := rdb.requeueActive(ctx, taskID, queueName)
		if err != nil {
			logger.Error("Shutdown recovery: failed to requeue task %s (queue=%s): %v", taskID, queueName, err)
			continue
		}
		recovered += n
	}
	if recovered > 0 {
		logger.Info(fmt.Sprintf("Shutdown recovery: moved %d active task(s) back to pending", recovered))
	}
}
