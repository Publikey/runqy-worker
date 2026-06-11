package worker

import (
	"context"
	"sync"
	"time"
)

// janitor periodically trims the :completed and :archived sorted sets, removing entries
// older than the retention window along with their task hashes and reverse-lookup keys.
//
// This is the worker-side replacement for asynq's janitor: the runqy server uses asynq only
// as a Client/Inspector (never asynq.Server), so asynq's own janitor never runs and these
// sorted sets would otherwise grow unbounded and eventually OOM Redis.
type janitor struct {
	redis    *redisClient
	redisMu  *sync.RWMutex // shared lock from processor for redis client swaps
	queues   []string
	interval time.Duration
	logger   Logger

	done chan struct{}
	wg   sync.WaitGroup
}

// newJanitor creates a new janitor.
// interval controls how often the finished sorted sets are scanned (default: 1m).
// Per-task expiry is encoded in each ZSet entry's score (now+ttl), so the janitor needs no
// retention parameter — it simply purges entries whose expiry score has passed.
func newJanitor(rdb *redisClient, redisMu *sync.RWMutex, queues []string, interval time.Duration, logger Logger) *janitor {
	if interval <= 0 {
		interval = 1 * time.Minute
	}
	return &janitor{
		redis:    rdb,
		redisMu:  redisMu,
		queues:   queues,
		interval: interval,
		logger:   logger,
		done:     make(chan struct{}),
	}
}

// start begins the cleanup loop.
func (j *janitor) start() {
	j.wg.Add(1)
	go j.loop()
}

// stop signals the janitor to stop and waits for it to finish.
func (j *janitor) stop() {
	close(j.done)
	j.wg.Wait()
}

// updateClient updates the Redis client after reconnection.
// Caller must hold redisMu write lock.
func (j *janitor) updateClient(rdb *redisClient) {
	j.redis = rdb
}

func (j *janitor) loop() {
	defer j.wg.Done()

	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-j.done:
			return
		case <-ticker.C:
			j.clean()
		}
	}
}

func (j *janitor) clean() {
	j.redisMu.RLock()
	rdb := j.redis
	j.redisMu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	n, err := rdb.cleanFinished(ctx, j.queues)
	if err != nil {
		j.logger.Error("Janitor error: %v", err)
		return
	}
	if n > 0 {
		j.logger.Info("Janitor: removed %d expired finished task(s)", n)
	}
}
