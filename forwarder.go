package worker

import (
	"context"
	"sync"
	"time"
)

// retryForwarder periodically scans the retry sorted sets and moves tasks
// whose retry time has passed back to the pending queue.
// This replaces the previous per-task goroutine approach, making retries
// resilient to worker restarts — any worker listening on the same queue
// can forward and pick up retry tasks.
type retryForwarder struct {
	redis    *redisClient
	redisMu  *sync.RWMutex // shared lock from processor for redis client swaps
	queues   []string
	interval time.Duration
	logger   Logger

	done chan struct{}
	wg   sync.WaitGroup
}

// newRetryForwarder creates a new retry forwarder.
// interval controls how often the retry sorted sets are scanned.
func newRetryForwarder(rdb *redisClient, redisMu *sync.RWMutex, queues []string, interval time.Duration, logger Logger) *retryForwarder {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &retryForwarder{
		redis:    rdb,
		redisMu:  redisMu,
		queues:   queues,
		interval: interval,
		logger:   logger,
		done:     make(chan struct{}),
	}
}

// start begins the forwarding loop.
func (f *retryForwarder) start() {
	f.wg.Add(1)
	go f.loop()
}

// stop signals the forwarder to stop and waits for it to finish.
func (f *retryForwarder) stop() {
	close(f.done)
	f.wg.Wait()
}

// updateClient updates the Redis client after reconnection.
func (f *retryForwarder) updateClient(rdb *redisClient) {
	// Caller must hold redisMu write lock
	f.redis = rdb
}

func (f *retryForwarder) loop() {
	defer f.wg.Done()

	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for {
		select {
		case <-f.done:
			return
		case <-ticker.C:
			f.forward()
		}
	}
}

func (f *retryForwarder) forward() {
	f.redisMu.RLock()
	rdb := f.redis
	f.redisMu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	n, err := rdb.forwardReady(ctx, f.queues)
	if err != nil {
		f.logger.Error("Retry forwarder error: %v", err)
		return
	}
	if n > 0 {
		f.logger.Info("Retry forwarder: moved %d task(s) to pending", n)
	}
}
