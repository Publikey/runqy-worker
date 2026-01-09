package worker

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// heartbeat sends periodic heartbeats to Redis to indicate worker is alive.
type heartbeat struct {
	rdb        *redis.Client
	workerID   string
	interval   time.Duration
	done       chan struct{}
	wg         sync.WaitGroup
	logger     Logger
	config     Config
}

// newHeartbeat creates a new heartbeat sender.
func newHeartbeat(rdb *redis.Client, cfg Config) *heartbeat {
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	workerID := fmt.Sprintf("%s:%d", hostname, pid)

	return &heartbeat{
		rdb:      rdb,
		workerID: workerID,
		interval: 5 * time.Second,
		done:     make(chan struct{}),
		logger:   cfg.Logger,
		config:   cfg,
	}
}

// start begins sending heartbeats.
func (h *heartbeat) start() {
	h.wg.Add(1)
	go h.loop()
}

// stop stops sending heartbeats.
func (h *heartbeat) stop() {
	close(h.done)
	h.wg.Wait()

	// Remove worker from registry
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.deregister(ctx)
}

// loop sends heartbeats at regular intervals.
func (h *heartbeat) loop() {
	defer h.wg.Done()

	ctx := context.Background()

	// Initial registration
	h.register(ctx)

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-h.done:
			return
		case <-ticker.C:
			h.beat(ctx)
		}
	}
}

// register adds the worker to the workers set.
func (h *heartbeat) register(ctx context.Context) {
	// Add to workers set with TTL
	h.rdb.SAdd(ctx, keyWorkers, h.workerID)

	// Store worker info
	workerKey := fmt.Sprintf(keyWorkerData, h.workerID)
	now := time.Now().Unix()

	data := map[string]interface{}{
		"worker_id":   h.workerID,
		"started_at":  now,
		"last_beat":   now,
		"concurrency": h.config.Concurrency,
		"queues":      fmt.Sprintf("%v", h.config.Queues),
		"status":      "running",
	}
	h.rdb.HSet(ctx, workerKey, data)
	h.rdb.Expire(ctx, workerKey, 30*time.Second)

	h.logger.Info("Worker registered:", h.workerID)
}

// beat updates the worker's last heartbeat time.
func (h *heartbeat) beat(ctx context.Context) {
	workerKey := fmt.Sprintf(keyWorkerData, h.workerID)
	now := time.Now().Unix()

	h.rdb.HSet(ctx, workerKey, "last_beat", now)
	h.rdb.Expire(ctx, workerKey, 30*time.Second)

	// Renew membership in workers set
	h.rdb.SAdd(ctx, keyWorkers, h.workerID)
}

// deregister removes the worker from the registry.
func (h *heartbeat) deregister(ctx context.Context) {
	h.rdb.SRem(ctx, keyWorkers, h.workerID)

	workerKey := fmt.Sprintf(keyWorkerData, h.workerID)
	h.rdb.HSet(ctx, workerKey, "status", "stopped")

	h.logger.Info("Worker deregistered:", h.workerID)
}
