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
	rdb         *redis.Client
	workerID    string
	interval    time.Duration
	done        chan struct{}
	wg          sync.WaitGroup
	logger      Logger
	config      Config
	queueStates map[string]*QueueState // supervised processes (may be nil or empty)
}

// newHeartbeat creates a new heartbeat sender.
func newHeartbeat(rdb *redis.Client, cfg Config, queueStates map[string]*QueueState) *heartbeat {
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	workerID := fmt.Sprintf("%s:%d", hostname, pid)

	return &heartbeat{
		rdb:         rdb,
		workerID:    workerID,
		interval:    5 * time.Second,
		done:        make(chan struct{}),
		logger:      cfg.Logger,
		config:      cfg,
		queueStates: queueStates,
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
		"healthy":     h.isHealthy(),
	}
	h.rdb.HSet(ctx, workerKey, data)
	h.rdb.Expire(ctx, workerKey, 30*time.Second)

	h.logger.Info("Worker registered: %s", h.workerID)
}

// beat updates the worker's last heartbeat time.
func (h *heartbeat) beat(ctx context.Context) {
	workerKey := fmt.Sprintf(keyWorkerData, h.workerID)
	now := time.Now().Unix()
	healthy := h.isHealthy()

	h.rdb.HSet(ctx, workerKey, map[string]interface{}{
		"last_beat": now,
		"healthy":   healthy,
	})
	h.rdb.Expire(ctx, workerKey, 30*time.Second)

	// Renew membership in workers set
	h.rdb.SAdd(ctx, keyWorkers, h.workerID)
}

// isHealthy returns true if the worker and all supervised processes are healthy.
func (h *heartbeat) isHealthy() bool {
	if h.queueStates == nil || len(h.queueStates) == 0 {
		return true // No supervisors = worker-only mode, always healthy
	}
	for _, state := range h.queueStates {
		if state.Supervisor != nil && !state.Supervisor.IsHealthy() {
			return false
		}
	}
	return true
}

// deregister removes the worker from the registry.
func (h *heartbeat) deregister(ctx context.Context) {
	h.rdb.SRem(ctx, keyWorkers, h.workerID)

	workerKey := fmt.Sprintf(keyWorkerData, h.workerID)
	h.rdb.HSet(ctx, workerKey, "status", "stopped")

	h.logger.Info("Worker deregistered: %s", h.workerID)
}
