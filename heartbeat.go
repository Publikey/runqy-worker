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
	mu          sync.RWMutex // protects rdb during reconnection
	workerID    string
	interval    time.Duration
	done        chan struct{}
	wg          sync.WaitGroup
	logger      Logger
	config      Config
	queueStates map[string]*QueueState // supervised processes (may be nil or empty)
	status      string                 // current status ("bootstrapping" or "running")
	statusMu    sync.RWMutex           // protects status field
	started     bool                   // whether heartbeat loop has been started
}

// newHeartbeat creates a new heartbeat sender.
// initialStatus should be "bootstrapping" or "running".
func newHeartbeat(rdb *redis.Client, cfg Config, queueStates map[string]*QueueState, initialStatus string) *heartbeat {
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	workerID := fmt.Sprintf("%s:%d", hostname, pid)

	if initialStatus == "" {
		initialStatus = "running"
	}

	return &heartbeat{
		rdb:         rdb,
		workerID:    workerID,
		interval:    5 * time.Second,
		done:        make(chan struct{}),
		logger:      cfg.Logger,
		config:      cfg,
		queueStates: queueStates,
		status:      initialStatus,
		started:     false,
	}
}

// start begins sending heartbeats.
func (h *heartbeat) start() {
	h.wg.Add(1)
	h.started = true
	go h.loop()
}

// isRunning returns true if the heartbeat loop has been started and not stopped.
func (h *heartbeat) isRunning() bool {
	if !h.started {
		return false
	}
	select {
	case <-h.done:
		return false
	default:
		return true
	}
}

// SetStatus updates the worker status (e.g., "bootstrapping" -> "running").
func (h *heartbeat) SetStatus(status string) {
	h.statusMu.Lock()
	h.status = status
	h.statusMu.Unlock()
}

// getStatus returns the current worker status.
func (h *heartbeat) getStatus() string {
	h.statusMu.RLock()
	defer h.statusMu.RUnlock()
	return h.status
}

// updateQueueStates updates the queue states after bootstrap completes.
func (h *heartbeat) updateQueueStates(queueStates map[string]*QueueState) {
	h.queueStates = queueStates
}

// updateQueues updates the queues config and re-registers to update Redis.
// This should be called after bootstrap completes to update the queue list.
func (h *heartbeat) updateQueues(queues map[string]int) {
	h.config.Queues = queues
	// Re-register to update Redis with correct queue information
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.register(ctx)
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
	h.mu.RLock()
	rdb := h.rdb
	h.mu.RUnlock()

	// Add to workers set with TTL
	leaseExpiration := float64(time.Now().Add(30 * time.Minute).Unix())
	if err := rdb.ZAdd(ctx, keyWorkers, redis.Z{Score: leaseExpiration, Member: h.workerID}).Err(); err != nil {
		h.logger.Error("Failed to add worker to workers set: %v", err)
	}

	// Store worker info
	workerKey := fmt.Sprintf(keyWorkerData, h.workerID)
	now := time.Now().Unix()

	data := map[string]interface{}{
		"worker_id":   h.workerID,
		"started_at":  now,
		"last_beat":   now,
		"concurrency": h.config.Concurrency,
		"queues":      fmt.Sprintf("%v", h.config.Queues),
		"status":      h.getStatus(),
		"healthy":     h.isHealthy(),
	}
	if err := rdb.HSet(ctx, workerKey, data).Err(); err != nil {
		h.logger.Error("Failed to set worker data: %v", err)
	}
	if err := rdb.Expire(ctx, workerKey, 30*time.Second).Err(); err != nil {
		h.logger.Error("Failed to set worker data expiry: %v", err)
	}

	h.logger.Info("Worker registered: %s", h.workerID)
}

// beat updates the worker's last heartbeat time.
func (h *heartbeat) beat(ctx context.Context) {
	h.mu.RLock()
	rdb := h.rdb
	h.mu.RUnlock()

	workerKey := fmt.Sprintf(keyWorkerData, h.workerID)
	now := time.Now().Unix()
	healthy := h.isHealthy()

	if err := rdb.HSet(ctx, workerKey, map[string]interface{}{
		"last_beat": now,
		"status":    h.getStatus(),
		"healthy":   healthy,
	}).Err(); err != nil {
		h.logger.Error("Heartbeat HSet failed: %v", err)
	}
	if err := rdb.Expire(ctx, workerKey, 30*time.Second).Err(); err != nil {
		h.logger.Error("Heartbeat Expire failed: %v", err)
	}

	// Renew membership in workers set
	leaseExpiration := float64(time.Now().Add(30 * time.Minute).Unix())
	if err := rdb.ZAdd(ctx, keyWorkers, redis.Z{Score: leaseExpiration, Member: h.workerID}).Err(); err != nil {
		h.logger.Error("Heartbeat ZAdd failed: %v", err)
	}
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
	h.mu.RLock()
	rdb := h.rdb
	h.mu.RUnlock()

	rdb.SRem(ctx, keyWorkers, h.workerID)

	workerKey := fmt.Sprintf(keyWorkerData, h.workerID)
	rdb.HSet(ctx, workerKey, "status", "stopped")

	h.logger.Info("Worker deregistered: %s", h.workerID)
}

// updateClient updates the Redis client after reconnection and re-registers the worker.
func (h *heartbeat) updateClient(client *redis.Client) {
	h.mu.Lock()
	h.rdb = client
	h.mu.Unlock()

	// Re-register the worker with the new client
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.register(ctx)
}
