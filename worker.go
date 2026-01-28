package worker

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// QueueState holds the bootstrap state for a single queue.
type QueueState struct {
	Name           string
	Priority       int
	SubQueues      []BootstrapSubQueueConfig // Sub-queues for this parent queue
	Response       *BootstrapResponse
	Deployment     *DeploymentResult
	Supervisor     *ProcessSupervisor
	StdioHandler   *StdioHandler
	OneShotHandler *OneShotHandler
	Mode           string // "long_running" or "one_shot"
}

// Worker processes tasks from Redis queues.
type Worker struct {
	config        Config
	rdb           *redis.Client
	mux           *ServeMux
	processor     *processor
	heartbeat     *heartbeat
	queueHandlers map[string]HandlerFunc // stored until processor is created

	// Multi-queue bootstrap state
	queueStates  map[string]*QueueState
	bootstrapped bool

	done   chan struct{}
	wg     sync.WaitGroup
	logger Logger
}

// New creates a new Worker with the given configuration.
// For bootstrap mode, call Bootstrap() before Run().
func New(cfg Config) *Worker {
	// Apply defaults
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.MaxRetry <= 0 {
		cfg.MaxRetry = 25
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = DefaultConfig().ShutdownTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = NewStdLogger()
	}
	if cfg.BootstrapRetries <= 0 {
		cfg.BootstrapRetries = 3
	}
	if cfg.BootstrapRetryDelay <= 0 {
		cfg.BootstrapRetryDelay = 5 * time.Second
	}
	if cfg.DeploymentDir == "" {
		cfg.DeploymentDir = "./deployment"
	}

	// Redis client is created during Bootstrap() or can be set directly
	var rdb *redis.Client
	if cfg.RedisAddr != "" && cfg.ServerURL == "" {
		opts := &redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		}
		if cfg.RedisTLS != nil {
			opts.TLSConfig = cfg.RedisTLS
		}
		rdb = redis.NewClient(opts)
	}

	return &Worker{
		config: cfg,
		rdb:    rdb,
		mux:    NewServeMux(),
		done:   make(chan struct{}),
		logger: cfg.Logger,
	}
}

// NewWithTLS creates a new Worker with TLS enabled.
func NewWithTLS(cfg Config) *Worker {
	if cfg.RedisTLS == nil {
		cfg.RedisTLS = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}
	return New(cfg)
}

// Bootstrap contacts the runqy-server, deploys code, and starts supervised processes for all queues.
// This must be called before Run() when using server-driven configuration.
func (w *Worker) Bootstrap(ctx context.Context) error {
	w.logger.Info("Starting multi-queue bootstrap process...")

	// Validate required config
	if w.config.ServerURL == "" {
		return fmt.Errorf("ServerURL is required for bootstrap")
	}
	if w.config.APIKey == "" {
		return fmt.Errorf("APIKey is required for bootstrap")
	}
	if len(w.config.QueueNames) == 0 {
		return fmt.Errorf("at least one queue is required for bootstrap")
	}

	w.queueStates = make(map[string]*QueueState)

	// Bootstrap each queue
	for i, queueName := range w.config.QueueNames {
		w.logger.Info("Bootstrapping queue %d/%d: %s", i+1, len(w.config.QueueNames), queueName)

		state, err := w.bootstrapQueue(ctx, queueName)
		if err != nil {
			// Clean up already-bootstrapped queues
			w.cleanupQueueStates()
			return fmt.Errorf("failed to bootstrap queue %s: %w", queueName, err)
		}

		w.queueStates[queueName] = state

		// Create Redis client from first queue response (all should have same Redis config)
		if w.rdb == nil {
			w.logger.Info("Connecting to Redis at %s...", state.Response.Redis.Addr)
			opts := &redis.Options{
				Addr:     state.Response.Redis.Addr,
				Password: state.Response.Redis.Password,
				DB:       state.Response.Redis.DB,
			}
			if state.Response.Redis.UseTLS {
				opts.TLSConfig = &tls.Config{
					MinVersion: tls.VersionTLS12,
				}
			}
			w.rdb = redis.NewClient(opts)

			// Test Redis connection
			if err := w.rdb.Ping(ctx).Err(); err != nil {
				w.cleanupQueueStates()
				return fmt.Errorf("failed to connect to Redis: %w", err)
			}
			w.logger.Info("Redis connection successful")
		}
	}

	// Build aggregated queue weights map from all SUB-queues
	w.config.Queues = make(map[string]int)
	for _, state := range w.queueStates {
		for _, sq := range state.SubQueues {
			w.config.Queues[sq.Name] = sq.Priority
		}
	}

	w.bootstrapped = true
	w.logger.Info("Multi-queue bootstrap completed: %d queues", len(w.queueStates))
	return nil
}

// bootstrapQueue handles bootstrap for a single queue.
func (w *Worker) bootstrapQueue(ctx context.Context, queueName string) (*QueueState, error) {
	// Create queue-specific config for bootstrap
	queueConfig := w.config
	queueConfig.Queue = queueName
	// Use unique deployment dir per queue
	queueConfig.DeploymentDir = filepath.Join(w.config.DeploymentDir, queueName)

	// Phase 1: Contact server for this queue
	w.logger.Info("[%s] Contacting server...", queueName)
	resp, err := doBootstrap(ctx, queueConfig, w.logger)
	if err != nil {
		return nil, fmt.Errorf("server bootstrap failed: %w", err)
	}

	// Phase 2: Deploy code for this queue
	w.logger.Info("[%s] Deploying code from %s (branch: %s)...", queueName, resp.Deployment.GitURL, resp.Deployment.Branch)
	deployment, err := deployCode(ctx, queueConfig, resp.Deployment, w.logger)
	if err != nil {
		return nil, fmt.Errorf("code deployment failed: %w", err)
	}

	// Determine sub-queues from response
	subQueues := resp.SubQueues
	if len(subQueues) == 0 {
		// Backward compatibility: if server doesn't return sub_queues,
		// create default sub-queue from queue config
		subQueues = []BootstrapSubQueueConfig{{
			Name:     queueName + ":default",
			Priority: resp.Queue.Priority,
		}}
	}

	state := &QueueState{
		Name:       queueName,
		Priority:   resp.Queue.Priority,
		SubQueues:  subQueues,
		Response:   resp,
		Deployment: deployment,
		Mode:       resp.Deployment.Mode,
	}
	if state.Mode == "" {
		state.Mode = "long_running"
	}

	// Phase 3: Set up handler based on mode
	w.logger.Info("[%s] Deployment mode: %s", queueName, state.Mode)

	switch state.Mode {
	case "one_shot":
		// One-shot mode: spawn new process per task
		w.logger.Info("[%s] Setting up one-shot handler...", queueName)
		if len(resp.Vaults) > 0 {
			w.logger.Info("[%s] Injecting %d vault entries as environment variables", queueName, len(resp.Vaults))
		}
		state.OneShotHandler = NewOneShotHandler(
			deployment.CodePath, // Use CodePath (includes code_path subdirectory)
			deployment.VenvPath,
			resp.Deployment.StartupCmd,
			resp.Deployment.EnvVars,
			resp.Vaults,
			resp.Deployment.StartupTimeoutSecs,
			w.logger,
		)
		// Register handler for ALL sub-queues
		for _, sq := range subQueues {
			w.HandleQueue(sq.Name, state.OneShotHandler.ProcessTask)
		}

	default: // "long_running"
		// Long-running mode: single supervised process
		w.logger.Info("[%s] Starting supervised process...", queueName)
		if len(resp.Vaults) > 0 {
			w.logger.Info("[%s] Injecting %d vault entries as environment variables", queueName, len(resp.Vaults))
		}
		state.Supervisor = newProcessSupervisor(deployment, resp.Deployment, resp.Vaults, w.logger)
		if err := state.Supervisor.Start(ctx); err != nil {
			return nil, fmt.Errorf("failed to start process: %w", err)
		}
		w.logger.Info("[%s] Supervised process started and ready", queueName)

		// Create stdio handler for communication with supervised process
		state.StdioHandler = NewStdioHandler(state.Supervisor.Stdin(), state.Supervisor.Stdout(), w.logger)
		state.StdioHandler.Start()
		// Register handler for ALL sub-queues
		for _, sq := range subQueues {
			w.HandleQueue(sq.Name, state.StdioHandler.ProcessTask)
		}
	}

	w.logger.Info("[%s] Bootstrap complete with %d sub-queues (priority=%d)", queueName, len(subQueues), state.Priority)
	return state, nil
}

// cleanupQueueStates cleans up all bootstrapped queue states.
func (w *Worker) cleanupQueueStates() {
	for name, state := range w.queueStates {
		if state.StdioHandler != nil {
			state.StdioHandler.Stop()
		}
		if state.Supervisor != nil {
			state.Supervisor.Stop()
		}
		deployDir := filepath.Join(w.config.DeploymentDir, name)
		os.RemoveAll(deployDir)
	}
}

// IsBootstrapped returns true if Bootstrap() has been called successfully.
func (w *Worker) IsBootstrapped() bool {
	return w.bootstrapped
}

// Supervisor returns the process supervisor for a specific queue.
// Returns nil if queue not found or not bootstrapped.
func (w *Worker) Supervisor(queueName string) *ProcessSupervisor {
	if state, ok := w.queueStates[queueName]; ok {
		return state.Supervisor
	}
	return nil
}

// QueueStates returns the map of all queue states.
func (w *Worker) QueueStates() map[string]*QueueState {
	return w.queueStates
}

// IsQueueHealthy returns true if a specific queue's supervisor is healthy.
func (w *Worker) IsQueueHealthy(queueName string) bool {
	state, ok := w.queueStates[queueName]
	if !ok {
		return false
	}
	if state.Supervisor == nil {
		return true // one_shot mode, always healthy
	}
	return state.Supervisor.IsHealthy()
}

// IsAllQueuesHealthy returns true if ALL supervisors are healthy.
func (w *Worker) IsAllQueuesHealthy() bool {
	for _, state := range w.queueStates {
		if state.Supervisor != nil && !state.Supervisor.IsHealthy() {
			return false
		}
	}
	return true
}

// GetUnhealthyQueues returns list of queue names that are not healthy.
func (w *Worker) GetUnhealthyQueues() []string {
	var unhealthy []string
	for name, state := range w.queueStates {
		if state.Supervisor != nil && !state.Supervisor.IsHealthy() {
			unhealthy = append(unhealthy, name)
		}
	}
	return unhealthy
}

// Handle registers a handler for the given task type.
func (w *Worker) Handle(taskType string, handler HandlerFunc) {
	w.mux.HandleFunc(taskType, handler)
}

// HandleFunc is an alias for Handle.
func (w *Worker) HandleFunc(taskType string, handler func(ctx context.Context, task *Task) error) {
	w.mux.HandleFunc(taskType, handler)
}

// Use adds middleware to the worker.
func (w *Worker) Use(mw MiddlewareFunc) {
	w.mux.Use(mw)
}

// HandleDefault sets the default handler for unregistered task types.
func (w *Worker) HandleDefault(handler HandlerFunc) {
	w.mux.SetDefaultFunc(handler)
}

// HandleQueue sets the handler for a specific queue.
// This takes precedence over task-type based handlers.
func (w *Worker) HandleQueue(queue string, handler HandlerFunc) {
	if w.processor == nil {
		// Store for later - processor not created yet
		if w.queueHandlers == nil {
			w.queueHandlers = make(map[string]HandlerFunc)
		}
		w.queueHandlers[queue] = handler
	} else {
		w.processor.SetQueueHandler(queue, handler)
	}
}

// Run starts the worker and blocks until shutdown.
// It handles SIGTERM and SIGINT for graceful shutdown.
func (w *Worker) Run() error {
	// Check prerequisites
	if w.rdb == nil {
		return fmt.Errorf("Redis client not configured - call Bootstrap() first or provide RedisAddr in config")
	}

	// Test Redis connection
	ctx := context.Background()
	if err := w.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Check all queue supervisors health if bootstrapped
	if w.bootstrapped {
		unhealthy := w.GetUnhealthyQueues()
		if len(unhealthy) > 0 {
			return fmt.Errorf("supervised processes are not healthy: %v", unhealthy)
		}
	}

	w.logger.Info("Worker starting...")
	w.logger.Info(fmt.Sprintf("Concurrency: %d", w.config.Concurrency))
	w.logger.Info("Queues: %s", formatQueues(w.config.Queues))
	if w.bootstrapped {
		w.logger.Info("Mode: bootstrapped (server-driven, %d queues)", len(w.queueStates))
	}

	// Create components
	redisClient := newRedisClient(w.rdb, w.logger)
	w.processor = newProcessor(redisClient, w.mux, w.config)
	w.heartbeat = newHeartbeat(w.rdb, w.config, w.queueStates)

	// Set queue states for per-queue health checks
	w.processor.SetQueueStates(w.queueStates)

	// Copy queue handlers to processor
	for queue, handler := range w.queueHandlers {
		w.processor.SetQueueHandler(queue, handler)
	}

	// Start components
	w.heartbeat.start()
	w.processor.start()

	w.logger.Info("Worker running. Press Ctrl+C to stop.")

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-quit:
		w.logger.Info(fmt.Sprintf("Received signal %v, shutting down...", sig))
	case <-w.done:
		w.logger.Info("Shutdown requested...")
	}

	return w.shutdown()
}

// Start starts the worker in the background (non-blocking).
// Cancel the context or call Shutdown() to stop the worker.
func (w *Worker) Start(ctx context.Context) error {
	// Check prerequisites
	if w.rdb == nil {
		return fmt.Errorf("Redis client not configured - call Bootstrap() first or provide RedisAddr in config")
	}

	// Test Redis connection
	if err := w.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Check all queue supervisors health if bootstrapped
	if w.bootstrapped {
		unhealthy := w.GetUnhealthyQueues()
		if len(unhealthy) > 0 {
			return fmt.Errorf("supervised processes are not healthy: %v", unhealthy)
		}
	}

	w.logger.Info("Worker starting...")
	w.logger.Info(fmt.Sprintf("Concurrency: %d", w.config.Concurrency))
	w.logger.Info("Queues: %s", formatQueues(w.config.Queues))

	// Create components
	redisClient := newRedisClient(w.rdb, w.logger)
	w.processor = newProcessor(redisClient, w.mux, w.config)
	w.heartbeat = newHeartbeat(w.rdb, w.config, w.queueStates)

	// Set queue states for per-queue health checks
	w.processor.SetQueueStates(w.queueStates)

	// Copy queue handlers to processor
	for queue, handler := range w.queueHandlers {
		w.processor.SetQueueHandler(queue, handler)
	}

	// Start components
	w.heartbeat.start()
	w.processor.start()

	w.logger.Info("Worker running.")

	// Watch for context cancellation in background
	go func() {
		select {
		case <-ctx.Done():
			w.shutdown()
		case <-w.done:
			w.shutdown()
		}
	}()

	return nil
}

// Shutdown initiates a graceful shutdown of the worker.
func (w *Worker) Shutdown() {
	close(w.done)
}

// shutdown performs the actual shutdown sequence.
func (w *Worker) shutdown() error {
	w.logger.Info("Stopping processor...")
	if w.processor != nil {
		w.processor.stop()
	}

	w.logger.Info("Stopping heartbeat...")
	if w.heartbeat != nil {
		w.heartbeat.stop()
	}

	// Stop all queue supervisors and handlers
	// IMPORTANT: Stop supervisor FIRST (kills process, closes stdout)
	// then stop StdioHandler (which waits for stdout to close)
	for name, state := range w.queueStates {
		if state.Supervisor != nil {
			w.logger.Info("Stopping supervised process for queue: %s", name)
			if err := state.Supervisor.Stop(); err != nil {
				w.logger.Error("Error stopping supervisor for queue %s: %v", name, err)
			}
		}
		if state.StdioHandler != nil {
			w.logger.Info("Stopping stdio handler for queue: %s", name)
			state.StdioHandler.Stop()
		}
	}

	// Clean up deployment directories
	if w.bootstrapped && w.config.DeploymentDir != "" {
		w.logger.Info("Cleaning up deployment directory: %s", w.config.DeploymentDir)
		if err := os.RemoveAll(w.config.DeploymentDir); err != nil {
			w.logger.Error("Error cleaning up deployment directory: %v", err)
		}
	}

	w.logger.Info("Closing Redis connection...")
	if w.rdb != nil {
		if err := w.rdb.Close(); err != nil {
			w.logger.Error("Error closing Redis: %v", err)
		}
	}

	w.logger.Info("Worker shutdown complete.")
	return nil
}

// GetServeMux returns the underlying ServeMux for advanced configuration.
func (w *Worker) GetServeMux() *ServeMux {
	return w.mux
}

// formatQueues formats the queue weights map for display.
// Output: "inference:high (9), simple:default (3), inference:low (1)"
func formatQueues(queues map[string]int) string {
	if len(queues) == 0 {
		return "(none)"
	}

	// Sort by priority descending
	type qp struct {
		name     string
		priority int
	}
	var pairs []qp
	for name, priority := range queues {
		pairs = append(pairs, qp{name, priority})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].priority > pairs[j].priority
	})

	// Format as "name (priority), ..."
	var parts []string
	for _, p := range pairs {
		parts = append(parts, fmt.Sprintf("%s (%d)", p.name, p.priority))
	}
	return strings.Join(parts, ", ")
}
