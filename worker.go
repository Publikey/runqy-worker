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
	Recovery       *processRecovery // Auto-recovery watcher (nil for one_shot mode)
	Mode           string           // "long_running" or "one_shot"
	RedisStorage   bool             // Whether to store results/completion in Redis
}

// Worker processes tasks from Redis queues.
type Worker struct {
	config        Config
	rdb           *redis.Client
	mux           *ServeMux
	processor     *processor
	heartbeat     *heartbeat
	reconnector   *redisReconnector
	queueHandlers map[string]HandlerFunc // stored until processor is created

	// Multi-queue bootstrap state
	queueStates  map[string]*QueueState
	bootstrapped bool
	logBuffer    *LogBuffer // shared log buffer for all queues

	// Redis connection config (stored during bootstrap for reconnection)
	redisConnConfig *RedisConnConfig

	done         chan struct{}
	shutdownOnce sync.Once
	wg           sync.WaitGroup
	logger       Logger
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
	var redisConnConfig *RedisConnConfig
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

		// Store config for reconnection
		redisConnConfig = &RedisConnConfig{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
			UseTLS:   cfg.RedisTLS != nil,
		}
	}

	return &Worker{
		config:          cfg,
		rdb:             rdb,
		redisConnConfig: redisConnConfig,
		mux:             NewServeMux(),
		done:            make(chan struct{}),
		logger:          cfg.Logger,
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
// Sub-queues of the same parent (e.g., "manyQueue.high" and "manyQueue.low") share a single
// code deployment and runtime process.
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
	w.logBuffer = NewLogBuffer(500)

	// Group configured queues by parent to share deployment and runtime
	parentGroups := groupQueuesByParent(w.config.QueueNames)

	// Get sorted list of unique parents for deterministic ordering
	var uniqueParents []string
	for parent := range parentGroups {
		uniqueParents = append(uniqueParents, parent)
	}
	sort.Strings(uniqueParents)

	// Phase 1: Contact server for first queue to get Redis credentials early
	// This allows us to register as "bootstrapping" before code deployment starts
	firstParent := uniqueParents[0]
	firstQueueConfig := w.config
	firstQueueConfig.Queue = firstParent

	w.logger.Info("Contacting server to get Redis credentials...")
	firstResp, err := doBootstrap(ctx, firstQueueConfig, w.logger)
	if err != nil {
		return fmt.Errorf("failed to contact server for initial config: %w", err)
	}

	// Phase 2: Connect to Redis and register as "bootstrapping" BEFORE code deployment
	w.logger.Info("Connecting to Redis at %s...", firstResp.Redis.Addr)

	// Store Redis config for later reconnection
	w.redisConnConfig = &RedisConnConfig{
		Addr:     firstResp.Redis.Addr,
		Password: firstResp.Redis.Password,
		DB:       firstResp.Redis.DB,
		UseTLS:   firstResp.Redis.UseTLS,
	}

	opts := &redis.Options{
		Addr:     firstResp.Redis.Addr,
		Password: firstResp.Redis.Password,
		DB:       firstResp.Redis.DB,
	}
	if firstResp.Redis.UseTLS {
		opts.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}
	w.rdb = redis.NewClient(opts)

	// Test Redis connection
	if err := w.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}
	w.logger.Info("Redis connection successful")

	// Initialize queues with parent queue names for early heartbeat
	// Priority 0 indicates "bootstrapping, priority unknown"
	// This will be replaced with actual sub-queues and priorities after bootstrap
	w.config.Queues = make(map[string]int)
	for _, parent := range uniqueParents {
		w.config.Queues[parent] = 0
	}

	// Start early heartbeat with "bootstrapping" status
	// This makes the worker visible in Redis while deploying code
	w.heartbeat = newHeartbeat(w.rdb, w.config, nil, "bootstrapping")
	w.heartbeat.setLogBuffer(w.logBuffer)
	w.heartbeat.start()
	w.logger.Info("Worker registered as bootstrapping")

	// Phase 3: Bootstrap each queue (deploy code, start processes)
	for i, parentQueue := range uniqueParents {
		group := parentGroups[parentQueue]
		if group.listenAll {
			w.logger.Info("Bootstrapping parent queue %d/%d: %s (listening on ALL sub-queues)",
				i+1, len(uniqueParents), parentQueue)
		} else {
			w.logger.Info("Bootstrapping parent queue %d/%d: %s (listening on specific sub-queues: %v)",
				i+1, len(uniqueParents), parentQueue, group.configuredSQs)
		}

		// For first queue, reuse the server response we already have
		var state *QueueState
		if i == 0 {
			state, err = w.bootstrapQueueWithResponse(ctx, group, firstResp)
		} else {
			state, err = w.bootstrapQueue(ctx, group)
		}
		if err != nil {
			// Clean up already-bootstrapped queues and stop heartbeat
			w.cleanupQueueStates()
			if w.heartbeat != nil {
				w.heartbeat.stop()
			}
			return fmt.Errorf("failed to bootstrap queue %s: %w", parentQueue, err)
		}

		// Key by PARENT queue name for shared runtime lookup
		w.queueStates[parentQueue] = state
	}

	// Build aggregated queue weights map from all SUB-queues
	w.config.Queues = make(map[string]int)
	for _, state := range w.queueStates {
		for _, sq := range state.SubQueues {
			w.config.Queues[sq.Name] = sq.Priority
		}
	}

	// Update heartbeat with queue states, queues config, and transition to "running"
	if w.heartbeat != nil {
		w.heartbeat.updateQueueStates(w.queueStates)
		w.heartbeat.SetStatus("running") // Set status first
		w.heartbeat.updateQueues(w.config.Queues) // Then re-register with correct queues and status
		w.logger.Info("Bootstrap complete, worker status: running")
	}

	w.bootstrapped = true
	w.logger.Info("Multi-queue bootstrap completed: %d parent queues", len(w.queueStates))
	return nil
}

// parentQueueGroup holds information about a parent queue and which sub-queues to listen on.
type parentQueueGroup struct {
	parent        string   // Parent queue name (e.g., "manyQueue")
	listenAll     bool     // If true, listen on all sub-queues from server response
	configuredSQs []string // Specific sub-queues configured (only used if listenAll is false)
}

// groupQueuesByParent groups queue names by their parent queue.
// Returns information about whether to listen on all sub-queues or specific ones.
// Examples:
//   - ["manyQueue.high", "manyQueue.low"] -> {parent: "manyQueue", listenAll: false, configuredSQs: [".high", ".low"]}
//   - ["manyQueue"] -> {parent: "manyQueue", listenAll: true}
//   - ["manyQueue", "manyQueue.high"] -> {parent: "manyQueue", listenAll: true} (parent overrides specific)
func groupQueuesByParent(queueNames []string) map[string]*parentQueueGroup {
	groups := make(map[string]*parentQueueGroup)
	for _, name := range queueNames {
		parent := parentQueueName(name)
		isParentOnly := (parent == name) // No dot means parent queue itself

		if _, exists := groups[parent]; !exists {
			groups[parent] = &parentQueueGroup{
				parent:        parent,
				listenAll:     false,
				configuredSQs: []string{},
			}
		}

		if isParentOnly {
			// Parent queue configured without sub-queue suffix → listen on all
			groups[parent].listenAll = true
		} else if !groups[parent].listenAll {
			// Specific sub-queue configured (and parent not already set to listenAll)
			groups[parent].configuredSQs = append(groups[parent].configuredSQs, name)
		}
	}
	return groups
}

// parentQueueName extracts the parent queue name from a sub-queue name.
// For example: "inference.high" -> "inference", "simple" -> "simple"
func parentQueueName(subQueueName string) string {
	if idx := strings.Index(subQueueName, "."); idx > 0 {
		return subQueueName[:idx]
	}
	return subQueueName
}

// bootstrapQueue handles bootstrap for a single parent queue.
// The group parameter contains the parent queue name and which sub-queues to listen on.
// This ensures all sub-queues of the same parent share the same code deployment and runtime.
func (w *Worker) bootstrapQueue(ctx context.Context, group *parentQueueGroup) (*QueueState, error) {
	parentQueue := group.parent

	// Create queue-specific config for bootstrap
	queueConfig := w.config
	queueConfig.Queue = parentQueue // Use PARENT queue for server registration

	// Phase 1: Contact server for this parent queue
	w.logger.Info("[%s] Contacting server...", parentQueue)
	resp, err := doBootstrap(ctx, queueConfig, w.logger)
	if err != nil {
		return nil, fmt.Errorf("server bootstrap failed: %w", err)
	}

	// Continue with code deployment and handler setup
	return w.bootstrapQueueWithResponse(ctx, group, resp)
}

// bootstrapQueueWithResponse handles bootstrap for a queue using a pre-fetched server response.
// This is used when we already have the server response (e.g., from early Redis credential fetch).
func (w *Worker) bootstrapQueueWithResponse(ctx context.Context, group *parentQueueGroup, resp *BootstrapResponse) (*QueueState, error) {
	parentQueue := group.parent

	// Create queue-specific config for bootstrap
	queueConfig := w.config
	queueConfig.Queue = parentQueue
	// Use PARENT queue for deployment directory (ensures deduplication across sub-queues)
	queueConfig.DeploymentDir = filepath.Join(w.config.DeploymentDir, parentQueue)

	// Deploy code for this parent queue (once, shared by all sub-queues)
	// Pass git token from server response (resolved from vault) for authentication
	if resp.GitToken != "" {
		queueConfig.GitToken = resp.GitToken
	}
	w.logger.Info("[%s] Deploying code from %s (branch: %s)...", parentQueue, resp.Deployment.GitURL, resp.Deployment.Branch)
	deployment, err := deployCode(ctx, queueConfig, resp.Deployment, w.logger)
	if err != nil {
		return nil, fmt.Errorf("code deployment failed: %w", err)
	}

	// Determine sub-queues from response
	allSubQueues := resp.SubQueues
	if len(allSubQueues) == 0 {
		// Backward compatibility: if server doesn't return sub_queues,
		// create default sub-queue from queue config
		allSubQueues = []BootstrapSubQueueConfig{{
			Name:     parentQueue + ".default",
			Priority: resp.Queue.Priority,
		}}
	}

	// Filter sub-queues based on configuration:
	// - If listenAll is true, use all sub-queues from server
	// - If listenAll is false, only use the specific sub-queues configured
	var subQueuesToListen []BootstrapSubQueueConfig
	if group.listenAll {
		subQueuesToListen = allSubQueues
	} else {
		// Build a set of configured sub-queue names for fast lookup
		configuredSet := make(map[string]bool)
		for _, sq := range group.configuredSQs {
			configuredSet[sq] = true
		}
		// Filter to only include configured sub-queues
		for _, sq := range allSubQueues {
			if configuredSet[sq.Name] {
				subQueuesToListen = append(subQueuesToListen, sq)
			}
		}
		// Warn if any configured sub-queues were not found in server response
		for _, configured := range group.configuredSQs {
			found := false
			for _, sq := range allSubQueues {
				if sq.Name == configured {
					found = true
					break
				}
			}
			if !found {
				w.logger.Warn("[%s] Configured sub-queue %q not found in server response", parentQueue, configured)
			}
		}
	}

	state := &QueueState{
		Name:         parentQueue,
		Priority:     resp.Queue.Priority,
		SubQueues:    subQueuesToListen, // Only the sub-queues we're actually listening on
		Response:     resp,
		Deployment:   deployment,
		Mode:         resp.Deployment.Mode,
		RedisStorage: resp.Deployment.RedisStorage,
	}
	if state.Mode == "" {
		state.Mode = "long_running"
	}

	// Set up handler based on mode (single runtime shared by all sub-queues)
	w.logger.Info("[%s] Deployment mode: %s", parentQueue, state.Mode)

	switch state.Mode {
	case "one_shot":
		// One-shot mode: spawn new process per task (shared handler for configured sub-queues)
		w.logger.Info("[%s] Setting up one-shot handler...", parentQueue)
		if len(resp.Vaults) > 0 {
			w.logger.Info("[%s] Injecting %d vault entries as environment variables", parentQueue, len(resp.Vaults))
		}
		state.OneShotHandler = NewOneShotHandler(
			deployment.CodePath, // Use CodePath (includes code_path subdirectory)
			deployment.VenvPath,
			resp.Deployment.StartupCmd,
			resp.Vaults,
			resp.Deployment.StartupTimeoutSecs,
			resp.Deployment.RedisStorage,
			w.logger,
		)
		// Register handler only for configured sub-queues (they share the same one-shot handler)
		for _, sq := range subQueuesToListen {
			w.HandleQueue(sq.Name, state.OneShotHandler.ProcessTask)
		}

	default: // "long_running"
		// Long-running mode: single supervised process shared by configured sub-queues
		w.logger.Info("[%s] Starting supervised process...", parentQueue)
		if len(resp.Vaults) > 0 {
			w.logger.Info("[%s] Injecting %d vault entries as environment variables", parentQueue, len(resp.Vaults))
		}
		state.Supervisor = newProcessSupervisor(deployment, resp.Deployment, resp.Vaults, w.logger)
		if w.logBuffer != nil {
			state.Supervisor.SetLogBuffer(w.logBuffer)
		}
		if err := state.Supervisor.Start(ctx); err != nil {
			return nil, fmt.Errorf("failed to start process: %w", err)
		}
		w.logger.Info("[%s] Supervised process started and ready", parentQueue)

		// Create stdio handler for communication with supervised process (shared by configured sub-queues)
		state.StdioHandler = NewStdioHandler(state.Supervisor.Stdin(), state.Supervisor.Stdout(), w.logger, resp.Deployment.RedisStorage)
		if w.logBuffer != nil {
			state.StdioHandler.SetLogBuffer(w.logBuffer)
		}
		state.StdioHandler.Start()

		// Set up auto-recovery for the supervised process
		if w.config.Recovery.Enabled {
			recovery := newProcessRecovery(parentQueue, state.Supervisor, w.config.Recovery, w.logger)
			recovery.SetOnRestart(func(sup *ProcessSupervisor) {
				state.StdioHandler.Reconnect(sup.Stdin(), sup.Stdout())
			})
			recovery.Start()
			state.Recovery = recovery
			w.logger.Info("[%s] Auto-recovery enabled (max_restarts=%d, cooldown=%v)",
				parentQueue, w.config.Recovery.MaxRestarts, w.config.Recovery.CooldownPeriod)
		}

		// Register handler only for configured sub-queues (they share the same stdio handler)
		for _, sq := range subQueuesToListen {
			w.HandleQueue(sq.Name, state.StdioHandler.ProcessTask)
		}
	}

	w.logger.Info("[%s] Bootstrap complete with %d sub-queues (priority=%d)", parentQueue, len(subQueuesToListen), state.Priority)
	return state, nil
}

// cleanupQueueStates cleans up all bootstrapped queue states.
func (w *Worker) cleanupQueueStates() {
	for name, state := range w.queueStates {
		// Stop recovery first to prevent restarts during cleanup
		if state.Recovery != nil {
			state.Recovery.Stop()
		}
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
// Accepts either parent queue name ("manyQueue") or sub-queue name ("manyQueue.high").
// Returns nil if queue not found or not bootstrapped.
func (w *Worker) Supervisor(queueName string) *ProcessSupervisor {
	// Try exact match (parent queue)
	if state, ok := w.queueStates[queueName]; ok {
		return state.Supervisor
	}
	// Try parent lookup (sub-queue name -> parent)
	parent := parentQueueName(queueName)
	if state, ok := w.queueStates[parent]; ok {
		return state.Supervisor
	}
	return nil
}

// QueueStates returns the map of all queue states.
func (w *Worker) QueueStates() map[string]*QueueState {
	return w.queueStates
}

// IsQueueHealthy returns true if a specific queue's supervisor is healthy.
// Accepts either parent queue name ("manyQueue") or sub-queue name ("manyQueue.high").
func (w *Worker) IsQueueHealthy(queueName string) bool {
	// Try exact match (parent queue)
	if state, ok := w.queueStates[queueName]; ok {
		if state.Supervisor == nil {
			return true // one_shot mode, always healthy
		}
		return state.Supervisor.IsHealthy()
	}
	// Try parent lookup (sub-queue name -> parent)
	parent := parentQueueName(queueName)
	if state, ok := w.queueStates[parent]; ok {
		if state.Supervisor == nil {
			return true // one_shot mode, always healthy
		}
		return state.Supervisor.IsHealthy()
	}
	return false
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

	// Only create heartbeat if not already started during bootstrap
	if w.heartbeat == nil {
		w.heartbeat = newHeartbeat(w.rdb, w.config, w.queueStates, "running")
	}

	// Set queue states for per-queue health checks
	w.processor.SetQueueStates(w.queueStates)

	// Copy queue handlers to processor
	for queue, handler := range w.queueHandlers {
		w.processor.SetQueueHandler(queue, handler)
	}

	// Initialize reconnector if we have Redis config (from bootstrap)
	if w.redisConnConfig != nil {
		w.logger.Info("Redis reconnection enabled (addr=%s)", w.redisConnConfig.Addr)
		w.reconnector = newRedisReconnector(*w.redisConnConfig, w.rdb, w.logger)
		w.reconnector.SetOnReconnect(func(client *redis.Client) {
			w.rdb = client
			w.heartbeat.updateClient(client)
			w.processor.updateClient(client, w.logger)
		})
		w.processor.SetReconnector(w.reconnector)
	} else {
		w.logger.Warn("Redis reconnection disabled (no connection config stored)")
	}

	// Start components (heartbeat may already be running from bootstrap)
	if !w.heartbeat.isRunning() {
		w.heartbeat.start()
	}
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

	// Only create heartbeat if not already started during bootstrap
	if w.heartbeat == nil {
		w.heartbeat = newHeartbeat(w.rdb, w.config, w.queueStates, "running")
	}

	// Set queue states for per-queue health checks
	w.processor.SetQueueStates(w.queueStates)

	// Copy queue handlers to processor
	for queue, handler := range w.queueHandlers {
		w.processor.SetQueueHandler(queue, handler)
	}

	// Initialize reconnector if we have Redis config (from bootstrap)
	if w.redisConnConfig != nil {
		w.logger.Info("Redis reconnection enabled (addr=%s)", w.redisConnConfig.Addr)
		w.reconnector = newRedisReconnector(*w.redisConnConfig, w.rdb, w.logger)
		w.reconnector.SetOnReconnect(func(client *redis.Client) {
			w.rdb = client
			w.heartbeat.updateClient(client)
			w.processor.updateClient(client, w.logger)
		})
		w.processor.SetReconnector(w.reconnector)
	} else {
		w.logger.Warn("Redis reconnection disabled (no connection config stored)")
	}

	// Start components (heartbeat may already be running from bootstrap)
	if !w.heartbeat.isRunning() {
		w.heartbeat.start()
	}
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
// Safe to call multiple times.
func (w *Worker) Shutdown() {
	w.shutdownOnce.Do(func() {
		close(w.done)
	})
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
	// IMPORTANT: Stop recovery FIRST (prevents restart during shutdown)
	// then stop supervisor (kills process, closes stdout)
	// then stop StdioHandler (which waits for stdout to close)
	for name, state := range w.queueStates {
		if state.Recovery != nil {
			w.logger.Info("Stopping recovery watcher for queue: %s", name)
			state.Recovery.Stop()
		}
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

	// Note: deployment directory is intentionally NOT cleaned up on shutdown.
	// Existing deployments are reused across restarts for fast restart.
	// Users can manually delete the deployment directory to force a fresh clone.

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
// Output: "inference.high (9), simple.default (3), inference.low (1)"
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
