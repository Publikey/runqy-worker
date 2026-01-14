package worker

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// Worker processes tasks from Redis queues.
type Worker struct {
	config        Config
	rdb           *redis.Client
	mux           *ServeMux
	processor     *processor
	heartbeat     *heartbeat
	queueHandlers map[string]HandlerFunc // stored until processor is created

	// Bootstrap state
	bootstrapResponse *BootstrapResponse
	supervisor        *ProcessSupervisor
	bootstrapped      bool

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
	if cfg.RedisAddr != "" {
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

// Bootstrap contacts the runqy-server, deploys code, and starts the supervised process.
// This must be called before Run() when using server-driven configuration.
func (w *Worker) Bootstrap(ctx context.Context) error {
	w.logger.Info("Starting bootstrap process...")

	// Validate required config
	if w.config.ServerURL == "" {
		return fmt.Errorf("ServerURL is required for bootstrap")
	}
	if w.config.APIKey == "" {
		return fmt.Errorf("APIKey is required for bootstrap")
	}
	if w.config.Queue == "" {
		return fmt.Errorf("Queue is required for bootstrap")
	}

	// Phase 1: Contact server
	w.logger.Info("Contacting server at %s...", w.config.ServerURL)
	resp, err := doBootstrap(ctx, w.config, w.logger)
	if err != nil {
		return fmt.Errorf("server bootstrap failed: %w", err)
	}
	w.bootstrapResponse = resp

	// Phase 2: Create Redis client from server config
	w.logger.Info("Connecting to Redis at %s...", resp.Redis.Addr)
	opts := &redis.Options{
		Addr:     resp.Redis.Addr,
		Password: resp.Redis.Password,
		DB:       resp.Redis.DB,
	}
	if resp.Redis.UseTLS {
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

	// Phase 3: Build queue configuration
	w.config.Queues = map[string]int{
		resp.Queue.Name: resp.Queue.Priority,
	}

	// Phase 4: Deploy code
	w.logger.Info("Deploying code from %s (branch: %s)...", resp.Deployment.GitURL, resp.Deployment.Branch)
	deployment, err := deployCode(ctx, w.config, resp.Deployment, w.logger)
	if err != nil {
		return fmt.Errorf("code deployment failed: %w", err)
	}

	// Phase 5: Start supervised process
	w.logger.Info("Starting FastAPI process...")
	w.supervisor = newProcessSupervisor(deployment, resp.Deployment, w.logger)
	if err := w.supervisor.Start(ctx); err != nil {
		return fmt.Errorf("failed to start Python process: %w", err)
	}
	w.logger.Info("FastAPI process started on localhost:8080")

	// Phase 6: Configure handlers from routes
	w.logger.Info("Configuring %d task handlers...", len(resp.Routes))
	for taskType, endpointPath := range resp.Routes {
		url := "http://localhost:8080" + endpointPath
		handlerCfg := &HandlerConfig{
			Type:   "http",
			URL:    url,
			Method: "POST",
		}
		handler, err := NewHandlerFromConfig(handlerCfg, w.config.Defaults, w.logger)
		if err != nil {
			return fmt.Errorf("failed to create handler for %s: %w", taskType, err)
		}
		w.Handle(taskType, handler)
		w.logger.Info("  %s -> %s", taskType, url)
	}

	w.bootstrapped = true
	w.logger.Info("Bootstrap completed successfully")
	return nil
}

// IsBootstrapped returns true if Bootstrap() has been called successfully.
func (w *Worker) IsBootstrapped() bool {
	return w.bootstrapped
}

// Supervisor returns the process supervisor (nil if not bootstrapped).
func (w *Worker) Supervisor() *ProcessSupervisor {
	return w.supervisor
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

	// Check supervisor health if bootstrapped
	if w.supervisor != nil && !w.supervisor.IsHealthy() {
		return fmt.Errorf("supervised process is not healthy")
	}

	w.logger.Info("Worker starting...")
	w.logger.Info(fmt.Sprintf("Concurrency: %d", w.config.Concurrency))
	w.logger.Info(fmt.Sprintf("Queues: %v", w.config.Queues))
	if w.bootstrapped {
		w.logger.Info("Mode: bootstrapped (server-driven)")
	}

	// Create components
	redisClient := newRedisClient(w.rdb, w.logger)
	w.processor = newProcessor(redisClient, w.mux, w.config)
	w.heartbeat = newHeartbeat(w.rdb, w.config, w.supervisor)

	// Set supervisor for health checks
	if w.supervisor != nil {
		w.processor.SetSupervisor(w.supervisor)
	}

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

	// Check supervisor health if bootstrapped
	if w.supervisor != nil && !w.supervisor.IsHealthy() {
		return fmt.Errorf("supervised process is not healthy")
	}

	w.logger.Info("Worker starting...")
	w.logger.Info(fmt.Sprintf("Concurrency: %d", w.config.Concurrency))
	w.logger.Info(fmt.Sprintf("Queues: %v", w.config.Queues))

	// Create components
	redisClient := newRedisClient(w.rdb, w.logger)
	w.processor = newProcessor(redisClient, w.mux, w.config)
	w.heartbeat = newHeartbeat(w.rdb, w.config, w.supervisor)

	// Set supervisor for health checks
	if w.supervisor != nil {
		w.processor.SetSupervisor(w.supervisor)
	}

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

	// Stop supervised Python process
	if w.supervisor != nil {
		w.logger.Info("Stopping supervised Python process...")
		if err := w.supervisor.Stop(); err != nil {
			w.logger.Error("Error stopping Python process:", err)
		}
	}

	w.logger.Info("Closing Redis connection...")
	if w.rdb != nil {
		if err := w.rdb.Close(); err != nil {
			w.logger.Error("Error closing Redis:", err)
		}
	}

	w.logger.Info("Worker shutdown complete.")
	return nil
}

// GetServeMux returns the underlying ServeMux for advanced configuration.
func (w *Worker) GetServeMux() *ServeMux {
	return w.mux
}
