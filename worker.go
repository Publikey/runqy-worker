package worker

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

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

	done   chan struct{}
	wg     sync.WaitGroup
	logger Logger
}

// New creates a new Worker with the given configuration.
func New(cfg Config) *Worker {
	// Apply defaults
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.Queues == nil || len(cfg.Queues) == 0 {
		cfg.Queues = map[string]int{"default": 1}
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

	// Create Redis client
	opts := &redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	}
	if cfg.RedisTLS != nil {
		opts.TLSConfig = cfg.RedisTLS
	}
	rdb := redis.NewClient(opts)

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
	// Test Redis connection
	ctx := context.Background()
	if err := w.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	w.logger.Info("Worker starting...")
	w.logger.Info(fmt.Sprintf("Concurrency: %d", w.config.Concurrency))
	w.logger.Info(fmt.Sprintf("Queues: %v", w.config.Queues))

	// Create components
	redisClient := newRedisClient(w.rdb, w.logger)
	w.processor = newProcessor(redisClient, w.mux, w.config)
	w.heartbeat = newHeartbeat(w.rdb, w.config)

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
	// Test Redis connection
	if err := w.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	w.logger.Info("Worker starting...")
	w.logger.Info(fmt.Sprintf("Concurrency: %d", w.config.Concurrency))
	w.logger.Info(fmt.Sprintf("Queues: %v", w.config.Queues))

	// Create components
	redisClient := newRedisClient(w.rdb, w.logger)
	w.processor = newProcessor(redisClient, w.mux, w.config)
	w.heartbeat = newHeartbeat(w.rdb, w.config)

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

	w.logger.Info("Closing Redis connection...")
	if err := w.rdb.Close(); err != nil {
		w.logger.Error("Error closing Redis:", err)
	}

	w.logger.Info("Worker shutdown complete.")
	return nil
}

// GetServeMux returns the underlying ServeMux for advanced configuration.
func (w *Worker) GetServeMux() *ServeMux {
	return w.mux
}
