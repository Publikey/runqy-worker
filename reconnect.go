package worker

import (
	"context"
	"crypto/tls"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConnConfig holds the configuration needed to create a Redis connection.
// This is stored during bootstrap so we can recreate the connection if needed.
type RedisConnConfig struct {
	Addr     string
	Password string
	DB       int
	UseTLS   bool
}

// redisReconnector manages Redis connection recovery.
type redisReconnector struct {
	mu              sync.Mutex
	config          RedisConnConfig
	currentClient   *redis.Client
	consecutiveErrs int
	lastReconnect   time.Time
	logger          Logger

	// Callbacks to update dependent components
	onReconnect func(client *redis.Client)
}

// newRedisReconnector creates a new reconnection manager.
func newRedisReconnector(config RedisConnConfig, client *redis.Client, logger Logger) *redisReconnector {
	return &redisReconnector{
		config:        config,
		currentClient: client,
		logger:        logger,
	}
}

// SetOnReconnect sets the callback that's called after successful reconnection.
func (r *redisReconnector) SetOnReconnect(fn func(client *redis.Client)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onReconnect = fn
}

// isConnectionError checks if an error indicates a broken Redis connection.
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := strings.ToLower(err.Error())

	// Common connection error patterns
	connectionPatterns := []string{
		"i/o timeout",
		"connection reset",
		"connection refused",
		"broken pipe",
		"eof",
		"use of closed network connection",
		"no connection could be made",
		"connection timed out",
		"network is unreachable",
		"host is down",
		"no route to host",
		"wsarecv",      // Windows socket errors
		"wsasend",      // Windows socket errors
		"forcibly closed", // Windows: "An existing connection was forcibly closed"
	}

	for _, pattern := range connectionPatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

// handleError tracks errors and triggers reconnection when threshold is hit.
// Returns true if a reconnection was attempted.
func (r *redisReconnector) handleError(err error) bool {
	if !isConnectionError(err) {
		r.logger.Debug("Error is not a connection error, skipping reconnection logic: %v", err)
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.consecutiveErrs++
	r.logger.Warn("Connection error detected, triggering reconnection: %v", err)

	// Trigger reconnection on first connection error - no reason to wait
	if r.consecutiveErrs >= 1 {
		// Rate limit: max once per 5 seconds
		if time.Since(r.lastReconnect) < 5*time.Second {
			r.logger.Warn("Redis reconnection rate-limited (last attempt was %v ago)", time.Since(r.lastReconnect))
			return false
		}

		r.logger.Warn("Attempting Redis reconnection...")
		return r.reconnectLocked()
	}

	return false
}

// reconnectLocked performs the reconnection. Must be called with mu held.
func (r *redisReconnector) reconnectLocked() bool {
	r.lastReconnect = time.Now()

	r.logger.Info("Closing old Redis connection...")

	// Close old client (ignore errors - it's likely already broken)
	if r.currentClient != nil {
		r.currentClient.Close()
	}

	// Create new client
	r.logger.Info("Creating new Redis connection to %s...", r.config.Addr)
	opts := &redis.Options{
		Addr:     r.config.Addr,
		Password: r.config.Password,
		DB:       r.config.DB,
	}
	if r.config.UseTLS {
		opts.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	newClient := redis.NewClient(opts)

	// Test the new connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r.logger.Info("Testing new Redis connection with PING...")
	if err := newClient.Ping(ctx).Err(); err != nil {
		r.logger.Error("Redis reconnection failed (PING error): %v", err)
		newClient.Close()
		return false
	}

	r.logger.Info("Redis PING successful, updating components...")
	r.currentClient = newClient
	r.consecutiveErrs = 0

	// Save callback and release lock before invoking it to avoid deadlocks
	callback := r.onReconnect
	r.mu.Unlock()

	// Notify dependent components (outside lock)
	if callback != nil {
		callback(newClient)
	}

	r.logger.Info("Redis reconnection complete")

	// Re-acquire lock (caller expects it held via defer r.mu.Unlock)
	r.mu.Lock()
	return true
}

// resetErrors resets the consecutive error count.
// Call this after a successful Redis operation.
func (r *redisReconnector) resetErrors() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.consecutiveErrs = 0
}

// GetClient returns the current Redis client.
func (r *redisReconnector) GetClient() *redis.Client {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentClient
}
