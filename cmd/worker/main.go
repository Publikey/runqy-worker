package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	worker "github.com/publikey/runqy-worker"
)

func main() {
	configPath := flag.String("config", "", "Path to config.yml (default: ./config.yml)")
	showVersion := flag.Bool("version", false, "Print version and exit")
	validate := flag.Bool("validate", false, "Validate config and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("runqy-worker %s\n", Version)
		fmt.Printf("  commit:  %s\n", Commit)
		fmt.Printf("  built:   %s\n", BuildDate)
		os.Exit(0)
	}

	// Load config
	cfg, err := worker.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Set version for bootstrap registration
	cfg.Version = Version

	if *validate {
		if err := validateConfig(cfg); err != nil {
			log.Fatalf("Configuration error: %v", err)
		}
		fmt.Println("Configuration is valid")
		os.Exit(0)
	}

	// Validate config
	if err := validateConfig(cfg); err != nil {
		log.Fatalf("Configuration error: %v", err)
	}

	// Create worker with minimal config
	w := worker.New(*cfg)

	// Bootstrap: contact server, deploy code, start Python process
	log.Printf("Starting runqy-worker %s...", Version)
	log.Printf("  Server: %s", cfg.ServerURL)
	log.Printf("  Queues: %v", cfg.QueueNames)
	log.Printf("  Concurrency: %d", cfg.Concurrency)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt during bootstrap
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
		<-quit
		log.Println("Interrupt received, cancelling bootstrap...")
		cancel()
	}()

	if err := w.Bootstrap(ctx); err != nil {
		log.Fatalf("Bootstrap failed: %v", err)
	}

	// Run worker (blocks until SIGTERM/SIGINT)
	if err := w.Run(); err != nil {
		log.Fatalf("Worker error: %v", err)
	}
}

// validateConfig validates the configuration for bootstrap mode.
func validateConfig(cfg *worker.Config) error {
	if cfg.ServerURL == "" {
		return fmt.Errorf("server.url is required")
	}
	if cfg.APIKey == "" {
		return fmt.Errorf("server.api_key is required")
	}
	if len(cfg.QueueNames) == 0 {
		return fmt.Errorf("worker.queues (or worker.queue) is required")
	}
	if cfg.Concurrency <= 0 {
		return fmt.Errorf("worker.concurrency must be > 0")
	}
	return nil
}
