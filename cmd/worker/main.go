package main

import (
	"flag"
	"fmt"
	"log"
	"os"

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

	if *validate {
		if err := validateConfig(cfg); err != nil {
			log.Fatalf("Configuration error: %v", err)
		}
		fmt.Println("Configuration is valid")
		os.Exit(0)
	}

	// Create worker
	w := worker.New(*cfg)

	// Register queue handlers from config
	handlerCount := 0
	for queueName, handlerCfg := range cfg.QueueHandlers {
		handler, err := worker.NewHandlerFromConfig(handlerCfg, cfg.Defaults, cfg.Logger)
		if err != nil {
			log.Fatalf("Failed to create handler for queue %q: %v", queueName, err)
		}
		w.HandleQueue(queueName, handler)
		handlerCount++
		log.Printf("Registered handler: queue=%s -> %s", queueName, describeHandler(handlerCfg))
	}

	if handlerCount == 0 {
		log.Fatal("No handlers configured. Add handler to each queue in config.yml")
	}

	// Run worker (blocks until SIGTERM/SIGINT)
	log.Printf("Starting runqy-worker %s...", Version)
	log.Printf("  Redis: %s", cfg.RedisAddr)
	log.Printf("  Concurrency: %d", cfg.Concurrency)
	log.Printf("  Queues: %v", cfg.Queues)

	if err := w.Run(); err != nil {
		log.Fatalf("Worker error: %v", err)
	}
}

// validateConfig validates the configuration.
func validateConfig(cfg *worker.Config) error {
	if cfg.RedisAddr == "" {
		return fmt.Errorf("redis.addr is required")
	}
	if len(cfg.Queues) == 0 {
		return fmt.Errorf("at least one queue is required")
	}

	// Check that each queue has a handler
	for queueName := range cfg.Queues {
		handlerCfg, ok := cfg.QueueHandlers[queueName]
		if !ok || handlerCfg == nil {
			return fmt.Errorf("queue %q has no handler configured", queueName)
		}

		handlerType := handlerCfg.Type
		if handlerType == "" {
			handlerType = "http"
		}

		if handlerType == "http" && handlerCfg.URL == "" {
			return fmt.Errorf("queue %q: http handler requires url", queueName)
		}
	}

	return nil
}

// describeHandler returns a short description of the handler config.
func describeHandler(cfg *worker.HandlerConfig) string {
	handlerType := cfg.Type
	if handlerType == "" {
		handlerType = "http"
	}

	switch handlerType {
	case "http":
		method := cfg.Method
		if method == "" {
			method = "POST"
		}
		return fmt.Sprintf("HTTP %s %s", method, cfg.URL)
	case "log":
		return "log"
	default:
		return handlerType
	}
}
