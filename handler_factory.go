package worker

import (
	"context"
	"fmt"
)

// NewHandlerFromConfig creates a Handler from a HandlerConfig.
func NewHandlerFromConfig(cfg *HandlerConfig, defaults DefaultsConfig, logger Logger) (HandlerFunc, error) {
	if cfg == nil {
		return nil, fmt.Errorf("handler config is nil")
	}

	handlerType := cfg.Type
	if handlerType == "" {
		handlerType = "http" // default
	}

	switch handlerType {
	case "http":
		if cfg.URL == "" {
			return nil, fmt.Errorf("http handler requires url")
		}
		h := NewHTTPHandler(cfg, defaults.HTTP, logger)
		return h.ProcessTask, nil

	case "log":
		return newLogHandler(logger), nil

	default:
		return nil, fmt.Errorf("unknown handler type: %s", handlerType)
	}
}

// newLogHandler creates a handler that just logs the task (useful for debugging).
func newLogHandler(logger Logger) HandlerFunc {
	return func(ctx context.Context, task *Task) error {
		logger.Info(fmt.Sprintf("LOG HANDLER: task_id=%s type=%s payload=%s retry=%d/%d",
			task.ID(),
			task.Type(),
			string(task.Payload()),
			task.RetryCount(),
			task.MaxRetry(),
		))
		return nil
	}
}
