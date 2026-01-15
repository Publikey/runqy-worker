package handler

import (
	"context"
	"io"
)

// NewStdioHandlerFromSupervisor creates a StdioHandler from process stdin/stdout.
// This is the primary way to create a handler for communicating with supervised processes.
func NewStdioHandlerFromSupervisor(stdin io.Writer, stdout io.Reader, logger Logger) *StdioHandler {
	return NewStdioHandler(stdin, stdout, logger)
}

// NewLogHandler creates a handler that just logs the task (useful for debugging).
func NewLogHandler(logger Logger) HandlerFunc {
	return func(ctx context.Context, task Task) error {
		logger.Info("LOG HANDLER: task_id=%s type=%s payload=%s retry=%d/%d",
			task.ID(),
			task.Type(),
			string(task.Payload()),
			task.RetryCount(),
			task.MaxRetry(),
		)
		return nil
	}
}
