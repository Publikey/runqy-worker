package handler

import (
	"context"
	"fmt"
	"io"
)

// Logger interface for handler package logging.
type Logger interface {
	Info(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Error(format string, args ...interface{})
	Debug(format string, args ...interface{})
}

// Task interface defines what handlers need from a task.
type Task interface {
	ID() string
	Type() string
	Payload() []byte
	RetryCount() int
	MaxRetry() int
	Queue() string
	ResultWriter() io.Writer
}

// HandlerConfig defines how to handle a specific task type.
type HandlerConfig struct {
	Type    string            // http (default), log
	URL     string            // HTTP endpoint URL
	Method  string            // HTTP method (default: POST)
	Timeout string            // Request timeout
	Headers map[string]string // Custom headers
	Auth    AuthConfig        // Authentication config
	RetryOn []int             // HTTP status codes to retry
	FailOn  []int             // HTTP status codes for permanent failure
}

// AuthConfig for HTTP authentication.
type AuthConfig struct {
	Type     string // basic, bearer, api_key
	Username string // For basic auth
	Password string // For basic auth
	Token    string // For bearer auth
	Header   string // For api_key auth (header name)
	Key      string // For api_key auth (key value)
}

// DefaultsConfig provides default values for handlers.
type DefaultsConfig struct {
	HTTP HTTPDefaultsConfig
}

// HTTPDefaultsConfig provides default values for HTTP handlers.
type HTTPDefaultsConfig struct {
	Timeout string
	RetryOn []int
	FailOn  []int
	Headers map[string]string
}

// SkipRetryError signals that the task should fail permanently without retry.
type SkipRetryError struct {
	Reason string
}

func (e *SkipRetryError) Error() string {
	return fmt.Sprintf("permanent failure: %s", e.Reason)
}

// NewSkipRetryError creates a new SkipRetryError.
func NewSkipRetryError(reason string) *SkipRetryError {
	return &SkipRetryError{Reason: reason}
}

// IsSkipRetry checks if an error is a SkipRetryError.
func IsSkipRetry(err error) bool {
	_, ok := err.(*SkipRetryError)
	return ok
}

// Handler processes a task.
type Handler interface {
	ProcessTask(ctx context.Context, task Task) error
}

// HandlerFunc is a function that processes a task.
type HandlerFunc func(ctx context.Context, task Task) error

// ProcessTask implements Handler interface.
func (fn HandlerFunc) ProcessTask(ctx context.Context, task Task) error {
	return fn(ctx, task)
}

// MiddlewareFunc is a function that wraps a Handler.
type MiddlewareFunc func(Handler) Handler

// ServeMux routes tasks to handlers based on task type.
type ServeMux struct {
	handlers       map[string]Handler
	defaultHandler Handler
	middlewares    []MiddlewareFunc
}

// NewServeMux creates a new ServeMux.
func NewServeMux() *ServeMux {
	return &ServeMux{
		handlers:    make(map[string]Handler),
		middlewares: make([]MiddlewareFunc, 0),
	}
}

// Handle registers a handler for the given task type.
func (mux *ServeMux) Handle(taskType string, handler Handler) {
	mux.handlers[taskType] = handler
}

// HandleFunc registers a handler function for the given task type.
func (mux *ServeMux) HandleFunc(taskType string, handler func(ctx context.Context, task Task) error) {
	mux.handlers[taskType] = HandlerFunc(handler)
}

// SetDefault sets the default handler for unregistered task types.
func (mux *ServeMux) SetDefault(handler Handler) {
	mux.defaultHandler = handler
}

// SetDefaultFunc sets the default handler function for unregistered task types.
func (mux *ServeMux) SetDefaultFunc(handler func(ctx context.Context, task Task) error) {
	mux.defaultHandler = HandlerFunc(handler)
}

// Use adds middleware to the mux.
func (mux *ServeMux) Use(mw MiddlewareFunc) {
	mux.middlewares = append(mux.middlewares, mw)
}

// ProcessTask routes the task to the appropriate handler.
func (mux *ServeMux) ProcessTask(ctx context.Context, task Task) error {
	handler, ok := mux.handlers[task.Type()]
	if !ok {
		if mux.defaultHandler != nil {
			handler = mux.defaultHandler
		} else {
			return fmt.Errorf("no handler registered for task type %q", task.Type())
		}
	}

	// Apply middlewares in reverse order (last added = outermost)
	h := handler
	for i := len(mux.middlewares) - 1; i >= 0; i-- {
		h = mux.middlewares[i](h)
	}

	return h.ProcessTask(ctx, task)
}

// HasHandler returns true if a handler is registered for the task type.
func (mux *ServeMux) HasHandler(taskType string) bool {
	_, ok := mux.handlers[taskType]
	return ok
}
