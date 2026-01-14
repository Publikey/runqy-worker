package worker

import (
	"context"
	"fmt"

	"github.com/publikey/runqy-worker/internal/handler"
)

// Handler processes a task.
type Handler interface {
	ProcessTask(ctx context.Context, task *Task) error
}

// HandlerFunc is a function that processes a task.
type HandlerFunc func(ctx context.Context, task *Task) error

// ProcessTask implements Handler interface.
func (fn HandlerFunc) ProcessTask(ctx context.Context, task *Task) error {
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
func (mux *ServeMux) Handle(taskType string, h Handler) {
	mux.handlers[taskType] = h
}

// HandleFunc registers a handler function for the given task type.
func (mux *ServeMux) HandleFunc(taskType string, h func(ctx context.Context, task *Task) error) {
	mux.handlers[taskType] = HandlerFunc(h)
}

// SetDefault sets the default handler for unregistered task types.
func (mux *ServeMux) SetDefault(h Handler) {
	mux.defaultHandler = h
}

// SetDefaultFunc sets the default handler function for unregistered task types.
func (mux *ServeMux) SetDefaultFunc(h func(ctx context.Context, task *Task) error) {
	mux.defaultHandler = HandlerFunc(h)
}

// Use adds middleware to the mux.
func (mux *ServeMux) Use(mw MiddlewareFunc) {
	mux.middlewares = append(mux.middlewares, mw)
}

// ProcessTask routes the task to the appropriate handler.
func (mux *ServeMux) ProcessTask(ctx context.Context, task *Task) error {
	h, ok := mux.handlers[task.Type()]
	if !ok {
		if mux.defaultHandler != nil {
			h = mux.defaultHandler
		} else {
			return fmt.Errorf("no handler registered for task type %q", task.Type())
		}
	}

	// Apply middlewares in reverse order (last added = outermost)
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

// NewHandlerFromConfig creates a Handler from a HandlerConfig.
// Uses types from config_yaml.go: HandlerConfig, DefaultsConfig
func NewHandlerFromConfig(cfg *HandlerConfig, defaults DefaultsConfig, logger Logger) (HandlerFunc, error) {
	// Convert our config types to internal handler types
	internalCfg := &handler.HandlerConfig{
		Type:    cfg.Type,
		URL:     cfg.URL,
		Method:  cfg.Method,
		Timeout: cfg.Timeout,
		Headers: cfg.Headers,
		Auth: handler.AuthConfig{
			Type:     cfg.Auth.Type,
			Username: cfg.Auth.Username,
			Password: cfg.Auth.Password,
			Token:    cfg.Auth.Token,
			Header:   cfg.Auth.Header,
			Key:      cfg.Auth.Key,
		},
		RetryOn: cfg.RetryOn,
		FailOn:  cfg.FailOn,
	}

	internalDefaults := handler.DefaultsConfig{
		HTTP: handler.HTTPDefaultsConfig{
			Timeout: defaults.HTTP.Timeout,
			RetryOn: defaults.HTTP.RetryOn,
			FailOn:  defaults.HTTP.FailOn,
			Headers: defaults.HTTP.Headers,
		},
	}

	// Create internal handler
	internalHandler, err := handler.NewHandlerFromConfig(internalCfg, internalDefaults, &loggerAdapter{logger})
	if err != nil {
		return nil, err
	}

	// Wrap it to return our HandlerFunc type
	return func(ctx context.Context, task *Task) error {
		return internalHandler(ctx, task)
	}, nil
}

// loggerAdapter adapts our Logger (variadic args) to handler.Logger (format + args)
type loggerAdapter struct {
	l Logger
}

func (a *loggerAdapter) Info(format string, args ...interface{}) {
	a.l.Info(fmt.Sprintf(format, args...))
}
func (a *loggerAdapter) Warn(format string, args ...interface{}) {
	a.l.Warn(fmt.Sprintf(format, args...))
}
func (a *loggerAdapter) Error(format string, args ...interface{}) {
	a.l.Error(fmt.Sprintf(format, args...))
}
func (a *loggerAdapter) Debug(format string, args ...interface{}) {
	a.l.Debug(fmt.Sprintf(format, args...))
}
