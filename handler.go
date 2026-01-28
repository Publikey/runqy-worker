package worker

import (
	"context"
	"fmt"
	"io"
	"time"

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

// StdioHandler communicates with a child process via stdin/stdout JSON lines.
// It wraps the internal handler.StdioHandler.
type StdioHandler struct {
	internal *handler.StdioHandler
}

// NewStdioHandler creates a StdioHandler from stdin/stdout pipes.
// Call Start() to begin reading responses, and Stop() to clean up.
func NewStdioHandler(stdin io.Writer, stdout io.Reader, logger Logger) *StdioHandler {
	return &StdioHandler{
		internal: handler.NewStdioHandler(stdin, stdout, &loggerAdapter{logger}),
	}
}

// Start begins reading responses from stdout in a background goroutine.
func (h *StdioHandler) Start() {
	h.internal.Start()
}

// Stop signals the handler to stop and waits for cleanup.
func (h *StdioHandler) Stop() {
	h.internal.Stop()
}

// ProcessTask sends a task to the child process and waits for the response.
func (h *StdioHandler) ProcessTask(ctx context.Context, task *Task) error {
	return h.internal.ProcessTask(ctx, task)
}

// OneShotHandler spawns a new process for each task.
// The process handles one task and exits.
type OneShotHandler struct {
	internal *handler.OneShotHandler
}

// NewOneShotHandler creates an OneShotHandler.
func NewOneShotHandler(
	repoPath string,
	venvPath string,
	startupCmd string,
	envVars map[string]string,
	vaultVars map[string]string,
	timeoutSecs int,
	logger Logger,
) *OneShotHandler {
	timeout := time.Duration(timeoutSecs) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return &OneShotHandler{
		internal: handler.NewOneShotHandler(
			repoPath,
			venvPath,
			startupCmd,
			envVars,
			vaultVars,
			timeout,
			&loggerAdapter{logger},
		),
	}
}

// ProcessTask spawns a new process, sends the task, and waits for response.
func (h *OneShotHandler) ProcessTask(ctx context.Context, task *Task) error {
	return h.internal.ProcessTask(ctx, task)
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
