package worker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// TaskRequest is the JSON payload sent to HTTP endpoints.
type TaskRequest struct {
	TaskID     string          `json:"task_id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	PayloadRaw string          `json:"payload_raw"`
	RetryCount int             `json:"retry_count"`
	MaxRetry   int             `json:"max_retry"`
	Queue      string          `json:"queue"`
}

// HTTPHandler makes HTTP requests to configured endpoints.
type HTTPHandler struct {
	config   *HandlerConfig
	defaults HTTPDefaultsConfig
	client   *http.Client
	logger   Logger

	// Resolved configuration
	timeout time.Duration
	retryOn map[int]bool
	failOn  map[int]bool
}

// NewHTTPHandler creates a new HTTPHandler from configuration.
func NewHTTPHandler(cfg *HandlerConfig, defaults HTTPDefaultsConfig, logger Logger) *HTTPHandler {
	h := &HTTPHandler{
		config:   cfg,
		defaults: defaults,
		logger:   logger,
		retryOn:  make(map[int]bool),
		failOn:   make(map[int]bool),
	}

	// Resolve timeout (config > defaults > hardcoded default)
	h.timeout = 30 * time.Second
	if defaults.Timeout != "" {
		if d, err := time.ParseDuration(defaults.Timeout); err == nil {
			h.timeout = d
		}
	}
	if cfg.Timeout != "" {
		if d, err := time.ParseDuration(cfg.Timeout); err == nil {
			h.timeout = d
		}
	}

	// Build retry status code map
	retryOn := defaults.RetryOn
	if len(cfg.RetryOn) > 0 {
		retryOn = cfg.RetryOn
	}
	if len(retryOn) == 0 {
		// Default retry codes
		retryOn = []int{429, 500, 502, 503, 504}
	}
	for _, code := range retryOn {
		h.retryOn[code] = true
	}

	// Build fail status code map
	failOn := defaults.FailOn
	if len(cfg.FailOn) > 0 {
		failOn = cfg.FailOn
	}
	if len(failOn) == 0 {
		// Default permanent failure codes
		failOn = []int{400, 422}
	}
	for _, code := range failOn {
		h.failOn[code] = true
	}

	// Create HTTP client with timeout
	h.client = &http.Client{
		Timeout: h.timeout,
	}

	return h
}

// ProcessTask sends the task to the configured HTTP endpoint.
func (h *HTTPHandler) ProcessTask(ctx context.Context, task *Task) error {
	req, err := h.buildRequest(ctx, task)
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}

	h.logger.Info(fmt.Sprintf("HTTP handler: sending %s to %s", task.Type(), h.config.URL))

	resp, err := h.client.Do(req)
	if err != nil {
		// Network errors should be retried
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	return h.handleResponse(resp, task)
}

// buildRequest creates the HTTP request for the task.
func (h *HTTPHandler) buildRequest(ctx context.Context, task *Task) (*http.Request, error) {
	// Build request payload
	payload := task.Payload()

	// Try to parse payload as JSON, otherwise use raw
	var jsonPayload json.RawMessage
	if json.Valid(payload) {
		jsonPayload = payload
	} else {
		jsonPayload = nil
	}

	reqBody := TaskRequest{
		TaskID:     task.ID(),
		Type:       task.Type(),
		Payload:    jsonPayload,
		PayloadRaw: base64.StdEncoding.EncodeToString(payload),
		RetryCount: task.RetryCount(),
		MaxRetry:   task.MaxRetry(),
		Queue:      task.Queue(),
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	method := h.config.Method
	if method == "" {
		method = "POST"
	}

	req, err := http.NewRequestWithContext(ctx, method, h.config.URL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set default headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "runqy-worker/1.0")

	// Apply default headers from config
	for k, v := range h.defaults.Headers {
		req.Header.Set(k, v)
	}

	// Apply handler-specific headers (override defaults)
	for k, v := range h.config.Headers {
		req.Header.Set(k, v)
	}

	// Apply authentication
	h.applyAuth(req)

	return req, nil
}

// applyAuth applies authentication to the request.
func (h *HTTPHandler) applyAuth(req *http.Request) {
	auth := h.config.Auth
	switch auth.Type {
	case "basic":
		req.SetBasicAuth(auth.Username, auth.Password)
	case "bearer":
		req.Header.Set("Authorization", "Bearer "+auth.Token)
	case "api_key":
		header := auth.Header
		if header == "" {
			header = "X-API-Key"
		}
		req.Header.Set(header, auth.Key)
	}
}

// handleResponse processes the HTTP response and determines success/retry/failure.
func (h *HTTPHandler) handleResponse(resp *http.Response, task *Task) error {
	body, _ := io.ReadAll(resp.Body)

	h.logger.Info(fmt.Sprintf("HTTP handler: received status %d for task %s", resp.StatusCode, task.ID()))

	// Success (2xx)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		// Write response body as task result if non-empty
		if len(body) > 0 {
			if _, err := task.ResultWriter().Write(body); err != nil {
				h.logger.Error(fmt.Sprintf("Failed to write task result: %v", err))
			}
		}
		return nil
	}

	// Permanent failure (configured fail_on codes or 4xx)
	if h.failOn[resp.StatusCode] || (resp.StatusCode >= 400 && resp.StatusCode < 500 && !h.retryOn[resp.StatusCode]) {
		return NewSkipRetryError(fmt.Sprintf("HTTP %d: %s", resp.StatusCode, truncateBody(body, 200)))
	}

	// Retry (configured retry_on codes or 5xx)
	if h.retryOn[resp.StatusCode] || resp.StatusCode >= 500 {
		return fmt.Errorf("HTTP %d: %s (will retry)", resp.StatusCode, truncateBody(body, 200))
	}

	// Unknown status code - treat as retryable
	return fmt.Errorf("HTTP %d: unexpected status code", resp.StatusCode)
}

// truncateBody truncates the response body for error messages.
func truncateBody(body []byte, maxLen int) string {
	s := string(body)
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}
