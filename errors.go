package worker

import "fmt"

// SkipRetryError signals that the task should fail permanently without retry.
// Use this for errors where retrying would not help (e.g., invalid input, 4xx HTTP responses).
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
