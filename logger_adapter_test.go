package worker

import (
	"fmt"
	"testing"
)

type recordLogger struct{ lines []string }

func (r *recordLogger) Debug(f string, a ...interface{}) {
	r.lines = append(r.lines, fmt.Sprintf(f, a...))
}
func (r *recordLogger) Info(f string, a ...interface{}) {
	r.lines = append(r.lines, fmt.Sprintf(f, a...))
}
func (r *recordLogger) Warn(f string, a ...interface{}) {
	r.lines = append(r.lines, fmt.Sprintf(f, a...))
}
func (r *recordLogger) Error(f string, a ...interface{}) {
	r.lines = append(r.lines, fmt.Sprintf(f, a...))
}
func (r *recordLogger) Fatal(f string, a ...interface{}) {
	r.lines = append(r.lines, fmt.Sprintf(f, a...))
}

// A child's stderr line with a '%' must come out verbatim, not as %!|(MISSING).
func TestLoggerAdapterFormatsOnce(t *testing.T) {
	rec := &recordLogger{}
	(&loggerAdapter{rec}).Info("[STDERR] %s", "Loading weights:  62%|██████▏   | 414/665")
	want := "[STDERR] Loading weights:  62%|██████▏   | 414/665"
	if len(rec.lines) != 1 || rec.lines[0] != want {
		t.Fatalf("got %q, want %q", rec.lines, want)
	}
}
