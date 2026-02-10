package worker

import (
	"sync"
	"time"
)

// LogLine represents a single captured log line.
type LogLine struct {
	Timestamp int64  `json:"ts"`
	Source    string `json:"src"` // "stderr" or "stdout"
	Text     string `json:"text"`
	Seq      uint64 `json:"seq"`
}

// LogBuffer is a thread-safe ring buffer for log lines.
type LogBuffer struct {
	mu    sync.Mutex
	lines []LogLine
	cap   int
	seq   uint64
}

// NewLogBuffer creates a new LogBuffer with the given capacity.
func NewLogBuffer(capacity int) *LogBuffer {
	if capacity <= 0 {
		capacity = 500
	}
	return &LogBuffer{
		lines: make([]LogLine, 0, capacity),
		cap:   capacity,
	}
}

// Append adds a log line to the buffer.
func (b *LogBuffer) Append(source, text string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.seq++
	line := LogLine{
		Timestamp: time.Now().UnixMilli(),
		Source:    source,
		Text:     text,
		Seq:      b.seq,
	}

	if len(b.lines) < b.cap {
		b.lines = append(b.lines, line)
	} else {
		// Shift left and append (ring buffer eviction)
		copy(b.lines, b.lines[1:])
		b.lines[len(b.lines)-1] = line
	}
}

// LinesSince returns all lines with sequence number greater than seq.
func (b *LogBuffer) LinesSince(seq uint64) []LogLine {
	b.mu.Lock()
	defer b.mu.Unlock()

	var result []LogLine
	for _, l := range b.lines {
		if l.Seq > seq {
			result = append(result, l)
		}
	}
	return result
}

// Last returns the last n lines from the buffer.
func (b *LogBuffer) Last(n int) []LogLine {
	b.mu.Lock()
	defer b.mu.Unlock()

	if n <= 0 || len(b.lines) == 0 {
		return nil
	}
	if n > len(b.lines) {
		n = len(b.lines)
	}
	start := len(b.lines) - n
	result := make([]LogLine, n)
	copy(result, b.lines[start:])
	return result
}
