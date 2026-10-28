package redis

import (
	"testing"
	"time"
)

func TestUnixSeconds(t *testing.T) {
	now := time.Now()
	for name, v := range map[string]int64{
		"seconds":      now.Unix(),
		"milliseconds": now.UnixMilli(),
		"nanoseconds":  now.UnixNano(),
	} {
		if got := UnixSeconds(v); got != now.Unix() {
			t.Errorf("%s: UnixSeconds(%d) = %d, want %d", name, v, got, now.Unix())
		}
	}
	if got := UnixSeconds(0); got != 0 {
		t.Errorf("UnixSeconds(0) = %d, want 0", got)
	}
}
