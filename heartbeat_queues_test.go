package worker

import "testing"

func TestEncodeQueues(t *testing.T) {
	tests := []struct {
		in   map[string]int
		want string
	}{
		{nil, "{}"},
		{map[string]int{}, "{}"},
		{map[string]int{"inference.low": 1, "inference.high": 10}, `{"inference.high":10,"inference.low":1}`},
	}
	for _, tt := range tests {
		if got := encodeQueues(tt.in); got != tt.want {
			t.Errorf("encodeQueues(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
