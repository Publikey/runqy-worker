package worker

import (
	"testing"
)

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

func TestHeartbeatAddActivity(t *testing.T) {
	h := &heartbeat{}
	data := map[string]interface{}{}
	h.addActivity(data)
	if len(data) != 0 {
		t.Fatalf("no activity source: expected no fields, got %v", data)
	}

	p := &processor{inflight: make(map[string]string)}
	h.setActivitySource(p.activity)
	h.addActivity(data)
	if data["active"] != 0 || data["last_task_at"] != nil {
		t.Fatalf("idle processor: got %v", data)
	}

	p.trackInflight("t1", "q.default")
	data = map[string]interface{}{}
	h.addActivity(data)
	if data["active"] != 1 || data["last_task_at"] == nil {
		t.Fatalf("busy processor: got %v", data)
	}
}
