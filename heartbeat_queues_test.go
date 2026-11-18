package worker

import (
	"encoding/json"
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

// The heartbeat carries the activity per sub-queue, so the autoscaler can ignore the
// sub-queues its idle trigger does not watch.
func TestHeartbeatActivityPerQueue(t *testing.T) {
	p := &processor{inflight: make(map[string]string)}
	h := &heartbeat{}
	h.setActivitySource(p.activity)
	h.setQueueActivitySource(p.activityByQueue)

	p.trackInflight("t1", "q.paid")
	p.trackInflight("t2", "q.free")
	p.trackInflight("t3", "q.free")
	p.untrackInflight("t1")

	data := map[string]interface{}{}
	h.addActivity(data)
	var got map[string]queueActivity
	if err := json.Unmarshal([]byte(data["activity"].(string)), &got); err != nil {
		t.Fatalf("activity is not JSON: %v (%v)", err, data["activity"])
	}
	if got["q.paid"].Active != 0 || got["q.paid"].LastTaskAt == 0 {
		t.Errorf("q.paid = %+v, want idle with a last task time", got["q.paid"])
	}
	if got["q.free"].Active != 2 || got["q.free"].LastTaskAt == 0 {
		t.Errorf("q.free = %+v, want 2 active", got["q.free"])
	}
	if data["active"] != 2 {
		t.Errorf("active = %v, want 2", data["active"])
	}
}
