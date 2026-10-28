package redis

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type nopLogger struct{}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}
func (nopLogger) Debug(string, ...any) {}

func newTestClient(t *testing.T) (*Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return NewClient(rdb, nopLogger{}), mr
}

// The server's enqueue (asynq) stores pending_since in nanoseconds: Dequeue must report it in
// seconds, otherwise pending_timeout never fires on a first attempt.
func TestDequeuePendingSinceNanoseconds(t *testing.T) {
	c, mr := newTestClient(t)
	ctx := context.Background()
	enqueuedAt := time.Now().Add(-90 * time.Second)

	mr.HSet("asynq:{q.default}:t:t1",
		"type", "task", "payload", `{"a":1}`, "retry", "0", "max_retry", "2",
		"queue", "q.default", "state", "pending",
		"pending_since", strconv.FormatInt(enqueuedAt.UnixNano(), 10))
	mr.Lpush("asynq:{q.default}:pending", "t1")

	task, err := c.Dequeue(ctx, []string{"q.default"}, time.Minute)
	if err != nil || task == nil {
		t.Fatalf("Dequeue = %v, %v", task, err)
	}
	if task.PendingSince != enqueuedAt.Unix() {
		t.Fatalf("PendingSince = %d, want %d (unix seconds)", task.PendingSince, enqueuedAt.Unix())
	}
}
