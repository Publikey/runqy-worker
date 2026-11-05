package redis

import (
	"context"
	"testing"
	"time"
)

// A completed task must never run again. Before the fix, a lease recoverer on another
// worker (whose clock ran ahead) could move a RUNNING task to retry; the owner then
// completed it (result written, webhook sent) but the retry entry stayed and the task
// was forwarded to pending and generated a second time.

const (
	raceQueue  = "q.default"
	raceTaskID = "t1"
)

func dequeueRaceTask(t *testing.T) (*Client, *TaskData) {
	t.Helper()
	c, mr := newTestClient(t)
	mr.HSet("asynq:{q.default}:t:t1",
		"type", "task", "payload", `{"a":1}`, "retry", "0", "max_retry", "3",
		"queue", raceQueue, "state", "pending")
	mr.Lpush("asynq:{q.default}:pending", raceTaskID)
	task, err := c.Dequeue(context.Background(), []string{raceQueue}, 2*time.Minute)
	if err != nil || task == nil {
		t.Fatalf("Dequeue = %v, %v", task, err)
	}
	return c, task
}

func inRetry(t *testing.T, c *Client) bool {
	t.Helper()
	_, err := c.Rdb.ZScore(context.Background(), "asynq:{q.default}:retry", raceTaskID).Result()
	return err == nil
}

func taskState(t *testing.T, c *Client) string {
	t.Helper()
	s, _ := c.Rdb.HGet(context.Background(), "asynq:{q.default}:t:t1", "state").Result()
	return s
}

// The exact prod sequence: recovered while running, then completed by its owner.
func TestCompleteAfterConcurrentRecoveryWins(t *testing.T) {
	c, task := dequeueRaceTask(t)
	ctx := context.Background()

	// A recoverer judged the lease expired (skewed clock): cutoff past the lease score.
	ok, err := c.RecoverToRetry(ctx, task, raceQueue, time.Minute, time.Now().Add(time.Hour))
	if err != nil || !ok {
		t.Fatalf("RecoverToRetry = %v, %v; want recovered", ok, err)
	}
	if !inRetry(t, c) {
		t.Fatal("task should be in retry after the recovery")
	}

	if err := c.Complete(ctx, raceTaskID, raceQueue, time.Hour); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if inRetry(t, c) {
		t.Fatal("completed task still in retry: it would run a second time")
	}
	if n, _ := c.Rdb.LLen(ctx, "asynq:{q.default}:pending").Result(); n != 0 {
		t.Fatalf("completed task still pending (%d)", n)
	}
	if s := taskState(t, c); s != StateCompleted {
		t.Fatalf("state = %q, want completed", s)
	}
}

// A lease renewed since the listing is not recovered.
func TestRecoverSkipsLiveLease(t *testing.T) {
	c, task := dequeueRaceTask(t)
	ctx := context.Background()

	ok, err := c.RecoverToRetry(ctx, task, raceQueue, time.Minute, time.Now().Add(-time.Minute))
	if err != nil || ok {
		t.Fatalf("RecoverToRetry = %v, %v; want left alone (lease still valid)", ok, err)
	}
	if inRetry(t, c) {
		t.Fatal("a live task was moved to retry")
	}
	if s := taskState(t, c); s != StateActive {
		t.Fatalf("state = %q, want active", s)
	}
}

// A task completed between the listing and the recovery is not recovered.
func TestRecoverSkipsCompletedTask(t *testing.T) {
	c, task := dequeueRaceTask(t)
	ctx := context.Background()

	if err := c.Complete(ctx, raceTaskID, raceQueue, time.Hour); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	for _, archive := range []bool{false, true} {
		var ok bool
		var err error
		if archive {
			ok, err = c.RecoverToArchive(ctx, task, raceQueue, time.Now().Add(time.Hour))
		} else {
			ok, err = c.RecoverToRetry(ctx, task, raceQueue, time.Minute, time.Now().Add(time.Hour))
		}
		if err != nil || ok {
			t.Fatalf("recover(archive=%v) = %v, %v; want left alone (completed)", archive, ok, err)
		}
	}
	if inRetry(t, c) || taskState(t, c) != StateCompleted {
		t.Fatalf("completed task disturbed: retry=%v state=%q", inRetry(t, c), taskState(t, c))
	}
}

// Leases are scored on the Redis clock, not the (possibly skewed) local one.
func TestLeaseUsesServerClock(t *testing.T) {
	c, mr := newTestClient(t)
	ctx := context.Background()
	server := time.Now().Add(-10 * time.Minute) // local host 10 min ahead of Redis
	mr.SetTime(server)

	if got := c.ServerNow(ctx); got.Unix() != server.Unix() {
		t.Fatalf("ServerNow = %v, want the Redis clock %v", got, server)
	}

	mr.HSet("asynq:{q.default}:t:t1",
		"type", "task", "payload", `{}`, "retry", "0", "max_retry", "3",
		"queue", raceQueue, "state", "pending")
	mr.Lpush("asynq:{q.default}:pending", raceTaskID)
	if _, err := c.Dequeue(ctx, []string{raceQueue}, 2*time.Minute); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	score, err := c.Rdb.ZScore(ctx, "asynq:{q.default}:lease", raceTaskID).Result()
	if err != nil {
		t.Fatalf("lease score: %v", err)
	}
	if want := float64(server.Add(2 * time.Minute).Unix()); score != want {
		t.Fatalf("lease score = %v, want %v (Redis clock + lease)", score, want)
	}
}
