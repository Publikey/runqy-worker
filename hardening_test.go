package worker

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// dummyRedisClient returns a Redis client that connects to a non-routable
// address. Operations will fail with a connection error but will not panic
// from a nil pointer dereference. This allows testing heartbeat lifecycle
// methods that call register/deregister without a real Redis server.
func dummyRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:0", // port 0 — nothing is listening
		DialTimeout: 1 * time.Millisecond,
		ReadTimeout: 1 * time.Millisecond,
	})
}

// ---------------------------------------------------------------------------
// Heartbeat double-close tests
// ---------------------------------------------------------------------------

// TestHeartbeatDoubleStop verifies that calling stop() twice on a heartbeat
// does not panic. The select guard in stop() should detect the already-closed
// done channel and return early on the second call.
func TestHeartbeatDoubleStop(t *testing.T) {
	h := &heartbeat{
		rdb:    dummyRedisClient(),
		done:   make(chan struct{}),
		logger: NewStdLogger(),
		config: DefaultConfig(),
	}

	// Manually close the done channel to simulate a prior stop.
	close(h.done)

	// Calling stop() again must not panic.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("heartbeat.stop() panicked on double-close: %v", r)
		}
	}()

	h.stop()
}

// TestHeartbeatStopWithoutStart verifies that calling stop() on a heartbeat
// that was never started (start() was never called, so the wg counter is 0)
// does not panic or hang.
func TestHeartbeatStopWithoutStart(t *testing.T) {
	h := &heartbeat{
		rdb:    dummyRedisClient(),
		done:   make(chan struct{}),
		logger: NewStdLogger(),
		config: DefaultConfig(),
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("heartbeat.stop() panicked without prior start: %v", r)
		}
	}()

	done := make(chan struct{})
	go func() {
		h.stop()
		close(done)
	}()

	select {
	case <-done:
		// stop() returned successfully.
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat.stop() hung when called without start()")
	}
}

// TestHeartbeatStopTwiceWithSimulatedStart verifies the full lifecycle: a
// simulated start (WaitGroup + goroutine waiting on done) followed by two
// consecutive stop() calls. The first close signals the goroutine to exit;
// the second stop() call must be a safe no-op.
//
// We simulate the loop goroutine rather than calling start() because the real
// loop calls register(), which attempts a Redis round-trip and would slow the
// test down waiting for the dial timeout.
func TestHeartbeatStopTwiceWithSimulatedStart(t *testing.T) {
	h := &heartbeat{
		rdb:    dummyRedisClient(),
		done:   make(chan struct{}),
		logger: NewStdLogger(),
		config: DefaultConfig(),
	}

	// Simulate what start() does: add to WaitGroup and launch a goroutine
	// that blocks on the done channel (mimics the heartbeat loop).
	h.wg.Add(1)
	h.started = true
	go func() {
		defer h.wg.Done()
		<-h.done
	}()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("heartbeat double-stop after simulated start panicked: %v", r)
		}
	}()

	// Close done manually (equivalent to what the first stop() would do).
	close(h.done)
	h.wg.Wait()

	// Now call stop() twice. The select guard should detect the already-closed
	// channel and return immediately without panicking.
	h.stop()
	h.stop()
}

// ---------------------------------------------------------------------------
// Processor graceful drain tests
// ---------------------------------------------------------------------------

// noopHandler is a Handler that does nothing. Used when we need to satisfy
// the interface but never expect ProcessTask to be called.
type noopHandler struct{}

func (noopHandler) ProcessTask(_ context.Context, _ *Task) error { return nil }

// newTestProcessor creates a processor suitable for testing stop/drain
// behavior. It uses a dummy Redis client (connects to port 0, so all
// operations fail with connection errors rather than nil-pointer panics)
// and a no-op handler.
func newTestProcessor(cfg Config) *processor {
	rdb := dummyRedisClient()
	return &processor{
		redis:         newRedisClient(rdb, cfg.Logger),
		handler:       noopHandler{},
		queueHandlers: make(map[string]HandlerFunc),
		config:        cfg,
		queues:        []string{"test.default"},
		done:          make(chan struct{}),
		stopping:      make(chan struct{}),
		logger:        cfg.Logger,
	}
}

// TestProcessorStopTimeout verifies that processor.stop() returns within a
// reasonable time (well under the shutdown timeout) when there are no
// in-flight tasks. Worker goroutines see the stopping channel and exit before
// doing meaningful Redis work.
func TestProcessorStopTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ShutdownTimeout = 100 * time.Millisecond
	cfg.Concurrency = 2

	p := newTestProcessor(cfg)
	p.start()

	deadline := time.After(2 * time.Second)
	done := make(chan struct{})
	go func() {
		p.stop()
		close(done)
	}()

	select {
	case <-done:
		// stop() returned successfully.
	case <-deadline:
		t.Fatal("processor.stop() did not return within 2s (ShutdownTimeout=100ms)")
	}
}

// TestProcessorStopReturnsQuickly verifies that stopping a processor with no
// in-flight tasks completes almost immediately, well within the shutdown
// timeout.
func TestProcessorStopReturnsQuickly(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ShutdownTimeout = 50 * time.Millisecond
	cfg.Concurrency = 1

	p := newTestProcessor(cfg)
	p.start()

	start := time.Now()
	p.stop()
	elapsed := time.Since(start)

	// stop() should complete well within 1 second. The shutdown timeout is
	// 50ms and there are no in-flight tasks, so the goroutines should exit
	// almost immediately via the stopping channel.
	if elapsed > 1*time.Second {
		t.Fatalf("processor.stop() took %v, expected < 1s", elapsed)
	}
}

// TestProcessorStopHighConcurrency verifies graceful drain with many worker
// goroutines. All should observe the stopping channel and exit cleanly.
func TestProcessorStopHighConcurrency(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ShutdownTimeout = 200 * time.Millisecond
	cfg.Concurrency = 16

	p := newTestProcessor(cfg)
	p.start()

	done := make(chan struct{})
	go func() {
		p.stop()
		close(done)
	}()

	select {
	case <-done:
		// All 16 worker goroutines drained successfully.
	case <-time.After(3 * time.Second):
		t.Fatal("processor.stop() with 16 workers did not return within 3s")
	}
}
