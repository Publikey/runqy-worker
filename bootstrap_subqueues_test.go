package worker

import (
	"context"
	"strings"
	"testing"
)

// A worker configured only on sub-queues the server does not know must fail its
// bootstrap (before deploying any code) instead of running without listening on anything.
func TestBootstrapFailsWhenNoConfiguredSubQueueExists(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeploymentDir = t.TempDir()
	w := New(cfg)
	resp := &BootstrapResponse{SubQueues: []BootstrapSubQueueConfig{{Name: "video.high", Priority: 5}}}
	resp.Deployment.GitURL = "https://invalid.example/should-not-be-cloned.git"
	group := &parentQueueGroup{parent: "video", configuredSQs: []string{"video.low"}}

	_, err := w.bootstrapQueueWithResponse(context.Background(), group, resp)
	if err == nil || !strings.Contains(err.Error(), "none of the configured sub-queues") {
		t.Fatalf("err = %v, want a missing sub-queue error", err)
	}
}
