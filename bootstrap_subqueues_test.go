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

// Queues without explicit sub-queues (server-side `.default`) must keep bootstrapping, both
// when the worker names the parent and when it names `<parent>.default`. The deployment then
// fails here (no git URL), past the sub-queue check.
func TestBootstrapDefaultSubQueueStillAccepted(t *testing.T) {
	cases := map[string]struct {
		group *parentQueueGroup
		subs  []BootstrapSubQueueConfig
	}{
		"parent, server lists .default":   {&parentQueueGroup{parent: "azure-image", listenAll: true}, []BootstrapSubQueueConfig{{Name: "azure-image.default", Priority: 5}}},
		"parent, old server without subs": {&parentQueueGroup{parent: "azure-image", listenAll: true}, nil},
		"explicit .default":               {&parentQueueGroup{parent: "azure-image", configuredSQs: []string{"azure-image.default"}}, []BootstrapSubQueueConfig{{Name: "azure-image.default", Priority: 5}}},
	}
	for name, tc := range cases {
		cfg := DefaultConfig()
		cfg.DeploymentDir = t.TempDir()
		resp := &BootstrapResponse{SubQueues: tc.subs}
		resp.Queue.Priority = 5
		_, err := New(cfg).bootstrapQueueWithResponse(context.Background(), tc.group, resp)
		if err != nil && strings.Contains(err.Error(), "none of the configured sub-queues") {
			t.Errorf("%s: rejected: %v", name, err)
		}
	}
}
