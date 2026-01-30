package worker

import (
	"testing"
)

func TestGroupQueuesByParent(t *testing.T) {
	tests := []struct {
		name           string
		queueNames     []string
		expectedGroups map[string]struct {
			listenAll     bool
			configuredSQs []string
		}
	}{
		{
			name:       "specific sub-queues only",
			queueNames: []string{"manyQueue.high", "manyQueue.low"},
			expectedGroups: map[string]struct {
				listenAll     bool
				configuredSQs []string
			}{
				"manyQueue": {
					listenAll:     false,
					configuredSQs: []string{"manyQueue.high", "manyQueue.low"},
				},
			},
		},
		{
			name:       "parent queue only (listen all)",
			queueNames: []string{"manyQueue"},
			expectedGroups: map[string]struct {
				listenAll     bool
				configuredSQs []string
			}{
				"manyQueue": {
					listenAll:     true,
					configuredSQs: []string{},
				},
			},
		},
		{
			name:       "parent overrides specific sub-queues",
			queueNames: []string{"manyQueue", "manyQueue.high"},
			expectedGroups: map[string]struct {
				listenAll     bool
				configuredSQs []string
			}{
				"manyQueue": {
					listenAll:     true,
					configuredSQs: []string{},
				},
			},
		},
		{
			name:       "specific sub-queues then parent (order reversed)",
			queueNames: []string{"manyQueue.high", "manyQueue"},
			expectedGroups: map[string]struct {
				listenAll     bool
				configuredSQs []string
			}{
				"manyQueue": {
					listenAll:     true,
					configuredSQs: []string{"manyQueue.high"}, // Already added before listenAll was set
				},
			},
		},
		{
			name:       "multiple parent queues",
			queueNames: []string{"inference.high", "simple.default"},
			expectedGroups: map[string]struct {
				listenAll     bool
				configuredSQs []string
			}{
				"inference": {
					listenAll:     false,
					configuredSQs: []string{"inference.high"},
				},
				"simple": {
					listenAll:     false,
					configuredSQs: []string{"simple.default"},
				},
			},
		},
		{
			name:       "mixed: one parent all, one parent specific",
			queueNames: []string{"inference", "simple.default"},
			expectedGroups: map[string]struct {
				listenAll     bool
				configuredSQs []string
			}{
				"inference": {
					listenAll:     true,
					configuredSQs: []string{},
				},
				"simple": {
					listenAll:     false,
					configuredSQs: []string{"simple.default"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := groupQueuesByParent(tt.queueNames)

			// Check number of groups
			if len(result) != len(tt.expectedGroups) {
				t.Errorf("got %d groups, want %d", len(result), len(tt.expectedGroups))
			}

			for parent, expected := range tt.expectedGroups {
				group, ok := result[parent]
				if !ok {
					t.Errorf("missing parent group %q", parent)
					continue
				}

				if group.listenAll != expected.listenAll {
					t.Errorf("group[%s].listenAll = %v, want %v", parent, group.listenAll, expected.listenAll)
				}

				if group.listenAll {
					// When listenAll is true, we don't care about configuredSQs
					continue
				}

				if len(group.configuredSQs) != len(expected.configuredSQs) {
					t.Errorf("group[%s].configuredSQs = %v, want %v", parent, group.configuredSQs, expected.configuredSQs)
					continue
				}

				// Check that all expected sub-queues are present (order may vary)
				sqSet := make(map[string]bool)
				for _, sq := range group.configuredSQs {
					sqSet[sq] = true
				}
				for _, expected := range expected.configuredSQs {
					if !sqSet[expected] {
						t.Errorf("group[%s].configuredSQs missing %q, got %v", parent, expected, group.configuredSQs)
					}
				}
			}
		})
	}
}

func TestParentQueueName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"inference.high", "inference"},
		{"inference.low", "inference"},
		{"manyQueue.high", "manyQueue"},
		{"simple.default", "simple"},
		{"inference", "inference"},
		{"simple", "simple"},
		{"a.b.c", "a"}, // Only first dot matters
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parentQueueName(tt.input)
			if result != tt.expected {
				t.Errorf("parentQueueName(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
