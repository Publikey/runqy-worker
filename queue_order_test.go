package worker

import (
	"reflect"
	"testing"
)

func TestQueueOrderReturnsEachQueueOnce(t *testing.T) {
	o := newQueueOrder(map[string]int{"a.high": 6, "a.normal": 4, "a.low": 2, "b.default": 0}, nil)
	for i := 0; i < 1000; i++ {
		got := o.next()
		if len(got) != 4 {
			t.Fatalf("got %v, want 4 queues", got)
		}
		seen := map[string]bool{}
		for _, q := range got {
			if seen[q] {
				t.Fatalf("duplicate queue %q in %v", q, got)
			}
			seen[q] = true
		}
	}
	if want := []string{"a.high", "a.low", "a.normal", "b.default"}; !reflect.DeepEqual(o.names, want) {
		t.Fatalf("names = %v, want %v", o.names, want)
	}
}

// With both queues backlogged, BRPOP serves the first one: it must come first in
// proportion to its weight (asynq semantics), and be redrawn at every dequeue.
func TestQueueOrderWeightedDistribution(t *testing.T) {
	o := newQueueOrder(map[string]int{"a.priority": 6, "a.normal": 4}, nil)
	const n = 20000
	first := 0
	for i := 0; i < n; i++ {
		if o.next()[0] == "a.priority" {
			first++
		}
	}
	if ratio := float64(first) / n; ratio < 0.57 || ratio > 0.63 {
		t.Fatalf("a.priority first %.3f of the time, want ~0.60", ratio)
	}
}

func TestQueueOrderWeightedThreeQueues(t *testing.T) {
	o := newQueueOrder(map[string]int{"a.priority": 6, "a.normal": 3, "a.low": 1}, nil)
	const n = 30000
	counts := map[string]int{}
	for i := 0; i < n; i++ {
		counts[o.next()[0]]++
	}
	for q, want := range map[string]float64{"a.priority": 0.6, "a.normal": 0.3, "a.low": 0.1} {
		if got := float64(counts[q]) / n; got < want-0.03 || got > want+0.03 {
			t.Errorf("%s first %.3f of the time, want ~%.2f", q, got, want)
		}
	}
}

func TestQueueOrderStrict(t *testing.T) {
	o := newQueueOrder(
		map[string]int{"a.normal": 4, "a.priority": 6, "a.low": 2},
		map[string]bool{"a": true},
	)
	want := []string{"a.priority", "a.normal", "a.low"}
	for i := 0; i < 1000; i++ {
		if got := o.next(); !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestQueueOrderStrictTiesByName(t *testing.T) {
	o := newQueueOrder(map[string]int{"a.y": 5, "a.x": 5, "a.z": 1}, map[string]bool{"a": true})
	want := []string{"a.x", "a.y", "a.z"}
	for i := 0; i < 100; i++ {
		if got := o.next(); !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// A strict parent keeps its sub-queues in priority order, while the order between
// parents stays weighted.
func TestQueueOrderStrictParentAmongWeighted(t *testing.T) {
	o := newQueueOrder(
		map[string]int{"a.high": 5, "a.low": 1, "b.default": 4},
		map[string]bool{"a": true},
	)
	const n = 20000
	bFirst := 0
	for i := 0; i < n; i++ {
		got := o.next()
		pos := map[string]int{}
		for j, q := range got {
			pos[q] = j
		}
		if pos["a.high"] > pos["a.low"] {
			t.Fatalf("strict parent out of order: %v", got)
		}
		if got[0] == "b.default" {
			bFirst++
		}
	}
	if ratio := float64(bFirst) / n; ratio < 0.37 || ratio > 0.43 {
		t.Fatalf("b.default first %.3f of the time, want ~0.40", ratio)
	}
}

func TestQueueOrderSingleQueue(t *testing.T) {
	o := newQueueOrder(map[string]int{"a.default": 3}, map[string]bool{"a": true})
	if got := o.next(); !reflect.DeepEqual(got, []string{"a.default"}) {
		t.Fatalf("got %v", got)
	}
}
