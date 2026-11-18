package worker

import (
	"math/rand"
	"sort"
)

// queueOrder decides in which order the queues are polled at each dequeue. BRPOP serves the
// first non-empty key in argument order, so this order is what applies the priorities.
//
// Default (weighted, like asynq): the order is drawn again at every dequeue, each queue
// coming first with a probability proportional to its weight. With {high: 6, low: 4} and
// both backlogged, high is served ~60% of the time; low is never starved.
//
// Strict (per parent queue, strict_priority): the parent's sub-queues are always polled in
// descending priority, so a lower sub-queue is only served when the higher ones are empty.
// Between different parent queues on the same worker, the order stays weighted.
//
// Idle-only (priority 0): a queue of priority 0 is always polled after every queue of
// priority >= 1 the worker listens on, whatever their parent. It is served only when all of
// those are empty, so it uses spare capacity. There is no preemption: an idle-only task that
// has started runs to its end, and a task arriving meanwhile waits for a free slot. Several
// idle-only queues are shuffled evenly between themselves.
type queueOrder struct {
	names    []string        // unique queue names, sorted (stable list for the maintenance loops)
	weighted []string        // queues of priority >= 1, sorted
	idle     []string        // queues of priority 0, sorted
	weights  map[string]int  // queue -> weight (0 = idle-only)
	strict   map[string]bool // parent queue -> strict priority
}

func newQueueOrder(queueWeights map[string]int, strictParents map[string]bool) *queueOrder {
	o := &queueOrder{
		names:   make([]string, 0, len(queueWeights)),
		weights: make(map[string]int, len(queueWeights)),
		strict:  make(map[string]bool, len(strictParents)),
	}
	for q, w := range queueWeights {
		if w < 0 {
			w = 0
		}
		o.names = append(o.names, q)
		o.weights[q] = w
	}
	sort.Strings(o.names)
	for _, q := range o.names {
		if o.weights[q] > 0 {
			o.weighted = append(o.weighted, q)
		} else {
			o.idle = append(o.idle, q)
		}
	}
	for parent, s := range strictParents {
		if s {
			o.strict[parent] = true
		}
	}
	return o
}

// next returns all queues in the order to poll them for the next dequeue.
func (o *queueOrder) next() []string {
	if len(o.names) <= 1 {
		return o.names
	}

	// Weighted draw without replacement: same distribution as asynq's shuffle of the
	// weight-expanded list followed by dedup.
	out := make([]string, 0, len(o.names))
	remaining := append([]string(nil), o.weighted...)
	total := 0
	for _, q := range remaining {
		total += o.weights[q]
	}
	for len(remaining) > 0 {
		r := rand.Intn(total)
		i := 0
		for ; i < len(remaining)-1; i++ {
			r -= o.weights[remaining[i]]
			if r < 0 {
				break
			}
		}
		out = append(out, remaining[i])
		total -= o.weights[remaining[i]]
		remaining = append(remaining[:i], remaining[i+1:]...)
	}

	// Idle-only queues come last, in a fresh even shuffle.
	tail := len(out)
	out = append(out, o.idle...)
	rand.Shuffle(len(o.idle), func(i, j int) {
		out[tail+i], out[tail+j] = out[tail+j], out[tail+i]
	})

	if len(o.strict) > 0 {
		o.applyStrict(out)
	}
	return out
}

// applyStrict reorders, in place, the sub-queues of each strict parent by descending
// weight (ties by name) within the slots they already occupy.
func (o *queueOrder) applyStrict(order []string) {
	slots := make(map[string][]int)
	for i, q := range order {
		if p := parentQueueName(q); o.strict[p] {
			slots[p] = append(slots[p], i)
		}
	}
	for _, idx := range slots {
		qs := make([]string, len(idx))
		for j, i := range idx {
			qs[j] = order[i]
		}
		sort.Slice(qs, func(a, b int) bool {
			if o.weights[qs[a]] != o.weights[qs[b]] {
				return o.weights[qs[a]] > o.weights[qs[b]]
			}
			return qs[a] < qs[b]
		})
		for j, i := range idx {
			order[i] = qs[j]
		}
	}
}
