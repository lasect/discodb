package scheduler

import "time"

type WorkPriority string

const (
	PriorityHigh   WorkPriority = "high"
	PriorityMedium WorkPriority = "medium"
	PriorityLow    WorkPriority = "low"
)

type TokenClass string

const (
	TokenClassWAL      TokenClass = "wal"
	TokenClassHeap     TokenClass = "heap"
	TokenClassIndex    TokenClass = "index"
	TokenClassCatalog  TokenClass = "catalog"
	TokenClassOverflow TokenClass = "overflow"
)

func AllTokenClasses() []TokenClass {
	return []TokenClass{
		TokenClassWAL,
		TokenClassHeap,
		TokenClassIndex,
		TokenClassCatalog,
		TokenClassOverflow,
	}
}

type APIRequest struct {
	ID           string       `json:"id"`
	Route        string       `json:"route"`
	TokenClass   TokenClass   `json:"token_class"`
	Priority     WorkPriority `json:"priority"`
	CostEstimate uint32       `json:"cost_estimate"`
	Deadline     *time.Time   `json:"deadline,omitempty"`
}

type APIResponse[T any] struct {
	RequestID string `json:"request_id"`
	Result    T      `json:"result"`
}

type WorkItem[T any] struct {
	Priority   WorkPriority `json:"priority"`
	Payload    T            `json:"payload"`
	EnqueuedAt time.Time    `json:"enqueued_at"`
	Retries    uint32       `json:"retries"`
}

func NewWorkItem[T any](payload T, priority WorkPriority) WorkItem[T] {
	return WorkItem[T]{
		Priority:   priority,
		Payload:    payload,
		EnqueuedAt: time.Now(),
	}
}

func (w *WorkItem[T]) IncrementRetry() {
	w.Retries++
}

type Queue[T any] struct {
	items []WorkItem[T]
}

func (q *Queue[T]) Push(item WorkItem[T]) {
	q.items = append(q.items, item)
}

func (q *Queue[T]) Pop() (WorkItem[T], bool) {
	if len(q.items) == 0 {
		return WorkItem[T]{}, false
	}
	item := q.items[0]
	q.items = q.items[1:]
	return item, true
}

func (q *Queue[T]) Len() int {
	return len(q.items)
}
