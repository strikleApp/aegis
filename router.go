package aegis

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// EndNode is the terminal sentinel symbol for graph completion.
const EndNode = "__end__"

// GraphNode represents a state transition function in a cyclic agent graph.
// It returns the name of the next node to transition to, or EndNode when finished.
type GraphNode[T any] func(rt *Runtime[T]) (nextNode string, err error)

// StateGraph coordinates cyclic agent workflows with explicit loop guardrails.
type StateGraph[T any] struct {
	nodes     map[string]GraphNode[T]
	entryNode string
	maxCycles int
}

// NewStateGraph creates a state graph with a configured entry node and safety ceiling.
func NewStateGraph[T any](entryNode string, maxCycles int) *StateGraph[T] {
	if maxCycles <= 0 {
		maxCycles = 25 // Hard safety ceiling against infinite LLM loops
	}
	return &StateGraph[T]{
		nodes:     make(map[string]GraphNode[T]),
		entryNode: entryNode,
		maxCycles: maxCycles,
	}
}

// AddNode registers a named node function into the graph.
func (g *StateGraph[T]) AddNode(name string, fn GraphNode[T]) {
	g.nodes[name] = fn
}

// Execute runs the cyclic graph with distributed lease fencing, cancellation checks, and cycle limits.
func (g *StateGraph[T]) Execute(rt *Runtime[T]) error {
	// 1. Acquire distributed lease fencing lock to prevent split-brain execution
	acquired, err := rt.AcquireLease()
	if err != nil {
		return fmt.Errorf("aegis: lease acquisition error: %w", err)
	}
	if !acquired {
		return ErrRunLocked
	}
	defer func() {
		_ = rt.ReleaseLease()
	}()

	current := g.entryNode

	for cycle := 0; cycle < g.maxCycles; cycle++ {
		if err := rt.Context().Err(); err != nil {
			return err
		}

		if current == EndNode {
			return nil
		}

		nodeFn, ok := g.nodes[current]
		if !ok {
			return fmt.Errorf("aegis: unknown graph node %q", current)
		}

		next, err := nodeFn(rt)
		if err != nil {
			return err
		}

		current = next
	}

	return ErrMaxCyclesReached
}

// RunParallelGroup uses errgroup.WithContext with strict concurrency limits to prevent goroutine exhaustion.
func RunParallelGroup(ctx context.Context, limit int, tasks ...func(ctx context.Context) error) error {
	if limit <= 0 {
		limit = 10
	}
	g, egCtx := errgroup.WithContext(ctx)
	g.SetLimit(limit)

	for _, task := range tasks {
		t := task
		g.Go(func() error {
			return t(egCtx)
		})
	}
	return g.Wait()
}

// MapParallel executes fn over each element in items concurrently with a bounded pool.
func MapParallel[I any, O any](ctx context.Context, limit int, items []I, fn func(ctx context.Context, item I) (O, error)) ([]O, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}

	results := make([]O, len(items))
	g, egCtx := errgroup.WithContext(ctx)
	g.SetLimit(limit)

	for i, item := range items {
		idx := i
		val := item
		g.Go(func() error {
			out, err := fn(egCtx, val)
			if err != nil {
				return err
			}
			results[idx] = out
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}
