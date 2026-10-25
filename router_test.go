package aegis

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type LoopState struct {
	Turns    int
	Approved bool
	Log      []string
}

func TestStateGraph_CyclicExecution(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	runID := "run-graph-cyclic"

	graph := NewStateGraph[LoopState]("planner", 10)

	graph.AddNode("planner", func(rt *Runtime[LoopState]) (string, error) {
		res, err := Step(rt, "planner_step", func(ctx context.Context, s *LoopState) (string, error) {
			s.Turns++
			s.Log = append(s.Log, fmt.Sprintf("plan_turn_%d", s.Turns))
			return fmt.Sprintf("plan-%d", s.Turns), nil
		})
		if err != nil {
			return "", err
		}
		if res == "" {
			return "", errors.New("empty plan")
		}
		return "critic", nil
	})

	graph.AddNode("critic", func(rt *Runtime[LoopState]) (string, error) {
		critique, err := Step(rt, "critic_step", func(ctx context.Context, s *LoopState) (string, error) {
			if s.Turns >= 3 {
				s.Approved = true
				return "pass", nil
			}
			return "retry", nil
		})
		if err != nil {
			return "", err
		}

		if critique == "pass" {
			return EndNode, nil
		}
		return "planner", nil // Cycle back to planner
	})

	rt := NewRuntime[LoopState](ctx, store, runID, &LoopState{})
	err := graph.Execute(rt)
	if err != nil {
		t.Fatalf("unexpected error during cyclic graph execution: %v", err)
	}

	state := rt.State()
	if !state.Approved || state.Turns != 3 {
		t.Fatalf("expected 3 turns and approved=true, got turns=%d approved=%v", state.Turns, state.Approved)
	}

	// Verify all versioned steps were recorded distinctly in the store
	expectedSteps := []string{
		"planner_step:1", "critic_step:1",
		"planner_step:2", "critic_step:2",
		"planner_step:3", "critic_step:3",
	}
	for _, stepID := range expectedSteps {
		_, hit, err := store.GetStep(ctx, runID, stepID)
		if err != nil || !hit {
			t.Fatalf("expected step %q to be checkpointed, hit=%v, err=%v", stepID, hit, err)
		}
	}
}

func TestStateGraph_MaxCyclesLimit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	graph := NewStateGraph[LoopState]("infinite_loop", 4)
	graph.AddNode("infinite_loop", func(rt *Runtime[LoopState]) (string, error) {
		return "infinite_loop", nil // Infinite cycle
	})

	rt := NewRuntime[LoopState](ctx, store, "run-infinite", &LoopState{})
	err := graph.Execute(rt)
	if !errors.Is(err, ErrMaxCyclesReached) {
		t.Fatalf("expected ErrMaxCyclesReached, got: %v", err)
	}
}

func TestRunParallelGroup(t *testing.T) {
	ctx := context.Background()
	var activeGoroutines int32
	var maxActiveGoroutines int32

	tasks := make([]func(ctx context.Context) error, 20)
	for i := 0; i < 20; i++ {
		tasks[i] = func(ctx context.Context) error {
			current := atomic.AddInt32(&activeGoroutines, 1)
			for {
				max := atomic.LoadInt32(&maxActiveGoroutines)
				if current <= max || atomic.CompareAndSwapInt32(&maxActiveGoroutines, max, current) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&activeGoroutines, -1)
			return nil
		}
	}

	limit := 4
	err := RunParallelGroup(ctx, limit, tasks...)
	if err != nil {
		t.Fatalf("unexpected error in RunParallelGroup: %v", err)
	}

	if maxActiveGoroutines > int32(limit) {
		t.Fatalf("expected max active goroutines <= %d, observed %d", limit, maxActiveGoroutines)
	}
}

func TestMapParallel(t *testing.T) {
	ctx := context.Background()
	items := []int{1, 2, 3, 4, 5, 6, 7, 8}

	results, err := MapParallel(ctx, 3, items, func(ctx context.Context, item int) (int, error) {
		return item * 10, nil
	})
	if err != nil {
		t.Fatalf("unexpected error in MapParallel: %v", err)
	}

	if len(results) != len(items) {
		t.Fatalf("expected %d results, got %d", len(items), len(results))
	}

	for i, v := range results {
		expected := items[i] * 10
		if v != expected {
			t.Fatalf("index %d: expected %d, got %d", i, expected, v)
		}
	}
}
