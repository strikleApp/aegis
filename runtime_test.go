package aegis

import (
	"context"
	"errors"
	"testing"
	"time"
)

type TestAgentState struct {
	Count int
}

func TestRuntime_CyclicStepVersioning(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	rt := NewRuntime[TestAgentState](ctx, store, "run-cycle-1", &TestAgentState{})

	v1 := rt.nextVersionedStepID("planner")
	v2 := rt.nextVersionedStepID("critic")
	v3 := rt.nextVersionedStepID("planner")

	if v1 != "planner:1" {
		t.Fatalf("expected planner:1, got %s", v1)
	}
	if v2 != "critic:1" {
		t.Fatalf("expected critic:1, got %s", v2)
	}
	if v3 != "planner:2" {
		t.Fatalf("expected planner:2, got %s", v3)
	}
}

func TestRuntime_StepMemoization(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	runID := "run-memo-1"

	executionCount := 0

	// First execution of step
	rt1 := NewRuntime[TestAgentState](ctx, store, runID, &TestAgentState{})
	res1, err := Step(rt1, "fetch_data", func(ctx context.Context, state *TestAgentState) (string, error) {
		executionCount++
		return "real_data", nil
	})
	if err != nil {
		t.Fatalf("unexpected error on Step 1: %v", err)
	}
	if res1 != "real_data" || executionCount != 1 {
		t.Fatalf("expected res=real_data count=1, got res=%s count=%d", res1, executionCount)
	}

	// Replay simulation with fresh runtime pointing at same store & runID
	rt2 := NewRuntime[TestAgentState](ctx, store, runID, &TestAgentState{})
	res2, err := Step(rt2, "fetch_data", func(ctx context.Context, state *TestAgentState) (string, error) {
		executionCount++ // Should NOT be called because fetch_data:1 is cached
		return "should_not_run", nil
	})
	if err != nil {
		t.Fatalf("unexpected error on replay Step: %v", err)
	}
	if res2 != "real_data" {
		t.Fatalf("expected cached real_data, got %s", res2)
	}
	if executionCount != 1 {
		t.Fatalf("expected executionCount to remain 1 after replay, got %d", executionCount)
	}
}

func TestRuntime_CostBudgetBreaker(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	rt := NewRuntime[TestAgentState](ctx, store, "run-budget-1", &TestAgentState{}, WithCostBudget[TestAgentState](0.05))

	// Track $0.03 (under $0.05 budget)
	if err := rt.TrackCost(0.03); err != nil {
		t.Fatalf("expected $0.03 to be accepted: %v", err)
	}
	if rt.CurrentCost() != 0.03 {
		t.Fatalf("expected cost 0.03, got %f", rt.CurrentCost())
	}

	// Track another $0.03 (total $0.06 > $0.05 budget) -> Should error with ErrBudgetExceeded
	err := rt.TrackCost(0.03)
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got: %v", err)
	}

	// Context should now be cancelled automatically
	if rt.Context().Err() == nil {
		t.Fatal("expected context to be cancelled after budget trip")
	}
}

type testHook struct {
	beforeCalls int
	afterCalls  int
}

func (h *testHook) BeforeStep(ctx context.Context, runID, stepID string) {
	h.beforeCalls++
}

func (h *testHook) AfterStep(ctx context.Context, runID, stepID string, duration time.Duration, err error) {
	h.afterCalls++
}

func TestRuntime_StepHooks(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	hook := &testHook{}

	rt := NewRuntime[TestAgentState](ctx, store, "run-hook-1", &TestAgentState{}, WithStepHooks[TestAgentState](hook))

	_, err := Step(rt, "hooked_step", func(ctx context.Context, s *TestAgentState) (string, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("step error: %v", err)
	}

	if hook.beforeCalls != 1 || hook.afterCalls != 1 {
		t.Fatalf("expected 1 before and 1 after call, got before=%d, after=%d", hook.beforeCalls, hook.afterCalls)
	}
}
