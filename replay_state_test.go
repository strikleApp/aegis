package aegis

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

type CrashReplayState struct {
	Turns    int      `json:"turns"`
	History  []string `json:"history"`
	Approved bool     `json:"approved"`
}

func TestReplay_StateMutationRestoredAfterCrash(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	runID := "crash-replay-run-1"

	var plannerCalls int32
	var criticCalls int32
	simulateCrash := true

	makeGraph := func() *StateGraph[CrashReplayState] {
		g := NewStateGraph[CrashReplayState]("planner", 10)

		g.AddNode("planner", func(rt *Runtime[CrashReplayState]) (string, error) {
			_, err := Step(rt, "plan_step", func(ctx context.Context, s *CrashReplayState) (string, error) {
				atomic.AddInt32(&plannerCalls, 1)
				s.Turns++
				s.History = append(s.History, fmt.Sprintf("plan_turn_%d", s.Turns))
				return fmt.Sprintf("plan_%d", s.Turns), nil
			})
			if err != nil {
				return "", err
			}
			return "critic", nil
		})

		g.AddNode("critic", func(rt *Runtime[CrashReplayState]) (string, error) {
			critique, err := Step(rt, "critique_step", func(ctx context.Context, s *CrashReplayState) (string, error) {
				atomic.AddInt32(&criticCalls, 1)

				// Simulate unexpected worker crash during turn 2 before critique passes
				if simulateCrash && s.Turns == 2 {
					return "", errors.New("simulated OOM pod termination")
				}

				if s.Turns >= 3 {
					s.Approved = true
					return "approve", nil
				}
				return "revise", nil
			})
			if err != nil {
				return "", err
			}

			if critique == "approve" {
				return EndNode, nil
			}
			return "planner", nil
		})

		return g
	}

	// === Phase 1: First worker run. Will crash on Turn 2 ===
	graph1 := makeGraph()
	rt1 := NewRuntime[CrashReplayState](ctx, store, runID, &CrashReplayState{})
	err1 := graph1.Execute(rt1)

	if err1 == nil {
		t.Fatal("expected simulated crash error on phase 1")
	}
	t.Logf("Phase 1 crashed as expected: %v", err1)

	// Verify partial state before crash: Turn 1 was committed, Turn 2 planner committed, critic failed
	recPlan1, hit1, _ := store.GetStep(ctx, runID, "plan_step:1")
	if !hit1 || len(recPlan1.StateSnapshot) == 0 {
		t.Fatalf("expected plan_step:1 to have recorded a state snapshot")
	}

	// === Phase 2: Fresh worker pod resumes execution with empty in-memory state ===
	simulateCrash = false // Fix issue so pod can complete
	graph2 := makeGraph()

	// CRITICAL: Fresh runtime with zeroed state! It must rebuild state purely from Step snapshots!
	freshZeroState := &CrashReplayState{}
	rt2 := NewRuntime[CrashReplayState](ctx, store, runID, freshZeroState)

	err2 := graph2.Execute(rt2)
	if err2 != nil {
		t.Fatalf("Phase 2 replay failed: %v", err2)
	}

	finalState := rt2.State()
	if !finalState.Approved {
		t.Fatalf("expected workflow to complete with Approved=true")
	}
	if finalState.Turns != 3 {
		t.Fatalf("expected Turns to equal 3 after replay, got: %d", finalState.Turns)
	}

	// Verify all 3 planner steps and 3 critic steps recorded
	expectedHistory := []string{"plan_turn_1", "plan_turn_2", "plan_turn_3"}
	if len(finalState.History) != 3 {
		t.Fatalf("expected history length 3, got %d: %v", len(finalState.History), finalState.History)
	}
	for i, h := range expectedHistory {
		if finalState.History[i] != h {
			t.Errorf("history[%d]: expected %q, got %q", i, h, finalState.History[i])
		}
	}

	// Planner turn 1 should NOT have been re-executed on the second pod!
	// Total calls to live planner fn: 1 (phase 1 turn 1) + 1 (phase 1 turn 2) + 0 (phase 2 turn 1 replay) + 0 (phase 2 turn 2 replay) + 1 (phase 2 turn 3 live) = 3
	if plannerCalls != 3 {
		t.Fatalf("expected exactly 3 live planner calls across crash and recovery, got %d", plannerCalls)
	}
}

func TestReplay_HumanInTheLoop_SignalSuspension(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	runID := "hitl-run-test"

	type OrderApproval struct {
		ApprovedBy string `json:"approved_by"`
		Allowed    bool   `json:"allowed"`
	}

	type TestOrderState struct {
		Amount float64
		Status string
	}

	graph := NewStateGraph[TestOrderState]("prepare", 5)

	graph.AddNode("prepare", func(rt *Runtime[TestOrderState]) (string, error) {
		_, err := Step(rt, "prep", func(ctx context.Context, s *TestOrderState) (string, error) {
			s.Status = "awaiting_review"
			return "ready", nil
		})
		if err != nil {
			return "", err
		}
		return "approval_gate", nil
	})

	graph.AddNode("approval_gate", func(rt *Runtime[TestOrderState]) (string, error) {
		sig, err := WaitForSignal[TestOrderState, OrderApproval](rt, "manager_signoff")
		if err != nil {
			return "", err // Returns ErrWorkflowSuspended
		}

		if sig.Allowed {
			rt.State().Status = "completed"
		} else {
			rt.State().Status = "rejected"
		}
		return EndNode, nil
	})

	// Run 1: Workflow runs and suspends
	rt1 := NewRuntime[TestOrderState](ctx, store, runID, &TestOrderState{Amount: 5000})
	err1 := graph.Execute(rt1)
	if !errors.Is(err1, ErrWorkflowSuspended) {
		t.Fatalf("expected ErrWorkflowSuspended, got %v", err1)
	}
	if rt1.State().Status != "awaiting_review" {
		t.Fatalf("expected state status to be awaiting_review, got %s", rt1.State().Status)
	}

	// External event: Human manager approves order via webhook
	sigPayload := OrderApproval{ApprovedBy: "alice@company.com", Allowed: true}
	errSig := ProvideSignal(ctx, store, runID, "manager_signoff", sigPayload)
	if errSig != nil {
		t.Fatalf("failed to provide signal: %v", errSig)
	}

	// Run 2: Workflow resumes with fresh runtime
	rt2 := NewRuntime[TestOrderState](ctx, store, runID, &TestOrderState{})
	err2 := graph.Execute(rt2)
	if err2 != nil {
		t.Fatalf("failed to resume workflow: %v", err2)
	}

	if rt2.State().Status != "completed" {
		t.Fatalf("expected status completed, got %s", rt2.State().Status)
	}
}

func TestMapParallelDurably(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	runID := "map-parallel-durable-run"

	rt := NewRuntime[CrashReplayState](ctx, store, runID, &CrashReplayState{})
	urls := []string{"https://site-a.com", "https://site-b.com", "https://site-c.com", "https://site-d.com"}

	var callCount int32
	results1, err := MapParallelDurably(rt, "scrape", 2, urls, func(ctx context.Context, url string) (string, error) {
		atomic.AddInt32(&callCount, 1)
		return "content_from_" + url, nil
	})
	if err != nil {
		t.Fatalf("unexpected error in MapParallelDurably: %v", err)
	}
	if len(results1) != 4 || callCount != 4 {
		t.Fatalf("expected 4 results and 4 live calls, got %d results and %d calls", len(results1), callCount)
	}

	// Replay: A second runtime attempts same operation
	rt2 := NewRuntime[CrashReplayState](ctx, store, runID, &CrashReplayState{})
	results2, err := MapParallelDurably(rt2, "scrape", 2, urls, func(ctx context.Context, url string) (string, error) {
		atomic.AddInt32(&callCount, 1) // Should NOT be called
		return "fresh_content", nil
	})
	if err != nil {
		t.Fatalf("unexpected error on replay: %v", err)
	}
	if len(results2) != 4 {
		t.Fatalf("expected 4 results on replay")
	}
	if callCount != 4 {
		t.Fatalf("expected callCount to remain 4 due to durable checkpoint hits, got %d", callCount)
	}
	if results2[0] != "content_from_https://site-a.com" {
		t.Fatalf("expected cached content, got %s", results2[0])
	}
}

func TestReplay_CyclicSignalVersioning(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	runID := "cyclic-signal-run"

	type ReviewDecision struct {
		Action string `json:"action"` // "revise" or "accept"
	}

	type CyclicReviewState struct {
		Attempts int
		Approved bool
	}

	graph := NewStateGraph[CyclicReviewState]("author", 10)

	graph.AddNode("author", func(rt *Runtime[CyclicReviewState]) (string, error) {
		_, err := Step(rt, "write_draft", func(ctx context.Context, s *CyclicReviewState) (string, error) {
			s.Attempts++
			return fmt.Sprintf("draft_v%d", s.Attempts), nil
		})
		if err != nil {
			return "", err
		}
		return "await_human", nil
	})

	graph.AddNode("await_human", func(rt *Runtime[CyclicReviewState]) (string, error) {
		decision, err := WaitForSignal[CyclicReviewState, ReviewDecision](rt, "review_decision")
		if err != nil {
			return "", err // Returns ErrWorkflowSuspended
		}

		if decision.Action == "accept" {
			rt.State().Approved = true
			return EndNode, nil
		}
		return "author", nil // Loop back to author
	})

	// Turn 1: Graph runs and suspends on signal:review_decision:1
	rt1 := NewRuntime[CyclicReviewState](ctx, store, runID, &CyclicReviewState{})
	err1 := graph.Execute(rt1)
	if !errors.Is(err1, ErrWorkflowSuspended) {
		t.Fatalf("expected ErrWorkflowSuspended on Turn 1, got %v", err1)
	}

	// Human provides signal for iteration 1: "revise"
	errSig1 := ProvideSignalVersion(ctx, store, runID, "review_decision", 1, ReviewDecision{Action: "revise"})
	if errSig1 != nil {
		t.Fatalf("failed to provide signal 1: %v", errSig1)
	}

	// Turn 2: Graph resumes, reads signal 1 ("revise"), loops back to "author" (Attempts=2), and hits signal:review_decision:2!
	// It must SUSPEND because signal 2 has NOT been provided yet!
	rt2 := NewRuntime[CyclicReviewState](ctx, store, runID, &CyclicReviewState{})
	err2 := graph.Execute(rt2)
	if !errors.Is(err2, ErrWorkflowSuspended) {
		t.Fatalf("expected ErrWorkflowSuspended on Turn 2 awaiting second signal, got %v", err2)
	}
	if rt2.State().Attempts != 2 {
		t.Fatalf("expected Attempts=2 on turn 2, got %d", rt2.State().Attempts)
	}

	// Human provides signal for iteration 2: "accept"
	errSig2 := ProvideSignalVersion(ctx, store, runID, "review_decision", 2, ReviewDecision{Action: "accept"})
	if errSig2 != nil {
		t.Fatalf("failed to provide signal 2: %v", errSig2)
	}

	// Turn 3: Graph resumes, reads signal 2 ("accept"), marks Approved=true and completes!
	rt3 := NewRuntime[CyclicReviewState](ctx, store, runID, &CyclicReviewState{})
	err3 := graph.Execute(rt3)
	if err3 != nil {
		t.Fatalf("failed to complete on turn 3: %v", err3)
	}
	if !rt3.State().Approved || rt3.State().Attempts != 2 {
		t.Fatalf("expected Approved=true and Attempts=2, got approved=%v, attempts=%d", rt3.State().Approved, rt3.State().Attempts)
	}
}
