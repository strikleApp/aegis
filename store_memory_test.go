package aegis

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStore_Lease(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	runID := "run-100"

	// Worker 1 acquires lease
	ok, err := store.AcquireLease(ctx, runID, "worker-1", 50*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("expected worker-1 to acquire lease, got ok=%v, err=%v", ok, err)
	}

	// Worker 2 should be rejected
	ok, err = store.AcquireLease(ctx, runID, "worker-2", 50*time.Millisecond)
	if err != nil || ok {
		t.Fatalf("expected worker-2 to be rejected, got ok=%v, err=%v", ok, err)
	}

	// Same worker renewing lease succeeds
	ok, err = store.AcquireLease(ctx, runID, "worker-1", 50*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("expected worker-1 renewal to succeed, got ok=%v, err=%v", ok, err)
	}

	// Release lease
	err = store.ReleaseLease(ctx, runID, "worker-1")
	if err != nil {
		t.Fatalf("unexpected error releasing lease: %v", err)
	}

	// Now worker 2 can acquire lease
	ok, err = store.AcquireLease(ctx, runID, "worker-2", 50*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("expected worker-2 to acquire lease after release, got ok=%v, err=%v", ok, err)
	}
}

func TestMemoryStore_LeaseExpiry(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	runID := "run-200"

	// Worker 1 acquires short lease
	ok, err := store.AcquireLease(ctx, runID, "worker-1", 20*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("expected worker-1 to acquire lease, got ok=%v", ok)
	}

	// Wait for expiration
	time.Sleep(30 * time.Millisecond)

	// Worker 2 acquires expired lease
	ok, err = store.AcquireLease(ctx, runID, "worker-2", 50*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("expected worker-2 to take over expired lease, got ok=%v", ok)
	}
}

func TestMemoryStore_Steps(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	runID := "run-300"

	// Verify cache miss
	_, hit, err := store.GetStep(ctx, runID, "step:1")
	if err != nil || hit {
		t.Fatalf("expected cache miss, got hit=%v, err=%v", hit, err)
	}

	// Save step
	rec := EventRecord{
		RunID:         runID,
		StepID:        "step:1",
		OutputPayload: []byte(`{"message":"hello"}`),
		Status:        "completed",
		CreatedAt:     time.Now().UTC(),
	}
	if err := store.SaveStep(ctx, runID, rec); err != nil {
		t.Fatalf("failed to save step: %v", err)
	}

	// Verify cache hit
	cached, hit, err := store.GetStep(ctx, runID, "step:1")
	if err != nil || !hit {
		t.Fatalf("expected cache hit, got hit=%v, err=%v", hit, err)
	}
	if string(cached.OutputPayload) != `{"message":"hello"}` {
		t.Fatalf("unexpected payload: %s", string(cached.OutputPayload))
	}
}
