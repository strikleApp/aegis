package aegis

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrRunLocked is returned when a workflow run is actively leased by another worker.
	ErrRunLocked = errors.New("aegis: workflow run is currently locked by another worker")

	// ErrMaxCyclesReached is returned when a StateGraph exceeds its configured cycle budget.
	ErrMaxCyclesReached = errors.New("aegis: maximum graph cycle transitions exceeded")

	// ErrBudgetExceeded is returned when execution exceeds the configured monetary/token cost limit.
	ErrBudgetExceeded = errors.New("aegis: cost or token budget exceeded")

	// ErrStepFailed is returned when an individual step execution fails.
	ErrStepFailed = errors.New("aegis: step execution failed")

	// ErrInvalidStepResult is returned when a cached step payload cannot be deserialized.
	ErrInvalidStepResult = errors.New("aegis: invalid step output payload")

	// ErrWorkflowSuspended is returned when execution pauses waiting for an external human or webhook signal.
	ErrWorkflowSuspended = errors.New("aegis: workflow suspended awaiting external signal")
)

// EventRecord represents an immutable, durable checkpoint for a single versioned step.
// It snapshots both the step return value (OutputPayload) and the mutated workflow state (StateSnapshot).
type EventRecord struct {
	RunID         string    `json:"run_id"`
	StepID        string    `json:"step_id"` // e.g. "planner:1", "map_scrape:1:3"
	TenantID      string    `json:"tenant_id,omitempty"`
	OutputPayload []byte    `json:"output_payload"` // Serialized return value R of Step()
	StateSnapshot []byte    `json:"state_snapshot"` // Serialized state T after Step() mutated it
	CostUSD       float64   `json:"cost_usd,omitempty"`
	Status        string    `json:"status"` // "completed", "suspended"
	Compressed    bool      `json:"compressed,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// Store abstracts durable checkpointing and distributed fencing leases.
// It enables pure database-agnostic runtime execution with zero-dependency memory stores,
// production Redis clusters, or disk-backed embedded databases.
type Store interface {
	// AcquireLease prevents split-brain execution across multiple worker pods.
	AcquireLease(ctx context.Context, runID, workerID string, leaseTTL time.Duration) (bool, error)

	// ReleaseLease atomically releases the distributed lease if owned by workerID.
	ReleaseLease(ctx context.Context, runID, workerID string) error

	// GetStep retrieves a memoized step checkpoint by runID and versioned stepID.
	GetStep(ctx context.Context, runID, versionedStepID string) (EventRecord, bool, error)

	// SaveStep persists a step checkpoint into the durable backing store.
	SaveStep(ctx context.Context, runID string, record EventRecord) error
}
