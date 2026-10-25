package aegis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// RuntimeOption configures runtime parameters.
type RuntimeOption[T any] func(*Runtime[T])

// WithWorkerID assigns an explicit worker instance ID for lease fencing.
func WithWorkerID[T any](workerID string) RuntimeOption[T] {
	return func(rt *Runtime[T]) {
		if workerID != "" {
			rt.workerID = workerID
		}
	}
}

// WithTenantID sets tenant identification for multi-tenant isolation.
func WithTenantID[T any](tenantID string) RuntimeOption[T] {
	return func(rt *Runtime[T]) {
		rt.tenantID = tenantID
	}
}

// WithLeaseTTL overrides default lease duration for fencing.
func WithLeaseTTL[T any](ttl time.Duration) RuntimeOption[T] {
	return func(rt *Runtime[T]) {
		if ttl > 0 {
			rt.leaseTTL = ttl
		}
	}
}

// WithCostBudget sets a monetary cost ceiling. Exceeding it aborts graph execution.
func WithCostBudget[T any](maxCostUSD float64) RuntimeOption[T] {
	return func(rt *Runtime[T]) {
		rt.maxCostUSD = maxCostUSD
	}
}

// Runtime maintains execution context, state, step versioning, and lease fencing.
type Runtime[T any] struct {
	ctx        context.Context
	cancel     context.CancelFunc
	store      Store
	runID      string
	workerID   string
	tenantID   string
	leaseTTL   time.Duration
	state      *T
	maxCostUSD float64

	mu             sync.RWMutex
	currentCostUSD float64
	stepCounts     map[string]int
	leased         bool
}

// NewRuntime initializes an active execution runtime for a workflow run.
func NewRuntime[T any](ctx context.Context, store Store, runID string, initialState *T, opts ...RuntimeOption[T]) *Runtime[T] {
	runCtx, cancel := context.WithCancel(ctx)
	rt := &Runtime[T]{
		ctx:        runCtx,
		cancel:     cancel,
		store:      store,
		runID:      runID,
		workerID:   fmt.Sprintf("worker-%d", time.Now().UnixNano()),
		leaseTTL:   30 * time.Second,
		state:      initialState,
		stepCounts: make(map[string]int),
	}

	for _, opt := range opts {
		opt(rt)
	}

	return rt
}

// Context returns the execution context.
func (rt *Runtime[T]) Context() context.Context {
	return rt.ctx
}

// RunID returns the workflow execution ID.
func (rt *Runtime[T]) RunID() string {
	return rt.runID
}

// TenantID returns the assigned tenant ID.
func (rt *Runtime[T]) TenantID() string {
	return rt.tenantID
}

// State returns the current workflow state.
func (rt *Runtime[T]) State() *T {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.state
}

// SetState updates the workflow state thread-safely.
func (rt *Runtime[T]) SetState(newState *T) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.state = newState
}

// TrackCost records monetary cost against the budget breaker.
func (rt *Runtime[T]) TrackCost(costUSD float64) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.maxCostUSD > 0 && (rt.currentCostUSD+costUSD) > rt.maxCostUSD {
		rt.cancel()
		return fmt.Errorf("%w: current $%.4f + added $%.4f exceeds ceiling $%.4f",
			ErrBudgetExceeded, rt.currentCostUSD, costUSD, rt.maxCostUSD)
	}

	rt.currentCostUSD += costUSD
	return nil
}

// CurrentCost returns the accumulated cost in USD.
func (rt *Runtime[T]) CurrentCost() float64 {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.currentCostUSD
}

// AcquireLease secures the distributed fencing lock before workflow execution.
func (rt *Runtime[T]) AcquireLease() (bool, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	ok, err := rt.store.AcquireLease(rt.ctx, rt.runID, rt.workerID, rt.leaseTTL)
	if err != nil {
		return false, err
	}
	rt.leased = ok
	return ok, nil
}

// ReleaseLease unlocks the distributed lease.
func (rt *Runtime[T]) ReleaseLease() error {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	if !rt.leased {
		return nil
	}
	err := rt.store.ReleaseLease(context.Background(), rt.runID, rt.workerID)
	rt.leased = false
	return err
}

// nextVersionedStepID generates a deterministic monotonic step identifier (e.g., "planner:1", "planner:2").
func (rt *Runtime[T]) nextVersionedStepID(baseStepID string) string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.stepCounts[baseStepID]++
	return fmt.Sprintf("%s:%d", baseStepID, rt.stepCounts[baseStepID])
}

// Step executes a named logic unit with deterministic memoization and versioned checkpointing.
// In cyclic graphs (e.g. Planner -> Tool -> Critic -> Planner), successive visits to the same step
// automatically version to planner:1, planner:2, ensuring correct replay without cache collisions.
func Step[T any, R any](rt *Runtime[T], baseStepID string, fn func(ctx context.Context, state *T) (R, error)) (R, error) {
	var zero R
	if err := rt.ctx.Err(); err != nil {
		return zero, err
	}

	versionedID := rt.nextVersionedStepID(baseStepID)

	// 1. Check hot cache for existing memoized step result
	if record, hit, err := rt.store.GetStep(rt.ctx, rt.runID, versionedID); err == nil && hit {
		if record.Status == "completed" {
			var cachedResult R
			if err := json.Unmarshal(record.OutputPayload, &cachedResult); err == nil {
				return cachedResult, nil
			}
		}
	}

	// 2. Execute fresh step logic
	result, err := fn(rt.ctx, rt.State())
	if err != nil {
		return zero, fmt.Errorf("%w: step %q: %v", ErrStepFailed, versionedID, err)
	}

	// 3. Serialize and checkpoint result
	payload, err := json.Marshal(result)
	if err != nil {
		return zero, fmt.Errorf("%w: failed to marshal step result %q: %v", ErrInvalidStepResult, versionedID, err)
	}

	record := EventRecord{
		RunID:         rt.runID,
		StepID:        versionedID,
		TenantID:      rt.tenantID,
		OutputPayload: payload,
		Status:        "completed",
		CreatedAt:     time.Now().UTC(),
	}

	if err := rt.store.SaveStep(rt.ctx, rt.runID, record); err != nil {
		return zero, fmt.Errorf("aegis: failed to checkpoint step %q: %w", versionedID, err)
	}

	return result, nil
}
