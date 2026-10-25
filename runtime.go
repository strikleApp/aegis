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

// WithStepHooks registers observability/tracing hooks around every step execution.
func WithStepHooks[T any](hooks ...StepHook) RuntimeOption[T] {
	return func(rt *Runtime[T]) {
		rt.hooks = append(rt.hooks, hooks...)
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
	hooks      []StepHook

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

// LeaseTTL returns the configured lease TTL.
func (rt *Runtime[T]) LeaseTTL() time.Duration {
	return rt.leaseTTL
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

// Step executes a named logic unit with deterministic memoization AND state snapshot restoration.
// In cyclic graphs (e.g. Planner -> Tool -> Critic -> Planner), successive visits to the same step
// automatically version to planner:1, planner:2, ensuring correct replay without cache collisions.
// When replaying from a cache hit, any mutations to state T are accurately restored from StateSnapshot.
func Step[T any, R any](rt *Runtime[T], baseStepID string, fn func(ctx context.Context, state *T) (R, error)) (R, error) {
	return executeVersionedStep(rt, rt.nextVersionedStepID(baseStepID), true, fn)
}

func executeVersionedStep[T any, R any](rt *Runtime[T], versionedID string, snapshotState bool, fn func(ctx context.Context, state *T) (R, error)) (R, error) {
	var zero R
	if err := rt.ctx.Err(); err != nil {
		return zero, err
	}

	start := time.Now()
	for _, h := range rt.hooks {
		h.BeforeStep(rt.ctx, rt.runID, versionedID)
	}
	var stepErr error
	defer func() {
		duration := time.Since(start)
		for _, h := range rt.hooks {
			h.AfterStep(rt.ctx, rt.runID, versionedID, duration, stepErr)
		}
	}()

	// 1. Check Store for existing memoized step result (Replay Path)
	if record, hit, err := rt.store.GetStep(rt.ctx, rt.runID, versionedID); err == nil && hit {
		if record.Status == "completed" {
			var cachedResult R
			if err := json.Unmarshal(record.OutputPayload, &cachedResult); err != nil {
				stepErr = fmt.Errorf("%w: unmarshal output %q: %v", ErrInvalidStepResult, versionedID, err)
				return zero, stepErr
			}
			// Restore mutated State snapshot so replay keeps state in sync!
			if snapshotState && len(record.StateSnapshot) > 0 {
				rt.mu.Lock()
				_ = json.Unmarshal(record.StateSnapshot, rt.state)
				rt.mu.Unlock()
			}
			if record.CostUSD > 0 {
				if err := rt.TrackCost(record.CostUSD); err != nil {
					stepErr = err
					return zero, err
				}
			}
			return cachedResult, nil
		}
	}

	// 2. Execute live step logic
	costBefore := rt.CurrentCost()
	result, err := fn(rt.ctx, rt.State())
	if err != nil {
		stepErr = fmt.Errorf("%w: step %q: %v", ErrStepFailed, versionedID, err)
		return zero, stepErr
	}
	stepCost := rt.CurrentCost() - costBefore

	// 3. Serialize both step output AND current state snapshot
	payload, err := json.Marshal(result)
	if err != nil {
		stepErr = fmt.Errorf("%w: marshal output %q: %v", ErrInvalidStepResult, versionedID, err)
		return zero, stepErr
	}

	var stateBytes []byte
	if snapshotState {
		rt.mu.RLock()
		stateBytes, _ = json.Marshal(rt.state)
		rt.mu.RUnlock()
	}

	record := EventRecord{
		RunID:         rt.runID,
		StepID:        versionedID,
		TenantID:      rt.tenantID,
		OutputPayload: payload,
		StateSnapshot: stateBytes,
		CostUSD:       stepCost,
		Status:        "completed",
		CreatedAt:     time.Now().UTC(),
	}

	if err := rt.store.SaveStep(rt.ctx, rt.runID, record); err != nil {
		stepErr = fmt.Errorf("aegis: failed to checkpoint step %q: %w", versionedID, err)
		return zero, stepErr
	}

	return result, nil
}

// WaitForSignal pauses workflow execution (returning ErrWorkflowSuspended) until ProvideSignal is called.
// It tracks invocation count so cyclic agent loops asking for approvals on multiple iterations
// never read stale signals from previous iterations.
func WaitForSignal[T any, S any](rt *Runtime[T], signalName string) (S, error) {
	var zero S
	versionedKey := rt.nextVersionedStepID("signal:" + signalName)

	// Check versioned key first (e.g. signal:approval:1, signal:approval:2)
	record, hit, err := rt.store.GetStep(rt.ctx, rt.runID, versionedKey)
	if err != nil {
		return zero, err
	}

	// Fallback for unversioned signals on first iteration
	if !hit && versionedKey == "signal:"+signalName+":1" {
		record, hit, err = rt.store.GetStep(rt.ctx, rt.runID, "signal:"+signalName)
		if err != nil {
			return zero, err
		}
	}

	if !hit || record.Status != "completed" {
		return zero, fmt.Errorf("%w: awaiting signal %q", ErrWorkflowSuspended, signalName)
	}
	var sig S
	if err := json.Unmarshal(record.OutputPayload, &sig); err != nil {
		return zero, fmt.Errorf("%w: invalid signal payload %q: %v", ErrInvalidStepResult, signalName, err)
	}
	return sig, nil
}

// ProvideSignal injects an external payload (from a human UI or webhook) into the Store so a suspended workflow can resume.
func ProvideSignal[S any](ctx context.Context, store Store, runID, signalName string, payload S) error {
	return ProvideSignalVersion(ctx, store, runID, signalName, 1, payload)
}

// ProvideSignalVersion injects an external payload for a specific cycle iteration of a signal.
func ProvideSignalVersion[S any](ctx context.Context, store Store, runID, signalName string, version int, payload S) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if version <= 0 {
		version = 1
	}

	rec := EventRecord{
		RunID:         runID,
		StepID:        fmt.Sprintf("signal:%s:%d", signalName, version),
		OutputPayload: raw,
		Status:        "completed",
		CreatedAt:     time.Now().UTC(),
	}
	if err := store.SaveStep(ctx, runID, rec); err != nil {
		return err
	}

	// Keep unversioned key in sync for version 1 backwards compatibility
	if version == 1 {
		rec.StepID = "signal:" + signalName
		_ = store.SaveStep(ctx, runID, rec)
	}
	return nil
}
