package aegis

import (
	"context"
	"sync"
	"time"
)

type memoryLease struct {
	workerID  string
	expiresAt time.Time
}

// MemoryStore provides an in-memory, thread-safe implementation of Store.
// It has zero external dependencies and is ideal for unit tests, local CLI runs,
// and embedded standalone agents.
type MemoryStore struct {
	mu     sync.RWMutex
	leases map[string]memoryLease
	steps  map[string]map[string]EventRecord
}

// NewMemoryStore initializes an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		leases: make(map[string]memoryLease),
		steps:  make(map[string]map[string]EventRecord),
	}
}

// AcquireLease attempts to acquire an exclusive execution lease for a workflow run.
func (m *MemoryStore) AcquireLease(ctx context.Context, runID, workerID string, leaseTTL time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if existing, exists := m.leases[runID]; exists {
		if now.Before(existing.expiresAt) && existing.workerID != workerID {
			return false, nil
		}
	}

	m.leases[runID] = memoryLease{
		workerID:  workerID,
		expiresAt: now.Add(leaseTTL),
	}
	return true, nil
}

// ReleaseLease atomically releases the lease if owned by workerID.
func (m *MemoryStore) ReleaseLease(ctx context.Context, runID, workerID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, exists := m.leases[runID]; exists {
		if existing.workerID == workerID {
			delete(m.leases, runID)
		}
	}
	return nil
}

// GetStep fetches a memoized checkpoint for a runID and versioned stepID.
func (m *MemoryStore) GetStep(ctx context.Context, runID, versionedStepID string) (EventRecord, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	runSteps, ok := m.steps[runID]
	if !ok {
		return EventRecord{}, false, nil
	}

	record, found := runSteps[versionedStepID]
	if !found {
		return EventRecord{}, false, nil
	}

	return record, true, nil
}

// SaveStep persists a step record in memory.
func (m *MemoryStore) SaveStep(ctx context.Context, runID string, record EventRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.steps[runID]; !ok {
		m.steps[runID] = make(map[string]EventRecord)
	}
	m.steps[runID][record.StepID] = record
	return nil
}
