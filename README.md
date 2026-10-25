# aegis ⚡
> Durable, event-driven agentic runtime in Go designed to outperform static state machines through deterministic event sourcing, cyclic loop versioning, state snapshot replay, and distributed backpressure.

[![Go Reference](https://pkg.go.dev/badge/github.com/strikleApp/aegis.svg)](https://pkg.go.dev/github.com/strikleApp/aegis)
[![Go Report Card](https://goreportcard.com/badge/github.com/strikleApp/aegis)](https://goreportcard.com/report/github.com/strikleApp/aegis)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## 1. System Topology & Data Flow

`aegis` provides a pure Go engine with pluggable hot storage (`MemoryStore` or `RedisStore`), sub-millisecond step memoization, and automatic crash recovery:

```text
                        [ Client / Webhook Trigger ]
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     Aegis Runtime (Pure Go SDK)                         │
│                                                                         │
│  ┌──────────────────────┐    ┌─────────────────────┐    ┌────────────┐  │
│  │ Cyclic StateGraph[T] │───►│ Versioned Step()    │───►│MapParallel │  │
│  │ (Loop Guardrails)    │    │ (step_id:1, :2...)  │    │ (Durably)  │  │
│  └──────────────────────┘    └──────────┬──────────┘    └────────────┘  │
└─────────────────────────────────────────┼───────────────────────────────┘
                                          │
                          Pluggable `Store` Interface
                                          │
             ┌────────────────────────────┴────────────────────────────┐
             ▼                                                         ▼
┌─────────────────────────┐                               ┌─────────────────────────┐
│  1. MemoryStore         │                               │  2. RedisStore          │
│  (Default / Unit Tests) │                               │  (Distributed Pods)     │
│  • 0 external deps      │                               │  • SET NX Lease Lock    │
│  • Sub-microsecond      │                               │  • HSET Step Checkpoints│
│  • Thread-safe RWMutex  │                               │  • Gzip Payloads > 4KB  │
└─────────────────────────┘                               └─────────────────────────┘
```

---

## 2. Core Architectural Primitives

### A. Monotonic Step Versioning & State Snapshot Replay
In naive frameworks, steps are keyed solely by a static `stepID`, causing cyclic loops to read stale cached outputs. Furthermore, caching only return values loses mutations to the shared workflow state during crash recovery.

* **Aegis Monotonic Counter:** The runtime tracks an invocation counter per step (`map[string]int`), auto-keying checkpoints as `planner:1`, `critic:1`, `planner:2`.
* **State Snapshot Restoration:** Every `EventRecord` saves both the step return payload (`OutputPayload`) **and** a serialized snapshot of `rt.State()` (`StateSnapshot`). If a pod terminates mid-workflow, the next worker restores mutated state seamlessly from the committed snapshots.

### B. Human-in-the-Loop (HITL) Without Memory Leakage
Pause workflows for human approval or external webhooks without keeping idle goroutines alive:
```go
// Node pauses execution and returns aegis.ErrWorkflowSuspended
sig, err := aegis.WaitForSignal[MyState, ApprovalSignal](rt, "manager_approval")

// External API or webhook resumes execution asynchronously
err := aegis.ProvideSignal(ctx, store, runID, "manager_approval", payload)
```

### C. Deterministic Parallel Fan-Out (`MapParallelDurably`)
Calling naive concurrent steps leads to non-deterministic `:1, :2` order races. `MapParallelDurably` pre-allocates a batch version on the caller goroutine and keys each item deterministically by slice index (`<baseStepID>:<batchVersion>:<itemIndex>`).

### D. Active Lease Heartbeat
`Execute()` acquires a distributed lease (`SET aegis:lock:{runID} {workerID} NX PX {ttl}`). A background heartbeat goroutine renews the lease every `leaseTTL / 3`, preventing split-brain execution during slow LLM inference steps.

---

## 3. Installation

```bash
go get github.com/strikleApp/aegis
```

Requires Go 1.23 or newer. Zero external C-bindings.

---

## 4. Quickstart: Cyclic Agent with State Recovery

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/strikleApp/aegis"
)

type ResearchState struct {
	Query     string
	Findings  []string
	Iteration int
	Approved  bool
}

func main() {
	ctx := context.Background()
	store := aegis.NewMemoryStore()

	graph := aegis.NewStateGraph[ResearchState]("researcher", 10)

	graph.AddNode("researcher", func(rt *aegis.Runtime[ResearchState]) (string, error) {
		finding, err := aegis.Step(rt, "search", func(ctx context.Context, s *ResearchState) (string, error) {
			s.Iteration++
			discovery := fmt.Sprintf("finding_source_%d_for_%s", s.Iteration, s.Query)
			s.Findings = append(s.Findings, discovery)
			return discovery, nil
		})
		if err != nil {
			return "", err
		}
		fmt.Printf("🔍 Turn %d: Discovered: %s\n", rt.State().Iteration, finding)
		return "reviewer", nil
	})

	graph.AddNode("reviewer", func(rt *aegis.Runtime[ResearchState]) (string, error) {
		approved, err := aegis.Step(rt, "review", func(ctx context.Context, s *ResearchState) (bool, error) {
			if s.Iteration >= 3 {
				s.Approved = true
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return "", err
		}

		if approved {
			return aegis.EndNode, nil
		}
		return "researcher", nil // Cyclic transition
	})

	state := &ResearchState{Query: "Distributed Consensus in Go"}
	rt := aegis.NewRuntime[ResearchState](ctx, store, "research-run-1", state)

	if err := graph.Execute(rt); err != nil {
		log.Fatalf("agent failed: %v", err)
	}

	fmt.Printf("✅ Completed in %d iterations with %d findings.\n", rt.State().Iteration, len(rt.State().Findings))
}
```

---

## 5. Repository Layout

```text
aegis/
├── go.mod                  # Clean Go 1.23 module (minimal dependencies)
├── README.md               # Architecture, API guide, and quickstart
├── types.go                # Store interface, EventRecord, and sentinel errors
├── runtime.go              # Runtime[T], Step() with StateSnapshot, WaitForSignal()
├── router.go               # StateGraph[T], lease heartbeat, MapParallelDurably()
├── middleware.go           # Hooks: logging, retries with exponential backoff
├── store_memory.go         # Zero-dep in-memory Store
├── store_redis.go          # Production Redis Store (Gzip > 4KB + Lua Fencing)
├── runtime_test.go         # Step memoization and cost budget breaker tests
├── router_test.go          # StateGraph cyclic routing & parallel fan-out tests
├── replay_state_test.go    # Pod crash simulation and state restoration tests
└── examples/
    ├── cyclic_research_agent/main.go  # Pure CLI cyclic research agent
    └── hitl_http_worker/main.go       # Human-in-the-Loop HTTP worker example
```

---

## 6. Running Tests

```bash
go test -v -race ./...
```
