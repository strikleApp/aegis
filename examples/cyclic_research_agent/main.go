package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aashish/aegis"
)

type ResearchState struct {
	Query     string
	Findings  []string
	Iteration int
	Finished  bool
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
				s.Finished = true
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return "", err
		}

		if approved {
			fmt.Println("✅ Reviewer approved findings.")
			return aegis.EndNode, nil
		}
		fmt.Println("🔄 Reviewer requested additional deep-dive cycle...")
		return "researcher", nil // Cyclic loop
	})

	state := &ResearchState{Query: "Distributed Consensus in Go"}
	rt := aegis.NewRuntime[ResearchState](ctx, store, "research-run-1", state)

	if err := graph.Execute(rt); err != nil {
		log.Fatalf("research agent failed: %v", err)
	}

	fmt.Printf("\nCompleted in %d iterations with %d total findings:\n", rt.State().Iteration, len(rt.State().Findings))
	for i, f := range rt.State().Findings {
		fmt.Printf("  %d. %s\n", i+1, f)
	}
}
