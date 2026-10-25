package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aashish/aegis"
)

type WorkflowInput struct {
	RunID string `json:"run_id"`
	Query string `json:"query"`
}

type WorkflowOutput struct {
	RunID   string   `json:"run_id"`
	Cycles  int      `json:"cycles"`
	Result  string   `json:"result"`
	History []string `json:"history"`
}

type AgentState struct {
	Query    string
	Answer   string
	Cycles   int
	Approved bool
	Log      []string
}

func main() {
	var (
		port        = flag.Int("port", 8080, "HTTP server listening port")
		redisAddr   = flag.String("redis", "", "Redis server address (defaults to REDIS_ADDR env)")
		healthCheck = flag.Bool("healthcheck", false, "Run healthcheck probe and exit")
	)
	flag.Parse()

	// 1. Handle Docker / Kubernetes healthcheck probe
	if *healthCheck {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", *port))
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		os.Exit(0)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// 2. Resolve backing store: Redis if configured, otherwise MemoryStore
	addr := *redisAddr
	if addr == "" {
		addr = os.Getenv("REDIS_ADDR")
	}

	var store aegis.Store
	if addr != "" {
		logger.Info("connecting to Redis store", "addr", addr)
		store = aegis.NewRedisStore(addr, 48*time.Hour)
	} else {
		logger.Info("no Redis address provided; running embedded MemoryStore")
		store = aegis.NewMemoryStore()
	}

	// 3. Define sample cyclic agent graph
	graph := aegis.NewStateGraph[AgentState]("planner", 15)

	graph.AddNode("planner", func(rt *aegis.Runtime[AgentState]) (string, error) {
		stepOut, err := aegis.Step(rt, "plan", func(ctx context.Context, s *AgentState) (string, error) {
			s.Cycles++
			s.Log = append(s.Log, fmt.Sprintf("plan_v%d", s.Cycles))
			return fmt.Sprintf("plan_for_%s_cycle_%d", s.Query, s.Cycles), nil
		})
		if err != nil {
			return "", err
		}
		slog.Info("executed planner step", "step_out", stepOut, "run_id", rt.RunID())
		return "critic", nil
	})

	graph.AddNode("critic", func(rt *aegis.Runtime[AgentState]) (string, error) {
		passed, err := aegis.Step(rt, "evaluate", func(ctx context.Context, s *AgentState) (bool, error) {
			if s.Cycles >= 2 {
				s.Approved = true
				s.Answer = fmt.Sprintf("Refined synthesis for %q after %d iterations", s.Query, s.Cycles)
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return "", err
		}

		if passed {
			return aegis.EndNode, nil
		}
		return "planner", nil
	})

	// 4. HTTP Router
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy","service":"aegis-worker"}`))
	})

	mux.HandleFunc("POST /api/v1/run", func(w http.ResponseWriter, r *http.Request) {
		var in WorkflowInput
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if in.RunID == "" {
			in.RunID = fmt.Sprintf("run-%d", time.Now().UnixNano())
		}

		initialState := &AgentState{Query: in.Query}
		rt := aegis.NewRuntime[AgentState](r.Context(), store, in.RunID, initialState)

		if err := graph.Execute(rt); err != nil {
			logger.Error("graph execution failed", "run_id", in.RunID, "error", err)
			http.Error(w, fmt.Sprintf("execution failed: %v", err), http.StatusInternalServerError)
			return
		}

		finalState := rt.State()
		resp := WorkflowOutput{
			RunID:   in.RunID,
			Cycles:  finalState.Cycles,
			Result:  finalState.Answer,
			History: finalState.Log,
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// 5. Graceful Shutdown
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		logger.Info("starting aegis worker daemon", "port", *port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server stopped with error", "error", err)
			os.Exit(1)
		}
	}()

	<-stopCh
	logger.Info("shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("forced shutdown error", "error", err)
	}
	logger.Info("worker daemon stopped")
}
