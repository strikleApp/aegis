package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/strikleApp/aegis"
)

type ApprovalSignal struct {
	ApprovedBy string `json:"approved_by"`
	Approved   bool   `json:"approved"`
	Comment    string `json:"comment"`
}

type OrderState struct {
	OrderID    string
	TotalUSD   float64
	ApprovedBy string
	Status     string
}

func main() {
	port := flag.Int("port", 8080, "HTTP server listening port")
	redisAddr := flag.String("redis", "", "Redis address (defaults to REDIS_ADDR env)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	addr := *redisAddr
	if addr == "" {
		addr = os.Getenv("REDIS_ADDR")
	}

	var store aegis.Store
	if addr != "" {
		logger.Info("connecting to Redis store", "addr", addr)
		store = aegis.NewRedisStore(addr, 48*time.Hour)
	} else {
		logger.Info("running with in-memory store")
		store = aegis.NewMemoryStore()
	}

	// Define Human-in-the-Loop Graph
	graph := aegis.NewStateGraph[OrderState]("validate", 10)

	graph.AddNode("validate", func(rt *aegis.Runtime[OrderState]) (string, error) {
		_, err := aegis.Step(rt, "check_inventory", func(ctx context.Context, s *OrderState) (string, error) {
			s.Status = "inventory_reserved"
			return "reserved", nil
		})
		if err != nil {
			return "", err
		}

		if rt.State().TotalUSD > 1000.0 {
			return "await_approval", nil
		}
		return "fulfill", nil
	})

	graph.AddNode("await_approval", func(rt *aegis.Runtime[OrderState]) (string, error) {
		// Suspends without holding memory if signal is not present yet
		sig, err := aegis.WaitForSignal[OrderState, ApprovalSignal](rt, "manager_approval")
		if err != nil {
			return "", err // Returns ErrWorkflowSuspended
		}

		if !sig.Approved {
			rt.State().Status = "rejected"
			return aegis.EndNode, nil
		}

		rt.State().ApprovedBy = sig.ApprovedBy
		return "fulfill", nil
	})

	graph.AddNode("fulfill", func(rt *aegis.Runtime[OrderState]) (string, error) {
		_, err := aegis.Step(rt, "ship", func(ctx context.Context, s *OrderState) (string, error) {
			s.Status = "shipped"
			return "tracking_12345", nil
		})
		if err != nil {
			return "", err
		}
		return aegis.EndNode, nil
	})

	mux := http.NewServeMux()

	// 1. Healthcheck
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})

	// 2. Start or Resume Workflow
	mux.HandleFunc("POST /api/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RunID    string  `json:"run_id"`
			OrderID  string  `json:"order_id"`
			TotalUSD float64 `json:"total_usd"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if req.RunID == "" {
			req.RunID = fmt.Sprintf("order-%d", time.Now().UnixNano())
		}

		initialState := &OrderState{
			OrderID:  req.OrderID,
			TotalUSD: req.TotalUSD,
			Status:   "pending",
		}

		rt := aegis.NewRuntime[OrderState](r.Context(), store, req.RunID, initialState)
		err := graph.Execute(rt)

		if errors.Is(err, aegis.ErrWorkflowSuspended) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"run_id":   req.RunID,
				"status":   "suspended",
				"message":  "Workflow suspended awaiting human approval",
				"order_id": rt.State().OrderID,
			})
			return
		}

		if err != nil {
			http.Error(w, fmt.Sprintf("workflow failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id":      req.RunID,
			"status":      rt.State().Status,
			"approved_by": rt.State().ApprovedBy,
		})
	})

	// 3. Inject Human Approval Signal
	mux.HandleFunc("POST /api/v1/orders/approve", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			RunID      string `json:"run_id"`
			ApprovedBy string `json:"approved_by"`
			Approved   bool   `json:"approved"`
			Comment    string `json:"comment"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid signal payload", http.StatusBadRequest)
			return
		}

		payload := ApprovalSignal{
			ApprovedBy: req.ApprovedBy,
			Approved:   req.Approved,
			Comment:    req.Comment,
		}

		if err := aegis.ProvideSignal(r.Context(), store, req.RunID, "manager_approval", payload); err != nil {
			http.Error(w, fmt.Sprintf("failed to provide signal: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status":  "signal_accepted",
			"message": "Signal recorded. Trigger the order endpoint again to resume.",
		})
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		logger.Info("starting HITL HTTP worker", "port", *port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
		}
	}()

	<-stopCh
	logger.Info("shutting down HTTP worker...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}
