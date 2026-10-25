package aegis

import (
	"context"
	"log/slog"
	"time"
)

// StepHook defines lifecycle interception hooks around individual step executions.
type StepHook interface {
	BeforeStep(ctx context.Context, runID, stepID string)
	AfterStep(ctx context.Context, runID, stepID string, duration time.Duration, err error)
}

// LoggingHook provides structured slog tracing for step executions.
type LoggingHook struct {
	logger *slog.Logger
}

// NewLoggingHook creates a hook that emits structured logs on step completion.
func NewLoggingHook(logger *slog.Logger) *LoggingHook {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingHook{logger: logger}
}

func (h *LoggingHook) BeforeStep(ctx context.Context, runID, stepID string) {
	h.logger.DebugContext(ctx, "aegis: step starting", "run_id", runID, "step_id", stepID)
}

func (h *LoggingHook) AfterStep(ctx context.Context, runID, stepID string, duration time.Duration, err error) {
	if err != nil {
		h.logger.ErrorContext(ctx, "aegis: step failed",
			"run_id", runID,
			"step_id", stepID,
			"duration_ms", duration.Milliseconds(),
			"error", err,
		)
		return
	}
	h.logger.InfoContext(ctx, "aegis: step completed",
		"run_id", runID,
		"step_id", stepID,
		"duration_ms", duration.Milliseconds(),
	)
}

// RetryStep executes fn with exponential backoff on retryable errors.
func RetryStep[T any, R any](
	rt *Runtime[T],
	baseStepID string,
	maxAttempts int,
	initialBackoff time.Duration,
	fn func(ctx context.Context, state *T) (R, error),
) (R, error) {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if initialBackoff <= 0 {
		initialBackoff = 50 * time.Millisecond
	}

	return Step(rt, baseStepID, func(ctx context.Context, s *T) (R, error) {
		var lastErr error
		backoff := initialBackoff

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			if err := ctx.Err(); err != nil {
				var zero R
				return zero, err
			}

			res, err := fn(ctx, s)
			if err == nil {
				return res, nil
			}

			lastErr = err
			if attempt == maxAttempts {
				break
			}

			select {
			case <-time.After(backoff):
				backoff *= 2
			case <-ctx.Done():
				var zero R
				return zero, ctx.Err()
			}
		}

		var zero R
		return zero, lastErr
	})
}
