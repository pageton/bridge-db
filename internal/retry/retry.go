// Package retry provides configurable retry logic with exponential backoff.
package retry

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/pageton/bridge-db/internal/logger"
)

// Config controls retry behaviour.
type Config struct {
	// MaxAttempts is the maximum number of attempts (including the first call).
	MaxAttempts int

	// InitialInterval is the delay before the first retry.
	InitialInterval time.Duration

	// MaxInterval caps the exponential backoff.
	MaxInterval time.Duration

	// Multiplier is the backoff multiplier (typically 2.0).
	Multiplier float64
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		MaxAttempts:     3,
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
	}
}

// Do executes fn, retrying on error up to cfg.MaxAttempts times.
func Do(ctx context.Context, cfg Config, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("retry cancelled: %w", err)
		}

		if lastErr = fn(); lastErr == nil {
			return nil
		}

		if attempt < cfg.MaxAttempts-1 {
			delay := backoff(cfg, attempt)
			logger.L().Warn("retry attempt failed, backing off",
				"attempt", attempt+1,
				"max_attempts", cfg.MaxAttempts,
				"backoff", delay,
				"error", lastErr,
			)

			select {
			case <-ctx.Done():
				return fmt.Errorf("retry cancelled during backoff: %w", ctx.Err())
			case <-time.After(delay):
			}
		}
	}
	return fmt.Errorf("retry exhausted after %d attempts: %w", cfg.MaxAttempts, lastErr)
}

// backoff calculates the delay for the given attempt index.
func backoff(cfg Config, attempt int) time.Duration {
	delay := float64(cfg.InitialInterval) * math.Pow(cfg.Multiplier, float64(attempt))
	d := time.Duration(delay)
	if d > cfg.MaxInterval {
		return cfg.MaxInterval
	}
	return d
}
