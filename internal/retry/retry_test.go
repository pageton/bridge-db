package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoff_InitialInterval(t *testing.T) {
	cfg := Config{InitialInterval: 100 * time.Millisecond, Multiplier: 2.0, MaxInterval: 30 * time.Second}
	d := backoff(cfg, 0)
	if d != 100*time.Millisecond {
		t.Errorf("backoff(0) = %v, want 100ms", d)
	}
}

func TestBackoff_Exponential(t *testing.T) {
	cfg := Config{InitialInterval: 100 * time.Millisecond, Multiplier: 2.0, MaxInterval: 30 * time.Second}
	d1 := backoff(cfg, 0)
	d2 := backoff(cfg, 1)
	d3 := backoff(cfg, 2)
	if d2 <= d1 {
		t.Errorf("backoff(1)=%v should be > backoff(0)=%v", d2, d1)
	}
	if d3 <= d2 {
		t.Errorf("backoff(2)=%v should be > backoff(1)=%v", d3, d2)
	}
}

func TestBackoff_MaxInterval(t *testing.T) {
	cfg := Config{InitialInterval: 100 * time.Millisecond, Multiplier: 10.0, MaxInterval: 500 * time.Millisecond}
	d := backoff(cfg, 5)
	if d > cfg.MaxInterval {
		t.Errorf("backoff(5) = %v, should be capped at %v", d, cfg.MaxInterval)
	}
}

func TestDo_SuccessFirstTry(t *testing.T) {
	cfg := Config{MaxAttempts: 3, InitialInterval: time.Millisecond}
	calls := 0
	err := Do(context.Background(), cfg, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestDo_SuccessAfterRetries(t *testing.T) {
	cfg := Config{MaxAttempts: 3, InitialInterval: time.Millisecond}
	calls := 0
	err := Do(context.Background(), cfg, func() error {
		calls++
		if calls < 3 {
			return errors.New("fail")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestDo_Exhausted(t *testing.T) {
	cfg := Config{MaxAttempts: 2, InitialInterval: time.Millisecond}
	err := Do(context.Background(), cfg, func() error {
		return errors.New("always fail")
	})
	if err == nil {
		t.Fatal("expected error when retries exhausted")
	}
}

func TestDo_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := Config{MaxAttempts: 3, InitialInterval: time.Millisecond}
	err := Do(ctx, cfg, func() error { return nil })
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxAttempts != 3 {
		t.Errorf("max_attempts = %d", cfg.MaxAttempts)
	}
	if cfg.Multiplier != 2.0 {
		t.Errorf("multiplier = %f", cfg.Multiplier)
	}
}
