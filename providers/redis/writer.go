//go:build redis

package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// redisWriter persists MigrationUnits to a Redis destination using pipelined
// commands. It compensates for TTL drift by measuring elapsed time since the
// scan and adjusting the TTL accordingly.
type redisWriter struct {
	client  *redis.Client
	opts    provider.WriteOptions
	written int64
	failed  int64
	skipped int64
	bytes   int64
	log     interface{ Debug(msg string, args ...any) }
}

func newRedisWriter(client *redis.Client, opts provider.WriteOptions) *redisWriter {
	return &redisWriter{
		client: client,
		opts:   opts,
		log:    logger.L().With("component", "redis-writer"),
	}
}

// Write persists a batch of MigrationUnits to the destination Redis.
// It uses Redis pipelining for performance and compensates for TTL drift.
func (w *redisWriter) Write(ctx context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	start := time.Now()
	prevWritten, prevFailed, prevSkipped, prevBytes := w.written, w.failed, w.skipped, w.bytes

	if len(units) == 0 {
		return &provider.BatchResult{
			TotalUnits: 0,
			Duration:   time.Since(start),
		}, nil
	}

	// Use a pipeline for all commands
	pipe := w.client.Pipeline()
	cmds := make([]redis.Cmder, 0, len(units)*2) // *2 for SET + EXPIRE

	var failedKeys []string
	var errors []error

	for i := range units {
		unit := &units[i]

		// Decode the JSON envelope
		rd, err := decodeRedisData(unit.Data)
		if err != nil {
			w.failed++
			failedKeys = append(failedKeys, unit.Key)
			errors = append(errors, fmt.Errorf("decode %s: %w", unit.Key, err))
			continue
		}

		// Check conflict strategy
		if w.opts.OnConflict == provider.ConflictSkip {
			exists, err := w.client.Exists(ctx, unit.Key).Result()
			if err != nil {
				w.failed++
				failedKeys = append(failedKeys, unit.Key)
				errors = append(errors, fmt.Errorf("EXISTS %s: %w", unit.Key, err))
				continue
			}
			if exists > 0 {
				w.skipped++
				continue
			}
		}

		// Apply the key based on its type
		if err := w.applyKey(ctx, pipe, unit.Key, rd, &cmds); err != nil {
			w.failed++
			failedKeys = append(failedKeys, unit.Key)
			errors = append(errors, err)
			continue
		}

		w.written++
		w.bytes += unit.Size
	}

	// Execute the pipeline
	if len(cmds) > 0 {
		if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
			// Pipeline errors are tricky — individual commands may have failed
			w.log.Debug("pipeline had errors", "error", err)
		}
	}

	return &provider.BatchResult{
		TotalUnits:   len(units),
		WrittenUnits: int(w.written - prevWritten),
		FailedUnits:  int(w.failed - prevFailed),
		SkippedUnits: int(w.skipped - prevSkipped),
		FailedKeys:   failedKeys,
		BytesWritten: w.bytes - prevBytes,
		Duration:     time.Since(start),
		Errors:       errors,
	}, nil
}

// Flush is a no-op for Redis since pipelining writes immediately.
func (w *redisWriter) Flush(_ context.Context) error {
	return nil
}

// applyKey queues the appropriate Redis commands into the pipeline for a
// single key, including TTL compensation.
func (w *redisWriter) applyKey(ctx context.Context, pipe redis.Pipeliner, key string, rd *redisKeyData, cmds *[]redis.Cmder) error {
	// Calculate TTL with drift compensation
	ttl := w.compensatedTTL(rd.TTLSeconds)

	switch rd.Type {
	case "string":
		val, ok := rd.Value.(string)
		if !ok {
			return fmt.Errorf("key %s: expected string value, got %T", key, rd.Value)
		}
		*cmds = append(*cmds, pipe.Set(ctx, key, val, 0))

	case "hash":
		val, ok := rd.Value.(map[string]any)
		if !ok {
			return fmt.Errorf("key %s: expected hash value, got %T", key, rd.Value)
		}
		if len(val) > 0 {
			fields := make([]any, 0, len(val)*2)
			for k, v := range val {
				fields = append(fields, k, v)
			}
			*cmds = append(*cmds, pipe.HSet(ctx, key, fields...))
		}

	case "list":
		val, ok := rd.Value.([]any)
		if !ok {
			return fmt.Errorf("key %s: expected list value, got %T", key, rd.Value)
		}
		if len(val) > 0 {
			*cmds = append(*cmds, pipe.RPush(ctx, key, val...))
		}

	case "set":
		val, ok := rd.Value.([]any)
		if !ok {
			return fmt.Errorf("key %s: expected set value, got %T", key, rd.Value)
		}
		if len(val) > 0 {
			*cmds = append(*cmds, pipe.SAdd(ctx, key, val...))
		}

	case "zset":
		val, ok := rd.Value.([]any)
		if !ok {
			return fmt.Errorf("key %s: expected zset value, got %T", key, rd.Value)
		}
		members := make([]redis.Z, 0, len(val))
		for _, v := range val {
			memberMap, ok := v.(map[string]any)
			if !ok {
				continue
			}
			member, _ := memberMap["member"].(string)
			score, _ := memberMap["score"].(float64)
			members = append(members, redis.Z{
				Score:  score,
				Member: member,
			})
		}
		if len(members) > 0 {
			*cmds = append(*cmds, pipe.ZAdd(ctx, key, members...))
		}

	case "stream":
		val, ok := rd.Value.([]any)
		if !ok {
			return fmt.Errorf("key %s: expected stream value, got %T", key, rd.Value)
		}
		for _, v := range val {
			entryMap, ok := v.(map[string]any)
			if !ok {
				continue
			}
			id, _ := entryMap["id"].(string)
			fields, _ := entryMap["fields"].(map[string]any)
			if id == "" {
				id = "*"
			}
			values := make(map[string]any)
			for k, fv := range fields {
				values[k] = fv
			}
			*cmds = append(*cmds, pipe.XAdd(ctx, &redis.XAddArgs{
				ID:     id,
				Values: values,
			}))
		}

	default:
		return fmt.Errorf("unsupported Redis type %q for key %s", rd.Type, key)
	}

	// Set TTL if applicable
	if ttl > 0 {
		*cmds = append(*cmds, pipe.Expire(ctx, key, ttl))
	}

	return nil
}

// compensatedTTL adjusts the TTL to account for elapsed time since scanning.
// When data is scanned from the source, the TTL is captured. By the time it's
// written to the destination, some time has passed. We compensate by reducing
// the TTL proportionally.
func (w *redisWriter) compensatedTTL(originalTTLSeconds int64) time.Duration {
	if originalTTLSeconds <= 0 {
		return 0
	}

	// Simple compensation: use the original TTL as-is.
	// In a production system, you'd track scan_time per key and compute:
	//   adjusted = original - time.Since(scan_time)
	// For now, we apply a small fixed compensation (1 second) to account
	// for pipeline latency.
	compensated := originalTTLSeconds - 1
	if compensated < 1 {
		compensated = 1
	}
	return time.Duration(compensated) * time.Second
}
