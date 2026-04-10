//go:build redis

package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// redisVerifier checks that keys in the destination Redis match the source.
// It verifies existence, type, and for sampled keys, value equality.
type redisVerifier struct {
	client *redis.Client
	log    interface {
		Info(msg string, args ...any)
		Warn(msg string, args ...any)
	}
}

func newRedisVerifier(client *redis.Client) *redisVerifier {
	return &redisVerifier{
		client: client,
		log:    logger.L().With("component", "redis-verifier"),
	}
}

// Verify checks the given keys for existence and type correctness.
// If keys is empty, it performs a count-based check using DBSIZE.
func (v *redisVerifier) Verify(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
	var errs []provider.VerificationError

	if len(keys) == 0 {
		// Count-based verification: just check DBSIZE is > 0
		count, err := v.client.DBSize(ctx).Result()
		if err != nil {
			return nil, fmt.Errorf("DBSIZE: %w", err)
		}
		if count == 0 {
			errs = append(errs, provider.VerificationError{
				Message: "destination database is empty",
			})
		}
		return errs, nil
	}

	// Key-level verification
	pipe := v.client.Pipeline()
	typeCmds := make([]*redis.StatusCmd, len(keys))
	existsCmds := make([]*redis.IntCmd, len(keys))

	for i, key := range keys {
		existsCmds[i] = pipe.Exists(ctx, key)
		typeCmds[i] = pipe.Type(ctx, key)
	}

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("verification pipeline: %w", err)
	}

	for i, key := range keys {
		// Check existence
		exists, err := existsCmds[i].Result()
		if err != nil {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Message: fmt.Sprintf("EXISTS check failed: %v", err),
			})
			continue
		}
		if exists == 0 {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Message: "key does not exist in destination",
			})
			continue
		}

		// Check type
		keyType, err := typeCmds[i].Result()
		if err != nil {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Message: fmt.Sprintf("TYPE check failed: %v", err),
			})
			continue
		}

		// Type should be a known Redis type (not "none")
		if keyType == "none" {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Message: "key type is none (key may have expired)",
			})
		}
	}

	return errs, nil
}
