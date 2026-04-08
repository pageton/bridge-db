//go:build redis

package redis

import (
	"fmt"
	"github.com/bytedance/sonic"
)

// redisKeyData is the JSON envelope for a Redis MigrationUnit's Data field.
// It carries the key's value alongside metadata needed to reconstruct it
// on the destination.
type redisKeyData struct {
	// Type is the Redis data type (string, hash, list, set, zset, stream).
	Type string `json:"type"`

	// Value holds the type-specific payload.
	//   string  – string
	//   hash    – map[string]string
	//   list    – []string
	//   set     – []string
	//   zset    – []sortedSetMember
	//   stream  – []streamEntry
	Value any `json:"value"`

	// TTLSeconds is the remaining TTL in seconds. 0 means no expiry.
	TTLSeconds int64 `json:"ttl_seconds"`
}

// sortedSetMember represents a member with its score in a sorted set.
type sortedSetMember struct {
	Member string  `json:"member"`
	Score  float64 `json:"score"`
}

// streamEntry represents a single entry in a Redis stream.
type streamEntry struct {
	ID     string            `json:"id"`
	Fields map[string]string `json:"fields"`
}

// encodeRedisData serialises a redisKeyData into JSON bytes.
func encodeRedisData(rd *redisKeyData) ([]byte, error) {
	return sonic.Marshal(rd)
}

// decodeRedisData deserialises JSON bytes into a redisKeyData.
func decodeRedisData(data []byte) (*redisKeyData, error) {
	var rd redisKeyData
	if err := sonic.Unmarshal(data, &rd); err != nil {
		return nil, fmt.Errorf("decode redis data: %w", err)
	}
	return &rd, nil
}
