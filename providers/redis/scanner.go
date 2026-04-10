//go:build redis

package redis

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// redisScanner enumerates keys from a Redis instance using SCAN.
type redisScanner struct {
	client    *redis.Client
	opts      provider.ScanOptions
	stats     provider.ScanStats
	cursor    uint64
	done      bool
	processed map[string]struct{}
	log       interface{ Info(msg string, args ...any) }
}

func newRedisScanner(client *redis.Client, opts provider.ScanOptions) *redisScanner {
	s := &redisScanner{
		client:    client,
		opts:      opts,
		processed: make(map[string]struct{}),
		log:       logger.L().With("component", "redis-scanner"),
	}

	if len(opts.ResumeToken) > 0 {
		var token redisResumeToken
		if err := sonic.Unmarshal(opts.ResumeToken, &token); err == nil {
			s.stats.TotalScanned = token.TotalScanned
			s.stats.TotalBytes = token.TotalBytes
			for _, k := range token.ProcessedKeys {
				s.processed[k] = struct{}{}
			}
			s.log.Info("resuming from checkpoint",
				"already_processed", len(s.processed),
				"total_scanned", token.TotalScanned,
			)
		}
	}

	return s
}

// Next returns the next batch of MigrationUnits. It returns io.EOF when
// all keys have been scanned.
func (s *redisScanner) Next(ctx context.Context) ([]provider.MigrationUnit, error) {
	if s.done {
		return nil, io.EOF
	}

	batchSize := s.opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	// Use SCAN to get the next batch of keys.
	// We scan more keys than batchSize because we need to read each key's
	// type and value individually, and we may need to skip keys on resume.
	var keys []string
	var nextCursor uint64

	scanCount := int64(batchSize * 2) // scan more keys to account for filtering
	match := s.opts.Pattern
	if match == "" {
		match = "*"
	}

	scanCmd := s.client.Scan(ctx, s.cursor, match, scanCount)
	if scanCmd.Err() != nil {
		return nil, fmt.Errorf("SCAN: %w", scanCmd.Err())
	}
	keys, nextCursor = scanCmd.Val()
	s.cursor = nextCursor

	if len(keys) == 0 && nextCursor == 0 {
		s.done = true
		return nil, io.EOF
	}

	// Read each key: type, value, TTL
	units := make([]provider.MigrationUnit, 0, len(keys))
	for _, key := range keys {
		if _, skip := s.processed[key]; skip {
			continue
		}

		unit, err := s.readKey(ctx, key)
		if err != nil {
			s.log.Info("failed to read key, skipping", "key", key, "error", err)
			continue
		}
		units = append(units, *unit)
		s.processed[key] = struct{}{}
		s.stats.TotalScanned++
		s.stats.TotalBytes += unit.Size
	}

	// If cursor wrapped to 0, we're done
	if nextCursor == 0 {
		s.done = true
	}

	if len(units) == 0 && s.done {
		return nil, io.EOF
	}

	return units, nil
}

// Stats returns current scan statistics.
func (s *redisScanner) Stats() provider.ScanStats {
	return s.stats
}

// readKey reads a single Redis key and returns it as a MigrationUnit.
// For large collection keys (hash, list, set, zset, stream), it uses
// chunked reads to avoid loading the entire value into memory.
func (s *redisScanner) readKey(ctx context.Context, key string) (*provider.MigrationUnit, error) {
	// Get the key type
	keyType, err := s.client.Type(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("TYPE %s: %w", key, err)
	}

	// Get TTL
	ttl, err := s.client.TTL(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("TTL %s: %w", key, err)
	}

	ttlSeconds := int64(0)
	if ttl > 0 {
		ttlSeconds = int64(ttl.Seconds())
	}

	// Read the value based on type
	var value any
	var dataType provider.DataType
	var size int64

	switch keyType {
	case "string":
		dataType = provider.DataTypeString
		val, err := s.client.Get(ctx, key).Result()
		if err != nil {
			return nil, fmt.Errorf("GET %s: %w", key, err)
		}
		value = val
		size = int64(len(val))

	case "hash":
		dataType = provider.DataTypeHash
		val, sz, err := s.readHashChunked(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("HGETALL %s: %w", key, err)
		}
		value = val
		size = sz

	case "list":
		dataType = provider.DataTypeList
		val, sz, err := s.readListChunked(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("LRANGE %s: %w", key, err)
		}
		value = val
		size = sz

	case "set":
		dataType = provider.DataTypeSet
		val, sz, err := s.readSetChunked(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("SMEMBERS %s: %w", key, err)
		}
		value = val
		size = sz

	case "zset":
		dataType = provider.DataTypeZSet
		val, sz, err := s.readZSetChunked(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("ZRANGE %s: %w", key, err)
		}
		value = val
		size = sz

	case "stream":
		dataType = provider.DataTypeStream
		val, sz, err := s.readStreamChunked(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("XRANGE %s: %w", key, err)
		}
		value = val
		size = sz

	default:
		return nil, fmt.Errorf("unsupported Redis type %q for key %s", keyType, key)
	}

	// Build the JSON envelope
	rd := &redisKeyData{
		Type:       keyType,
		Value:      value,
		TTLSeconds: ttlSeconds,
	}
	data, err := encodeRedisData(rd)
	if err != nil {
		return nil, fmt.Errorf("encode key %s: %w", key, err)
	}

	return &provider.MigrationUnit{
		Key:      key,
		DataType: dataType,
		Data:     data,
		Meta:     provider.UnitMeta{TTL: ttlSeconds},
		Size:     size,
	}, nil
}

// ---------------------------------------------------------------------------
// Chunked reads for large collection keys
// ---------------------------------------------------------------------------

// hashChunkSize controls how many fields are fetched per HSCAN iteration.
const hashChunkSize = 500

// readHashChunked reads a Redis hash using HSCAN to avoid loading the entire
// value at once. For small hashes it falls back to HGETALL for simplicity.
func (s *redisScanner) readHashChunked(ctx context.Context, key string) (map[string]string, int64, error) {
	// Check size heuristic: use HLEN to decide strategy.
	length, err := s.client.HLen(ctx, key).Result()
	if err != nil {
		return nil, 0, err
	}

	// Small hash: use HGETALL (faster for small sets).
	if length < hashChunkSize {
		val, err := s.client.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, 0, err
		}
		var sz int64
		for k, v := range val {
			sz += int64(len(k) + len(v))
		}
		return val, sz, nil
	}

	// Large hash: use HSCAN with cursor.
	result := make(map[string]string, length)
	var cursor uint64
	var totalSize int64

	for {
		var batch []string
		batch, cursor, err = s.client.HScan(ctx, key, cursor, "*", hashChunkSize).Result()
		if err != nil {
			return nil, 0, err
		}
		for i := 0; i < len(batch)-1; i += 2 {
			result[batch[i]] = batch[i+1]
			totalSize += int64(len(batch[i]) + len(batch[i+1]))
		}
		if cursor == 0 {
			break
		}
	}

	return result, totalSize, nil
}

// listChunkSize controls how many elements are fetched per LRANGE iteration.
const listChunkSize = 500

// readListChunked reads a Redis list using paginated LRANGE.
func (s *redisScanner) readListChunked(ctx context.Context, key string) ([]string, int64, error) {
	length, err := s.client.LLen(ctx, key).Result()
	if err != nil {
		return nil, 0, err
	}

	if length < listChunkSize {
		val, err := s.client.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return nil, 0, err
		}
		var sz int64
		for _, v := range val {
			sz += int64(len(v))
		}
		return val, sz, nil
	}

	result := make([]string, 0, length)
	var totalSize int64
	var offset int64

	for offset < length {
		end := offset + listChunkSize - 1
		if end >= length {
			end = length - 1
		}
		batch, err := s.client.LRange(ctx, key, offset, end).Result()
		if err != nil {
			return nil, 0, err
		}
		for _, v := range batch {
			result = append(result, v)
			totalSize += int64(len(v))
		}
		offset += listChunkSize
	}

	return result, totalSize, nil
}

// setChunkSize controls how many members are fetched per SSCAN iteration.
const setChunkSize = 500

// readSetChunked reads a Redis set using SSCAN.
func (s *redisScanner) readSetChunked(ctx context.Context, key string) ([]string, int64, error) {
	length, err := s.client.SCard(ctx, key).Result()
	if err != nil {
		return nil, 0, err
	}

	if length < setChunkSize {
		val, err := s.client.SMembers(ctx, key).Result()
		if err != nil {
			return nil, 0, err
		}
		var sz int64
		for _, v := range val {
			sz += int64(len(v))
		}
		return val, sz, nil
	}

	result := make([]string, 0, length)
	var cursor uint64
	var totalSize int64

	for {
		var batch []string
		batch, cursor, err = s.client.SScan(ctx, key, cursor, "*", setChunkSize).Result()
		if err != nil {
			return nil, 0, err
		}
		for _, v := range batch {
			result = append(result, v)
			totalSize += int64(len(v))
		}
		if cursor == 0 {
			break
		}
	}

	return result, totalSize, nil
}

// zsetChunkSize controls how many members are fetched per ZSCAN iteration.
const zsetChunkSize = 500

// readZSetChunked reads a Redis sorted set using ZSCAN.
func (s *redisScanner) readZSetChunked(ctx context.Context, key string) ([]sortedSetMember, int64, error) {
	length, err := s.client.ZCard(ctx, key).Result()
	if err != nil {
		return nil, 0, err
	}

	if length < zsetChunkSize {
		val, err := s.client.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return nil, 0, err
		}
		members := make([]sortedSetMember, len(val))
		var sz int64
		for i, z := range val {
			members[i] = sortedSetMember{
				Member: z.Member.(string),
				Score:  z.Score,
			}
			sz += int64(len(members[i].Member)) + 8
		}
		return members, sz, nil
	}

	result := make([]sortedSetMember, 0, length)
	var cursor uint64
	var totalSize int64

	for {
		var vals []string
		vals, cursor, err = s.client.ZScan(ctx, key, cursor, "*", zsetChunkSize).Result()
		if err != nil {
			return nil, 0, err
		}
		// ZScan returns alternating member, score strings.
		for i := 0; i+1 < len(vals); i += 2 {
			score, _ := strconv.ParseFloat(vals[i+1], 64)
			result = append(result, sortedSetMember{
				Member: vals[i],
				Score:  score,
			})
			totalSize += int64(len(vals[i])) + 8
		}
		if cursor == 0 {
			break
		}
	}

	return result, totalSize, nil
}

// streamChunkSize controls how many entries are fetched per XRANGE iteration.
const streamChunkSize = 100

// readStreamChunked reads a Redis stream using paginated XRANGE.
func (s *redisScanner) readStreamChunked(ctx context.Context, key string) ([]streamEntry, int64, error) {
	// Get the first and last IDs to determine range.
	entries, err := s.client.XRevRangeN(ctx, key, "+", "-", 1).Result()
	if err != nil {
		return nil, 0, err
	}
	if len(entries) == 0 {
		return nil, 0, nil
	}

	result := make([]streamEntry, 0)
	var totalSize int64
	lastID := "-"

	for {
		batch, err := s.client.XRangeN(ctx, key, lastID, "+", streamChunkSize).Result()
		if err != nil {
			return nil, 0, err
		}
		if len(batch) == 0 {
			break
		}

		for _, msg := range batch {
			fields := make(map[string]string, len(msg.Values))
			for k, v := range msg.Values {
				if str, ok := v.(string); ok {
					fields[k] = str
				} else {
					fields[k] = fmt.Sprintf("%v", v)
				}
			}
			result = append(result, streamEntry{
				ID:     msg.ID,
				Fields: fields,
			})
			totalSize += int64(len(msg.ID))
			for k, v := range fields {
				totalSize += int64(len(k) + len(v))
			}
			lastID = msg.ID
		}

		if int64(len(batch)) < streamChunkSize {
			break
		}
	}

	return result, totalSize, nil
}

// ---------------------------------------------------------------------------
// Resume token helpers
// ---------------------------------------------------------------------------

// redisResumeToken is stored in the checkpoint for deterministic Redis resume.
type redisResumeToken struct {
	TotalScanned  int64    `json:"total_scanned"`
	TotalBytes    int64    `json:"total_bytes"`
	ProcessedKeys []string `json:"processed_keys,omitempty"`
}
