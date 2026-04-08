package transform

import (
	"context"
	"strings"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	RegisterTransformer("redis", "mongodb", func() Transformer {
		return &RedisToMongoDBTransformer{}
	})
}

type RedisToMongoDBTransformer struct{}

func (t *RedisToMongoDBTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	result := make([]provider.MigrationUnit, 0, len(units))

	for _, unit := range units {
		transformed := t.transformUnit(unit)
		result = append(result, transformed...)
	}

	return result, nil
}

func (t *RedisToMongoDBTransformer) transformUnit(unit provider.MigrationUnit) []provider.MigrationUnit {
	var rd struct {
		Type       string `json:"type"`
		Value      any    `json:"value"`
		TTLSeconds int64  `json:"ttl_seconds"`
	}
	if err := sonic.Unmarshal(unit.Data, &rd); err != nil {
		return []provider.MigrationUnit{unit}
	}

	collection := collectionFromKey(unit.Key)

	switch rd.Type {
	case "hash":
		return t.transformHash(unit.Key, collection, rd.Value, rd.TTLSeconds)
	case "string":
		return t.transformString(unit.Key, collection, rd.Value, rd.TTLSeconds)
	case "list":
		return t.transformList(unit.Key, collection, rd.Value, rd.TTLSeconds)
	case "set":
		return t.transformSet(unit.Key, collection, rd.Value, rd.TTLSeconds)
	case "zset":
		return t.transformZSet(unit.Key, collection, rd.Value, rd.TTLSeconds)
	case "stream":
		return t.transformStream(unit.Key, collection, rd.Value, rd.TTLSeconds)
	default:
		return []provider.MigrationUnit{unit}
	}
}

func (t *RedisToMongoDBTransformer) transformHash(key, collection string, value any, ttl int64) []provider.MigrationUnit {
	fields, ok := value.(map[string]any)
	if !ok {
		return nil
	}

	doc := make(map[string]any, len(fields)+1)
	for k, v := range fields {
		doc[k] = v
	}
	doc["_id"] = key

	return []provider.MigrationUnit{buildMongoUnit(key, collection, doc)}
}

func (t *RedisToMongoDBTransformer) transformString(key, collection string, value any, ttl int64) []provider.MigrationUnit {
	str, ok := value.(string)
	if !ok {
		return nil
	}

	var parsed map[string]any
	if err := sonic.Unmarshal([]byte(str), &parsed); err == nil {
		parsed["_id"] = key
		return []provider.MigrationUnit{buildMongoUnit(key, collection, parsed)}
	}

	doc := map[string]any{
		"_id":   key,
		"value": str,
		"_ttl":  ttl,
	}
	return []provider.MigrationUnit{buildMongoUnit(key, collection, doc)}
}

func (t *RedisToMongoDBTransformer) transformList(key, collection string, value any, ttl int64) []provider.MigrationUnit {
	items, ok := value.([]any)
	if !ok {
		return nil
	}

	doc := map[string]any{
		"_id":   key,
		"items": items,
		"_ttl":  ttl,
	}
	return []provider.MigrationUnit{buildMongoUnit(key, collection, doc)}
}

func (t *RedisToMongoDBTransformer) transformSet(key, collection string, value any, ttl int64) []provider.MigrationUnit {
	members, ok := value.([]any)
	if !ok {
		return nil
	}

	doc := map[string]any{
		"_key":    key,
		"members": members,
		"_ttl":    ttl,
	}
	return []provider.MigrationUnit{buildMongoUnit(key, collection, doc)}
}

func (t *RedisToMongoDBTransformer) transformZSet(key, collection string, value any, ttl int64) []provider.MigrationUnit {
	members, ok := value.([]any)
	if !ok {
		return nil
	}

	doc := map[string]any{
		"_key":    key,
		"members": members,
		"_ttl":    ttl,
	}
	return []provider.MigrationUnit{buildMongoUnit(key, collection, doc)}
}

func (t *RedisToMongoDBTransformer) transformStream(key, collection string, value any, ttl int64) []provider.MigrationUnit {
	entries, ok := value.([]any)
	if !ok {
		return nil
	}

	units := make([]provider.MigrationUnit, 0, len(entries))
	for _, entry := range entries {
		entryMap, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		entryMap["_stream"] = key
		entryID, _ := entryMap["id"].(string)
		units = append(units, buildMongoUnit(key+":"+entryID, collection, entryMap))
	}
	return units
}

func buildMongoUnit(key, collection string, doc map[string]any) provider.MigrationUnit {
	envelope := map[string]any{
		"collection":  collection,
		"document_id": key,
		"document":    doc,
	}

	data, _ := sonic.Marshal(envelope)

	return provider.MigrationUnit{
		Key:      key,
		DataType: provider.DataTypeDocument,
		Data:     data,
		Size:     int64(len(data)),
	}
}

func collectionFromKey(key string) string {
	parts := strings.SplitN(key, ":", 2)
	return parts[0]
}

func (t *RedisToMongoDBTransformer) NeedsSchema() bool            { return false }
func (t *RedisToMongoDBTransformer) SetSchema(_ *provider.Schema) {}
