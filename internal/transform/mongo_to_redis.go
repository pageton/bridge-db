package transform

import (
	"context"
	"fmt"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	RegisterTransformer("mongodb", "redis", func() Transformer {
		return &MongoToRedisTransformer{}
	})
}

type MongoToRedisTransformer struct {
	cfg TransformerConfig
}

func (t *MongoToRedisTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	result := make([]provider.MigrationUnit, 0, len(units))

	for _, unit := range units {
		transformed, err := t.transformUnit(unit)
		if err != nil {
			continue
		}
		result = append(result, transformed)
	}

	return result, nil
}

func (t *MongoToRedisTransformer) transformUnit(unit provider.MigrationUnit) (provider.MigrationUnit, error) {
	var envelope map[string]any
	if err := sonic.Unmarshal(unit.Data, &envelope); err != nil {
		return unit, fmt.Errorf("unmarshal mongo envelope: %w", err)
	}

	docRaw, ok := envelope["document"]
	if !ok {
		return unit, fmt.Errorf("missing document field")
	}

	doc, ok := docRaw.(map[string]any)
	if !ok {
		return unit, fmt.Errorf("document is not a map")
	}

	collection, _ := envelope["collection"].(string)
	if collection == "" {
		collection = unit.Table
	}

	hashKey := unit.Key

	fields := make(map[string]any)
	for k, v := range doc {
		switch val := v.(type) {
		case map[string]any, []any:
			b, _ := sonic.Marshal(val)
			fields[k] = string(b)
		default:
			fields[k] = fmt.Sprintf("%v", val)
		}
	}

	redisEnvelope := map[string]any{
		"type":        "hash",
		"value":       fields,
		"ttl_seconds": 0,
	}

	data, err := sonic.Marshal(redisEnvelope)
	if err != nil {
		return unit, fmt.Errorf("marshal redis envelope: %w", err)
	}

	return provider.MigrationUnit{
		Key:      hashKey,
		Table:    collection,
		DataType: provider.DataTypeHash,
		Data:     data,
		Size:     int64(len(data)),
	}, nil
}

func (t *MongoToRedisTransformer) NeedsSchema() bool               { return false }
func (t *MongoToRedisTransformer) SetSchema(_ *provider.Schema)    {}
func (t *MongoToRedisTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }
