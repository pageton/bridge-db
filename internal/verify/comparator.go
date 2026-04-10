package verify

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/pageton/bridge-db/pkg/provider"
	"github.com/jackc/pgx/v5/pgtype"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// compareRecords reads records from both sides and compares field-by-field,
// recording mismatches into the TableResult.
func compareRecords(
	_ context.Context,
	srcReader, dstReader provider.VerifyReader,
	table string,
	keys []string,
	tr *TableResult,
) {
	srcRecords, err := srcReader.ReadRecords(context.Background(), keys)
	if err != nil {
		tr.Mismatches = append(tr.Mismatches, MismatchDetail{
			Category: MismatchValueDiff,
			Table:    table,
			Message:  fmt.Sprintf("source read failed: %v", err),
		})
		tr.SampleMismatch = len(keys)
		return
	}

	dstRecords, err := dstReader.ReadRecords(context.Background(), keys)
	if err != nil {
		tr.Mismatches = append(tr.Mismatches, MismatchDetail{
			Category: MismatchValueDiff,
			Table:    table,
			Message:  fmt.Sprintf("destination read failed: %v", err),
		})
		tr.SampleMismatch = len(keys)
		return
	}

	for _, key := range keys {
		src, srcOK := srcRecords[key]
		dst, dstOK := dstRecords[key]

		if srcOK && !dstOK {
			tr.Mismatches = append(tr.Mismatches, MismatchDetail{
				Category: MismatchMissingInDst,
				Table:    table,
				Key:      key,
				Message:  fmt.Sprintf("key %q not found in destination", key),
			})
			tr.SampleMismatch++
			continue
		}
		if !srcOK && dstOK {
			tr.Mismatches = append(tr.Mismatches, MismatchDetail{
				Category: MismatchExtraInDst,
				Table:    table,
				Key:      key,
				Message:  fmt.Sprintf("key %q exists in destination but not in source", key),
			})
			tr.SampleMismatch++
			continue
		}

		src, dst = normalizeRecordsForComparison(src, dst)

		diffs := compareFields(src, dst)
		if len(diffs) > 0 {
			tr.Mismatches = append(tr.Mismatches, MismatchDetail{
				Category:   MismatchValueDiff,
				Table:      table,
				Key:        key,
				FieldDiffs: diffs,
				Message:    fmt.Sprintf("key %q has %d field difference(s)", key, len(diffs)),
			})
			tr.SampleMismatch++
		}
	}
}

func normalizeRecordsForComparison(src, dst map[string]any) (map[string]any, map[string]any) {
	if flat, ok := flattenRedisHashRecord(dst); ok {
		dst = flat
	}
	if flat, ok := flattenRedisHashRecord(src); ok {
		src = flat
	}
	return src, dst
}

func flattenRedisHashRecord(record map[string]any) (map[string]any, bool) {
	if record == nil {
		return nil, false
	}
	typ, _ := record["type"].(string)
	if typ != "hash" {
		return nil, false
	}
	value, ok := record["value"].(map[string]any)
	if ok {
		return value, true
	}
	if stringMap, ok := record["value"].(map[string]string); ok {
		flat := make(map[string]any, len(stringMap))
		for k, v := range stringMap {
			flat[k] = v
		}
		return flat, true
	}
	return nil, false
}

// compareFields compares two records field-by-field and returns differences.
func compareFields(src, dst map[string]any) []FieldDiff {
	var diffs []FieldDiff

	allFields := make(map[string]bool)
	for k := range src {
		allFields[k] = true
	}
	for k := range dst {
		allFields[k] = true
	}

	for field := range allFields {
		srcVal, srcOK := src[field]
		dstVal, dstOK := dst[field]

		if !srcOK {
			diffs = append(diffs, FieldDiff{Field: field, SourceValue: nil, DestValue: dstVal})
			continue
		}
		if !dstOK {
			diffs = append(diffs, FieldDiff{Field: field, SourceValue: srcVal, DestValue: nil})
			continue
		}

		if !valuesEqual(srcVal, dstVal) {
			diffs = append(diffs, FieldDiff{Field: field, SourceValue: srcVal, DestValue: dstVal})
		}
	}

	return diffs
}

// valuesEqual compares two values with type coercion for common database types.
func valuesEqual(a, b any) bool {
	a = normalizeComparableValue(a)
	b = normalizeComparableValue(b)

	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	if reflect.DeepEqual(a, b) {
		return true
	}

	aFloat, aIsFloat := toFloat64(a)
	bFloat, bIsFloat := toFloat64(b)
	if aIsFloat && bIsFloat {
		return math.Abs(aFloat-bFloat) < 1e-9
	}

	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return aStr == bStr
}

func normalizeComparableValue(v any) any {
	switch x := v.(type) {
	case bson.ObjectID:
		return x.Hex()
	case []byte:
		return string(x)
	case fmt.Stringer:
		return x.String()
	default:
		return v
	}
}

// toFloat64 attempts to convert a value to float64 for numeric comparison.
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	case pgtype.Numeric:
		f, err := n.Float64Value()
		return f.Float64, err == nil && f.Valid
	default:
		return 0, false
	}
}
