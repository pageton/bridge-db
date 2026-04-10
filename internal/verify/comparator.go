package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pageton/bridge-db/pkg/provider"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// compareRecords reads records from both sides and compares field-by-field,
// recording mismatches into the TableResult.
func compareRecords(
	ctx context.Context,
	srcReader, dstReader provider.VerifyReader,
	table string,
	keys []string,
	tr *TableResult,
) {
	srcRecords, err := srcReader.ReadRecords(ctx, keys)
	if err != nil {
		tr.Mismatches = append(tr.Mismatches, MismatchDetail{
			Category: MismatchValueDiff,
			Table:    table,
			Message:  fmt.Sprintf("source read failed: %v", err),
		})
		tr.SampleMismatch = len(keys)
		return
	}

	dstRecords, err := dstReader.ReadRecords(ctx, keys)
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

		normSrc := normalizeComparableValue(srcVal)
		normDst := normalizeComparableValue(dstVal)

		if !valuesEqual(normSrc, normDst) {
			diffs = append(diffs, FieldDiff{Field: field, SourceValue: normSrc, DestValue: normDst})
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

	aTime, aIsTime := toTimeValue(a)
	bTime, bIsTime := toTimeValue(b)
	if aIsTime && bIsTime {
		return aTime.Equal(bTime) || aTime.Truncate(time.Second).Equal(bTime.Truncate(time.Second))
	}

	aDec, aIsDec := toBigRat(a)
	bDec, bIsDec := toBigRat(b)
	if aIsDec && bIsDec {
		return aDec.Cmp(bDec) == 0
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

func toBigRat(v any) (*big.Rat, bool) {
	switch n := v.(type) {
	case float64:
		r := new(big.Rat)
		if _, ok := r.SetString(strconv.FormatFloat(n, 'f', -1, 64)); ok {
			return r, true
		}
	case float32:
		r := new(big.Rat)
		if _, ok := r.SetString(strconv.FormatFloat(float64(n), 'f', -1, 32)); ok {
			return r, true
		}
	case int:
		return new(big.Rat).SetInt64(int64(n)), true
	case int8:
		return new(big.Rat).SetInt64(int64(n)), true
	case int16:
		return new(big.Rat).SetInt64(int64(n)), true
	case int32:
		return new(big.Rat).SetInt64(int64(n)), true
	case int64:
		return new(big.Rat).SetInt64(n), true
	case uint:
		return new(big.Rat).SetUint64(uint64(n)), true
	case uint8:
		return new(big.Rat).SetUint64(uint64(n)), true
	case uint16:
		return new(big.Rat).SetUint64(uint64(n)), true
	case uint32:
		return new(big.Rat).SetUint64(uint64(n)), true
	case uint64:
		return new(big.Rat).SetUint64(n), true
	case string:
		r := new(big.Rat)
		if _, ok := r.SetString(n); ok {
			return r, true
		}
	case []byte:
		r := new(big.Rat)
		if _, ok := r.SetString(string(n)); ok {
			return r, true
		}
	case json.Number:
		r := new(big.Rat)
		if _, ok := r.SetString(n.String()); ok {
			return r, true
		}
	case pgtype.Numeric:
		norm := normalizePGNumeric(n)
		if s, ok := norm.(string); ok {
			r := new(big.Rat)
			if _, ok := r.SetString(s); ok {
				return r, true
			}
		}
	}
	return nil, false
}

func normalizeComparableValue(v any) any {
	switch x := v.(type) {
	case bson.ObjectID:
		return x.Hex()
	case []byte:
		return string(x)
	case pgtype.Numeric:
		return normalizePGNumeric(x)
	case fmt.Stringer:
		return x.String()
	default:
		return v
	}
}

func normalizePGNumeric(n pgtype.Numeric) any {
	if !n.Valid {
		return nil
	}
	if n.NaN {
		return "NaN"
	}
	if n.InfinityModifier == pgtype.Infinity {
		return "Infinity"
	}
	if n.InfinityModifier == pgtype.NegativeInfinity {
		return "-Infinity"
	}

	if n.Exp >= 0 {
		intVal := new(big.Int).Set(n.Int)
		if n.Exp > 0 {
			pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n.Exp)), nil)
			intVal.Mul(intVal, pow)
		}
		return intVal.String()
	}

	abs := new(big.Int).Set(n.Int)
	neg := abs.Sign() < 0
	if neg {
		abs.Abs(abs)
	}
	str := abs.String()
	scale := int(-n.Exp)
	if len(str) <= scale {
		str = strings.Repeat("0", scale-len(str)+1) + str
	}
	idx := len(str) - scale
	str = str[:idx] + "." + str[idx:]
	str = strings.TrimRight(str, "0")
	str = strings.TrimRight(str, ".")
	if str == "" {
		str = "0"
	}
	if neg && str != "0" {
		str = "-" + str
	}
	return str
}

func toTimeValue(v any) (time.Time, bool) {
	switch x := v.(type) {
	case time.Time:
		return x.UTC(), true
	case string:
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02 15:04:05.999999 -0700 MST",
			"2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05.999999",
			"2006-01-02T15:04:05",
		} {
			if t, err := time.Parse(layout, x); err == nil {
				return t.UTC(), true
			}
		}
		return time.Time{}, false
	default:
		return time.Time{}, false
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
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case pgtype.Numeric:
		f, err := n.Float64Value()
		return f.Float64, err == nil && f.Valid
	default:
		return 0, false
	}
}
