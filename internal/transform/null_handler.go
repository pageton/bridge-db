package transform

import (
	"fmt"
	"math"
	"strconv"
)

// NullPolicy determines how null values are handled during transformation.
type NullPolicy int

const (
	// NullPassThrough silently copies nulls (default behavior).
	NullPassThrough NullPolicy = iota

	// NullDrop removes key-value pairs where the value is nil.
	NullDrop

	// NullReplace substitutes nil with a type-appropriate default value.
	NullReplace

	// NullError returns an error if any nil value is encountered.
	NullError
)

// NullHandler applies the configured null policy to data maps.
type NullHandler struct {
	Policy NullPolicy
}

// Apply walks the data map and enforces the null policy.
// Returns an error only when Policy is NullError and at least one nil value is found.
func (h *NullHandler) Apply(data map[string]any) (map[string]any, error) {
	if h == nil || h.Policy == NullPassThrough {
		return data, nil
	}

	switch h.Policy {
	case NullDrop:
		return h.dropNulls(data), nil
	case NullReplace:
		return h.replaceNulls(data), nil
	case NullError:
		return h.errorOnNull(data)
	default:
		return data, nil
	}
}

func (h *NullHandler) dropNulls(data map[string]any) map[string]any {
	result := make(map[string]any, len(data))
	for k, v := range data {
		if v != nil {
			result[k] = v
		}
	}
	return result
}

func (h *NullHandler) replaceNulls(data map[string]any) map[string]any {
	result := make(map[string]any, len(data))
	for k, v := range data {
		if v == nil {
			result[k] = ""
		} else {
			result[k] = v
		}
	}
	return result
}

func (h *NullHandler) errorOnNull(data map[string]any) (map[string]any, error) {
	for k, v := range data {
		if v == nil {
			return data, fmt.Errorf("null value found for key %q", k)
		}
	}
	return data, nil
}

// NullPolicyFromString parses a null policy string from config.
// Returns NullPassThrough for unrecognized values.
func NullPolicyFromString(s string) NullPolicy {
	switch s {
	case "drop":
		return NullDrop
	case "replace":
		return NullReplace
	case "error":
		return NullError
	default:
		return NullPassThrough
	}
}

// isNil checks if a value is nil, handling interface-wrapped nils.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	switch val := v.(type) {
	case float64:
		return math.IsNaN(val)
	case string:
		return val == ""
	case bool:
		return false
	case []any:
		return len(val) == 0
	case map[string]any:
		return len(val) == 0
	}
	return false
}

// coerceType attempts to convert a value to the specified target type.
// Supported targets: "string", "int", "float", "bool".
// Returns the original value if conversion fails.
func coerceType(v any, targetType string) any {
	if v == nil {
		return v
	}

	switch targetType {
	case "string":
		return fmt.Sprintf("%v", v)
	case "int":
		return coerceInt(v)
	case "float":
		return coerceFloat(v)
	case "bool":
		return coerceBool(v)
	default:
		return v
	}
}

func coerceInt(v any) any {
	switch val := v.(type) {
	case float64:
		return int64(val)
	case int:
		return int64(val)
	case int64:
		return val
	case string:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return int64(f)
		}
	}
	return v
}

func coerceFloat(v any) any {
	switch val := v.(type) {
	case float64:
		return val
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return v
}

func coerceBool(v any) any {
	switch val := v.(type) {
	case bool:
		return val
	case string:
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
	case float64:
		return val != 0
	case int:
		return val != 0
	case int64:
		return val != 0
	}
	return v
}
