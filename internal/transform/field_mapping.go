package transform

import (
	"fmt"
	"strings"

	"github.com/pageton/bridge-db/internal/config"
)

// FieldMappingApplier applies user-defined field mappings to data maps.
type FieldMappingApplier struct {
	tableMappings    map[string][]config.FieldMapping
	wildcardMappings []config.FieldMapping
}

// NewFieldMappingApplier builds an applier from the config mappings.
// Returns nil if mappings is empty.
func NewFieldMappingApplier(mappings map[string][]config.FieldMapping) *FieldMappingApplier {
	if len(mappings) == 0 {
		return nil
	}
	a := &FieldMappingApplier{
		tableMappings: make(map[string][]config.FieldMapping, len(mappings)),
	}
	for table, rules := range mappings {
		if table == "*" {
			a.wildcardMappings = rules
		} else {
			a.tableMappings[table] = rules
		}
	}
	return a
}

// Apply applies mappings for the given table to the data map.
// Falls back to wildcard mappings if no table-specific rules exist.
// Returns the data map unchanged if no mappings match.
func (a *FieldMappingApplier) Apply(table string, data map[string]any) (map[string]any, error) {
	if a == nil {
		return data, nil
	}

	rules := a.tableMappings[table]
	if len(rules) == 0 {
		rules = a.wildcardMappings
	}
	if len(rules) == 0 {
		return data, nil
	}

	result := make(map[string]any, len(data))
	for k, v := range data {
		result[k] = v
	}

	for _, m := range rules {
		action := m.Action
		if action == "" {
			if m.Destination != "" && m.Destination != m.Source {
				action = "rename"
			} else {
				continue
			}
		}

		switch action {
		case "drop":
			delete(result, m.Source)

		case "rename":
			v, ok := result[m.Source]
			if !ok {
				continue
			}
			delete(result, m.Source)
			result[m.Destination] = v

		case "convert":
			v, ok := result[m.Source]
			if !ok {
				continue
			}
			converted, err := applyConvert(v, m.Convert)
			if err != nil {
				return nil, fmt.Errorf("convert field %q: %w", m.Source, err)
			}
			dst := m.Destination
			if dst == "" {
				dst = m.Source
			}
			result[dst] = converted
		}
	}

	return result, nil
}

// applyConvert handles the "convert" action using the Convert spec string.
// Supported formats: "string", "int", "float", "bool", "timestamp:srcDialect:dstDialect".
func applyConvert(v any, spec string) (any, error) {
	if spec == "" {
		return v, nil
	}

	// Check for timestamp conversion: "timestamp:src:dst"
	if strings.HasPrefix(spec, "timestamp:") {
		parts := strings.SplitN(spec, ":", 3)
		if len(parts) == 3 {
			s, ok := v.(string)
			if !ok {
				return v, nil
			}
			return ConvertTimestamp(s, Dialect(parts[1]), Dialect(parts[2])), nil
		}
	}

	switch spec {
	case "string":
		return fmt.Sprintf("%v", v), nil
	case "int":
		return coerceInt(v), nil
	case "float":
		return coerceFloat(v), nil
	case "bool":
		return coerceBool(v), nil
	default:
		return v, nil
	}
}
