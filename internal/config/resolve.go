package config

import "fmt"

// ResolveConfig dispatches a raw config value to the appropriate resolution path.
// T is the concrete config type (e.g. PostgresConfig).
// fromMap handles the map[string]string case (typically tunnel-resolved addresses).
func ResolveConfig[T any](raw any, providerName string, fromMap func(map[string]string) (*T, error)) (*T, error) {
	switch v := raw.(type) {
	case *T:
		if v == nil {
			return nil, fmt.Errorf("nil %s config", providerName)
		}
		return v, nil
	case T:
		return &v, nil
	case map[string]string:
		return fromMap(v)
	default:
		return nil, fmt.Errorf("unsupported %s config type: %T", providerName, raw)
	}
}
