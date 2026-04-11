package config

import (
	"fmt"
	"net"
	"reflect"
	"strconv"
)

// ProviderConfig returns the resolved provider-specific config struct.
func ProviderConfig(cfg *ConnectionConfig) any {
	return cfg.Resolved()
}

// ProviderConfigWithTunnel returns the resolved provider-specific config struct with the
// host/port rewritten to the tunnel's local address while preserving all other
// provider-specific fields. SQLite configs are returned as-is (no host/port).
func ProviderConfigWithTunnel(cfg *ConnectionConfig, tunnelAddr string) (any, error) {
	if tunnelAddr == "" {
		return ProviderConfig(cfg), nil
	}

	host, port, err := splitTunnelAddr(tunnelAddr)
	if err != nil {
		return nil, err
	}

	// SQLite has no host/port — return as-is.
	if cfg.Provider == "sqlite" {
		return ProviderConfig(cfg), nil
	}

	rc := cfg.Resolved()
	if rc == nil {
		return nil, fmt.Errorf("missing %s config", cfg.Provider)
	}

	// All non-SQLite providers have Host (string) and Port (int) as their first
	// two exported fields. Use reflection to clone the underlying struct and
	// override those fields with the tunnel address.
	rv := reflect.ValueOf(rc)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return rc, nil
	}

	clone := reflect.New(rv.Type()).Elem()
	clone.Set(rv)

	if h := clone.FieldByName("Host"); h.IsValid() && h.Kind() == reflect.String {
		h.SetString(host)
	}
	if p := clone.FieldByName("Port"); p.IsValid() && p.Kind() == reflect.Int {
		p.SetInt(int64(port))
	}

	return clone.Addr().Interface(), nil
}

func splitTunnelAddr(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid tunnel address %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid tunnel port %q: %w", portStr, err)
	}
	return host, port, nil
}
