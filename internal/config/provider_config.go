package config

import (
	"fmt"
	"net"
	"strconv"
)

// ProviderConfig returns the resolved provider-specific config struct.
func ProviderConfig(cfg *ConnectionConfig) any {
	switch cfg.Provider {
	case "redis":
		return cfg.Redis
	case "mongodb":
		return cfg.MongoDB
	case "postgres":
		return cfg.Postgres
	case "mysql":
		return cfg.MySQL
	case "sqlite":
		return cfg.SQLite
	case "mariadb":
		return cfg.MariaDB
	case "cockroachdb":
		return cfg.CockroachDB
	case "mssql":
		return cfg.MSSQL
	default:
		return nil
	}
}

// ProviderConfigWithTunnel returns the provider-specific config struct with the
// host/port rewritten to the tunnel's local address while preserving all other
// provider-specific fields.
func ProviderConfigWithTunnel(cfg *ConnectionConfig, tunnelAddr string) (any, error) {
	if tunnelAddr == "" {
		return ProviderConfig(cfg), nil
	}

	host, port, err := splitTunnelAddr(tunnelAddr)
	if err != nil {
		return nil, err
	}

	switch cfg.Provider {
	case "redis":
		if cfg.Redis == nil {
			return nil, fmt.Errorf("missing redis config")
		}
		resolved := *cfg.Redis
		resolved.Host = host
		resolved.Port = port
		return &resolved, nil
	case "mongodb":
		if cfg.MongoDB == nil {
			return nil, fmt.Errorf("missing mongodb config")
		}
		resolved := *cfg.MongoDB
		resolved.Host = host
		resolved.Port = port
		return &resolved, nil
	case "postgres":
		if cfg.Postgres == nil {
			return nil, fmt.Errorf("missing postgres config")
		}
		resolved := *cfg.Postgres
		resolved.Host = host
		resolved.Port = port
		return &resolved, nil
	case "mysql":
		if cfg.MySQL == nil {
			return nil, fmt.Errorf("missing mysql config")
		}
		resolved := *cfg.MySQL
		resolved.Host = host
		resolved.Port = port
		return &resolved, nil
	case "sqlite":
		return ProviderConfig(cfg), nil
	case "mariadb":
		if cfg.MariaDB == nil {
			return nil, fmt.Errorf("missing mariadb config")
		}
		resolved := *cfg.MariaDB
		resolved.Host = host
		resolved.Port = port
		return &resolved, nil
	case "cockroachdb":
		if cfg.CockroachDB == nil {
			return nil, fmt.Errorf("missing cockroachdb config")
		}
		resolved := *cfg.CockroachDB
		resolved.Host = host
		resolved.Port = port
		return &resolved, nil
	case "mssql":
		if cfg.MSSQL == nil {
			return nil, fmt.Errorf("missing mssql config")
		}
		resolved := *cfg.MSSQL
		resolved.Host = host
		resolved.Port = port
		return &resolved, nil
	default:
		return nil, fmt.Errorf("unknown provider %q", cfg.Provider)
	}
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
