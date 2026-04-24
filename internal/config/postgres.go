package config

import (
	"fmt"
	"net/url"
	"strconv"
)

// PostgresConfig holds PostgreSQL-specific connection parameters.
type PostgresConfig struct {
	Host     string `yaml:"host" json:"host"`
	Port     *int   `yaml:"port" json:"port"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Database string `yaml:"database" json:"database"`
	SSLMode  string `yaml:"ssl_mode" json:"ssl_mode"`
}

// DefaultPostgresConfig returns a PostgresConfig with sensible defaults.
func DefaultPostgresConfig() PostgresConfig {
	return PostgresConfig{
		Host:     "127.0.0.1",
		Port:     IntPtr(5432),
		Username: "postgres",
		SSLMode:  "require",
	}
}

// Address returns the host:port string.
func (c PostgresConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.GetPort())
}

// DSN returns a PostgreSQL driver-compatible data source name.
func (c PostgresConfig) DSN() string {
	if len(c.Host) > 0 && c.Host[0] == '/' {
		return fmt.Sprintf("host=%s user=%s password=xxxxx dbname=%s sslmode=%s",
			c.Host, c.Username, c.Database, c.SSLMode)
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=xxxxx dbname=%s sslmode=%s",
		c.Host, c.GetPort(), c.Username, c.Database, c.SSLMode)
}

func (c PostgresConfig) DSNWithPassword() string {
	if len(c.Host) > 0 && c.Host[0] == '/' {
		return fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=%s",
			c.Host, c.Username, c.Password, c.Database, c.SSLMode)
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.GetPort(), c.Username, c.Password, c.Database, c.SSLMode)
}

// ParsePostgresURL parses a postgres:// or postgresql:// URL into a PostgresConfig.
//
// Supported formats:
//
//	postgres://[username:password@]host:port/database[?sslmode=disable]
//	postgresql:///database?host=/run/postgresql&user=root&sslmode=disable
//
// When no host authority is present (e.g. postgresql:///db), the host is taken
// from the "host" query parameter. A host starting with "/" is treated as a Unix
// socket directory.
func ParsePostgresURL(rawURL string) (PostgresConfig, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return PostgresConfig{}, fmt.Errorf("invalid postgres URL: %w", err)
	}

	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return PostgresConfig{}, fmt.Errorf("unsupported postgres URL scheme: %s", u.Scheme)
	}

	cfg := DefaultPostgresConfig()

	// Query params — parse early so host can be overridden.
	q := u.Query()
	if sslMode := q.Get("sslmode"); sslMode != "" {
		cfg.SSLMode = sslMode
	}
	if qHost := q.Get("host"); qHost != "" {
		cfg.Host = qHost
	}

	if u.Host != "" {
		host := u.Hostname()
		if host != "" {
			cfg.Host = host
		}
		if portStr := u.Port(); portStr != "" {
			if p, err := strconv.Atoi(portStr); err == nil {
				cfg.Port = IntPtr(p)
			}
		}
	}

	if u.User != nil {
		cfg.Username = u.User.Username()
		if pass, ok := u.User.Password(); ok {
			cfg.Password = pass
		}
	}
	// user query param overrides URL user
	if qUser := q.Get("user"); qUser != "" {
		cfg.Username = qUser
	}

	// Path is the database name (/myapp)
	if len(u.Path) > 1 {
		cfg.Database = u.Path[1:] // strip leading /
	}

	return cfg, nil
}

// Validate checks that required fields are present.
func (c PostgresConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("postgres host is required")
	}
	// Port is only required for TCP connections (not Unix sockets).
	if len(c.Host) > 0 && c.Host[0] != '/' {
		if c.GetPort() <= 0 || c.GetPort() > 65535 {
			return fmt.Errorf("postgres port must be between 1 and 65535")
		}
	}
	if c.Database == "" {
		return fmt.Errorf("postgres database name is required")
	}
	return nil
}

func (c PostgresConfig) GetHost() string { return c.Host }
func (c PostgresConfig) GetPort() int {
	if c.Port == nil {
		return 0
	}
	return *c.Port
}
func (c PostgresConfig) GetDatabase() string { return c.Database }
