package config

import (
	"fmt"
	"net/url"
	"strconv"
)

// RedisConfig holds Redis-specific connection parameters.
type RedisConfig struct {
	Host     string `yaml:"host" json:"host"`
	Port     *int   `yaml:"port" json:"port"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	DB       *int   `yaml:"db" json:"db"`
	TLS      *bool  `yaml:"tls" json:"tls"`
}

// DefaultRedisConfig returns a RedisConfig with sensible defaults.
func DefaultRedisConfig() RedisConfig {
	return RedisConfig{
		Host: "127.0.0.1",
		Port: IntPtr(6379),
		DB:   IntPtr(0),
		TLS:  BoolPtr(false),
	}
}

// Address returns the host:port string.
func (c RedisConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.GetPort())
}

// ParseRedisURL parses a redis:// URL into a RedisConfig.
// Format: redis://[username:password@]host:port/db
func ParseRedisURL(rawURL string) (RedisConfig, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return RedisConfig{}, fmt.Errorf("invalid redis URL: %w", err)
	}

	if u.Scheme != "redis" && u.Scheme != "rediss" {
		return RedisConfig{}, fmt.Errorf("unsupported redis URL scheme: %s", u.Scheme)
	}

	cfg := DefaultRedisConfig()
	cfg.TLS = BoolPtr(u.Scheme == "rediss")

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

	// Path is the database number (/0, /1, etc.)
	if len(u.Path) > 1 {
		dbStr := u.Path[1:] // strip leading /
		if db, err := strconv.Atoi(dbStr); err == nil {
			cfg.DB = IntPtr(db)
		}
	}

	return cfg, nil
}

// Validate checks that required fields are present.
func (c RedisConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("redis host is required")
	}
	if c.GetPort() <= 0 || c.GetPort() > 65535 {
		return fmt.Errorf("redis port must be between 1 and 65535")
	}
	if c.GetDB() < 0 || c.GetDB() > 15 {
		return fmt.Errorf("redis db must be between 0 and 15")
	}
	return nil
}

func (c RedisConfig) GetHost() string { return c.Host }
func (c RedisConfig) GetPort() int {
	if c.Port == nil {
		return 0
	}
	return *c.Port
}
func (c RedisConfig) GetDB() int {
	if c.DB == nil {
		return 0
	}
	return *c.DB
}
func (c RedisConfig) GetTLS() bool {
	if c.TLS == nil {
		return false
	}
	return *c.TLS
}
func (c RedisConfig) GetDatabase() string {
	return fmt.Sprintf("%d", c.GetDB())
}
