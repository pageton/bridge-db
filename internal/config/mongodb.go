package config

import (
	"fmt"
	"net/url"
	"strconv"
)

// MongoDBConfig holds MongoDB-specific connection parameters.
type MongoDBConfig struct {
	Host       string `yaml:"host" json:"host"`
	Port       int    `yaml:"port" json:"port"`
	Username   string `yaml:"username" json:"username"`
	Password   string `yaml:"password" json:"password"`
	Database   string `yaml:"database" json:"database"`
	AuthSource string `yaml:"auth_source" json:"auth_source"`
	TLS        bool   `yaml:"tls" json:"tls"`
}

// DefaultMongoDBConfig returns a MongoDBConfig with sensible defaults.
func DefaultMongoDBConfig() MongoDBConfig {
	return MongoDBConfig{
		Host:       "127.0.0.1",
		Port:       27017,
		AuthSource: "admin",
	}
}

// Address returns the host:port string.
func (c MongoDBConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// ParseMongoDBURL parses a mongodb:// URL into a MongoDBConfig.
// Format: mongodb://[username:password@]host:port/database[?options]
func ParseMongoDBURL(rawURL string) (MongoDBConfig, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return MongoDBConfig{}, fmt.Errorf("invalid mongodb URL: %w", err)
	}

	if u.Scheme != "mongodb" && u.Scheme != "mongodb+srv" {
		return MongoDBConfig{}, fmt.Errorf("unsupported mongodb URL scheme: %s", u.Scheme)
	}

	cfg := DefaultMongoDBConfig()

	if u.Host != "" {
		host := u.Hostname()
		if host != "" {
			cfg.Host = host
		}
		if portStr := u.Port(); portStr != "" {
			if p, err := strconv.Atoi(portStr); err == nil {
				cfg.Port = p
			}
		}
	}

	if u.User != nil {
		cfg.Username = u.User.Username()
		if pass, ok := u.User.Password(); ok {
			cfg.Password = pass
		}
	}

	// Path is the database name (/myapp)
	if len(u.Path) > 1 {
		cfg.Database = u.Path[1:] // strip leading /
	}

	// Query params
	if authSrc := u.Query().Get("authSource"); authSrc != "" {
		cfg.AuthSource = authSrc
	}
	if tlsVal := u.Query().Get("tls"); tlsVal == "true" || tlsVal == "1" {
		cfg.TLS = true
	}

	return cfg, nil
}

// Validate checks that required fields are present.
func (c MongoDBConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("mongodb host is required")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("mongodb port must be between 1 and 65535")
	}
	if c.Database == "" {
		return fmt.Errorf("mongodb database name is required")
	}
	return nil
}
