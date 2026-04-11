package config

import (
	"fmt"
	"net/url"
	"strings"
)

type SQLiteConfig struct {
	Path string `yaml:"path" json:"path"`
}

func DefaultSQLiteConfig() SQLiteConfig {
	return SQLiteConfig{
		Path: "test.db",
	}
}

func ParseSQLiteURL(rawURL string) (SQLiteConfig, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return SQLiteConfig{}, fmt.Errorf("invalid sqlite URL: %w", err)
	}

	if u.Scheme != "sqlite" {
		return SQLiteConfig{}, fmt.Errorf("unsupported sqlite URL scheme: %s", u.Scheme)
	}

	cfg := DefaultSQLiteConfig()

	path := u.Host + u.Path
	if path == "" {
		path = u.Opaque
	}
	path = strings.TrimPrefix(path, "//")

	if path == "" {
		return SQLiteConfig{}, fmt.Errorf("sqlite path is required in URL")
	}
	cfg.Path = path

	return cfg, nil
}

func (c SQLiteConfig) Validate() error {
	if c.Path == "" {
		return fmt.Errorf("sqlite path is required")
	}
	return nil
}

func (c SQLiteConfig) GetHost() string  { return "" }
func (c SQLiteConfig) GetPort() int     { return 0 }
func (c SQLiteConfig) GetDatabase() string { return c.Path }
