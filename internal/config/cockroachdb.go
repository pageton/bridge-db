package config

import (
	"fmt"
	"net/url"
	"strconv"
)

type CockroachDBConfig struct {
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Database string `yaml:"database" json:"database"`
	SSLMode  string `yaml:"ssl_mode" json:"ssl_mode"`
}

func DefaultCockroachDBConfig() CockroachDBConfig {
	return CockroachDBConfig{
		Host:     "127.0.0.1",
		Port:     26257,
		Username: "root",
		SSLMode:  "require",
	}
}

func (c CockroachDBConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func (c CockroachDBConfig) DSN() string {
	if len(c.Host) > 0 && c.Host[0] == '/' {
		return fmt.Sprintf("host=%s user=%s password=xxxxx dbname=%s sslmode=%s",
			c.Host, c.Username, c.Database, c.SSLMode)
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=xxxxx dbname=%s sslmode=%s",
		c.Host, c.Port, c.Username, c.Database, c.SSLMode)
}

func (c CockroachDBConfig) DSNWithPassword() string {
	if len(c.Host) > 0 && c.Host[0] == '/' {
		return fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=%s",
			c.Host, c.Username, c.Password, c.Database, c.SSLMode)
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.Username, c.Password, c.Database, c.SSLMode)
}

func ParseCockroachDBURL(rawURL string) (CockroachDBConfig, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return CockroachDBConfig{}, fmt.Errorf("invalid cockroachdb URL: %w", err)
	}

	if u.Scheme != "cockroachdb" && u.Scheme != "crdb" {
		return CockroachDBConfig{}, fmt.Errorf("unsupported cockroachdb URL scheme: %s", u.Scheme)
	}

	cfg := DefaultCockroachDBConfig()

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
	if qUser := q.Get("user"); qUser != "" {
		cfg.Username = qUser
	}

	if len(u.Path) > 1 {
		cfg.Database = u.Path[1:]
	}

	return cfg, nil
}

func (c CockroachDBConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("cockroachdb host is required")
	}
	if len(c.Host) > 0 && c.Host[0] != '/' {
		if c.Port <= 0 || c.Port > 65535 {
			return fmt.Errorf("cockroachdb port must be between 1 and 65535")
		}
	}
	if c.Database == "" {
		return fmt.Errorf("cockroachdb database name is required")
	}
	return nil
}

func (c CockroachDBConfig) GetHost() string  { return c.Host }
func (c CockroachDBConfig) GetPort() int     { return c.Port }
func (c CockroachDBConfig) GetDatabase() string { return c.Database }
