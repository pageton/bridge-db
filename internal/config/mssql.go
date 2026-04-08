package config

import (
	"fmt"
	"net/url"
	"strconv"
)

type MSSQLConfig struct {
	Host      string `yaml:"host" json:"host"`
	Port      int    `yaml:"port" json:"port"`
	Username  string `yaml:"username" json:"username"`
	Password  string `yaml:"password" json:"password"`
	Database  string `yaml:"database" json:"database"`
	Instance  string `yaml:"instance" json:"instance"`
	Encrypt   bool   `yaml:"encrypt" json:"encrypt"`
	TrustCert bool   `yaml:"trust_cert" json:"trust_cert"`
}

func DefaultMSSQLConfig() MSSQLConfig {
	return MSSQLConfig{
		Host:      "127.0.0.1",
		Port:      1433,
		Username:  "sa",
		Encrypt:   false,
		TrustCert: false,
	}
}

func (c MSSQLConfig) Address() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

func (c MSSQLConfig) DSN() string {
	dsn := fmt.Sprintf("sqlserver://%s@%s:%d",
		url.UserPassword(c.Username, c.Password).String(),
		c.Host, c.Port)
	if c.Database != "" {
		dsn += "?database=" + url.QueryEscape(c.Database)
	}
	if c.Encrypt {
		dsn += "&encrypt=true"
	}
	if c.TrustCert {
		dsn += "&TrustServerCertificate=true"
	}
	return dsn
}

func ParseMSSQLURL(rawURL string) (MSSQLConfig, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return MSSQLConfig{}, fmt.Errorf("invalid mssql URL: %w", err)
	}

	if u.Scheme != "mssql" && u.Scheme != "sqlserver" {
		return MSSQLConfig{}, fmt.Errorf("unsupported mssql URL scheme: %s", u.Scheme)
	}

	cfg := DefaultMSSQLConfig()

	q := u.Query()
	if enc := q.Get("encrypt"); enc == "true" || enc == "1" {
		cfg.Encrypt = true
	}
	if tc := q.Get("TrustServerCertificate"); tc == "true" || tc == "1" {
		cfg.TrustCert = true
	}
	if inst := q.Get("instance"); inst != "" {
		cfg.Instance = inst
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

	if len(u.Path) > 1 {
		cfg.Database = u.Path[1:]
	}
	if db := q.Get("database"); db != "" {
		cfg.Database = db
	}

	return cfg, nil
}

func (c MSSQLConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("mssql host is required")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("mssql port must be between 1 and 65535")
	}
	if c.Database == "" {
		return fmt.Errorf("mssql database name is required")
	}
	return nil
}
