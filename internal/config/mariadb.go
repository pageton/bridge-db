package config

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
)

type MariaDBConfig struct {
	Host     string `yaml:"host" json:"host"`
	Port     *int   `yaml:"port" json:"port"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Database string `yaml:"database" json:"database"`
}

func DefaultMariaDBConfig() MariaDBConfig {
	return MariaDBConfig{
		Host: "127.0.0.1",
		Port: IntPtr(3306),
	}
}

func (c MariaDBConfig) Address() string {
	if c.GetPort() == 0 {
		return fmt.Sprintf("unix(%s)", c.Host)
	}
	return fmt.Sprintf("tcp(%s:%d)", c.Host, c.GetPort())
}

func (c MariaDBConfig) IsUnixSocket() bool {
	return c.GetPort() == 0 && len(c.Host) > 0
}

func (c MariaDBConfig) DSN() string {
	if c.IsUnixSocket() {
		return fmt.Sprintf("%s:***@unix(%s)/%s",
			c.Username, c.Host, c.Database)
	}
	return fmt.Sprintf("%s:***@tcp(%s:%d)/%s",
		c.Username, c.Host, c.GetPort(), c.Database)
}

func (c MariaDBConfig) DSNWithPassword() string {
	if c.IsUnixSocket() {
		return fmt.Sprintf("%s:%s@unix(%s)/%s",
			c.Username, c.Password, c.Host, c.Database)
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		c.Username, c.Password, c.Host, c.GetPort(), c.Database)
}

var (
	mariadbTCP  = regexp.MustCompile(`tcp\(([^:]+):(\d+)\)`)
	mariadbUnix = regexp.MustCompile(`unix\((.+)\)`)
)

func ParseMariaDBURL(rawURL string) (MariaDBConfig, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return MariaDBConfig{}, fmt.Errorf("invalid mariadb URL: %w", err)
	}

	if u.Scheme != "mariadb" {
		return MariaDBConfig{}, fmt.Errorf("unsupported mariadb URL scheme: %s", u.Scheme)
	}

	cfg := DefaultMariaDBConfig()

	if u.Host != "" {
		if matches := mariadbUnix.FindStringSubmatch(u.Host); len(matches) == 2 {
			cfg.Host = matches[1]
			cfg.Port = IntPtr(0)
		} else if matches := mariadbTCP.FindStringSubmatch(u.Host); len(matches) == 3 {
			cfg.Host = matches[1]
			if p, err := strconv.Atoi(matches[2]); err == nil {
				cfg.Port = IntPtr(p)
			}
		} else {
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

	return cfg, nil
}

func (c MariaDBConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("mariadb host is required")
	}
	if !c.IsUnixSocket() && (c.GetPort() <= 0 || c.GetPort() > 65535) {
		return fmt.Errorf("mariadb port must be between 1 and 65535")
	}
	if c.Database == "" {
		return fmt.Errorf("mariadb database name is required")
	}
	return nil
}

func (c MariaDBConfig) GetHost() string { return c.Host }
func (c MariaDBConfig) GetPort() int {
	if c.Port == nil {
		return 0
	}
	return *c.Port
}
func (c MariaDBConfig) GetDatabase() string { return c.Database }
