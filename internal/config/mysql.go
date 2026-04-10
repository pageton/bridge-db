package config

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
)

// MySQLConfig holds MySQL-specific connection parameters.
type MySQLConfig struct {
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Database string `yaml:"database" json:"database"`
}

// DefaultMySQLConfig returns a MySQLConfig with sensible defaults.
func DefaultMySQLConfig() MySQLConfig {
	return MySQLConfig{
		Host: "127.0.0.1",
		Port: 3306,
	}
}

// Address returns the host:port string in TCP format, or unix(path) for sockets.
func (c MySQLConfig) Address() string {
	if c.Port == 0 {
		return fmt.Sprintf("unix(%s)", c.Host)
	}
	return fmt.Sprintf("tcp(%s:%d)", c.Host, c.Port)
}

// IsUnixSocket returns true if the host is a Unix socket path.
func (c MySQLConfig) IsUnixSocket() bool {
	return c.Port == 0 && len(c.Host) > 0
}

// DSN returns a MySQL driver-compatible data source name with masked password.
func (c MySQLConfig) DSN() string {
	if c.IsUnixSocket() {
		return fmt.Sprintf("%s:***@unix(%s)/%s",
			c.Username, c.Host, c.Database)
	}
	return fmt.Sprintf("%s:***@tcp(%s:%d)/%s",
		c.Username, c.Host, c.Port, c.Database)
}

// DSNWithPassword returns the real MySQL DSN for driver use only.
func (c MySQLConfig) DSNWithPassword() string {
	if c.IsUnixSocket() {
		return fmt.Sprintf("%s:%s@unix(%s)/%s",
			c.Username, c.Password, c.Host, c.Database)
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		c.Username, c.Password, c.Host, c.Port, c.Database)
}

// tcpPattern matches tcp(host:port) in a MySQL URL.
var tcpPattern = regexp.MustCompile(`tcp\(([^:]+):(\d+)\)`)

// unixPattern matches unix(/path/to/socket) in a MySQL URL.
var unixPattern = regexp.MustCompile(`unix\((.+)\)`)

// ParseMySQLURL parses a mysql:// URL into a MySQLConfig.
// Format: mysql://[username:password@][tcp(host:port)|unix(/path/to/socket)]/database
func ParseMySQLURL(rawURL string) (MySQLConfig, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return MySQLConfig{}, fmt.Errorf("invalid mysql URL: %w", err)
	}

	if u.Scheme != "mysql" {
		return MySQLConfig{}, fmt.Errorf("unsupported mysql URL scheme: %s", u.Scheme)
	}

	cfg := DefaultMySQLConfig()

	// Extract host:port from tcp(host:port) or unix(/path/to/socket) in the host field
	if u.Host != "" {
		if matches := unixPattern.FindStringSubmatch(u.Host); len(matches) == 2 {
			// Unix socket connection
			cfg.Host = matches[1]
			cfg.Port = 0 // no port for Unix sockets
		} else if matches := tcpPattern.FindStringSubmatch(u.Host); len(matches) == 3 {
			cfg.Host = matches[1]
			if p, err := strconv.Atoi(matches[2]); err == nil {
				cfg.Port = p
			}
		} else {
			// Fallback: treat as plain host:port
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

	return cfg, nil
}

// Validate checks that required fields are present.
func (c MySQLConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("mysql host is required")
	}
	if !c.IsUnixSocket() && (c.Port <= 0 || c.Port > 65535) {
		return fmt.Errorf("mysql port must be between 1 and 65535")
	}
	if c.Database == "" {
		return fmt.Errorf("mysql database name is required")
	}
	return nil
}
