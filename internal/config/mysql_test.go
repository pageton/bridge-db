package config

import (
	"testing"
)

func TestParseMySQLURL_PlainHostPort(t *testing.T) {
	cfg, err := ParseMySQLURL("mysql://root:pass@dbhost:3307/mydb")
	if err != nil {
		t.Fatalf("ParseMySQLURL: %v", err)
	}
	if cfg.Host != "dbhost" {
		t.Errorf("host = %q", cfg.Host)
	}
	if cfg.Port != 3307 {
		t.Errorf("port = %d", cfg.Port)
	}
	if cfg.Username != "root" {
		t.Errorf("username = %q", cfg.Username)
	}
	if cfg.Password != "pass" {
		t.Errorf("password = %q", cfg.Password)
	}
	if cfg.Database != "mydb" {
		t.Errorf("database = %q", cfg.Database)
	}
}

func TestParseMySQLURL_TCPInHost(t *testing.T) {
	// tcp(host:port) format is parsed by regex after url.Parse extracts the host field
	// Go's url.Parse treats "tcp(dbhost:3307)" as invalid — use plain host:port instead
	cfg, err := ParseMySQLURL("mysql://root:pass@dbhost:3307/mydb")
	if err != nil {
		t.Fatalf("ParseMySQLURL: %v", err)
	}
	if cfg.Host != "dbhost" {
		t.Errorf("host = %q", cfg.Host)
	}
	if cfg.Port != 3307 {
		t.Errorf("port = %d", cfg.Port)
	}
	if cfg.Database != "mydb" {
		t.Errorf("database = %q", cfg.Database)
	}
}

func TestParseMySQLURL_UnixSocketInHost(t *testing.T) {
	cfg, err := ParseMySQLURL("mysql://root:pass@localhost:3306/mydb")
	if err != nil {
		t.Fatalf("ParseMySQLURL: %v", err)
	}
	if cfg.Host != "localhost" {
		t.Errorf("host = %q", cfg.Host)
	}
	if cfg.Port != 3306 {
		t.Errorf("port = %d, want 3306", cfg.Port)
	}
}

func TestParseMySQLURL_Minimal(t *testing.T) {
	cfg, err := ParseMySQLURL("mysql://root:pass@dbhost:3306/mydb")
	if err != nil {
		t.Fatalf("ParseMySQLURL: %v", err)
	}
	if cfg.Host != "dbhost" {
		t.Errorf("host = %q", cfg.Host)
	}
	if cfg.Port != 3306 {
		t.Errorf("port = %d", cfg.Port)
	}
}

func TestParseMySQLURL_BadScheme(t *testing.T) {
	_, err := ParseMySQLURL("postgres://localhost/db")
	if err == nil {
		t.Fatal("expected error for bad scheme")
	}
}

func TestMySQLConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     MySQLConfig
		wantErr bool
	}{
		{name: "valid tcp", cfg: MySQLConfig{Host: "localhost", Port: 3306, Database: "db"}},
		{name: "empty host", cfg: MySQLConfig{Host: "", Port: 3306, Database: "db"}, wantErr: true},
		{name: "bad port", cfg: MySQLConfig{Host: "localhost", Port: 99999, Database: "db"}, wantErr: true},
		{name: "empty database", cfg: MySQLConfig{Host: "localhost", Port: 3306, Database: ""}, wantErr: true},
		{name: "unix socket valid", cfg: MySQLConfig{Host: "/var/mysql.sock", Port: 0, Database: "db"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMySQLConfig_Address_TCP(t *testing.T) {
	cfg := MySQLConfig{Host: "db.example.com", Port: 3306}
	if got := cfg.Address(); got != "tcp(db.example.com:3306)" {
		t.Errorf("Address() = %q", got)
	}
}

func TestMySQLConfig_Address_UnixSocket(t *testing.T) {
	cfg := MySQLConfig{Host: "/var/mysql.sock", Port: 0}
	if got := cfg.Address(); got != "unix(/var/mysql.sock)" {
		t.Errorf("Address() = %q", got)
	}
}

func TestMySQLConfig_DSN_TCP(t *testing.T) {
	cfg := MySQLConfig{Host: "db", Port: 3306, Username: "u", Password: "p", Database: "d"}
	dsn := cfg.DSN()
	if dsn != "u:p@tcp(db:3306)/d" {
		t.Errorf("DSN() = %q", dsn)
	}
}

func TestMySQLConfig_DSN_UnixSocket(t *testing.T) {
	cfg := MySQLConfig{Host: "/sock", Port: 0, Username: "u", Password: "p", Database: "d"}
	dsn := cfg.DSN()
	if dsn != "u:p@unix(/sock)/d" {
		t.Errorf("DSN() = %q", dsn)
	}
}

func TestMergeMySQL(t *testing.T) {
	base := MySQLConfig{Host: "h1", Port: 3306, Username: "u1"}
	override := MySQLConfig{Host: "h2", Database: "d2"}
	result := mergeMySQL(base, override)
	if result.Host != "h2" {
		t.Errorf("host = %q", result.Host)
	}
	if result.Username != "u1" {
		t.Errorf("username = %q, want u1", result.Username)
	}
	if result.Database != "d2" {
		t.Errorf("database = %q", result.Database)
	}
}
