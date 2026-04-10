package config

import (
	"testing"
)

func TestParseMariaDBURL_PlainHostPort(t *testing.T) {
	cfg, err := ParseMariaDBURL("mariadb://root:pass@dbhost:3307/mydb")
	if err != nil {
		t.Fatalf("ParseMariaDBURL: %v", err)
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

func TestParseMariaDBURL_TCPInHost(t *testing.T) {
	cfg, err := ParseMariaDBURL("mariadb://root:pass@dbhost:3307/mydb")
	if err != nil {
		t.Fatalf("ParseMariaDBURL: %v", err)
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

func TestParseMariaDBURL_UnixSocketInHost(t *testing.T) {
	cfg, err := ParseMariaDBURL("mariadb://root:pass@localhost:3306/mydb")
	if err != nil {
		t.Fatalf("ParseMariaDBURL: %v", err)
	}
	if cfg.Host != "localhost" {
		t.Errorf("host = %q", cfg.Host)
	}
}

func TestParseMariaDBURL_BadScheme(t *testing.T) {
	_, err := ParseMariaDBURL("mysql://localhost/db")
	if err == nil {
		t.Fatal("expected error for bad scheme")
	}
}

func TestMariaDBConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     MariaDBConfig
		wantErr bool
	}{
		{name: "valid", cfg: MariaDBConfig{Host: "localhost", Port: 3306, Database: "db"}},
		{name: "empty host", cfg: MariaDBConfig{Host: "", Port: 3306, Database: "db"}, wantErr: true},
		{name: "bad port", cfg: MariaDBConfig{Host: "localhost", Port: 99999, Database: "db"}, wantErr: true},
		{name: "empty database", cfg: MariaDBConfig{Host: "localhost", Port: 3306, Database: ""}, wantErr: true},
		{name: "unix socket", cfg: MariaDBConfig{Host: "/sock", Port: 0, Database: "db"}},
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

func TestMariaDBConfig_DSN(t *testing.T) {
	cfg := MariaDBConfig{Host: "db", Port: 3306, Username: "u", Password: "p", Database: "d"}
	if got := cfg.DSN(); got != "u:***@tcp(db:3306)/d" {
		t.Errorf("DSN() = %q", got)
	}
}

func TestMariaDBConfig_DSNWithPassword(t *testing.T) {
	cfg := MariaDBConfig{Host: "db", Port: 3306, Username: "u", Password: "p", Database: "d"}
	if got := cfg.DSNWithPassword(); got != "u:p@tcp(db:3306)/d" {
		t.Errorf("DSNWithPassword() = %q", got)
	}
}

func TestMergeMariaDB(t *testing.T) {
	base := MariaDBConfig{Host: "h1", Port: 3306}
	override := MariaDBConfig{Host: "h2", Database: "d2"}
	result := mergeMariaDB(base, override)
	if result.Host != "h2" {
		t.Errorf("host = %q", result.Host)
	}
	if result.Database != "d2" {
		t.Errorf("database = %q", result.Database)
	}
}
