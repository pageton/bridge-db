package config

import (
	"testing"
)

func TestParsePostgresURL_Full(t *testing.T) {
	cfg, err := ParsePostgresURL("postgres://admin:secret@dbhost:5433/mydb?sslmode=disable")
	if err != nil {
		t.Fatalf("ParsePostgresURL: %v", err)
	}
	if cfg.Host != "dbhost" {
		t.Errorf("host = %q", cfg.Host)
	}
	if cfg.Port != 5433 {
		t.Errorf("port = %d", cfg.Port)
	}
	if cfg.Username != "admin" {
		t.Errorf("username = %q", cfg.Username)
	}
	if cfg.Password != "secret" {
		t.Errorf("password = %q", cfg.Password)
	}
	if cfg.Database != "mydb" {
		t.Errorf("database = %q", cfg.Database)
	}
	if cfg.SSLMode != "disable" {
		t.Errorf("sslmode = %q", cfg.SSLMode)
	}
}

func TestParsePostgresURL_PostgresqlScheme(t *testing.T) {
	cfg, err := ParsePostgresURL("postgresql://localhost/testdb")
	if err != nil {
		t.Fatalf("ParsePostgresURL: %v", err)
	}
	if cfg.Database != "testdb" {
		t.Errorf("database = %q", cfg.Database)
	}
}

func TestParsePostgresURL_BadScheme(t *testing.T) {
	_, err := ParsePostgresURL("mysql://localhost/db")
	if err == nil {
		t.Fatal("expected error for bad scheme")
	}
}

func TestPostgresConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     PostgresConfig
		wantErr bool
	}{
		{name: "valid", cfg: PostgresConfig{Host: "localhost", Port: 5432, Database: "mydb"}},
		{name: "empty host", cfg: PostgresConfig{Host: "", Port: 5432, Database: "mydb"}, wantErr: true},
		{name: "empty database", cfg: PostgresConfig{Host: "localhost", Port: 5432, Database: ""}, wantErr: true},
		{name: "bad port", cfg: PostgresConfig{Host: "localhost", Port: 0, Database: "mydb"}, wantErr: true},
		{name: "unix socket no port needed", cfg: PostgresConfig{Host: "/run/postgresql", Database: "mydb"}},
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

func TestPostgresConfig_DSN_TCP(t *testing.T) {
	cfg := PostgresConfig{Host: "db.example.com", Port: 5432, Username: "u", Password: "p", Database: "d", SSLMode: "require"}
	dsn := cfg.DSN()
	if dsn != "host=db.example.com port=5432 user=u password=xxxxx dbname=d sslmode=require" {
		t.Errorf("DSN() = %q", dsn)
	}
}

func TestPostgresConfig_DSN_UnixSocket(t *testing.T) {
	cfg := PostgresConfig{Host: "/run/pg", Username: "u", Password: "p", Database: "d", SSLMode: "disable"}
	dsn := cfg.DSN()
	if dsn != "host=/run/pg user=u password=xxxxx dbname=d sslmode=disable" {
		t.Errorf("DSN() = %q", dsn)
	}
}

func TestPostgresConfig_Address(t *testing.T) {
	cfg := PostgresConfig{Host: "db.example.com", Port: 5433}
	if got := cfg.Address(); got != "db.example.com:5433" {
		t.Errorf("Address() = %q", got)
	}
}

func TestParsePostgresURL_QueryParams(t *testing.T) {
	cfg, err := ParsePostgresURL("postgresql:///mydb?host=/run/postgresql&user=custom&sslmode=require")
	if err != nil {
		t.Fatalf("ParsePostgresURL: %v", err)
	}
	if cfg.Host != "/run/postgresql" {
		t.Errorf("host = %q", cfg.Host)
	}
	if cfg.Username != "custom" {
		t.Errorf("username = %q", cfg.Username)
	}
	if cfg.SSLMode != "require" {
		t.Errorf("sslmode = %q", cfg.SSLMode)
	}
	if cfg.Database != "mydb" {
		t.Errorf("database = %q", cfg.Database)
	}
}

func TestMergePostgres(t *testing.T) {
	base := PostgresConfig{Host: "h1", Port: 5432, Username: "u1", SSLMode: "prefer"}
	override := PostgresConfig{Host: "h2", SSLMode: "require"}
	result := mergeStruct(base, override)
	if result.Host != "h2" {
		t.Errorf("host = %q", result.Host)
	}
	if result.Port != 5432 {
		t.Errorf("port = %d, want 5432 (not overridden)", result.Port)
	}
	if result.SSLMode != "require" {
		t.Errorf("sslmode = %q", result.SSLMode)
	}
}
