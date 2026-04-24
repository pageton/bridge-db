package config

import (
	"testing"
)

func TestParseCockroachDBURL_Full(t *testing.T) {
	cfg, err := ParseCockroachDBURL("cockroachdb://root:pass@crhost:26258/bank?sslmode=disable")
	if err != nil {
		t.Fatalf("ParseCockroachDBURL: %v", err)
	}
	if cfg.Host != "crhost" {
		t.Errorf("host = %q", cfg.Host)
	}
	if *cfg.Port != 26258 {
		t.Errorf("port = %d", *cfg.Port)
	}
	if cfg.Username != "root" {
		t.Errorf("username = %q", cfg.Username)
	}
	if cfg.Password != "pass" {
		t.Errorf("password = %q", cfg.Password)
	}
	if cfg.Database != "bank" {
		t.Errorf("database = %q", cfg.Database)
	}
	if cfg.SSLMode != "disable" {
		t.Errorf("sslmode = %q", cfg.SSLMode)
	}
}

func TestParseCockroachDBURL_CRDBScheme(t *testing.T) {
	cfg, err := ParseCockroachDBURL("crdb://localhost:26257/testdb")
	if err != nil {
		t.Fatalf("ParseCockroachDBURL: %v", err)
	}
	if cfg.Database != "testdb" {
		t.Errorf("database = %q", cfg.Database)
	}
}

func TestParseCockroachDBURL_BadScheme(t *testing.T) {
	_, err := ParseCockroachDBURL("postgres://localhost/db")
	if err == nil {
		t.Fatal("expected error for bad scheme")
	}
}

func TestCockroachDBConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     CockroachDBConfig
		wantErr bool
	}{
		{name: "valid", cfg: CockroachDBConfig{Host: "localhost", Port: IntPtr(26257), Database: "db"}},
		{name: "empty host", cfg: CockroachDBConfig{Host: "", Port: IntPtr(26257), Database: "db"}, wantErr: true},
		{name: "empty database", cfg: CockroachDBConfig{Host: "localhost", Port: IntPtr(26257), Database: ""}, wantErr: true},
		{name: "bad port", cfg: CockroachDBConfig{Host: "localhost", Port: IntPtr(0), Database: "db"}, wantErr: true},
		{name: "unix socket", cfg: CockroachDBConfig{Host: "/run/crdb", Database: "db"}},
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

func TestCockroachDBConfig_DSN(t *testing.T) {
	cfg := CockroachDBConfig{Host: "crdb", Port: IntPtr(26257), Username: "u", Password: "p", Database: "d", SSLMode: "require"}
	dsn := cfg.DSN()
	if dsn != "host=crdb port=26257 user=u password=xxxxx dbname=d sslmode=require" {
		t.Errorf("DSN() = %q", dsn)
	}
}

func TestDefaultCockroachDBConfig(t *testing.T) {
	cfg := DefaultCockroachDBConfig()
	if *cfg.Port != 26257 {
		t.Errorf("default port = %d", *cfg.Port)
	}
	if cfg.Username != "root" {
		t.Errorf("default username = %q", cfg.Username)
	}
}

func TestMergeCockroachDB(t *testing.T) {
	base := CockroachDBConfig{Host: "h1", Port: IntPtr(26257), SSLMode: "prefer"}
	override := CockroachDBConfig{Host: "h2", Database: "d2", SSLMode: "require"}
	result := mergeStruct(base, override)
	if result.Host != "h2" {
		t.Errorf("host = %q", result.Host)
	}
	if result.Database != "d2" {
		t.Errorf("database = %q", result.Database)
	}
	if result.SSLMode != "require" {
		t.Errorf("sslmode = %q", result.SSLMode)
	}
}
