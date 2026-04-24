package config

import (
	"testing"
)

func TestParseMSSQLURL_Full(t *testing.T) {
	cfg, err := ParseMSSQLURL("mssql://sa:pass@sqlhost:1434/mydb?encrypt=true&TrustServerCertificate=true")
	if err != nil {
		t.Fatalf("ParseMSSQLURL: %v", err)
	}
	if cfg.Host != "sqlhost" {
		t.Errorf("host = %q", cfg.Host)
	}
	if *cfg.Port != 1434 {
		t.Errorf("port = %d", *cfg.Port)
	}
	if cfg.Username != "sa" {
		t.Errorf("username = %q", cfg.Username)
	}
	if cfg.Password != "pass" {
		t.Errorf("password = %q", cfg.Password)
	}
	if cfg.Database != "mydb" {
		t.Errorf("database = %q", cfg.Database)
	}
	if !*cfg.Encrypt {
		t.Error("encrypt should be true")
	}
	if !*cfg.TrustCert {
		t.Error("trust_cert should be true")
	}
}

func TestParseMSSQLURL_SQLServerScheme(t *testing.T) {
	cfg, err := ParseMSSQLURL("sqlserver://localhost/testdb")
	if err != nil {
		t.Fatalf("ParseMSSQLURL: %v", err)
	}
	if cfg.Database != "testdb" {
		t.Errorf("database = %q", cfg.Database)
	}
}

func TestParseMSSQLURL_Instance(t *testing.T) {
	cfg, err := ParseMSSQLURL("mssql://sa@localhost/sqlexpress?instance=SQLEXPRESS")
	if err != nil {
		t.Fatalf("ParseMSSQLURL: %v", err)
	}
	if cfg.Instance != "SQLEXPRESS" {
		t.Errorf("instance = %q", cfg.Instance)
	}
}

func TestParseMSSQLURL_BadScheme(t *testing.T) {
	_, err := ParseMSSQLURL("postgres://localhost/db")
	if err == nil {
		t.Fatal("expected error for bad scheme")
	}
}

func TestMSSQLConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     MSSQLConfig
		wantErr bool
	}{
		{name: "valid", cfg: MSSQLConfig{Host: "localhost", Port: IntPtr(1433), Database: "db"}},
		{name: "empty host", cfg: MSSQLConfig{Host: "", Port: IntPtr(1433), Database: "db"}, wantErr: true},
		{name: "bad port", cfg: MSSQLConfig{Host: "localhost", Port: IntPtr(0), Database: "db"}, wantErr: true},
		{name: "empty database", cfg: MSSQLConfig{Host: "localhost", Port: IntPtr(1433), Database: ""}, wantErr: true},
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

func TestMSSQLConfig_Address(t *testing.T) {
	cfg := MSSQLConfig{Host: "sql.example.com", Port: IntPtr(1433)}
	if got := cfg.Address(); got != "sql.example.com:1433" {
		t.Errorf("Address() = %q", got)
	}
}

func TestDefaultMSSQLConfig(t *testing.T) {
	cfg := DefaultMSSQLConfig()
	if *cfg.Port != 1433 {
		t.Errorf("default port = %d", *cfg.Port)
	}
	if cfg.Username != "sa" {
		t.Errorf("default username = %q", cfg.Username)
	}
	if !*cfg.Encrypt {
		t.Error("default encrypt should be true")
	}
}

func TestMergeMSSQL(t *testing.T) {
	base := MSSQLConfig{Host: "h1", Port: IntPtr(1433), Username: "u1"}
	override := MSSQLConfig{Host: "h2", Database: "d2", Encrypt: BoolPtr(true), TrustCert: BoolPtr(true)}
	result := mergeStruct(base, override)
	if result.Host != "h2" {
		t.Errorf("host = %q", result.Host)
	}
	if result.Database != "d2" {
		t.Errorf("database = %q", result.Database)
	}
	if !*result.Encrypt {
		t.Error("encrypt should be true")
	}
	if !*result.TrustCert {
		t.Error("trust_cert should be true")
	}
	if result.Username != "u1" {
		t.Errorf("username = %q, want u1", result.Username)
	}
}
