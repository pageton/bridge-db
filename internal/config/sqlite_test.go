package config

import (
	"testing"
)

func TestParseSQLiteURL_HostAndPath(t *testing.T) {
	cfg, err := ParseSQLiteURL("sqlite:///data/myapp.db")
	if err != nil {
		t.Fatalf("ParseSQLiteURL: %v", err)
	}
	// url.Parse returns Host="" and Path="/data/myapp.db" for sqlite:///data/myapp.db
	// The parser joins host+path and strips leading "//"
	// For triple-slash URLs, result is "/data/myapp.db" (absolute path)
	if cfg.Path != "/data/myapp.db" {
		t.Errorf("path = %q, want /data/myapp.db", cfg.Path)
	}
}

func TestParseSQLiteURL_Opaque(t *testing.T) {
	cfg, err := ParseSQLiteURL("sqlite:test.db")
	if err != nil {
		t.Fatalf("ParseSQLiteURL: %v", err)
	}
	if cfg.Path != "test.db" {
		t.Errorf("path = %q", cfg.Path)
	}
}

func TestParseSQLiteURL_EmptyPath(t *testing.T) {
	_, err := ParseSQLiteURL("sqlite:")
	if err == nil {
		t.Fatal("expected error for empty path")
	}
}

func TestParseSQLiteURL_BadScheme(t *testing.T) {
	_, err := ParseSQLiteURL("mysql://localhost/db")
	if err == nil {
		t.Fatal("expected error for bad scheme")
	}
}

func TestSQLiteConfig_Validate(t *testing.T) {
	if err := (SQLiteConfig{Path: "test.db"}).Validate(); err != nil {
		t.Errorf("valid config: %v", err)
	}
	if err := (SQLiteConfig{Path: ""}).Validate(); err == nil {
		t.Error("expected error for empty path")
	}
}

func TestDefaultSQLiteConfig(t *testing.T) {
	cfg := DefaultSQLiteConfig()
	if cfg.Path != "test.db" {
		t.Errorf("default path = %q", cfg.Path)
	}
}

func TestMergeSQLite(t *testing.T) {
	base := SQLiteConfig{Path: "old.db"}
	override := SQLiteConfig{Path: "new.db"}
	result := mergeSQLite(base, override)
	if result.Path != "new.db" {
		t.Errorf("path = %q", result.Path)
	}
}

func TestMergeSQLite_NoOverride(t *testing.T) {
	base := SQLiteConfig{Path: "old.db"}
	override := SQLiteConfig{}
	result := mergeSQLite(base, override)
	if result.Path != "old.db" {
		t.Errorf("path = %q, want old.db when override empty", result.Path)
	}
}
