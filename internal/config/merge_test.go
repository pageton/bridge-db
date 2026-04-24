package config

import (
	"testing"
)

type optionalStringConfig struct {
	Host     string
	Password *string
}

func TestMergeStruct_BoolOverride_TrueToFalse(t *testing.T) {
	// Simulate: default Encrypt=true, override with Encrypt=false.
	base := MSSQLConfig{
		Host:      "server.local",
		Port:      IntPtr(1433),
		Username:  "sa",
		Database:  "mydb",
		Encrypt:   BoolPtr(true),
		TrustCert: BoolPtr(false),
	}

	// Override sets Encrypt to false. With value-type bool, IsZero() would
	// skip this field. With *bool, the non-nil pointer is copied correctly.
	override := MSSQLConfig{
		Encrypt: BoolPtr(false),
	}

	result := mergeStruct(base, override)

	if *result.Encrypt != false {
		t.Errorf("Encrypt = %v, want false (overriding true→false)", *result.Encrypt)
	}
	if result.Host != "server.local" {
		t.Errorf("Host = %q, want %q (should not be changed)", result.Host, "server.local")
	}
	if *result.Port != 1433 {
		t.Errorf("Port = %d, want 1433 (should not be changed)", *result.Port)
	}
	if result.Database != "mydb" {
		t.Errorf("Database = %q, want %q (should not be changed)", result.Database, "mydb")
	}
}

func TestMergeStruct_IntOverride_NonZeroToZero(t *testing.T) {
	// Simulate: default Port=1433, override with Port=0.
	base := MSSQLConfig{
		Host:      "server.local",
		Port:      IntPtr(1433),
		Username:  "sa",
		Database:  "mydb",
		Encrypt:   BoolPtr(true),
		TrustCert: BoolPtr(false),
	}

	override := MSSQLConfig{
		Port: IntPtr(0),
	}

	result := mergeStruct(base, override)

	if *result.Port != 0 {
		t.Errorf("Port = %d, want 0 (overriding 1433→0)", *result.Port)
	}
	if *result.Encrypt != true {
		t.Errorf("Encrypt = %v, want true (should not be changed)", *result.Encrypt)
	}
}

func TestMergeStruct_PartialOverride(t *testing.T) {
	// Only some fields are overridden; the rest should stay unchanged.
	base := RedisConfig{
		Host:     "redis.prod",
		Port:     IntPtr(6379),
		Password: "secret",
		DB:       IntPtr(2),
		TLS:      BoolPtr(true),
	}

	// Override only Host and DB. Port, Password, TLS should be unchanged.
	override := RedisConfig{
		Host: "redis.staging",
		DB:   IntPtr(0),
	}

	result := mergeStruct(base, override)

	if result.Host != "redis.staging" {
		t.Errorf("Host = %q, want redis.staging", result.Host)
	}
	if *result.Port != 6379 {
		t.Errorf("Port = %d, want 6379 (not overridden)", *result.Port)
	}
	if result.Password != "secret" {
		t.Errorf("Password = %q, want secret (not overridden)", result.Password)
	}
	if *result.DB != 0 {
		t.Errorf("DB = %d, want 0 (overriding 2→0)", *result.DB)
	}
	if *result.TLS != true {
		t.Errorf("TLS = %v, want true (not overridden)", *result.TLS)
	}
}

func TestMergeStruct_BoolOverride_FalseToTrue(t *testing.T) {
	base := RedisConfig{
		Host: "127.0.0.1",
		Port: IntPtr(6379),
		DB:   IntPtr(0),
		TLS:  BoolPtr(false),
	}

	override := RedisConfig{
		TLS: BoolPtr(true),
	}

	result := mergeStruct(base, override)

	if *result.TLS != true {
		t.Errorf("TLS = %v, want true (overriding false→true)", *result.TLS)
	}
}

func TestMergeStruct_StringPointerOverride_NonEmptyToEmpty(t *testing.T) {
	base := optionalStringConfig{
		Host:     "db.local",
		Password: StringPtr("secret"),
	}

	override := optionalStringConfig{
		Password: StringPtr(""),
	}

	result := mergeStruct(base, override)

	if result.Password == nil {
		t.Fatal("Password = nil, want non-nil empty-string override")
	}
	if *result.Password != "" {
		t.Errorf("Password = %q, want empty string", *result.Password)
	}
	if result.Host != "db.local" {
		t.Errorf("Host = %q, want %q (should not be changed)", result.Host, "db.local")
	}
}

func TestMergeStruct_NilOverrideFieldsSkipped(t *testing.T) {
	// Override with a zero-value struct — all pointer fields are nil,
	// so nothing should be copied.
	base := PostgresConfig{
		Host:     "db.local",
		Port:     IntPtr(5432),
		Username: "admin",
		Database: "app",
		SSLMode:  "require",
	}

	// Empty override — Port is nil (not provided).
	override := PostgresConfig{}

	result := mergeStruct(base, override)

	if *result.Port != 5432 {
		t.Errorf("Port = %d, want 5432 (nil override should not change)", *result.Port)
	}
	if result.Host != "db.local" {
		t.Errorf("Host = %q, want db.local (empty string override should not change)", result.Host)
	}
}

func TestResolve_OverrideEncryptFalse(t *testing.T) {
	// End-to-end test: resolve MSSQL config with Encrypt=false override.
	cfg := &ConnectionConfig{
		Provider: "mssql",
		URL:      "mssql://sa:pass@localhost:1433/mydb",
		MSSQL: &MSSQLConfig{
			Encrypt: BoolPtr(false),
		},
	}

	if err := cfg.Resolve(); err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	if *cfg.MSSQL.Encrypt != false {
		t.Errorf("Encrypt = %v, want false (URL default is true, override to false)", *cfg.MSSQL.Encrypt)
	}
}

func TestResolve_OverridePortZero(t *testing.T) {
	// End-to-end test: resolve MySQL config with Port=0 for Unix socket.
	cfg := &ConnectionConfig{
		Provider: "mysql",
		URL:      "mysql://root:pass@localhost:3306/mydb",
		MySQL: &MySQLConfig{
			Port: IntPtr(0),
			Host: "/var/run/mysqld/mysqld.sock",
		},
	}

	if err := cfg.Resolve(); err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}

	if *cfg.MySQL.Port != 0 {
		t.Errorf("Port = %d, want 0 (overriding 3306→0 for Unix socket)", *cfg.MySQL.Port)
	}
	if cfg.MySQL.Host != "/var/run/mysqld/mysqld.sock" {
		t.Errorf("Host = %q, want socket path", cfg.MySQL.Host)
	}
}
