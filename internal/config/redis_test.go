package config

import (
	"testing"
)

func TestParseRedisURL_Full(t *testing.T) {
	cfg, err := ParseRedisURL("redis://user:pass@remotehost:6380/3")
	if err != nil {
		t.Fatalf("ParseRedisURL: %v", err)
	}
	if cfg.Host != "remotehost" {
		t.Errorf("host = %q, want remotehost", cfg.Host)
	}
	if *cfg.Port != 6380 {
		t.Errorf("port = %d, want 6380", *cfg.Port)
	}
	if cfg.Username != "user" {
		t.Errorf("username = %q", cfg.Username)
	}
	if cfg.Password != "pass" {
		t.Errorf("password = %q", cfg.Password)
	}
	if *cfg.DB != 3 {
		t.Errorf("db = %d, want 3", *cfg.DB)
	}
}

func TestParseRedisURL_TLS(t *testing.T) {
	cfg, err := ParseRedisURL("rediss://localhost:6379/0")
	if err != nil {
		t.Fatalf("ParseRedisURL: %v", err)
	}
	if !*cfg.TLS {
		t.Error("expected TLS = true for rediss://")
	}
}

func TestParseRedisURL_Minimal(t *testing.T) {
	cfg, err := ParseRedisURL("redis://localhost")
	if err != nil {
		t.Fatalf("ParseRedisURL: %v", err)
	}
	if cfg.Host != "localhost" {
		t.Errorf("host = %q", cfg.Host)
	}
	if *cfg.Port != 6379 {
		t.Errorf("port = %d, want default 6379", *cfg.Port)
	}
}

func TestParseRedisURL_BadScheme(t *testing.T) {
	_, err := ParseRedisURL("http://localhost:6379")
	if err == nil {
		t.Fatal("expected error for bad scheme")
	}
}

func TestRedisConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     RedisConfig
		wantErr bool
	}{
		{name: "valid", cfg: RedisConfig{Host: "localhost", Port: IntPtr(6379), DB: IntPtr(0)}},
		{name: "empty host", cfg: RedisConfig{Host: "", Port: IntPtr(6379), DB: IntPtr(0)}, wantErr: true},
		{name: "zero port", cfg: RedisConfig{Host: "localhost", Port: IntPtr(0), DB: IntPtr(0)}, wantErr: true},
		{name: "port too high", cfg: RedisConfig{Host: "localhost", Port: IntPtr(70000), DB: IntPtr(0)}, wantErr: true},
		{name: "db negative", cfg: RedisConfig{Host: "localhost", Port: IntPtr(6379), DB: IntPtr(-1)}, wantErr: true},
		{name: "db too high", cfg: RedisConfig{Host: "localhost", Port: IntPtr(6379), DB: IntPtr(16)}, wantErr: true},
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

func TestRedisConfig_Address(t *testing.T) {
	cfg := RedisConfig{Host: "10.0.0.1", Port: IntPtr(6380)}
	if got := cfg.Address(); got != "10.0.0.1:6380" {
		t.Errorf("Address() = %q", got)
	}
}

func TestMergeRedis_Overrides(t *testing.T) {
	base := RedisConfig{Host: "h1", Port: IntPtr(6379), Username: "u1", Password: "p1", DB: IntPtr(0)}
	override := RedisConfig{Host: "h2", Port: IntPtr(6380), DB: IntPtr(5), TLS: BoolPtr(true)}
	result := mergeStruct(base, override)
	if result.Host != "h2" {
		t.Errorf("host = %q, want h2", result.Host)
	}
	if *result.Port != 6380 {
		t.Errorf("port = %d, want 6380", *result.Port)
	}
	if result.Username != "u1" {
		t.Errorf("username = %q, want u1 (not overridden)", result.Username)
	}
	if result.Password != "p1" {
		t.Errorf("password = %q, want p1 (not overridden)", result.Password)
	}
	if *result.DB != 5 {
		t.Errorf("db = %d, want 5", *result.DB)
	}
	if !*result.TLS {
		t.Error("TLS should be true")
	}
}

func TestResolveRedis_WithOverride(t *testing.T) {
	c := ConnectionConfig{
		Provider: "redis",
		URL:      "redis://localhost:6379/0",
		Redis:    &RedisConfig{Host: "override-host"},
	}
	if err := c.Resolve(); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if c.Redis.Host != "override-host" {
		t.Errorf("host = %q, want override-host", c.Redis.Host)
	}
}

func TestResolveRedis_NoURL(t *testing.T) {
	c := ConnectionConfig{
		Provider: "redis",
	}
	if err := c.Resolve(); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if c.Redis == nil {
		t.Fatal("Redis config should be non-nil after resolve")
	}
	if c.Redis.Host != "127.0.0.1" {
		t.Errorf("default host = %q", c.Redis.Host)
	}
}
