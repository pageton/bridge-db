package cli

import (
	"testing"

	"github.com/pageton/bridge-db/internal/config"
)

func TestResolveCliProviderConfig_WithTunnelPreservesProviderFields(t *testing.T) {
	conn := &config.ConnectionConfig{
		Provider: "postgres",
		Postgres: &config.PostgresConfig{
			Host:     "db.internal",
			Port:     5432,
			Username: "bridge",
			Password: "secret",
			Database: "app",
			SSLMode:  "disable",
		},
	}

	raw, err := resolveCliProviderConfig(conn, "127.0.0.1:15432")
	if err != nil {
		t.Fatalf("resolveCliProviderConfig() error = %v", err)
	}

	got, ok := raw.(*config.PostgresConfig)
	if !ok {
		t.Fatalf("resolveCliProviderConfig() type = %T, want *config.PostgresConfig", raw)
	}

	if got.Host != "127.0.0.1" || got.Port != 15432 {
		t.Fatalf("tunneled address = %s:%d, want 127.0.0.1:15432", got.Host, got.Port)
	}
	if got.Username != "bridge" {
		t.Errorf("username = %q, want bridge", got.Username)
	}
	if got.Password != "secret" {
		t.Errorf("password = %q, want secret", got.Password)
	}
	if got.Database != "app" {
		t.Errorf("database = %q, want app", got.Database)
	}
	if got.SSLMode != "disable" {
		t.Errorf("sslmode = %q, want disable", got.SSLMode)
	}
}
