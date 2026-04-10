package config

import "testing"

func TestProviderConfigWithTunnel_PreservesPostgresFields(t *testing.T) {
	conn := &ConnectionConfig{
		Provider: "postgres",
		Postgres: &PostgresConfig{
			Host:     "db.internal",
			Port:     5432,
			Username: "bridge",
			Password: "secret",
			Database: "app",
			SSLMode:  "disable",
		},
	}

	raw, err := ProviderConfigWithTunnel(conn, "127.0.0.1:15432")
	if err != nil {
		t.Fatalf("ProviderConfigWithTunnel() error = %v", err)
	}

	got, ok := raw.(*PostgresConfig)
	if !ok {
		t.Fatalf("ProviderConfigWithTunnel() type = %T, want *PostgresConfig", raw)
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

	if conn.Postgres.Host != "db.internal" || conn.Postgres.Port != 5432 {
		t.Fatal("ProviderConfigWithTunnel() mutated original postgres config")
	}
}

func TestProviderConfigWithTunnel_PreservesMongoDBFields(t *testing.T) {
	conn := &ConnectionConfig{
		Provider: "mongodb",
		MongoDB: &MongoDBConfig{
			Host:       "mongo.internal",
			Port:       27017,
			Username:   "bridge",
			Password:   "secret",
			Database:   "app",
			AuthSource: "admin",
			TLS:        true,
		},
	}

	raw, err := ProviderConfigWithTunnel(conn, "127.0.0.1:27018")
	if err != nil {
		t.Fatalf("ProviderConfigWithTunnel() error = %v", err)
	}

	got, ok := raw.(*MongoDBConfig)
	if !ok {
		t.Fatalf("ProviderConfigWithTunnel() type = %T, want *MongoDBConfig", raw)
	}

	if got.Host != "127.0.0.1" || got.Port != 27018 {
		t.Fatalf("tunneled address = %s:%d, want 127.0.0.1:27018", got.Host, got.Port)
	}
	if got.Username != "bridge" || got.Password != "secret" || got.Database != "app" {
		t.Fatalf("credentials/database were not preserved: %+v", *got)
	}
	if got.AuthSource != "admin" {
		t.Errorf("auth_source = %q, want admin", got.AuthSource)
	}
	if !got.TLS {
		t.Error("tls = false, want true")
	}
}

func TestProviderConfigWithTunnel_PreservesRedisFields(t *testing.T) {
	conn := &ConnectionConfig{
		Provider: "redis",
		Redis: &RedisConfig{
			Host:     "redis.internal",
			Port:     6379,
			Username: "bridge",
			Password: "secret",
			DB:       9,
			TLS:      true,
		},
	}

	raw, err := ProviderConfigWithTunnel(conn, "127.0.0.1:16379")
	if err != nil {
		t.Fatalf("ProviderConfigWithTunnel() error = %v", err)
	}

	got, ok := raw.(*RedisConfig)
	if !ok {
		t.Fatalf("ProviderConfigWithTunnel() type = %T, want *RedisConfig", raw)
	}

	if got.Host != "127.0.0.1" || got.Port != 16379 {
		t.Fatalf("tunneled address = %s:%d, want 127.0.0.1:16379", got.Host, got.Port)
	}
	if got.Username != "bridge" || got.Password != "secret" {
		t.Fatalf("credentials were not preserved: %+v", *got)
	}
	if got.DB != 9 {
		t.Errorf("db = %d, want 9", got.DB)
	}
	if !got.TLS {
		t.Error("tls = false, want true")
	}
}
