package config

import (
	"testing"
)

func TestParseMongoDBURL_Full(t *testing.T) {
	cfg, err := ParseMongoDBURL("mongodb://admin:pass@mongohost:27018/mydb?authSource=custom")
	if err != nil {
		t.Fatalf("ParseMongoDBURL: %v", err)
	}
	if cfg.Host != "mongohost" {
		t.Errorf("host = %q", cfg.Host)
	}
	if cfg.Port != 27018 {
		t.Errorf("port = %d", cfg.Port)
	}
	if cfg.Username != "admin" {
		t.Errorf("username = %q", cfg.Username)
	}
	if cfg.Password != "pass" {
		t.Errorf("password = %q", cfg.Password)
	}
	if cfg.Database != "mydb" {
		t.Errorf("database = %q", cfg.Database)
	}
	if cfg.AuthSource != "custom" {
		t.Errorf("auth_source = %q", cfg.AuthSource)
	}
}

func TestParseMongoDBURL_SRV(t *testing.T) {
	cfg, err := ParseMongoDBURL("mongodb+srv://cluster.example.com/mydb")
	if err != nil {
		t.Fatalf("ParseMongoDBURL: %v", err)
	}
	if cfg.Host != "cluster.example.com" {
		t.Errorf("host = %q", cfg.Host)
	}
}

func TestParseMongoDBURL_BadScheme(t *testing.T) {
	_, err := ParseMongoDBURL("redis://localhost")
	if err == nil {
		t.Fatal("expected error for bad scheme")
	}
}

func TestMongoDBConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     MongoDBConfig
		wantErr bool
	}{
		{name: "valid", cfg: MongoDBConfig{Host: "localhost", Port: 27017, Database: "db"}},
		{name: "empty host", cfg: MongoDBConfig{Host: "", Port: 27017, Database: "db"}, wantErr: true},
		{name: "bad port", cfg: MongoDBConfig{Host: "localhost", Port: 0, Database: "db"}, wantErr: true},
		{name: "empty database", cfg: MongoDBConfig{Host: "localhost", Port: 27017, Database: ""}, wantErr: true},
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

func TestMongoDBConfig_Address(t *testing.T) {
	cfg := MongoDBConfig{Host: "mongo.example.com", Port: 27018}
	if got := cfg.Address(); got != "mongo.example.com:27018" {
		t.Errorf("Address() = %q", got)
	}
}

func TestDefaultMongoDBConfig(t *testing.T) {
	cfg := DefaultMongoDBConfig()
	if cfg.Port != 27017 {
		t.Errorf("default port = %d", cfg.Port)
	}
	if cfg.AuthSource != "admin" {
		t.Errorf("default auth_source = %q", cfg.AuthSource)
	}
}

func TestMergeMongoDB(t *testing.T) {
	base := MongoDBConfig{Host: "h1", Port: 27017, AuthSource: "admin"}
	override := MongoDBConfig{Host: "h2", Database: "d2", AuthSource: "custom"}
	result := mergeMongoDB(base, override)
	if result.Host != "h2" {
		t.Errorf("host = %q", result.Host)
	}
	if result.Database != "d2" {
		t.Errorf("database = %q", result.Database)
	}
	if result.AuthSource != "custom" {
		t.Errorf("auth_source = %q", result.AuthSource)
	}
}
