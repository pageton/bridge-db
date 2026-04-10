package provider

import (
	"context"
	"testing"
)

// ---------------------------------------------------------------------------
// Capabilities struct helpers
// ---------------------------------------------------------------------------

func TestCapabilities_String(t *testing.T) {
	c := Capabilities{Schema: true, Transactions: true, Verification: VerifyCross, Incremental: true}
	s := c.String()
	if s == "" {
		t.Fatal("String() returned empty")
	}
}

func TestSupportsVerification(t *testing.T) {
	tests := []struct {
		name string
		cap  Capabilities
		min  VerificationLevel
		want bool
	}{
		{"cross supports cross", Capabilities{Verification: VerifyCross}, VerifyCross, true},
		{"cross supports basic", Capabilities{Verification: VerifyCross}, VerifyBasic, true},
		{"basic supports basic", Capabilities{Verification: VerifyBasic}, VerifyBasic, true},
		{"basic does not support cross", Capabilities{Verification: VerifyBasic}, VerifyCross, false},
		{"none supports nothing", Capabilities{Verification: VerifyNone}, VerifyBasic, false},
		{"none cross", Capabilities{Verification: VerifyNone}, VerifyCross, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cap.SupportsVerification(tt.min)
			if got != tt.want {
				t.Errorf("SupportsVerification(%s) = %v, want %v", tt.min, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// EffectiveVerifyLevel
// ---------------------------------------------------------------------------

func TestEffectiveVerifyLevel(t *testing.T) {
	tests := []struct {
		name string
		src  Capabilities
		dst  Capabilities
		want VerificationLevel
	}{
		{
			"both cross",
			Capabilities{Verification: VerifyCross},
			Capabilities{Verification: VerifyCross},
			VerifyCross,
		},
		{
			"src cross dst basic",
			Capabilities{Verification: VerifyCross},
			Capabilities{Verification: VerifyBasic},
			VerifyBasic,
		},
		{
			"both basic",
			Capabilities{Verification: VerifyBasic},
			Capabilities{Verification: VerifyBasic},
			VerifyBasic,
		},
		{
			"src none dst cross",
			Capabilities{Verification: VerifyNone},
			Capabilities{Verification: VerifyCross},
			VerifyNone,
		},
		{
			"both none",
			Capabilities{Verification: VerifyNone},
			Capabilities{Verification: VerifyNone},
			VerifyNone,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EffectiveVerifyLevel(tt.src, tt.dst)
			if got != tt.want {
				t.Errorf("EffectiveVerifyLevel() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SupportsSchemaMigration
// ---------------------------------------------------------------------------

func TestSupportsSchemaMigration(t *testing.T) {
	tests := []struct {
		name string
		src  Capabilities
		dst  Capabilities
		want bool
	}{
		{
			"both support schema",
			Capabilities{Schema: true},
			Capabilities{Schema: true},
			true,
		},
		{
			"src lacks schema",
			Capabilities{Schema: false},
			Capabilities{Schema: true},
			false,
		},
		{
			"dst lacks schema",
			Capabilities{Schema: true},
			Capabilities{Schema: false},
			false,
		},
		{
			"neither supports schema",
			Capabilities{Schema: false},
			Capabilities{Schema: false},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SupportsSchemaMigration(tt.src, tt.dst)
			if got != tt.want {
				t.Errorf("SupportsSchemaMigration() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// KnownCapabilities
// ---------------------------------------------------------------------------

func TestKnownCapabilities(t *testing.T) {
	tests := []struct {
		name       string
		provider   string
		wantSchema bool
		wantTx     bool
		wantVerify VerificationLevel
		wantIncr   bool
	}{
		{"postgres", "postgres", true, true, VerifyCross, true},
		{"mysql", "mysql", true, true, VerifyCross, true},
		{"mariadb", "mariadb", true, true, VerifyCross, true},
		{"cockroachdb", "cockroachdb", true, true, VerifyCross, true},
		{"mssql", "mssql", true, true, VerifyCross, true},
		{"sqlite", "sqlite", true, false, VerifyCross, true},
		{"mongodb", "mongodb", true, true, VerifyCross, true},
		{"redis", "redis", false, false, VerifyCross, true},
		{"unknown", "unknown", false, false, VerificationLevel(""), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := KnownCapabilities(tt.provider)
			if c.Schema != tt.wantSchema {
				t.Errorf("Schema = %v, want %v", c.Schema, tt.wantSchema)
			}
			if c.Transactions != tt.wantTx {
				t.Errorf("Transactions = %v, want %v", c.Transactions, tt.wantTx)
			}
			if c.Verification != tt.wantVerify {
				t.Errorf("Verification = %q, want %q", c.Verification, tt.wantVerify)
			}
			if c.Incremental != tt.wantIncr {
				t.Errorf("Incremental = %v, want %v", c.Incremental, tt.wantIncr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ProviderCapabilities with CapableProvider
// ---------------------------------------------------------------------------

type mockCapableProvider struct {
	caps Capabilities
}

func (m *mockCapableProvider) Name() string                                     { return "mock" }
func (m *mockCapableProvider) Connect(_ context.Context, _, _ any) error        { return nil }
func (m *mockCapableProvider) Close() error                                     { return nil }
func (m *mockCapableProvider) Ping(_ context.Context) error                     { return nil }
func (m *mockCapableProvider) Scanner(_ context.Context, _ ScanOptions) Scanner { return nil }
func (m *mockCapableProvider) Writer(_ context.Context, _ WriteOptions) Writer  { return nil }
func (m *mockCapableProvider) Verifier(_ context.Context) Verifier              { return nil }
func (m *mockCapableProvider) SchemaMigrator(_ context.Context) SchemaMigrator  { return nil }
func (m *mockCapableProvider) DryRun() Provider                                 { return m }
func (m *mockCapableProvider) Capabilities() Capabilities                       { return m.caps }

func TestProviderCapabilities_CapableProvider(t *testing.T) {
	expected := Capabilities{Schema: true, Transactions: false, Verification: VerifyBasic, Incremental: true}
	p := &mockCapableProvider{caps: expected}
	got := ProviderCapabilities(p)

	if got != expected {
		t.Errorf("ProviderCapabilities() = %v, want %v", got, expected)
	}
}

func TestProviderCapabilities_LegacyFallback(t *testing.T) {
	// A provider that doesn't implement CapableProvider should fall back
	// to inference from optional interfaces.
	var legacy Provider = &legacyProvider{}
	got := ProviderCapabilities(legacy)
	if got.Verification != VerifyCross {
		t.Errorf("expected VerifyCross from TableEnumerator, got %q", got.Verification)
	}
}

type legacyProvider struct{}

func (l *legacyProvider) Name() string                                     { return "postgres" }
func (l *legacyProvider) Connect(_ context.Context, _, _ any) error        { return nil }
func (l *legacyProvider) Close() error                                     { return nil }
func (l *legacyProvider) Ping(_ context.Context) error                     { return nil }
func (l *legacyProvider) Scanner(_ context.Context, _ ScanOptions) Scanner { return nil }
func (l *legacyProvider) Writer(_ context.Context, _ WriteOptions) Writer  { return nil }
func (l *legacyProvider) Verifier(_ context.Context) Verifier              { return nil }
func (l *legacyProvider) SchemaMigrator(_ context.Context) SchemaMigrator  { return nil }
func (l *legacyProvider) DryRun() Provider                                 { return l }
func (l *legacyProvider) EnumerateTables(_ context.Context) (map[string]int64, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// Cross-provider capability combinations
// ---------------------------------------------------------------------------

func TestCrossProviderVerifyLevel(t *testing.T) {
	// SQL to SQL: both cross => cross
	src := KnownCapabilities("postgres")
	dst := KnownCapabilities("mysql")
	if EffectiveVerifyLevel(src, dst) != VerifyCross {
		t.Error("postgres->mysql should support cross verification")
	}

	// SQL to Redis: both cross => cross (Redis supports TableEnumerator etc.)
	src = KnownCapabilities("postgres")
	dst = KnownCapabilities("redis")
	if EffectiveVerifyLevel(src, dst) != VerifyCross {
		t.Error("postgres->redis should support cross verification")
	}

	// MongoDB to Postgres: both cross => cross
	src = KnownCapabilities("mongodb")
	dst = KnownCapabilities("postgres")
	if EffectiveVerifyLevel(src, dst) != VerifyCross {
		t.Error("mongodb->postgres should support cross verification")
	}
}

func TestCrossProviderSchemaMigration(t *testing.T) {
	// SQL to SQL: both have schema => supported
	if !SupportsSchemaMigration(KnownCapabilities("postgres"), KnownCapabilities("mysql")) {
		t.Error("postgres->mysql should support schema migration")
	}
	if !SupportsSchemaMigration(KnownCapabilities("mysql"), KnownCapabilities("cockroachdb")) {
		t.Error("mysql->cockroachdb should support schema migration")
	}

	// Redis lacks schema => not supported
	if SupportsSchemaMigration(KnownCapabilities("redis"), KnownCapabilities("postgres")) {
		t.Error("redis->postgres should not support schema migration")
	}
	if SupportsSchemaMigration(KnownCapabilities("postgres"), KnownCapabilities("redis")) {
		t.Error("postgres->redis should not support schema migration")
	}

	// MongoDB has schema (index) => supported
	if !SupportsSchemaMigration(KnownCapabilities("postgres"), KnownCapabilities("mongodb")) {
		t.Error("postgres->mongodb should support schema migration")
	}
}
