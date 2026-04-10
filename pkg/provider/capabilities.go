package provider

import "fmt"

// Capabilities describes what a provider supports.
// Providers declare their capabilities via the Cap() method so the pipeline
// can adapt its behaviour — skipping schema migration when either side lacks
// schema support, selecting the best verification strategy, warning when
// transactions or incremental migration are unavailable, etc.
type Capabilities struct {
	// Schema is true when the provider can inspect and create DDL schemas
	// (tables, columns, indexes). SQL databases set this to true; Redis
	// sets it to false. MongoDB sets it to true for index migration.
	Schema bool

	// Transactions is true when the provider supports multi-statement
	// transactions with rollback. Used to decide whether to wrap batch
	// writes in a transaction and whether FK deferral is meaningful.
	Transactions bool

	// Verification describes the verification level a provider supports.
	//   "none"     — no verification possible
	//   "basic"    — destination-only count check (via Verifier interface)
	//   "cross"    — full source-vs-destination with count, sampling, and
	//                checksum comparison (implements TableEnumerator,
	//                VerifyReader, and Checksummer)
	Verification VerificationLevel

	// Incremental is true when the provider supports resuming from a
	// checkpoint (resume tokens, cursor-based scanning). All providers
	// set this to true; the pipeline uses it to decide whether to
	// record per-table progress.
	Incremental bool
}

// VerificationLevel classifies how deeply a provider can verify data.
type VerificationLevel string

const (
	// VerifyNone means the provider cannot verify migrated data.
	VerifyNone VerificationLevel = "none"

	// VerifyBasic means the provider supports destination-only checks.
	VerifyBasic VerificationLevel = "basic"

	// VerifyCross means the provider supports full cross-database
	// verification (table enumeration, record reading, checksums).
	VerifyCross VerificationLevel = "cross"
)

// String returns a human-readable summary of all capabilities.
func (c Capabilities) String() string {
	return fmt.Sprintf("schema=%v tx=%v verify=%s incremental=%v",
		c.Schema, c.Transactions, c.Verification, c.Incremental)
}

// SupportsVerification returns true if the provider can verify at the given
// level or above. VerifyCross satisfies VerifyBasic.
func (c Capabilities) SupportsVerification(min VerificationLevel) bool {
	if c.Verification == VerifyNone {
		return false
	}
	if min == VerifyBasic {
		return c.Verification == VerifyBasic || c.Verification == VerifyCross
	}
	return c.Verification == min
}

// EffectiveVerifyLevel returns the highest verification level achievable
// given both source and destination capabilities. Cross-verification
// requires both sides to support it.
func EffectiveVerifyLevel(src, dst Capabilities) VerificationLevel {
	if src.Verification == VerifyCross && dst.Verification == VerifyCross {
		return VerifyCross
	}
	if src.SupportsVerification(VerifyBasic) && dst.SupportsVerification(VerifyBasic) {
		return VerifyBasic
	}
	return VerifyNone
}

// SupportsSchemaMigration returns true if both providers support schema
// inspection/creation, meaning the pipeline can run the schema migration
// phase.
func SupportsSchemaMigration(src, dst Capabilities) bool {
	return src.Schema && dst.Schema
}

// CapableProvider is an optional interface that providers implement to
// declare their capabilities. If a provider does not implement this
// interface, the pipeline infers capabilities from the provider name
// and the return values of SchemaMigrator/Verifier (the legacy behaviour).
type CapableProvider interface {
	Provider
	Capabilities() Capabilities
}

// ProviderCapabilities calls Capabilities() if the provider implements
// CapableProvider, otherwise falls back to inference from the legacy
// optional interfaces.
func ProviderCapabilities(p Provider) Capabilities {
	if cp, ok := p.(CapableProvider); ok {
		return cp.Capabilities()
	}
	return inferCapabilities(p)
}

// inferCapabilities builds a Capabilities struct by probing the legacy
// optional interfaces. This provides backwards compatibility for providers
// that have not yet been updated to implement CapableProvider.
func inferCapabilities(p Provider) Capabilities {
	cap := Capabilities{
		Incremental:  true, // assumed for all providers
		Verification: VerifyNone,
	}

	// Probe verification support
	if _, ok := p.(TableEnumerator); ok {
		cap.Verification = VerifyCross
	} else {
		cap.Verification = VerifyBasic
	}

	// Probe schema support — determined by provider name since
	// SchemaMigrator requires a live connection.
	switch p.Name() {
	case "postgres", "mysql", "mariadb", "cockroachdb", "mssql", "sqlite":
		cap.Schema = true
		cap.Transactions = true
	case "mongodb":
		cap.Schema = true // index migration
		cap.Transactions = true
	case "redis":
		cap.Schema = false
		cap.Transactions = false
	}

	return cap
}

// knownCapabilities is the static capability table for all built-in providers.
// Used by KnownCapabilities for pre-connect validation and dry-run preview.
var knownCapabilities = map[string]Capabilities{
	"postgres":    {Schema: true, Transactions: true, Verification: VerifyCross, Incremental: true},
	"mysql":       {Schema: true, Transactions: true, Verification: VerifyCross, Incremental: true},
	"mariadb":     {Schema: true, Transactions: true, Verification: VerifyCross, Incremental: true},
	"cockroachdb": {Schema: true, Transactions: true, Verification: VerifyCross, Incremental: true},
	"mssql":       {Schema: true, Transactions: true, Verification: VerifyCross, Incremental: true},
	"sqlite":      {Schema: true, Transactions: false, Verification: VerifyCross, Incremental: true},
	"mongodb":     {Schema: true, Transactions: true, Verification: VerifyCross, Incremental: true},
	"redis":       {Schema: false, Transactions: false, Verification: VerifyCross, Incremental: true},
}

// KnownCapabilities returns the static capabilities for a named provider.
// Returns a zero-value Capabilities (all disabled) for unknown providers.
func KnownCapabilities(name string) Capabilities {
	if c, ok := knownCapabilities[name]; ok {
		return c
	}
	return Capabilities{}
}
