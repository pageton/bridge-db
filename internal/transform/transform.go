package transform

import (
	"context"

	"github.com/pageton/bridge-db/pkg/provider"
)

// Transformer converts MigrationUnits from one provider's format to another.
// Implementations are registered per source→destination pair.
type Transformer interface {
	// Transform converts a batch of units from source format to destination format.
	Transform(ctx context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error)

	// NeedsSchema returns true if the transformer requires schema information
	// to perform its conversion (e.g. SQL type mapping).
	NeedsSchema() bool

	// SetSchema provides the source schema for transformers that need it.
	SetSchema(schema *provider.Schema)
}

// IsNoopTransformer returns true if the transformer is a no-op passthrough
// (same-database migration where no data conversion is needed).
// Used by the pipeline to skip the transform phase.
func IsNoopTransformer(t Transformer) bool {
	_, ok := t.(NoopTransformer)
	return ok
}

// NoopTransformer is a passthrough transformer used for same-database migrations.
type NoopTransformer struct{}

func (NoopTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return units, nil
}
func (NoopTransformer) NeedsSchema() bool            { return false }
func (NoopTransformer) SetSchema(_ *provider.Schema) {}

// ConfigurableTransformer is an optional interface that transformers implement
// to receive runtime configuration (null policy, field mappings, dialect hints).
type ConfigurableTransformer interface {
	Transformer
	Configure(cfg TransformerConfig)
}

// TypeMapperProvider is an optional interface for transformers that provide
// type mapping for schema migration.
type TypeMapperProvider interface {
	Transformer
	TypeMapper() provider.TypeMapper
}
