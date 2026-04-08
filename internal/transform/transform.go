// Package transform defines the cross-database transformation interfaces.
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

// NoopTransformer is a passthrough transformer used for same-database migrations.
type NoopTransformer struct{}

func (NoopTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return units, nil
}
func (NoopTransformer) NeedsSchema() bool            { return false }
func (NoopTransformer) SetSchema(_ *provider.Schema) {}
