package transform

import (
	"context"

	"github.com/pageton/bridge-db/pkg/provider"
)

func init() {
	RegisterTransformer("mongodb", "postgres", func() Transformer {
		return &mongoDBToPostgresTransformer{}
	})
}

type mongoDBToPostgresTransformer struct {
	cfg TransformerConfig
}

func (t *mongoDBToPostgresTransformer) Transform(_ context.Context, units []provider.MigrationUnit) ([]provider.MigrationUnit, error) {
	return MongoDBToSQL(units, SQLEnvelopeConfig{SchemaName: "public"}, &t.cfg)
}

func (t *mongoDBToPostgresTransformer) NeedsSchema() bool               { return false }
func (t *mongoDBToPostgresTransformer) SetSchema(_ *provider.Schema)    {}
func (t *mongoDBToPostgresTransformer) Configure(cfg TransformerConfig) { t.cfg = cfg }
