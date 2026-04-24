package transform

// TransformerConfig carries runtime configuration into transformers.
type TransformerConfig struct {
	NullHandler  *NullHandler
	FieldMapping *FieldMappingApplier
	SrcDialect   Dialect
	DstDialect   Dialect
}
