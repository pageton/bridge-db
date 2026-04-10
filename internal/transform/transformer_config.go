package transform

import "sync"

// TransformerConfig carries runtime configuration into transformers.
type TransformerConfig struct {
	NullHandler  *NullHandler
	FieldMapping *FieldMappingApplier
	SrcDialect   Dialect
	DstDialect   Dialect
}

var (
	configMu     sync.RWMutex
	globalConfig TransformerConfig
)

// SetGlobalConfig sets the default TransformerConfig used by all transformers
// created via GetTransformer. Must be called before GetTransformer.
func SetGlobalConfig(cfg TransformerConfig) {
	configMu.Lock()
	defer configMu.Unlock()
	globalConfig = cfg
}

// GlobalConfig returns the current global transformer configuration.
func GlobalConfig() TransformerConfig {
	configMu.RLock()
	defer configMu.RUnlock()
	return globalConfig
}
