package transform

import (
	"fmt"
	"sync"
)

// pairKey uniquely identifies a source→destination transformer pair.
type pairKey struct {
	src string
	dst string
}

var (
	regMu sync.RWMutex
	reg   = make(map[pairKey]func() Transformer)
)

// RegisterTransformer registers a transformer constructor for a source→destination pair.
func RegisterTransformer(srcProvider, dstProvider string, ctor func() Transformer) {
	regMu.Lock()
	defer regMu.Unlock()

	key := pairKey{src: srcProvider, dst: dstProvider}
	if _, exists := reg[key]; exists {
		panic(fmt.Sprintf("transformer for %s→%s already registered", srcProvider, dstProvider))
	}
	reg[key] = ctor
}

// GetTransformer returns a transformer for the given pair, or a NoopTransformer
// if no specific transformer is registered (same-database migration).
// If the transformer implements ConfigurableTransformer, cfg is injected
// automatically.
func GetTransformer(srcProvider, dstProvider string, cfg TransformerConfig) Transformer {
	regMu.RLock()
	ctor, ok := reg[pairKey{src: srcProvider, dst: dstProvider}]
	regMu.RUnlock()

	if !ok {
		return NoopTransformer{}
	}
	t := ctor()
	if ct, ok := t.(ConfigurableTransformer); ok {
		ct.Configure(cfg)
	}
	return t
}

// HasTransformer returns true if a transformer is registered for the given pair.
func HasTransformer(srcProvider, dstProvider string) bool {
	regMu.RLock()
	_, ok := reg[pairKey{src: srcProvider, dst: dstProvider}]
	regMu.RUnlock()
	return ok
}
