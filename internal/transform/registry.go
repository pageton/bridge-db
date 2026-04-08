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
func GetTransformer(srcProvider, dstProvider string) Transformer {
	regMu.RLock()
	ctor, ok := reg[pairKey{src: srcProvider, dst: dstProvider}]
	regMu.RUnlock()

	if !ok {
		return NoopTransformer{}
	}
	return ctor()
}
