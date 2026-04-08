package provider

import (
	"fmt"
	"sync"
)

// registry maps provider names to constructor functions.
var (
	registryMu sync.RWMutex
	registry   = make(map[string]func() Provider)
)

// Register adds a provider constructor under the given name.
// It panics if a provider is registered twice with the same name.
func Register(name string, ctor func() Provider) {
	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("provider %q already registered", name))
	}
	registry[name] = ctor
}

// New creates a Provider by name. Returns an error if the name is unknown.
func New(name string) (Provider, error) {
	registryMu.RLock()
	ctor, ok := registry[name]
	registryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown provider %q (available: %v)", name, AvailableProviders())
	}
	return ctor(), nil
}

// AvailableProviders returns the names of all registered providers.
func AvailableProviders() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}
