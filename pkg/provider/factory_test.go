package provider

import (
	"sync"
	"testing"
)

func TestAvailableProviders_DeterministicOrder(t *testing.T) {
	// Register several providers concurrently to maximise the chance of
	// non-deterministic map iteration order.
	var wg sync.WaitGroup
	names := []string{"zebra", "alpha", "mango", "bravo", "charlie"}
	for _, n := range names {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			Register(name, func() Provider { return nil })
		}(n)
	}
	wg.Wait()

	// Call multiple times — every invocation must return the same order.
	var prev []string
	for i := range 10 {
		got := AvailableProviders()
		if prev != nil {
			if len(got) != len(prev) {
				t.Fatalf("iteration %d: length mismatch %d vs %d", i, len(got), len(prev))
			}
			for j := range got {
				if got[j] != prev[j] {
					t.Fatalf("iteration %d: order mismatch at index %d: %q vs %q", i, j, got[j], prev[j])
				}
			}
		}
		prev = got
	}

	// Verify sorted order.
	for i := 1; i < len(prev); i++ {
		if prev[i-1] > prev[i] {
			t.Fatalf("not sorted: %q > %q at index %d", prev[i-1], prev[i], i)
		}
	}

	// Clean up registry so other tests aren't affected.
	registryMu.Lock()
	for _, n := range names {
		delete(registry, n)
	}
	registryMu.Unlock()
}
