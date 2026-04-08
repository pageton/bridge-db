package transform

import (
	"sync"
	"testing"
)

func TestRegisterAndGetTransformer(t *testing.T) {
	regMu.Lock()
	orig := reg
	reg = make(map[pairKey]func() Transformer)
	regMu.Unlock()
	t.Cleanup(func() {
		regMu.Lock()
		reg = orig
		regMu.Unlock()
	})

	RegisterTransformer("src", "dst", func() Transformer {
		return NoopTransformer{}
	})

	got := GetTransformer("src", "dst")
	if got == nil {
		t.Fatal("expected transformer, got nil")
	}
	if _, ok := got.(NoopTransformer); !ok {
		t.Error("expected NoopTransformer concrete type")
	}
}

func TestGetTransformer_UnregisteredPair(t *testing.T) {
	got := GetTransformer("nonexistent", "pair")
	if _, ok := got.(NoopTransformer); !ok {
		t.Error("unregistered pair should return NoopTransformer")
	}
}

func TestRegisterTransformer_DuplicatePanics(t *testing.T) {
	regMu.Lock()
	orig := reg
	reg = make(map[pairKey]func() Transformer)
	regMu.Unlock()
	t.Cleanup(func() {
		regMu.Lock()
		reg = orig
		regMu.Unlock()
	})

	RegisterTransformer("a", "b", func() Transformer { return NoopTransformer{} })

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on duplicate registration")
		}
	}()
	RegisterTransformer("a", "b", func() Transformer { return NoopTransformer{} })
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	regMu.Lock()
	orig := reg
	reg = make(map[pairKey]func() Transformer)
	regMu.Unlock()
	t.Cleanup(func() {
		regMu.Lock()
		reg = orig
		regMu.Unlock()
	})

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			RegisterTransformer("src"+string(rune(i)), "dst"+string(rune(i)), func() Transformer {
				return NoopTransformer{}
			})
		}(i)
		go func(i int) {
			defer wg.Done()
			GetTransformer("src"+string(rune(i)), "dst"+string(rune(i)))
		}(i)
	}
	wg.Wait()
}
