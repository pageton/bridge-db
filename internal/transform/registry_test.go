package transform

import (
	"sync"
	"sync/atomic"
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

	got := GetTransformer("src", "dst", TransformerConfig{})
	if got == nil {
		t.Fatal("expected transformer, got nil")
	}
	if _, ok := got.(NoopTransformer); !ok {
		t.Error("expected NoopTransformer concrete type")
	}
}

func TestGetTransformer_UnregisteredPair(t *testing.T) {
	got := GetTransformer("nonexistent", "pair", TransformerConfig{})
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
			GetTransformer("src"+string(rune(i)), "dst"+string(rune(i)), TransformerConfig{})
		}(i)
	}
	wg.Wait()
}

// configCaptureTransformer records the TransformerConfig it receives via Configure.
type configCaptureTransformer struct {
	NoopTransformer
	received TransformerConfig
}

func (c *configCaptureTransformer) Configure(cfg TransformerConfig) { c.received = cfg }

func TestGetTransformer_InjectsConfig(t *testing.T) {
	regMu.Lock()
	orig := reg
	reg = make(map[pairKey]func() Transformer)
	regMu.Unlock()
	t.Cleanup(func() {
		regMu.Lock()
		reg = orig
		regMu.Unlock()
	})

	RegisterTransformer("cfgSrc", "cfgDst", func() Transformer {
		return &configCaptureTransformer{}
	})

	cfg := TransformerConfig{
		SrcDialect: "postgres",
		DstDialect: "mysql",
	}

	got := GetTransformer("cfgSrc", "cfgDst", cfg)
	cc := got.(*configCaptureTransformer)
	if cc.received.SrcDialect != "postgres" {
		t.Errorf("SrcDialect = %q, want %q", cc.received.SrcDialect, "postgres")
	}
	if cc.received.DstDialect != "mysql" {
		t.Errorf("DstDialect = %q, want %q", cc.received.DstDialect, "mysql")
	}
}

func TestGetTransformer_ConfigIsolationBetweenCalls(t *testing.T) {
	regMu.Lock()
	orig := reg
	reg = make(map[pairKey]func() Transformer)
	regMu.Unlock()
	t.Cleanup(func() {
		regMu.Lock()
		reg = orig
		regMu.Unlock()
	})

	RegisterTransformer("isoSrc", "isoDst", func() Transformer {
		return &configCaptureTransformer{}
	})

	cfgA := TransformerConfig{SrcDialect: "postgres", DstDialect: "mysql"}
	cfgB := TransformerConfig{SrcDialect: "mongodb", DstDialect: "redis"}

	tA := GetTransformer("isoSrc", "isoDst", cfgA).(*configCaptureTransformer)
	tB := GetTransformer("isoSrc", "isoDst", cfgB).(*configCaptureTransformer)

	// Each call should get its own config — no cross-contamination.
	if tA.received.SrcDialect != "postgres" {
		t.Errorf("tA SrcDialect = %q, want %q", tA.received.SrcDialect, "postgres")
	}
	if tB.received.SrcDialect != "mongodb" {
		t.Errorf("tB SrcDialect = %q, want %q", tB.received.SrcDialect, "mongodb")
	}
}

func TestGetTransformer_ConcurrentIsolation(t *testing.T) {
	regMu.Lock()
	orig := reg
	reg = make(map[pairKey]func() Transformer)
	regMu.Unlock()
	t.Cleanup(func() {
		regMu.Lock()
		reg = orig
		regMu.Unlock()
	})

	RegisterTransformer("concSrc", "concDst", func() Transformer {
		return &configCaptureTransformer{}
	})

	var errors atomic.Int64
	var wg sync.WaitGroup

	for i := range 200 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dialect := Dialect(rune('a' + i%26))
			cfg := TransformerConfig{SrcDialect: dialect}
			got := GetTransformer("concSrc", "concDst", cfg)
			cc := got.(*configCaptureTransformer)
			if cc.received.SrcDialect != dialect {
				errors.Add(1)
			}
		}(i)
	}
	wg.Wait()

	if n := errors.Load(); n > 0 {
		t.Errorf("config leakage detected: %d/%d goroutines received wrong config", n, 200)
	}
}
