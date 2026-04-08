package tunnel

import (
	"context"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Port != 22 {
		t.Errorf("default port = %d, want 22", cfg.Port)
	}
	if cfg.Enabled {
		t.Error("default enabled should be false")
	}
}

func TestNoopTunnel(t *testing.T) {
	var _ Tunnel = NoopTunnel{}

	n := NoopTunnel{}
	addr, err := n.Open(context.Background())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if addr != "" {
		t.Errorf("addr = %q, want empty", addr)
	}
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n.LocalAddr() != "" {
		t.Errorf("LocalAddr = %q, want empty", n.LocalAddr())
	}
	if n.IsOpen() {
		t.Error("IsOpen should be false")
	}
}

func TestNewPool(t *testing.T) {
	p := NewPool()
	if p == nil {
		t.Fatal("NewPool returned nil")
	}
}

func TestPool_OpenAll_DisabledConfigs(t *testing.T) {
	p := NewPool()
	configs := map[string]Config{
		"source":      {Enabled: false},
		"destination": {Enabled: false},
	}
	if err := p.OpenAll(context.Background(), configs); err != nil {
		t.Fatalf("OpenAll: %v", err)
	}
	if p.TunnelCount() != 0 {
		t.Errorf("TunnelCount = %d, want 0", p.TunnelCount())
	}
}

func TestPool_OpenAll_MissingRemoteHost(t *testing.T) {
	p := NewPool()
	configs := map[string]Config{
		"source": {Enabled: true, Host: "bastion", Port: 22, Username: "u"},
	}
	err := p.OpenAll(context.Background(), configs)
	if err == nil {
		t.Fatal("expected error for missing remote_host")
	}
}

func TestPool_Get_NoTunnel(t *testing.T) {
	p := NewPool()
	got := p.Get("nonexistent")
	if _, ok := got.(NoopTunnel); !ok {
		t.Error("expected NoopTunnel for missing name")
	}
}

func TestPool_ResolvedAddr_NoTunnel(t *testing.T) {
	p := NewPool()
	if addr := p.ResolvedAddr("nonexistent"); addr != "" {
		t.Errorf("addr = %q, want empty", addr)
	}
}

func TestPool_AllHealthy_NoTunnels(t *testing.T) {
	p := NewPool()
	if !p.AllHealthy() {
		t.Error("empty pool should be healthy")
	}
}

func TestPool_AllHealthy_NoopTunnels(t *testing.T) {
	p := NewPool()
	configs := map[string]Config{
		"source": {Enabled: false},
	}
	if err := p.OpenAll(context.Background(), configs); err != nil {
		t.Fatalf("OpenAll: %v", err)
	}
	if !p.AllHealthy() {
		t.Error("pool with noop tunnels should be healthy")
	}
}

func TestPool_CloseAll_Empty(t *testing.T) {
	p := NewPool()
	if err := p.CloseAll(); err != nil {
		t.Fatalf("CloseAll on empty pool: %v", err)
	}
}

func TestPool_CloseAll_AfterNoops(t *testing.T) {
	p := NewPool()
	configs := map[string]Config{
		"source":      {Enabled: false},
		"destination": {Enabled: false},
	}
	if err := p.OpenAll(context.Background(), configs); err != nil {
		t.Fatalf("OpenAll: %v", err)
	}
	if err := p.CloseAll(); err != nil {
		t.Fatalf("CloseAll: %v", err)
	}
	got := p.Get("source")
	if _, ok := got.(NoopTunnel); !ok {
		t.Error("expected NoopTunnel after CloseAll")
	}
}
