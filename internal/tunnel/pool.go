package tunnel

import (
	"context"
	"fmt"
	"sync"

	"github.com/pageton/bridge-db/internal/logger"
)

// Pool manages multiple SSH tunnels (typically source and destination).
// It ensures atomicity: if opening one tunnel fails, all already-opened
// tunnels are closed before returning the error.
type Pool struct {
	mu      sync.Mutex
	tunnels map[string]Tunnel
	configs map[string]Config
}

// NewPool creates an empty tunnel pool.
func NewPool() *Pool {
	return &Pool{
		tunnels: make(map[string]Tunnel),
		configs: make(map[string]Config),
	}
}

// OpenAll opens SSH tunnels for all configurations that have SSH enabled.
// The configs map keys are connection names (e.g. "source", "destination").
//
// On failure, any already-opened tunnels are closed before returning the error,
// ensuring the pool is left in a clean state.
func (p *Pool) OpenAll(ctx context.Context, configs map[string]Config) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	log := logger.L().With("component", "tunnel_pool")

	// Track which tunnels we opened in this call for rollback.
	var opened []string

	for name, cfg := range configs {
		if !cfg.Enabled {
			p.tunnels[name] = NoopTunnel{}
			p.configs[name] = cfg
			log.Info("tunnel disabled, using noop", "connection", name)
			continue
		}

		if cfg.RemoteHost == "" || cfg.RemotePort == 0 {
			// Rollback already-opened tunnels
			for _, openedName := range opened {
				if t, ok := p.tunnels[openedName]; ok {
					_ = t.Close()
					delete(p.tunnels, openedName)
				}
			}
			return fmt.Errorf("tunnel %q: remote_host and remote_port are required", name)
		}

		t := NewSSHTunnel(cfg)
		if _, err := t.Open(ctx); err != nil {
			log.Error("failed to open tunnel", "connection", name, "error", err)

			// Rollback already-opened tunnels
			for _, openedName := range opened {
				if tun, ok := p.tunnels[openedName]; ok {
					_ = tun.Close()
					delete(p.tunnels, openedName)
				}
			}
			return fmt.Errorf("tunnel %q: %w", name, err)
		}

		p.tunnels[name] = t
		p.configs[name] = cfg
		opened = append(opened, name)

		log.Info("tunnel opened",
			"connection", name,
			"local_addr", t.LocalAddr(),
			"remote_addr", fmt.Sprintf("%s:%d", cfg.RemoteHost, cfg.RemotePort),
		)
	}

	return nil
}

// CloseAll closes all tunnels in reverse order and clears the pool.
func (p *Pool) CloseAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error

	// Close in reverse order of insertion (destination first, then source).
	// Build a reversed list of names.
	names := make([]string, 0, len(p.tunnels))
	for name := range p.tunnels {
		names = append(names, name)
	}
	// Reverse
	for i := len(names)/2 - 1; i >= 0; i-- {
		opp := len(names) - 1 - i
		names[i], names[opp] = names[opp], names[i]
	}

	for _, name := range names {
		if t, ok := p.tunnels[name]; ok {
			if err := t.Close(); err != nil && firstErr == nil {
				firstErr = fmt.Errorf("close tunnel %q: %w", name, err)
			}
			delete(p.tunnels, name)
		}
	}

	// Clear configs
	p.configs = make(map[string]Config)

	return firstErr
}

// Get returns the tunnel for the named connection.
// Returns NoopTunnel if no tunnel exists for that name.
func (p *Pool) Get(name string) Tunnel {
	p.mu.Lock()
	defer p.mu.Unlock()

	if t, ok := p.tunnels[name]; ok {
		return t
	}
	return NoopTunnel{}
}

// ResolvedAddr returns the local address for a connection, or empty string
// if no tunnel is configured for that connection.
func (p *Pool) ResolvedAddr(name string) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	if t, ok := p.tunnels[name]; ok {
		return t.LocalAddr()
	}
	return ""
}

// AllHealthy reports whether all tunnels in the pool are open and healthy.
func (p *Pool) AllHealthy() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, t := range p.tunnels {
		if _, ok := t.(NoopTunnel); ok {
			continue // noop tunnels are always "healthy"
		}
		if !t.IsOpen() {
			return false
		}
	}
	return true
}

// TunnelCount returns the number of active (non-noop) tunnels.
func (p *Pool) TunnelCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	count := 0
	for _, t := range p.tunnels {
		if _, ok := t.(NoopTunnel); !ok {
			count++
		}
	}
	return count
}
