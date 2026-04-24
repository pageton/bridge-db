package bridge

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/internal/progress"
	"github.com/pageton/bridge-db/internal/transform"
	"github.com/pageton/bridge-db/internal/tunnel"
	"github.com/pageton/bridge-db/pkg/provider"
)

// scanResult is a batch of migration units produced by the scanner goroutine.
type scanResult struct {
	batchID int
	units   []provider.MigrationUnit
	err     error
}

// migrationState carries per-run state that accumulates across pipeline steps.
type migrationState struct {
	summary      *provider.MigrationSummary
	checkpoint   *Checkpoint
	startBatchID int
	allErrors    []error
}

// Pipeline orchestrates a full migration from source to destination.
// It manages tunnel lifecycle, provider connections, scanning, transformation,
// writing, verification, checkpointing, and progress reporting.
type Pipeline struct {
	config     *config.MigrationConfig
	opts       PipelineOptions
	reporter   provider.ProgressReporter
	metrics    *progress.MetricsCollector
	checkpoint CheckpointStore
	tunnels    *tunnel.Pool

	src         provider.Provider
	dst         provider.Provider
	transformer transform.Transformer
	srcCaps     provider.Capabilities
	dstCaps     provider.Capabilities

	keyRing       []string
	keyRingHead   int
	keyRingLen    int
	writtenKeys   sync.Map // map[string]bool — lock-free dedup checks
	totalWritten  int64
	startTime     time.Time
	scannedTables []string
	tableSet      map[string]bool
	keysEvicted   bool // true when key dedup set exceeded MaxWrittenKeys

	// keyMu protects keyRing for concurrent ring buffer access.
	keyMu sync.Mutex
	// cpMu serialises checkpoint writes across concurrent workers.
	cpMu           sync.Mutex
	batchesSinceCP int

	pauseCond *sync.Cond
	paused    atomic.Bool
	cancelFn  context.CancelFunc
}

const defaultKeyRingSize = 1024

// NewPipeline creates a new pipeline from the given configuration.
// The caller must invoke Run() to start the migration.
func NewPipeline(cfg *config.MigrationConfig, opts PipelineOptions, reporter provider.ProgressReporter, cpStore CheckpointStore) (*Pipeline, error) {
	if reporter == nil {
		reporter = progress.NopReporter{}
	}
	if cpStore == nil {
		var err error
		cpStore, err = NewFileCheckpointStore(cfg.Checkpoint.Path)
		if err != nil {
			return nil, fmt.Errorf("create checkpoint store: %w", err)
		}
	}
	p := &Pipeline{
		config:     cfg,
		opts:       opts,
		reporter:   reporter,
		metrics:    progress.NewMetricsCollector(),
		checkpoint: cpStore,
		tunnels:    tunnel.NewPool(),
		keyRing:    make([]string, defaultKeyRingSize),
		tableSet:   make(map[string]bool),
	}
	p.pauseCond = sync.NewCond(&sync.Mutex{})
	return p, nil
}

// stepLog returns a logger pre-loaded with step and phase context for structured
// logging. Every pipeline step should use this instead of creating ad-hoc loggers.
func (p *Pipeline) stepLog(step int, phase string) *slog.Logger {
	return logger.L().With("component", "pipeline", "step", step, "phase", phase)
}

// visiblePhases defines the ordered set of phases shown to the user.
// The step number and total are derived from this slice's position and length.
var visiblePhases = []provider.PhaseDesc{
	{Phase: provider.PhaseInit, Description: "Validating config"},
	{Phase: provider.PhaseTunnel, Description: "Validating connections"},
	{Phase: provider.PhaseConnect, Description: "Connecting to databases"},
	{Phase: provider.PhaseSchemaMigration, Description: "Inspecting schema"},
	{Phase: provider.PhasePlanning, Description: "Building migration plan"},
	{Phase: provider.PhaseScanning, Description: "Transferring data"},
	{Phase: provider.PhaseVerifying, Description: "Verifying data"},
	{Phase: provider.PhaseFinalizing, Description: "Cleaning up"},
}

// phaseDesc returns a numbered PhaseDesc for a given MigrationPhase.
func phaseDesc(phase provider.MigrationPhase) provider.PhaseDesc {
	for i, vp := range visiblePhases {
		if vp.Phase == phase {
			return provider.PhaseDesc{
				Phase:       phase,
				Step:        i + 1,
				TotalSteps:  len(visiblePhases),
				Description: vp.Description,
			}
		}
	}
	return provider.PhaseDesc{Phase: phase, Step: 0, TotalSteps: len(visiblePhases), Description: string(phase)}
}

// Pause suspends the pipeline at the next batch boundary.
func (p *Pipeline) Pause() {
	p.paused.Store(true)
	p.reporter.OnPhaseChange(provider.PhasePaused)
}

// Resume resumes a paused pipeline.
func (p *Pipeline) Resume() {
	p.paused.Store(false)
	p.pauseCond.Broadcast()
}

// Cancel cancels the pipeline context.
func (p *Pipeline) Cancel() {
	if p.cancelFn != nil {
		p.cancelFn()
	}
}

// abort cleans up and returns an error result.
func (p *Pipeline) abort(err error) error {
	p.reporter.OnPhaseChange(provider.PhaseError)
	_ = p.tunnels.CloseAll()
	return err
}

// buildTunnelConfigs creates tunnel configurations from the migration config.
func (p *Pipeline) buildTunnelConfigs() map[string]tunnel.Config {
	configs := make(map[string]tunnel.Config)

	if p.config.Source.SSH != nil && p.config.Source.SSH.Enabled {
		cfg := *p.config.Source.SSH
		cfg.RemoteHost = hostFromConnection(p.config.Source)
		cfg.RemotePort = portFromConnection(p.config.Source)
		configs["source"] = cfg
	}

	if p.config.Destination.SSH != nil && p.config.Destination.SSH.Enabled {
		cfg := *p.config.Destination.SSH
		cfg.RemoteHost = hostFromConnection(p.config.Destination)
		cfg.RemotePort = portFromConnection(p.config.Destination)
		configs["destination"] = cfg
	}

	return configs
}

// resolveProviderConfig returns the provider-specific config struct,
// with tunnel-resolved address if applicable.
func (p *Pipeline) resolveProviderConfig(side string, connCfg *config.ConnectionConfig) (any, error) {
	addr := p.tunnels.ResolvedAddr(side)
	return config.ProviderConfigWithTunnel(connCfg, addr)
}
