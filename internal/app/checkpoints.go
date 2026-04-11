package app

import (
	"context"
	"fmt"

	"github.com/pageton/bridge-db/internal/bridge"
	"github.com/pageton/bridge-db/internal/config"
)

type CheckpointState struct {
	Path              string   `json:"path"`
	SourceProvider    string   `json:"source_provider"`
	DestProvider      string   `json:"dest_provider"`
	ConfigHash        string   `json:"config_hash"`
	LastBatchID       int      `json:"last_batch_id"`
	TotalWritten      int64    `json:"total_written"`
	TablesCompleted   []string `json:"tables_completed,omitempty"`
	LastTableScanning string   `json:"last_table_scanning,omitempty"`
	Version           int      `json:"version"`
	Timestamp         string   `json:"timestamp,omitempty"`
}

type ResumeValidationResult struct {
	Valid      bool             `json:"valid"`
	Reason     string           `json:"reason,omitempty"`
	Checkpoint *CheckpointState `json:"checkpoint,omitempty"`
}

type CheckpointService struct{}

func (CheckpointService) LoadCheckpoint(ctx context.Context, path string) (*CheckpointState, error) {
	store, err := bridge.NewFileCheckpointStore(path)
	if err != nil {
		return nil, err
	}
	cp, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return nil, fmt.Errorf("no checkpoint found at %s", path)
	}
	return checkpointState(path, cp), nil
}

func (CheckpointService) ValidateResume(ctx context.Context, cfg *config.MigrationConfig, path string) (*ResumeValidationResult, error) {
	store, err := bridge.NewFileCheckpointStore(path)
	if err != nil {
		return nil, err
	}
	cp, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return &ResumeValidationResult{Valid: false, Reason: fmt.Sprintf("no checkpoint found at %s", path)}, nil
	}
	if cp.SourceProvider != cfg.Source.Provider {
		return &ResumeValidationResult{Valid: false, Reason: fmt.Sprintf("checkpoint source provider %q does not match current %q", cp.SourceProvider, cfg.Source.Provider), Checkpoint: checkpointState(path, cp)}, nil
	}
	if cp.DestProvider != cfg.Destination.Provider {
		return &ResumeValidationResult{Valid: false, Reason: fmt.Sprintf("checkpoint destination provider %q does not match current %q", cp.DestProvider, cfg.Destination.Provider), Checkpoint: checkpointState(path, cp)}, nil
	}
	currentHash := bridge.ComputeConfigHash(cfg)
	if cp.ConfigHash != "" && cp.ConfigHash != currentHash {
		return &ResumeValidationResult{Valid: false, Reason: fmt.Sprintf("config hash mismatch: checkpoint=%s current=%s", shortHash(cp.ConfigHash), shortHash(currentHash)), Checkpoint: checkpointState(path, cp)}, nil
	}
	return &ResumeValidationResult{Valid: true, Checkpoint: checkpointState(path, cp)}, nil
}

func checkpointState(path string, cp *bridge.Checkpoint) *CheckpointState {
	state := &CheckpointState{
		Path:              path,
		SourceProvider:    cp.SourceProvider,
		DestProvider:      cp.DestProvider,
		ConfigHash:        cp.ConfigHash,
		LastBatchID:       cp.LastBatchID,
		TotalWritten:      cp.TotalWritten,
		TablesCompleted:   cp.TablesCompleted,
		LastTableScanning: cp.LastTableScanning,
		Version:           cp.Version,
	}
	if !cp.Timestamp.IsZero() {
		state.Timestamp = cp.Timestamp.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	return state
}

func shortHash(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}
