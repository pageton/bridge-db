package bridge

import (
	"context"
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Checkpoint records migration progress so it can be resumed after interruption.
type Checkpoint struct {
	// SourceProvider is the source database provider name.
	SourceProvider string `json:"source_provider"`

	// DestProvider is the destination database provider name.
	DestProvider string `json:"dest_provider"`

	// StartTime is when the original migration began.
	StartTime time.Time `json:"start_time"`

	// LastBatchID is the ID of the last successfully written batch.
	LastBatchID int `json:"last_batch_id"`

	// TablesCompleted lists tables/collections that have been fully migrated.
	TablesCompleted []string `json:"tables_completed"`

	// ResumeToken is an opaque cursor for resuming the scanner.
	ResumeToken []byte `json:"resume_token,omitempty"`

	// Timestamp is when this checkpoint was saved.
	Timestamp time.Time `json:"timestamp"`
}

// CheckpointStore persists migration checkpoints.
type CheckpointStore interface {
	// Save persists a checkpoint. It overwrites any existing checkpoint.
	Save(ctx context.Context, cp *Checkpoint) error

	// Load reads the most recent checkpoint. Returns nil if no checkpoint exists.
	Load(ctx context.Context) (*Checkpoint, error)

	// Clear removes the checkpoint file.
	Clear(ctx context.Context) error
}

// FileCheckpointStore implements CheckpointStore using a JSON file.
// Writes are atomic (write to temp file, then rename).
type FileCheckpointStore struct {
	path string
	mu   sync.Mutex
}

// NewFileCheckpointStore creates a new file-based checkpoint store.
// The path is cleaned and validated to prevent path traversal.
func NewFileCheckpointStore(path string) (*FileCheckpointStore, error) {
	cleaned := filepath.Clean(path)
	if strings.Contains(cleaned, "..") {
		return nil, fmt.Errorf("checkpoint path must not contain '..': %s", path)
	}
	if cleaned == "" {
		return nil, errors.New("checkpoint path must not be empty")
	}
	return &FileCheckpointStore{path: cleaned}, nil
}

// Save writes the checkpoint to a JSON file atomically.
func (s *FileCheckpointStore) Save(_ context.Context, cp *Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := sonic.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	tmpPath := s.path + ".tmp"
	if dir := filepath.Dir(s.path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create checkpoint directory: %w", err)
		}
	}
	if err := os.WriteFile(tmpPath, data, 0o600); err != nil {
		return fmt.Errorf("write checkpoint temp: %w", err)
	}

	if err := os.Rename(tmpPath, s.path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	return nil
}

// Load reads the checkpoint from the JSON file.
// Returns nil if the file does not exist.
func (s *FileCheckpointStore) Load(_ context.Context) (*Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}

	if len(data) == 0 {
		return nil, nil
	}

	var cp Checkpoint
	if err := sonic.Unmarshal(data, &cp); err != nil {
		// Corrupt checkpoint — log warning and start fresh
		return nil, fmt.Errorf("parse checkpoint (will start fresh): %w", err)
	}

	return &cp, nil
}

// Clear removes the checkpoint file.
func (s *FileCheckpointStore) Clear(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := os.Remove(s.path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove checkpoint: %w", err)
	}
	return nil
}
