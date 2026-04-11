package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
)

// Checkpoint records migration progress so it can be resumed after interruption.
type Checkpoint struct {
	// SourceProvider is the source database provider name.
	SourceProvider string `json:"source_provider"`

	// DestProvider is the destination database provider name.
	DestProvider string `json:"dest_provider"`

	// ConfigHash is a hash of key config fields, used to detect incompatible changes
	// between the original and resumed runs.
	ConfigHash string `json:"config_hash"`

	// StartTime is when the original migration began.
	StartTime time.Time `json:"start_time"`

	// LastBatchID is the ID of the last successfully written batch.
	LastBatchID int `json:"last_batch_id"`

	// TotalWritten is the cumulative count of units written to the destination.
	TotalWritten int64 `json:"total_written"`

	// TablesCompleted lists tables/collections whose scanner cursor was fully
	// exhausted AND all rows were written.  On resume, scanners skip these
	// tables entirely via name-based filtering.  Partially-scanned tables are
	// NOT included — they will be re-scanned and dedup prevents duplicate writes.
	TablesCompleted []string `json:"tables_completed"`

	// LastTableScanning is the table that was in progress when the checkpoint
	// was saved. Empty when the last save happened at a table boundary.
	LastTableScanning string `json:"last_table_scanning,omitempty"`

	// RowsScannedInTable tracks how many rows were scanned in the current table
	// before the checkpoint was saved. Used for diagnostics and future mid-table
	// resume support.
	RowsScannedInTable int64 `json:"rows_scanned_in_table,omitempty"`

	// WrittenKeys is a set of keys written to the destination since the last
	// fully-completed table.  Used for duplicate write prevention on resume.
	// Only keys from in-progress and subsequent tables are retained; keys from
	// fully-completed tables are not needed since those tables are skipped by
	// name on resume.
	WrittenKeys []string `json:"written_keys,omitempty"`

	// ResumeToken is an opaque cursor for resuming the scanner.
	ResumeToken []byte `json:"resume_token,omitempty"`

	// Timestamp is when this checkpoint was saved.
	Timestamp time.Time `json:"timestamp"`

	// Version is the checkpoint schema version.
	Version int `json:"version"`

	// Checksum is a SHA-256 over the checkpoint content (excluding this field)
	// for detecting file corruption or partial writes.
	Checksum string `json:"checksum,omitempty"`
}

const checkpointVersion = 3

// minSupportedVersion is the oldest checkpoint version that can be loaded.
// v1 and v2 checkpoints used a different TablesCompleted semantics (included
// partially-scanned tables) and are rejected to prevent silent data loss.
const minSupportedVersion = 3

// Validate checks a loaded checkpoint for consistency.
func (cp *Checkpoint) Validate() error {
	if cp.SourceProvider == "" {
		return errors.New("checkpoint missing source_provider")
	}
	if cp.DestProvider == "" {
		return errors.New("checkpoint missing dest_provider")
	}
	if cp.StartTime.IsZero() {
		return errors.New("checkpoint missing start_time")
	}
	if cp.Version < minSupportedVersion {
		return fmt.Errorf("checkpoint version %d is too old (minimum supported: %d). "+
			"Delete the checkpoint file to start a fresh migration",
			cp.Version, minSupportedVersion)
	}
	if cp.Version > checkpointVersion {
		return fmt.Errorf("checkpoint version %d is newer than supported (%d). "+
			"Upgrade bridge-db to resume this migration",
			cp.Version, checkpointVersion)
	}
	return nil
}

// computeChecksum returns a SHA-256 hash over all checkpoint fields except
// the Checksum field itself. Used to detect file corruption.
func (cp *Checkpoint) computeChecksum() string {
	h := sha256.New()
	h.Write([]byte(cp.SourceProvider))
	h.Write([]byte{0})
	h.Write([]byte(cp.DestProvider))
	h.Write([]byte{0})
	h.Write([]byte(cp.ConfigHash))
	h.Write([]byte{0})
	h.Write([]byte(cp.StartTime.Format(time.RFC3339Nano)))
	h.Write([]byte{0})
	// Write numeric fields as fixed-width.
	fmt.Fprintf(h, "%d", cp.LastBatchID) //nolint:errcheck
	h.Write([]byte{0})
	fmt.Fprintf(h, "%d", cp.TotalWritten) //nolint:errcheck
	h.Write([]byte{0})
	for _, t := range cp.TablesCompleted {
		h.Write([]byte(t))
		h.Write([]byte{0})
	}
	h.Write([]byte(cp.LastTableScanning))
	h.Write([]byte{0})
	fmt.Fprintf(h, "%d", cp.RowsScannedInTable) //nolint:errcheck
	h.Write([]byte{0})
	for _, k := range cp.WrittenKeys {
		h.Write([]byte(k))
		h.Write([]byte{0})
	}
	h.Write(cp.ResumeToken)
	h.Write([]byte{0})
	h.Write([]byte(cp.Timestamp.Format(time.RFC3339Nano)))
	h.Write([]byte{0})
	fmt.Fprintf(h, "%d", cp.Version) //nolint:errcheck
	return hex.EncodeToString(h.Sum(nil))
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
func (s *FileCheckpointStore) Save(ctx context.Context, cp *Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check cancellation before acquiring resources.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("checkpoint save cancelled: %w", err)
	}

	// Compute and set checksum before serializing.
	cp.Checksum = cp.computeChecksum()

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
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("checkpoint save cancelled before write: %w", err)
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
func (s *FileCheckpointStore) Load(ctx context.Context) (*Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("checkpoint load cancelled: %w", err)
	}

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
		return nil, fmt.Errorf("parse checkpoint (will start fresh): %w", err)
	}

	// Validate loaded checkpoint.
	if err := cp.Validate(); err != nil {
		return nil, fmt.Errorf("invalid checkpoint: %w", err)
	}

	// Verify checksum (if present in the file).
	if cp.Checksum != "" {
		expected := cp.computeChecksum()
		if cp.Checksum != expected {
			return nil, fmt.Errorf("checkpoint checksum mismatch (file may be corrupted). "+
				"Expected %s, got %s. Delete the checkpoint file to start fresh",
				expected[:8], cp.Checksum[:8])
		}
	}

	return &cp, nil
}

// Clear removes the checkpoint file.
func (s *FileCheckpointStore) Clear(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("checkpoint clear cancelled: %w", err)
	}

	err := os.Remove(s.path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove checkpoint: %w", err)
	}
	return nil
}
