package bridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// maybeCheckpoint saves a checkpoint when the throttle interval has been met.
func (p *Pipeline) maybeCheckpoint(ctx context.Context, batchID int, scanner provider.Scanner) {
	p.cpMu.Lock()
	p.batchesSinceCP++
	shouldSave := p.opts.CheckpointInterval == 0 || p.batchesSinceCP >= p.opts.CheckpointInterval
	if shouldSave {
		p.batchesSinceCP = 0
	}
	p.cpMu.Unlock()

	if shouldSave {
		p.saveCheckpoint(ctx, batchID, scanner)
	}
}

// saveCheckpoint persists the current migration progress.
func (p *Pipeline) saveCheckpoint(ctx context.Context, batchID int, scanner provider.Scanner) {
	p.cpMu.Lock()
	defer p.cpMu.Unlock()

	stats := scanner.Stats()

	p.keyMu.Lock()
	keysFlat := p.writtenKeysFlat()
	p.keyMu.Unlock()
	totalWritten := atomic.LoadInt64(&p.totalWritten)

	var token []byte
	if stats.TotalScanned > 0 {
		token = encodeResumeToken(p.config.Source.Provider, stats, keysFlat)
	}

	completedCount := stats.TablesDone
	if completedCount > len(p.scannedTables) {
		completedCount = len(p.scannedTables)
	}
	completedTables := make([]string, completedCount)
	copy(completedTables, p.scannedTables[:completedCount])

	var lastTableScanning string
	if completedCount < len(p.scannedTables) {
		lastTableScanning = p.scannedTables[completedCount]
	}

	cp := &Checkpoint{
		SourceProvider:    p.config.Source.Provider,
		DestProvider:      p.config.Destination.Provider,
		ConfigHash:        computeConfigHash(p.config),
		StartTime:         p.startTime,
		LastBatchID:       batchID,
		TotalWritten:      totalWritten,
		TablesCompleted:   completedTables,
		LastTableScanning: lastTableScanning,
		WrittenKeys:       keysFlat,
		ResumeToken:       token,
		Timestamp:         time.Now(),
		Version:           checkpointVersion,
	}

	if err := p.checkpoint.Save(ctx, cp); err != nil {
		logger.L().Warn("failed to save checkpoint", "error", err)
		return
	}

	logger.L().Debug("checkpoint saved",
		"batch", batchID,
		"written", totalWritten,
		"tables_completed", len(completedTables),
		"keys_tracked", len(keysFlat),
		"last_table", lastTableScanning,
	)
}

func resumeToken(cp *Checkpoint) []byte {
	if cp == nil {
		return nil
	}
	return cp.ResumeToken
}

func lastBatchID(cp *Checkpoint) int {
	if cp == nil {
		return 0
	}
	return cp.LastBatchID
}

func tablesCompleted(cp *Checkpoint) []string {
	if cp == nil {
		return nil
	}
	return cp.TablesCompleted
}

// encodeResumeToken creates a provider-specific resume token from scan stats.
func encodeResumeToken(providerName string, stats provider.ScanStats, writtenKeys []string) []byte {
	token := map[string]any{
		"total_scanned": stats.TotalScanned,
		"total_bytes":   stats.TotalBytes,
		"tables_done":   stats.TablesDone,
		"tables_total":  stats.TablesTotal,
	}
	if providerName == "redis" && len(writtenKeys) > 0 {
		token["processed_keys"] = writtenKeys
	}
	data, err := sonic.Marshal(token)
	if err != nil {
		return nil
	}
	return data
}

func hostFromConnection(cfg config.ConnectionConfig) string {
	if r := cfg.Resolved(); r != nil {
		return r.GetHost()
	}
	return ""
}

func portFromConnection(cfg config.ConnectionConfig) int {
	if r := cfg.Resolved(); r != nil {
		return r.GetPort()
	}
	return 0
}

// dbFromConnection returns the database name from connection config.
func dbFromConnection(cfg config.ConnectionConfig) string {
	if r := cfg.Resolved(); r != nil {
		return r.GetDatabase()
	}
	return ""
}

// computeConfigHash returns a deterministic hash of key config fields.
func computeConfigHash(cfg *config.MigrationConfig) string {
	h := sha256.New()

	h.Write([]byte(cfg.Source.Provider))
	h.Write([]byte{0})
	h.Write([]byte(cfg.Destination.Provider))
	h.Write([]byte{0})

	h.Write([]byte(hostFromConnection(cfg.Source)))
	h.Write([]byte{0})
	fmt.Fprintf(h, "%d", portFromConnection(cfg.Source)) //nolint:errcheck
	h.Write([]byte{0})
	h.Write([]byte(dbFromConnection(cfg.Source)))
	h.Write([]byte{0})
	h.Write([]byte(hostFromConnection(cfg.Destination)))
	h.Write([]byte{0})
	fmt.Fprintf(h, "%d", portFromConnection(cfg.Destination)) //nolint:errcheck
	h.Write([]byte{0})
	h.Write([]byte(dbFromConnection(cfg.Destination)))
	h.Write([]byte{0})

	fmt.Fprintf(h, "%d", cfg.Pipeline.BatchSize) //nolint:errcheck
	h.Write([]byte{0})
	h.Write([]byte(string(cfg.Pipeline.ConflictStrategy)))
	h.Write([]byte{0})
	h.Write([]byte(cfg.Pipeline.FKHandling))
	h.Write([]byte{0})

	for table, mappings := range cfg.Transform.Mappings {
		h.Write([]byte(table))
		h.Write([]byte{0})
		for _, m := range mappings {
			h.Write([]byte(m.Source))
			h.Write([]byte{0})
			h.Write([]byte(m.Destination))
			h.Write([]byte{0})
		}
	}
	h.Write([]byte{0})

	return hex.EncodeToString(h.Sum(nil))
}

// ComputeConfigHash returns a deterministic hash of key config fields used to
// validate checkpoint resume compatibility.
func ComputeConfigHash(cfg *config.MigrationConfig) string {
	return computeConfigHash(cfg)
}
