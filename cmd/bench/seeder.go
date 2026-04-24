//go:build sqlite

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/pageton/bridge-db/internal/bridge"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/pkg/provider"
)

// SeedProvider generates a SQLite source database, then migrates it to the
// target provider using the real bridge.Pipeline. This ensures the source
// database has realistic data for any provider, not just SQLite.
//
// For SQLite sources, this is a no-op (the caller already has the .db file).
// For all other providers, it runs: SQLite → target_provider migration.
//
// Returns the URL of the seeded source database and any error.
func SeedProvider(ctx context.Context, ep ProviderEndpoint, tmpDir string, size DatasetSizeConfig, seed int64, seedBatchSize int, seedWorkers int) (string, int64, error) {
	if ep.IsSQLite() {
		// SQLite: generate directly, return the file path as URL
		srcPath := filepath.Join(tmpDir, "source.db")
		gen := NewGenerator(seed)
		if err := gen.GenerateDatabase(srcPath, size); err != nil {
			return "", 0, fmt.Errorf("generate sqlite source: %w", err)
		}

		info, _ := os.Stat(srcPath)
		var dataSize int64
		if info != nil {
			dataSize = info.Size()
		}
		return srcPath, dataSize, nil
	}

	// Non-SQLite: generate SQLite temp, then migrate to target provider
	sqlitePath := filepath.Join(tmpDir, "_seed_source.db")
	targetURL := ep.URL
	if err := cleanupEndpoint(ctx, ep); err != nil {
		return "", 0, fmt.Errorf("cleanup %s before seed: %w", ep.Provider, err)
	}

	fmt.Fprintf(os.Stderr, "    seeding %s source database...\n", ep.Provider)

	// Step 1: Generate SQLite source
	gen := NewGenerator(seed)
	if err := gen.GenerateDatabase(sqlitePath, size); err != nil {
		return "", 0, fmt.Errorf("generate seed sqlite: %w", err)
	}

	sqliteInfo, _ := os.Stat(sqlitePath)
	var dataSize int64
	if sqliteInfo != nil {
		dataSize = sqliteInfo.Size()
	}
	fmt.Fprintf(os.Stderr, "    generated seed data: %.1f MB\n", float64(dataSize)/(1024*1024))

	// Step 2: Migrate SQLite → target provider
	cpPath := filepath.Join(tmpDir, "_seed_checkpoint.json")
	seedBatchSize, seedWorkers = benchmarkSeedSettings(size, seedBatchSize, seedWorkers)

	migrationCfg := &config.MigrationConfig{
		Source: config.ConnectionConfig{
			Provider: "sqlite",
			SQLite:   &config.SQLiteConfig{Path: sqlitePath},
		},
		Destination: config.ConnectionConfig{
			Provider: ep.Provider,
			URL:      targetURL,
		},
		Pipeline: config.PipelineConfig{
			BatchSize:        seedBatchSize,
			Verify:           false,
			ConflictStrategy: provider.ConflictOverwrite,
			MigrateSchema:    seedNeedsSchema(ep.Provider),
			FKHandling:       "skip",
			MaxRetries:       3,
			RetryBackoff:     500 * time.Millisecond,
		},
		Checkpoint: config.CheckpointConfig{
			Enabled: false,
			Path:    cpPath,
		},
		Transform: config.DefaultTransformConfig(),
		Logging:   config.LoggingConfig{Level: "error"},
	}

	// Resolve URL-based configs
	if err := config.Resolve(migrationCfg); err != nil {
		return "", 0, fmt.Errorf("resolve seed config: %w", err)
	}

	opts := bridge.DefaultPipelineOptions()
	opts.BatchSize = seedBatchSize
	opts.Verify = false
	opts.WriteWorkers = seedWorkers
	opts.Parallel = max(seedWorkers*2, 2)
	opts.CheckpointEnabled = false
	opts.MaxWrittenKeys = benchmarkMaxWrittenKeys(size.Records)
	opts.MigrateSchema = seedNeedsSchema(ep.Provider)
	opts.FKHandling = "skip"

	fmt.Fprintf(os.Stderr, "    seed settings: batch=%d workers=%d\n", seedBatchSize, seedWorkers)

	seedStart := time.Now()
	pipeline, err := bridge.NewPipeline(migrationCfg, opts, &benchReporter{}, nil)
	if err != nil {
		return "", 0, fmt.Errorf("create seed pipeline: %w", err)
	}

	result, err := pipeline.Run(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("seed pipeline run: %w", err)
	}
	seedDur := time.Since(seedStart)

	fmt.Fprintf(os.Stderr, "    seeded %s in %v (%d rows, %d errors)\n",
		ep.Provider, seedDur, result.Summary.TotalWritten, result.Summary.TotalFailed)

	// Clean up temp SQLite seed file
	os.Remove(sqlitePath)
	os.Remove(cpPath)

	return targetURL, dataSize, nil
}

// DropDatabase drops all benchmark tables from the target URL.
// This is used to clean up between benchmark runs.
func DropDatabase(ep ProviderEndpoint, tmpDir string) error {
	if ep.IsSQLite() {
		// SQLite temp files are cleaned up by the caller
		return nil
	}
	// For remote databases, we rely on ConflictStrategy: overwrite.
	// Full DROP is provider-specific and not worth the complexity here.
	return nil
}

func seedNeedsSchema(providerName string) bool {
	switch providerName {
	case "redis", "mongodb":
		return false
	default:
		return true
	}
}

func benchmarkSeedSettings(size DatasetSizeConfig, requestedBatchSize int, requestedWorkers int) (batchSize int, workers int) {
	workers = requestedWorkers
	if workers <= 0 {
		workers = min(runtime.NumCPU(), 4)
	}
	if workers < 1 {
		workers = 1
	}

	batchSize = requestedBatchSize
	if batchSize <= 0 {
		batchSize = 500
		switch size.Name {
		case "medium", "large":
			batchSize = 1000
		}
	}

	return batchSize, workers
}
