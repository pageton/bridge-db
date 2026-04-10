package cockroachdb

import (
	"context"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

type dryRunProvider struct {
	inner *CockroachDBProvider
}

func (d *dryRunProvider) Name() string { return "cockroachdb" }

func (d *dryRunProvider) Connect(ctx context.Context, srcConfig, dstConfig any) error {
	return d.inner.Connect(ctx, srcConfig, dstConfig)
}

func (d *dryRunProvider) Close() error { return d.inner.Close() }

func (d *dryRunProvider) Ping(ctx context.Context) error { return d.inner.Ping(ctx) }

func (d *dryRunProvider) Scanner(ctx context.Context, opts provider.ScanOptions) provider.Scanner {
	return d.inner.Scanner(ctx, opts)
}

func (d *dryRunProvider) Writer(_ context.Context, opts provider.WriteOptions) provider.Writer {
	return &dryRunWriter{
		opts: opts,
		log:  logger.L().With("component", "cockroachdb-dryrun-writer"),
	}
}

func (d *dryRunProvider) Verifier(ctx context.Context) provider.Verifier {
	return d.inner.Verifier(ctx)
}

func (d *dryRunProvider) SchemaMigrator(ctx context.Context) provider.SchemaMigrator {
	return d.inner.SchemaMigrator(ctx)
}

func (d *dryRunProvider) DryRun() provider.Provider { return d }

// Capabilities returns the inner provider's capabilities.
func (d *dryRunProvider) Capabilities() provider.Capabilities {
	return d.inner.Capabilities()
}

type dryRunWriter struct {
	opts provider.WriteOptions
	log  interface {
		Debug(msg string, args ...any)
		Warn(msg string, args ...any)
	}
}

func (w *dryRunWriter) Write(_ context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	w.log.Debug("dry-run: would write batch", "unit_count", len(units))

	var totalBytes int64
	for _, u := range units {
		totalBytes += u.Size
	}

	return &provider.BatchResult{
		TotalUnits:   len(units),
		WrittenUnits: len(units),
		BytesWritten: totalBytes,
		Duration:     0,
	}, nil
}

func (w *dryRunWriter) Flush(_ context.Context) error {
	return nil
}
