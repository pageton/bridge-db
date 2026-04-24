package cli

import (
	"testing"

	"github.com/pageton/bridge-db/pkg/provider"
)

func TestBuildMigrateOptions_MapsFlags(t *testing.T) {
	batchSize = 2048
	dryRun = true
	verify = false
	migrateSchema = false
	conflictStrategy = "skip"
	fkHandling = "ordered"
	maxRetries = 7
	failFast = true
	checkpoint = false
	resume = true
	parallel = 9
	writeWorkers = 3
	maxBatchBytes = 123456
	maxWrittenKeys = 321
	checkpointInterval = 5
	verifySampleMode = "count"
	verifySamplePct = 12.5
	verifySampleN = 77
	verifyNoChecksum = true
	verifyMaxKeys = 222

	opts := buildMigrateOptions()

	if opts.BatchSize != 2048 {
		t.Fatalf("BatchSize = %d, want 2048", opts.BatchSize)
	}
	if !opts.DryRun {
		t.Fatal("DryRun = false, want true")
	}
	if opts.Verify {
		t.Fatal("Verify = true, want false")
	}
	if opts.MigrateSchema {
		t.Fatal("MigrateSchema = true, want false")
	}
	if opts.ConflictStrategy != provider.ConflictSkip {
		t.Fatalf("ConflictStrategy = %q, want %q", opts.ConflictStrategy, provider.ConflictSkip)
	}
	if opts.FKHandling != "ordered" {
		t.Fatalf("FKHandling = %q, want %q", opts.FKHandling, "ordered")
	}
	if opts.MaxRetries != 7 || !opts.FailFast {
		t.Fatalf("retry flags not mapped: MaxRetries=%d FailFast=%v", opts.MaxRetries, opts.FailFast)
	}
	if opts.CheckpointEnabled {
		t.Fatal("CheckpointEnabled = true, want false")
	}
	if !opts.Resume {
		t.Fatal("Resume = false, want true")
	}
	if opts.Parallel != 9 || opts.WriteWorkers != 3 {
		t.Fatalf("parallel flags not mapped: Parallel=%d WriteWorkers=%d", opts.Parallel, opts.WriteWorkers)
	}
	if opts.MaxBatchBytes != 123456 || opts.MaxWrittenKeys != 321 || opts.CheckpointInterval != 5 {
		t.Fatalf("batch/checkpoint flags not mapped: %+v", opts)
	}
	if opts.VerifyOptions.SampleMode != "count" || opts.VerifyOptions.SampleN != 77 {
		t.Fatalf("verify sample flags not mapped: %+v", opts.VerifyOptions)
	}
	if opts.VerifyOptions.SamplePct != 12.5 {
		t.Fatalf("SamplePct = %f, want 12.5", opts.VerifyOptions.SamplePct)
	}
	if opts.VerifyOptions.ChecksumComparison {
		t.Fatal("ChecksumComparison = true, want false")
	}
	if opts.VerifyOptions.MaxSampleKeys != 222 {
		t.Fatalf("MaxSampleKeys = %d, want 222", opts.VerifyOptions.MaxSampleKeys)
	}
	if opts.VerifyOptions.Seed != 42 {
		t.Fatalf("Seed = %d, want 42", opts.VerifyOptions.Seed)
	}
}
