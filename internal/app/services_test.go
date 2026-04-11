package app

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/pageton/bridge-db/providers/cockroachdb"
	_ "github.com/pageton/bridge-db/providers/mariadb"
	_ "github.com/pageton/bridge-db/providers/mysql"
	_ "github.com/pageton/bridge-db/providers/postgres"

	"github.com/pageton/bridge-db/internal/bridge"
	"github.com/pageton/bridge-db/internal/config"
)

func TestProviderRegistryService_ListProviders(t *testing.T) {
	svc := ProviderRegistryService{}
	providers, err := svc.ListProviders(context.Background())
	if err != nil {
		t.Fatalf("ListProviders: %v", err)
	}
	if len(providers) == 0 {
		t.Fatal("expected providers")
	}
	for _, p := range providers {
		if p.Name == "" {
			t.Fatal("provider name should not be empty")
		}
		if p.Kind == "" {
			t.Fatalf("provider %q kind should not be empty", p.Name)
		}
	}
}

func TestCheckpointService_ValidateResumeMissingCheckpoint(t *testing.T) {
	cfg := &config.MigrationConfig{}
	cfg.Source.Provider = "postgres"
	cfg.Destination.Provider = "mysql"
	result, err := (CheckpointService{}).ValidateResume(context.Background(), cfg, filepath.Join(t.TempDir(), "missing.json"))
	if err != nil {
		t.Fatalf("ValidateResume: %v", err)
	}
	if result.Valid {
		t.Fatal("expected invalid resume result")
	}
	if result.Reason == "" {
		t.Fatal("expected invalid reason")
	}
}

func TestPersistentRunStatusService_LoadsStoredRun(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "runs")
	store, err := NewPersistentRunStatusService(dir)
	if err != nil {
		t.Fatalf("NewPersistentRunStatusService: %v", err)
	}

	finished := time.Now().UTC()
	store.Create(&MigrationRun{
		RunID:               "run_test_persisted",
		Status:              RunStatusSucceeded,
		Phase:               "complete",
		StartedAt:           finished.Add(-time.Minute),
		FinishedAt:          &finished,
		SourceProvider:      "postgres",
		DestinationProvider: "mysql",
		Progress: MigrationProgress{
			Phase:     "complete",
			UpdatedAt: finished,
		},
		done: make(chan struct{}),
	})

	reloaded, err := NewPersistentRunStatusService(dir)
	if err != nil {
		t.Fatalf("reload store: %v", err)
	}
	run, err := reloaded.Get("run_test_persisted")
	if err != nil {
		t.Fatalf("Get persisted run: %v", err)
	}
	if run.RunID != "run_test_persisted" {
		t.Fatalf("unexpected run id %q", run.RunID)
	}
	if run.Status != RunStatusSucceeded {
		t.Fatalf("unexpected status %s", run.Status)
	}
}

func TestPlanningService_ExplainPlan(t *testing.T) {
	plan := &bridge.MigrationPlan{
		SourceProvider:   "postgres",
		DestProvider:     "mysql",
		CrossDB:          true,
		SchemaMigration:  true,
		EstimatedBatches: 4,
		Verification:     "cross",
		UnsupportedFields: []bridge.UnsupportedField{{
			Table:  "users",
			Field:  "profile_json",
			Reason: "lossy conversion",
		}},
	}

	got, err := (PlanningService{}).ExplainPlan(context.Background(), plan)
	if err != nil {
		t.Fatalf("ExplainPlan: %v", err)
	}
	if got.Summary == "" {
		t.Fatal("expected summary")
	}
	if len(got.WhatWillHappen) == 0 {
		t.Fatal("expected explanation bullets")
	}
	if len(got.WhatMightFail) == 0 {
		t.Fatal("expected unsupported field explanation")
	}
}
