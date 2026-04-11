package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/pageton/bridge-db/internal/bridge"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/pkg/provider"
)

type MigrationRunStatusType string

const (
	RunStatusPending   MigrationRunStatusType = "PENDING"
	RunStatusRunning   MigrationRunStatusType = "RUNNING"
	RunStatusSucceeded MigrationRunStatusType = "SUCCEEDED"
	RunStatusFailed    MigrationRunStatusType = "FAILED"
	RunStatusError     MigrationRunStatusType = "ERROR"
)

type MigrationProgress struct {
	Phase              string    `json:"phase"`
	CurrentTable       string    `json:"current_table,omitempty"`
	RecordsScanned     int64     `json:"records_scanned"`
	RecordsWritten     int64     `json:"records_written"`
	RecordsFailed      int64     `json:"records_failed"`
	RecordsSkipped     int64     `json:"records_skipped"`
	BytesTransferred   int64     `json:"bytes_transferred"`
	Throughput         float64   `json:"throughput_records_per_sec"`
	EstimatedRemaining string    `json:"estimated_remaining,omitempty"`
	CurrentBatchID     int       `json:"current_batch_id,omitempty"`
	TablesCompleted    int       `json:"tables_completed,omitempty"`
	TablesTotal        int       `json:"tables_total,omitempty"`
	UpdatedAt          time.Time `json:"updated_at"`
}

type MigrationRunResult struct {
	Duration           string   `json:"duration,omitempty"`
	RecordsWritten     int64    `json:"records_written"`
	RecordsFailed      int64    `json:"records_failed"`
	RecordsSkipped     int64    `json:"records_skipped"`
	BytesTransferred   int64    `json:"bytes_transferred"`
	VerificationOK     bool     `json:"verification_ok"`
	Warnings           []string `json:"warnings,omitempty"`
	Errors             []string `json:"errors,omitempty"`
	VerificationStatus string   `json:"verification_status,omitempty"`
}

type MigrationRun struct {
	RunID               string                 `json:"run_id"`
	Status              MigrationRunStatusType `json:"status"`
	Phase               string                 `json:"phase"`
	StartedAt           time.Time              `json:"started_at"`
	FinishedAt          *time.Time             `json:"finished_at,omitempty"`
	SourceProvider      string                 `json:"source_provider"`
	DestinationProvider string                 `json:"destination_provider"`
	Progress            MigrationProgress      `json:"progress"`
	Result              *MigrationRunResult    `json:"result,omitempty"`
	Error               string                 `json:"error,omitempty"`
	CheckpointPath      string                 `json:"checkpoint_path,omitempty"`
	Resumed             bool                   `json:"resumed,omitempty"`
	Plan                *bridge.MigrationPlan  `json:"plan,omitempty"`

	done chan struct{}
}

func (r *MigrationRun) clone() *MigrationRun {
	if r == nil {
		return nil
	}
	cp := *r
	if r.Result != nil {
		result := *r.Result
		cp.Result = &result
	}
	return &cp
}

type RunStatusService struct {
	mu       sync.RWMutex
	runs     map[string]*MigrationRun
	storeDir string
}

func NewRunStatusService() *RunStatusService {
	return &RunStatusService{runs: make(map[string]*MigrationRun)}
}

func NewPersistentRunStatusService(storeDir string) (*RunStatusService, error) {
	cleaned := filepath.Clean(storeDir)
	if cleaned == "" {
		return nil, fmt.Errorf("run status directory must not be empty")
	}
	if err := os.MkdirAll(cleaned, 0o755); err != nil {
		return nil, fmt.Errorf("create run status directory: %w", err)
	}
	return &RunStatusService{runs: make(map[string]*MigrationRun), storeDir: cleaned}, nil
}

func (s *RunStatusService) Create(run *MigrationRun) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[run.RunID] = run
	_ = s.persistLocked(run)
}

func (s *RunStatusService) Update(runID string, fn func(*MigrationRun)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok {
		loaded, err := s.loadLocked(runID)
		if err != nil {
			return err
		}
		run = loaded
	}
	fn(run)
	return s.persistLocked(run)
}

func (s *RunStatusService) Get(runID string) (*MigrationRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok {
		loaded, err := s.loadLocked(runID)
		if err != nil {
			return nil, err
		}
		return loaded.clone(), nil
	}
	return run.clone(), nil
}

func (s *RunStatusService) Wait(runID string, ctx context.Context) (*MigrationRun, error) {
	run, err := s.Get(runID)
	if err != nil {
		return nil, err
	}
	select {
	case <-run.done:
		return s.Get(runID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *RunStatusService) persistLocked(run *MigrationRun) error {
	if s.storeDir == "" || run == nil {
		return nil
	}
	copy := run.clone()
	copy.done = nil
	data, err := sonic.MarshalIndent(copy, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal run status: %w", err)
	}
	path := s.runFilePath(run.RunID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write run status temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename run status: %w", err)
	}
	return nil
}

func (s *RunStatusService) loadLocked(runID string) (*MigrationRun, error) {
	if run, ok := s.runs[runID]; ok {
		return run, nil
	}
	if s.storeDir == "" {
		return nil, fmt.Errorf("unknown run_id %q", runID)
	}
	data, err := os.ReadFile(s.runFilePath(runID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("unknown run_id %q", runID)
		}
		return nil, fmt.Errorf("read run status: %w", err)
	}
	var run MigrationRun
	if err := sonic.Unmarshal(data, &run); err != nil {
		return nil, fmt.Errorf("unmarshal run status: %w", err)
	}
	run.done = make(chan struct{})
	close(run.done)
	s.runs[runID] = &run
	return &run, nil
}

// List returns summary records for all known runs. It enumerates in-memory
// runs and any persisted run files on disk, ordered by started_at descending.
func (s *RunStatusService) List() ([]MigrationRunSummary, error) {
	s.mu.RLock()
	memCopy := make(map[string]*MigrationRun, len(s.runs))
	for k, v := range s.runs {
		memCopy[k] = v
	}
	s.mu.RUnlock()

	// Collect from disk if persistent.
	if s.storeDir != "" {
		entries, err := os.ReadDir(s.storeDir)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("read runs directory: %w", err)
		}
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
				continue
			}
			runID := entry.Name()[:len(entry.Name())-len(".json")]
			if _, ok := memCopy[runID]; ok {
				continue // already in memory
			}
			s.mu.Lock()
			loaded, err := s.loadLocked(runID)
			s.mu.Unlock()
			if err != nil {
				continue // skip corrupted files
			}
			memCopy[runID] = loaded
		}
	}

	summaries := make([]MigrationRunSummary, 0, len(memCopy))
	for _, run := range memCopy {
		summaries = append(summaries, run.summary())
	}
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].StartedAt.After(summaries[j].StartedAt)
	})
	return summaries, nil
}

// MigrationRunSummary is a lightweight record for listing runs without
// transferring full progress/result/plan payloads.
type MigrationRunSummary struct {
	RunID               string                 `json:"run_id"`
	Status              MigrationRunStatusType `json:"status"`
	Phase               string                 `json:"phase"`
	StartedAt           time.Time              `json:"started_at"`
	FinishedAt          *time.Time             `json:"finished_at,omitempty"`
	SourceProvider      string                 `json:"source_provider"`
	DestinationProvider string                 `json:"destination_provider"`
	RecordsWritten      int64                  `json:"records_written"`
	RecordsFailed       int64                  `json:"records_failed"`
	Error               string                 `json:"error,omitempty"`
}

func (r *MigrationRun) summary() MigrationRunSummary {
	s := MigrationRunSummary{
		RunID:               r.RunID,
		Status:              r.Status,
		Phase:               r.Phase,
		StartedAt:           r.StartedAt,
		FinishedAt:          r.FinishedAt,
		SourceProvider:      r.SourceProvider,
		DestinationProvider: r.DestinationProvider,
		RecordsWritten:      r.Progress.RecordsWritten,
		RecordsFailed:       r.Progress.RecordsFailed,
		Error:               r.Error,
	}
	return s
}

func (s *RunStatusService) runFilePath(runID string) string {
	return filepath.Join(s.storeDir, runID+".json")
}

type statusReporter struct {
	runs  *RunStatusService
	runID string
}

type multiReporter struct {
	reporters []provider.ProgressReporter
}

func (m multiReporter) OnBatchStart(batchID int, unitCount int) {
	for _, r := range m.reporters {
		r.OnBatchStart(batchID, unitCount)
	}
}
func (m multiReporter) OnBatchComplete(batchID int, result *provider.BatchResult) {
	for _, r := range m.reporters {
		r.OnBatchComplete(batchID, result)
	}
}
func (m multiReporter) OnMigrationComplete(summary *provider.MigrationSummary) {
	for _, r := range m.reporters {
		r.OnMigrationComplete(summary)
	}
}
func (m multiReporter) OnError(err error, unit *provider.MigrationUnit) {
	for _, r := range m.reporters {
		r.OnError(err, unit)
	}
}
func (m multiReporter) OnPhaseChange(phase provider.MigrationPhase) {
	for _, r := range m.reporters {
		r.OnPhaseChange(phase)
	}
}
func (m multiReporter) OnPhaseStart(desc provider.PhaseDesc) {
	for _, r := range m.reporters {
		r.OnPhaseStart(desc)
	}
}
func (m multiReporter) OnPhaseDone(desc provider.PhaseDesc, dur time.Duration, err error) {
	for _, r := range m.reporters {
		r.OnPhaseDone(desc, dur, err)
	}
}
func (m multiReporter) OnProgress(stats provider.ProgressStats) {
	for _, r := range m.reporters {
		r.OnProgress(stats)
	}
}

func (r statusReporter) OnBatchStart(_ int, _ int)                        {}
func (r statusReporter) OnBatchComplete(_ int, _ *provider.BatchResult)   {}
func (r statusReporter) OnMigrationComplete(_ *provider.MigrationSummary) {}
func (r statusReporter) OnError(err error, _ *provider.MigrationUnit) {
	_ = r.runs.Update(r.runID, func(run *MigrationRun) {
		run.Error = err.Error()
	})
}
func (r statusReporter) OnPhaseChange(phase provider.MigrationPhase) {
	_ = r.runs.Update(r.runID, func(run *MigrationRun) {
		run.Phase = string(phase)
		run.Progress.Phase = string(phase)
		run.Progress.UpdatedAt = time.Now().UTC()
	})
}
func (r statusReporter) OnPhaseStart(desc provider.PhaseDesc) {
	_ = r.runs.Update(r.runID, func(run *MigrationRun) {
		run.Status = RunStatusRunning
		run.Phase = string(desc.Phase)
		run.Progress.Phase = string(desc.Phase)
		run.Progress.UpdatedAt = time.Now().UTC()
	})
}
func (r statusReporter) OnPhaseDone(desc provider.PhaseDesc, _ time.Duration, err error) {
	_ = r.runs.Update(r.runID, func(run *MigrationRun) {
		run.Phase = string(desc.Phase)
		run.Progress.Phase = string(desc.Phase)
		run.Progress.UpdatedAt = time.Now().UTC()
		if err != nil && err != provider.ErrPhaseSkipped {
			run.Error = err.Error()
		}
	})
}
func (r statusReporter) OnProgress(stats provider.ProgressStats) {
	_ = r.runs.Update(r.runID, func(run *MigrationRun) {
		run.Progress = migrationProgressFromStats(stats)
		run.Phase = string(stats.Phase)
	})
}

func migrationProgressFromStats(stats provider.ProgressStats) MigrationProgress {
	progress := MigrationProgress{
		Phase:            string(stats.Phase),
		CurrentTable:     stats.CurrentTable,
		RecordsScanned:   stats.TotalScanned,
		RecordsWritten:   stats.TotalWritten,
		RecordsFailed:    stats.TotalFailed,
		RecordsSkipped:   stats.TotalSkipped,
		BytesTransferred: stats.BytesTransferred,
		Throughput:       stats.Throughput,
		CurrentBatchID:   stats.CurrentBatchID,
		TablesCompleted:  stats.TablesCompleted,
		TablesTotal:      stats.TablesTotal,
		UpdatedAt:        time.Now().UTC(),
	}
	if stats.EstimatedRemain > 0 {
		progress.EstimatedRemaining = stats.EstimatedRemain.Round(time.Second).String()
	}
	return progress
}

type MigrationService struct {
	runs *RunStatusService
}

func NewMigrationService(runs *RunStatusService) *MigrationService {
	return &MigrationService{runs: runs}
}

func (s *MigrationService) newRun(cfg *config.MigrationConfig) *MigrationRun {
	return &MigrationRun{
		RunID:               fmt.Sprintf("run_%d", time.Now().UTC().UnixNano()),
		Status:              RunStatusPending,
		Phase:               string(provider.PhaseInit),
		StartedAt:           time.Now().UTC(),
		SourceProvider:      cfg.Source.Provider,
		DestinationProvider: cfg.Destination.Provider,
		Progress: MigrationProgress{
			Phase:     string(provider.PhaseInit),
			UpdatedAt: time.Now().UTC(),
		},
		done: make(chan struct{}),
	}
}

func (s *MigrationService) finishRun(runID string, result *bridge.RunResult, runErr error) {
	finished := time.Now().UTC()
	_ = s.runs.Update(runID, func(run *MigrationRun) {
		defer close(run.done)
		run.FinishedAt = &finished
		if result != nil {
			run.CheckpointPath = result.CheckpointPath
			run.Resumed = result.Resumed
			run.Plan = result.Plan
		}
		if runErr != nil {
			run.Status = RunStatusFailed
			run.Error = runErr.Error()
			return
		}
		if result == nil || result.Summary == nil {
			run.Status = RunStatusError
			run.Error = "migration completed without summary"
			return
		}
		run.Status = RunStatusSucceeded
		run.Phase = string(provider.PhaseComplete)
		run.Progress.Phase = string(provider.PhaseComplete)
		run.Progress.RecordsScanned = result.Summary.TotalScanned
		run.Progress.RecordsWritten = result.Summary.TotalWritten
		run.Progress.RecordsFailed = result.Summary.TotalFailed
		run.Progress.RecordsSkipped = result.Summary.TotalSkipped
		run.Progress.BytesTransferred = result.Summary.BytesTransferred
		run.Progress.UpdatedAt = finished
		run.Result = &MigrationRunResult{
			Duration:         result.Summary.Duration.Round(time.Millisecond).String(),
			RecordsWritten:   result.Summary.TotalWritten,
			RecordsFailed:    result.Summary.TotalFailed,
			RecordsSkipped:   result.Summary.TotalSkipped,
			BytesTransferred: result.Summary.BytesTransferred,
			VerificationOK:   result.Summary.VerificationOK,
		}
		if result.VerificationReport != nil {
			run.Result.VerificationStatus = string(result.VerificationReport.Status)
		}
		for _, err := range result.Summary.Errors {
			run.Result.Errors = append(run.Result.Errors, err.Error())
		}
	})
}

func (s *MigrationService) StartRun(ctx context.Context, cfg *config.MigrationConfig, opts bridge.PipelineOptions) (*MigrationRun, error) {
	run := s.newRun(cfg)
	runID := run.RunID
	s.runs.Create(run)

	pipeline, err := bridge.NewPipeline(cfg, opts, statusReporter{runs: s.runs, runID: runID}, nil)
	if err != nil {
		finished := time.Now().UTC()
		_ = s.runs.Update(runID, func(run *MigrationRun) {
			run.Status = RunStatusError
			run.Error = err.Error()
			run.FinishedAt = &finished
			close(run.done)
		})
		return s.runs.Get(runID)
	}

	go func() {
		result, runErr := pipeline.Run(ctx)
		s.finishRun(runID, result, runErr)
	}()

	return s.runs.Get(runID)
}

// RunSync executes a migration synchronously while still recording run status.
// An optional external reporter can be composed with the internal status reporter.
func (s *MigrationService) RunSync(ctx context.Context, cfg *config.MigrationConfig, opts bridge.PipelineOptions, reporter provider.ProgressReporter) (*bridge.RunResult, *MigrationRun, error) {
	run := s.newRun(cfg)
	s.runs.Create(run)
	reporters := []provider.ProgressReporter{statusReporter{runs: s.runs, runID: run.RunID}}
	if reporter != nil {
		reporters = append(reporters, reporter)
	}
	combined := multiReporter{reporters: reporters}
	pipeline, err := bridge.NewPipeline(cfg, opts, combined, nil)
	if err != nil {
		finished := time.Now().UTC()
		_ = s.runs.Update(run.RunID, func(run *MigrationRun) {
			run.Status = RunStatusError
			run.Error = err.Error()
			run.FinishedAt = &finished
			close(run.done)
		})
		r, _ := s.runs.Get(run.RunID)
		return nil, r, err
	}
	result, runErr := pipeline.Run(ctx)
	s.finishRun(run.RunID, result, runErr)
	r, getErr := s.runs.Get(run.RunID)
	if getErr != nil {
		return result, nil, getErr
	}
	return result, r, runErr
}

func (s *MigrationService) GetRun(_ context.Context, runID string) (*MigrationRun, error) {
	return s.runs.Get(runID)
}
