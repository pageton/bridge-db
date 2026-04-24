// cmd/bench is a standalone benchmark tool for bridge-db.
// It generates realistic datasets, runs real migrations via the pipeline engine,
// and produces structured performance reports.
//
// Usage:
//
//	go run -tags sqlite ./cmd/bench [flags]
//	go run -tags "mongodb,mssql,sqlite,redis" ./cmd/bench [flags]
//	make benchmark
//	make benchmark-large
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/pageton/bridge-db/internal/bridge"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/pageton/bridge-db/pkg/provider"
)

func main() {
	// -------------------------------------------------------------------------
	// Flags
	// -------------------------------------------------------------------------
	var (
		scenariosStr string
		sizes        string
		batchSizes   string
		workers      string
		maxWorkers   int
		verify       bool
		verifyMode   string
		resumeTest   bool
		output       string
		jsonOutput   string
		keepDBs      bool
		seed         int64
		warmup       int
		runs         int
		seedWorkers  int
		seedBatch    int
	)

	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	fs.StringVar(&scenariosStr, "scenarios", "sqlite→sqlite", "provider scenarios: \"src→dst\" (semicolon-separated), or groups: sql-same, sql-cross, nosql-same, nosql-cross, all-same, all-cross, all")
	fs.StringVar(&sizes, "sizes", "small", "comma-separated dataset sizes: small,medium,large")
	fs.StringVar(&batchSizes, "batch-sizes", "1000", "comma-separated batch sizes to test")
	fs.StringVar(&workers, "workers", "auto", "comma-separated worker counts (auto,1,2,4,8)")
	fs.IntVar(&maxWorkers, "max-workers", 0, "expand workers as 1,2,4,... up to this value when -workers=auto")
	fs.BoolVar(&verify, "verify", true, "run with verification enabled")
	fs.StringVar(&verifyMode, "verify-mode", "full", "verification mode: full, none")
	fs.BoolVar(&resumeTest, "resume", false, "run resume/interruption benchmark")
	fs.StringVar(&output, "output", "", "write markdown report to file")
	fs.StringVar(&jsonOutput, "json", "", "write JSON results to file")
	fs.BoolVar(&keepDBs, "keep", false, "keep source/destination databases after benchmarks")
	fs.Int64Var(&seed, "seed", 42, "random seed for deterministic data generation")
	fs.IntVar(&warmup, "warmup", 0, "number of warmup runs (excluded from results)")
	fs.IntVar(&runs, "runs", 1, "number of runs per configuration (median reported)")
	fs.IntVar(&seedWorkers, "seed-workers", 0, "writer count for benchmark source seeding (0 = auto)")
	fs.IntVar(&seedBatch, "seed-batch-size", 0, "batch size for benchmark source seeding (0 = auto)")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "bridge-db benchmark tool\n\n")
		fmt.Fprintf(fs.Output(), "Usage:\n")
		fmt.Fprintf(fs.Output(), "  go run -tags sqlite ./cmd/bench [flags]\n")
		fmt.Fprintf(fs.Output(), "  go run -tags \"mongodb,mssql,sqlite,redis\" ./cmd/bench -scenarios all-same\n\n")
		fmt.Fprintf(fs.Output(), "Dataset sizes:\n")
		fmt.Fprintf(fs.Output(), "  small  = ~10K records, ~2MB\n")
		fmt.Fprintf(fs.Output(), "  medium = ~100K records, ~20MB\n")
		fmt.Fprintf(fs.Output(), "  large  = ~1M records, ~200-500MB\n\n")
		fmt.Fprintf(fs.Output(), "Scenario formats:\n")
		fmt.Fprintf(fs.Output(), "  src→dst         e.g. sqlite→postgres, mysql→mongodb\n")
		fmt.Fprintf(fs.Output(), "  src:url→dst:url  custom connection URLs\n")
		fmt.Fprintf(fs.Output(), "  Groups: sql-same, sql-cross, nosql-same, nosql-cross, all-same, all-cross, all\n\n")
		fmt.Fprintf(fs.Output(), "Examples:\n")
		fmt.Fprintf(fs.Output(), "  go run -tags sqlite ./cmd/bench\n")
		fmt.Fprintf(fs.Output(), "  go run -tags sqlite ./cmd/bench -scenarios 'sqlite→sqlite' -sizes small,medium\n")
		fmt.Fprintf(fs.Output(), "  go run -tags sqlite ./cmd/bench -scenarios sql-same -sizes large\n")
		fmt.Fprintf(fs.Output(), "  go run -tags \"mongodb,sqlite\" ./cmd/bench -scenarios 'sqlite→mongodb;mongodb→sqlite'\n")
		fmt.Fprintf(fs.Output(), "  go run -tags \"mongodb,mssql,sqlite,redis\" ./cmd/bench -scenarios all-same -sizes small\n")
		fmt.Fprintf(fs.Output(), "\nFlags:\n\n")
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[1:])

	switch verifyMode {
	case "full":
		// honor -verify as-is
	case "none":
		verify = false
	default:
		fmt.Fprintf(os.Stderr, "invalid -verify-mode %q (must be full or none)\n", verifyMode)
		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// Hardware info
	// -------------------------------------------------------------------------
	hw := collectHardwareInfo()
	printHardwareInfo(hw)

	// -------------------------------------------------------------------------
	// Parse scenarios
	// -------------------------------------------------------------------------
	scenarios, err := ExpandScenarioGroups(parseCSV(scenariosStr))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing scenarios: %v\n", err)
		os.Exit(1)
	}

	// Filter to only compiled-in providers and warn about missing ones
	WarnUnavailable(scenarios)
	scenarios = FilterAvailable(scenarios)
	if len(scenarios) == 0 {
		fmt.Fprintf(os.Stderr, "no scenarios available (all providers missing build tags)\n")
		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// Parse inputs
	// -------------------------------------------------------------------------
	parsedSizes := parseCSV(sizes)
	parsedBatchSizes := parseCSVInt(batchSizes)
	parsedWorkers := resolveWorkers(workers, maxWorkers, runtime.NumCPU())

	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "=== Benchmark Configuration ===\n")
	fmt.Fprintf(os.Stderr, "  Scenarios:   %s\n", formatScenarios(scenarios))
	fmt.Fprintf(os.Stderr, "  Sizes:       %v\n", parsedSizes)
	fmt.Fprintf(os.Stderr, "  Batch sizes: %v\n", parsedBatchSizes)
	fmt.Fprintf(os.Stderr, "  Workers:     %v\n", parsedWorkers)
	fmt.Fprintf(os.Stderr, "  Verify:      %v (%s)\n", verify, verifyMode)
	fmt.Fprintf(os.Stderr, "  Resume test: %v\n", resumeTest)
	fmt.Fprintf(os.Stderr, "  Runs:        %d (warmup: %d)\n", runs, warmup)
	fmt.Fprintf(os.Stderr, "  Seed:        %d\n", seed)
	fmt.Fprintf(os.Stderr, "\n")

	// -------------------------------------------------------------------------
	// Run benchmarks
	// -------------------------------------------------------------------------
	runner := &BenchmarkRunner{
		Seed:          seed,
		KeepDBs:       keepDBs,
		HW:            hw,
		SeedWorkers:   seedWorkers,
		SeedBatchSize: seedBatch,
	}

	var results []BenchmarkResult

	for _, sc := range scenarios {
		fmt.Fprintf(os.Stderr, "=== Scenario: %s ===\n", sc.Label())

		for _, size := range parsedSizes {
			sizeConfig, ok := DatasetSizeByName(size)
			if !ok {
				fmt.Fprintf(os.Stderr, "warning: unknown size %q, skipping\n", size)
				continue
			}

			fmt.Fprintf(os.Stderr, "  preparing source for size=%s...\n", size)
			prepared, err := runner.prepareSource(sc, sizeConfig)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  ERROR: %v\n", err)
				continue
			}
			defer runner.cleanupPreparedSource(prepared)

			for _, bs := range parsedBatchSizes {
				for _, w := range parsedWorkers {
					benchCfg := BenchmarkConfig{
						Scenario:  sc,
						Size:      sizeConfig,
						BatchSize: bs,
						Workers:   w,
						Verify:    verify,
					}

					// Warmup
					for i := 0; i < warmup; i++ {
						fmt.Fprintf(os.Stderr, "  warmup %d/%d: %s size=%s batch=%d workers=%d\n",
							i+1, warmup, sc.Label(), size, bs, w)
						_, _ = runner.runPrepared(benchCfg, prepared)
					}

					// Measured runs
					var runResults []BenchmarkResult
					for i := 0; i < runs; i++ {
						fmt.Fprintf(os.Stderr, "  run %d/%d: %s size=%s batch=%d workers=%d\n",
							i+1, runs, sc.Label(), size, bs, w)
						result, err := runner.runPrepared(benchCfg, prepared)
						if err != nil {
							fmt.Fprintf(os.Stderr, "  ERROR: %v\n", err)
							continue
						}
						runResults = append(runResults, result)
					}

					if len(runResults) > 0 {
						results = append(results, medianResult(runResults))
					}
				}
			}
		}

		// Resume test per scenario (sqlite→sqlite only for now)
		if resumeTest {
			for _, size := range parsedSizes {
				sizeConfig, ok := DatasetSizeByName(size)
				if !ok {
					continue
				}
				fmt.Fprintf(os.Stderr, "\n=== Resume Benchmark: %s size=%s ===\n", sc.Label(), size)
				resumeResult, err := runner.runResumeTest(BenchmarkConfig{
					Scenario:  sc,
					Size:      sizeConfig,
					BatchSize: 1000,
					Workers:   1,
					Verify:    true,
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "  resume test ERROR: %v\n", err)
				} else {
					results = append(results, resumeResult)
				}
			}
		}
	}

	// -------------------------------------------------------------------------
	// Output results
	// -------------------------------------------------------------------------
	if len(results) == 0 {
		fmt.Fprintf(os.Stderr, "No benchmark results collected.\n")
		os.Exit(1)
	}

	printMarkdownTable(os.Stderr, hw, results)

	if output != "" {
		f, err := os.Create(output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create report file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		writeFullReport(f, hw, results)
		fmt.Fprintf(os.Stderr, "\nReport written to %s\n", output)
	}

	if jsonOutput != "" {
		data, err := sonic.MarshalIndent(struct {
			Hardware  HardwareInfo      `json:"hardware"`
			GoVersion string            `json:"go_version"`
			Results   []BenchmarkResult `json:"results"`
		}{
			Hardware:  hw,
			GoVersion: runtime.Version(),
			Results:   results,
		}, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal JSON: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(jsonOutput, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write JSON: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "JSON results written to %s\n", jsonOutput)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func parseCSV(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		v = strings.TrimSpace(v)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func parseCSVInt(s string) []int {
	var out []int
	for _, v := range strings.Split(s, ",") {
		v = strings.TrimSpace(v)
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		out = []int{1000}
	}
	return out
}

func resolveWorkers(spec string, maxWorkers, numCPU int) []int {
	var out []int
	for _, v := range strings.Split(spec, ",") {
		v = strings.TrimSpace(v)
		if v == "auto" {
			limit := numCPU
			if maxWorkers > 0 {
				limit = maxWorkers
			}
			for w := 1; w < limit; w *= 2 {
				out = append(out, w)
			}
			if len(out) == 0 || out[len(out)-1] != limit {
				out = append(out, limit)
			}
			continue
		}
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		limit := numCPU
		if maxWorkers > 0 {
			limit = maxWorkers
		}
		out = []int{limit}
	}
	// de-dup while preserving order
	seen := make(map[int]bool)
	uniq := out[:0]
	for _, n := range out {
		if !seen[n] {
			seen[n] = true
			uniq = append(uniq, n)
		}
	}
	return uniq
}

func medianResult(results []BenchmarkResult) BenchmarkResult {
	if len(results) == 1 {
		return results[0]
	}
	sorted := make([]BenchmarkResult, len(results))
	copy(sorted, results)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j].TotalDuration < sorted[i].TotalDuration {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	return sorted[len(sorted)/2]
}

func formatScenarios(scenarios []Scenario) string {
	names := make([]string, len(scenarios))
	for i, sc := range scenarios {
		names[i] = sc.Label()
	}
	if len(names) <= 4 {
		return strings.Join(names, ", ")
	}
	return strings.Join(names[:3], ", ") + fmt.Sprintf(" ... (%d total)", len(names))
}

// Ensure imports are referenced.
var _ = provider.AvailableProviders
var _ = bridge.PipelineOptions{}
var _ = config.MigrationConfig{}
