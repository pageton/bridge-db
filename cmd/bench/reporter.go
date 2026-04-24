package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/bytedance/sonic"
)

// ---------------------------------------------------------------------------
// Hardware detection
// ---------------------------------------------------------------------------

// HardwareInfo captures the machine specs for result interpretation.
type HardwareInfo struct {
	CPUModel  string `json:"cpu_model"`
	CPUCores  int    `json:"cpu_cores"`
	RAMGB     string `json:"ram_gb"`
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	DiskType  string `json:"disk_type,omitempty"` // detected if possible
}

func collectHardwareInfo() HardwareInfo {
	hw := HardwareInfo{
		CPUCores:  runtime.NumCPU(),
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
	}

	// CPU model from /proc/cpuinfo on Linux
	if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "model name") {
				if idx := strings.Index(line, ":"); idx >= 0 {
					hw.CPUModel = strings.TrimSpace(line[idx+1:])
					break
				}
			}
		}
	}

	// RAM from /proc/meminfo on Linux
	if data, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				parts := strings.Fields(line)
				if len(parts) >= 2 {
					kb := 0
					fmt.Sscanf(parts[1], "%d", &kb)
					gb := float64(kb) / (1024 * 1024)
					hw.RAMGB = fmt.Sprintf("%.0fGB", gb)
				}
				break
			}
		}
	}

	// Disk type heuristic: scan /sys/block/* for the root filesystem device.
	if hw.DiskType == "" {
		hw.DiskType = "unknown"
		// Find the device backing / so we check the right disk.
		rootDev := resolveRootDevice()
		if rootDev != "" {
			rotPath := "/sys/block/" + rootDev + "/queue/rotational"
			if rot, err := os.ReadFile(rotPath); err == nil {
				rot = []byte(strings.TrimSpace(string(rot)))
				if string(rot) == "0" {
					hw.DiskType = "NVMe"
				} else if string(rot) == "1" {
					hw.DiskType = "HDD"
				}
			}
		}
	}

	return hw
}

// resolveRootDevice finds the block device name (e.g. "nvme0n1", "sda") that
// backs the root filesystem by reading /proc/mounts and mapping it to /sys/block/.
func resolveRootDevice() string {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == "/" {
			dev := fields[0]
			// Strip partition suffix: nvme0n1p2 -> nvme0n1, sda2 -> sda
			if idx := strings.Index(dev, "/dev/"); idx == 0 {
				dev = dev[5:]
			}
			// Handle LVM: /dev/mapper/vg-lv -> skip, too complex
			if strings.Contains(dev, "/") {
				dev = filepath.Base(dev)
			}
			// nvme0n1p2 -> nvme0n1
			for _, suffix := range []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"} {
				dev = strings.TrimSuffix(dev, suffix)
			}
			// sda2 -> sda (strip trailing digits after single letter prefix)
			if len(dev) >= 3 && dev[0] == 's' && dev[1] == 'd' {
				for len(dev) > 3 && dev[len(dev)-1] >= '0' && dev[len(dev)-1] <= '9' {
					dev = dev[:len(dev)-1]
				}
			}
			// Verify the block device exists
			if _, err := os.Stat("/sys/block/" + dev); err == nil {
				return dev
			}
			return dev
		}
	}
	return ""
}

func printHardwareInfo(hw HardwareInfo) {
	fmt.Fprintf(os.Stderr, "=== Hardware Information ===\n")
	fmt.Fprintf(os.Stderr, "  CPU:         %s\n", hw.CPUModel)
	fmt.Fprintf(os.Stderr, "  Cores:       %d\n", hw.CPUCores)
	fmt.Fprintf(os.Stderr, "  RAM:         %s\n", hw.RAMGB)
	fmt.Fprintf(os.Stderr, "  OS:          %s/%s\n", hw.OS, hw.Arch)
	fmt.Fprintf(os.Stderr, "  Go:          %s\n", hw.GoVersion)
	fmt.Fprintf(os.Stderr, "  Disk:        %s\n", hw.DiskType)
}

// ---------------------------------------------------------------------------
// Markdown table output
// ---------------------------------------------------------------------------

func printMarkdownTable(w io.Writer, hw HardwareInfo, results []BenchmarkResult) {
	fmt.Fprintf(w, "\n## Benchmark Results\n\n")
	fmt.Fprintf(w, "### Environment\n\n")
	fmt.Fprintf(w, "| Property | Value |\n")
	fmt.Fprintf(w, "|----------|-------|\n")
	fmt.Fprintf(w, "| CPU | %s |\n", hw.CPUModel)
	fmt.Fprintf(w, "| Cores | %d |\n", hw.CPUCores)
	fmt.Fprintf(w, "| RAM | %s |\n", hw.RAMGB)
	fmt.Fprintf(w, "| OS | %s/%s |\n", hw.OS, hw.Arch)
	fmt.Fprintf(w, "| Go | %s |\n", hw.GoVersion)
	fmt.Fprintf(w, "| Disk | %s |\n", hw.DiskType)
	fmt.Fprintf(w, "\n")

	// Separate normal results from resume results
	var normal, resume []BenchmarkResult
	for _, r := range results {
		if r.IsResumeTest {
			resume = append(resume, r)
		} else {
			normal = append(normal, r)
		}
	}

	if len(normal) > 0 {
		fmt.Fprintf(w, "### Migration Performance\n\n")
		fmt.Fprintf(w, "| Scenario | Size | Records | Data | Batch | Workers | Verify | Time | Rows/s | MB/s | Peak RAM |\n")
		fmt.Fprintf(w, "|----------|------|--------:|-----:|------:|--------:|--------|-----:|-------:|-----:|---------:|\n")
		for _, r := range normal {
			fmt.Fprintf(w, "| %s | %s | %s | %s | %d | %d | %v | %v | %s | %s | %s |\n",
				r.Scenario,
				r.SizeName,
				formatInt(r.Records),
				formatBytes(r.DataSizeBytes),
				r.BatchSize,
				r.Workers,
				r.Verify,
				r.TotalDuration.Round(time.Millisecond),
				formatFloat(r.RowsPerSec),
				formatFloat(r.MBPerSec),
				formatRAM(r.PeakRAMMB),
			)
		}
		fmt.Fprintf(w, "\n")
	}

	if len(resume) > 0 {
		fmt.Fprintf(w, "### Resume Performance\n\n")
		fmt.Fprintf(w, "| Size | Records | Before Interrupt | After Resume | Recovery Time | Correct |\n")
		fmt.Fprintf(w, "|------|--------:|-----------------:|-------------:|--------------:|--------|\n")
		for _, r := range resume {
			fmt.Fprintf(w, "| %s | %s | %s | %s | %v | %v |\n",
				r.SizeName,
				formatInt(r.Records),
				formatInt64(r.RecordsBefore),
				formatInt64(r.RecordsAfter),
				r.RecoveryTime.Round(time.Millisecond),
				r.DataCorrect,
			)
		}
		fmt.Fprintf(w, "\n")
	}

	// Parameter exploration analysis
	printParameterAnalysis(w, hw, normal)
}

func printParameterAnalysis(w io.Writer, hw HardwareInfo, results []BenchmarkResult) {
	if len(results) < 2 {
		return
	}

	fmt.Fprintf(w, "### Parameter Analysis\n\n")

	// Group by size
	bySize := make(map[string][]BenchmarkResult)
	for _, r := range results {
		bySize[r.SizeName] = append(bySize[r.SizeName], r)
	}

	for _, sizeName := range sortedKeys(bySize) {
		rs := bySize[sizeName]
		fmt.Fprintf(w, "#### %s dataset (%s records)\n\n", sizeName, formatInt(rs[0].Records))

		// Batch size analysis (fixed workers), grouped by scenario
		analyzeBatchSizes(w, rs, hw.CPUCores)

		// Worker analysis (fixed batch size), grouped by scenario
		analyzeWorkers(w, rs, hw.CPUCores)

		// Verify on/off
		analyzeVerify(w, rs)
	}
}

func analyzeBatchSizes(w io.Writer, results []BenchmarkResult, cpuCores int) {
	// Group by scenario+workers, then compare batch sizes within each group.
	// Only emit a table when a scenario has multiple batch sizes at the same
	// worker count.
	type key struct {
		scenario string
		workers  int
	}
	groups := make(map[key][]BenchmarkResult)
	for _, r := range results {
		k := key{r.Scenario, r.Workers}
		groups[k] = append(groups[k], r)
	}

	// Sort keys for deterministic output
	var keys []key
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].scenario != keys[j].scenario {
			return keys[i].scenario < keys[j].scenario
		}
		return keys[i].workers < keys[j].workers
	})

	for _, k := range keys {
		rs := groups[k]
		if len(rs) < 2 {
			continue
		}
		sort.Slice(rs, func(i, j int) bool { return rs[i].BatchSize < rs[j].BatchSize })

		fmt.Fprintf(w, "**%s** — batch size comparison (workers=%d):\n\n", k.scenario, k.workers)
		fmt.Fprintf(w, "| Batch | Rows/s | MB/s | Time | Peak RAM |\n")
		fmt.Fprintf(w, "|------:|-------:|-----:|-----:|---------:|\n")
		for _, r := range rs {
			fmt.Fprintf(w, "| %d | %s | %s | %v | %s |\n",
				r.BatchSize,
				formatFloat(r.RowsPerSec),
				formatFloat(r.MBPerSec),
				r.TotalDuration.Round(time.Millisecond),
				formatRAM(r.PeakRAMMB),
			)
		}
		fmt.Fprintf(w, "\n")

		// Find optimal batch size (best rows/s)
		best := rs[0]
		for _, r := range rs[1:] {
			if r.RowsPerSec > best.RowsPerSec {
				best = r
			}
		}
		fmt.Fprintf(w, "> Optimal batch size at workers=%d: **%d** (%.0f rows/s)\n\n",
			k.workers, best.BatchSize, best.RowsPerSec)
	}
}

func analyzeWorkers(w io.Writer, results []BenchmarkResult, cpuCores int) {
	// Group by scenario+batch, then compare workers within each group.
	type key struct {
		scenario string
		batch    int
	}
	groups := make(map[key][]BenchmarkResult)
	for _, r := range results {
		k := key{r.Scenario, r.BatchSize}
		groups[k] = append(groups[k], r)
	}

	// Sort keys for deterministic output
	var keys []key
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].scenario != keys[j].scenario {
			return keys[i].scenario < keys[j].scenario
		}
		return keys[i].batch < keys[j].batch
	})

	for _, k := range keys {
		rs := groups[k]
		if len(rs) < 2 {
			continue
		}
		sort.Slice(rs, func(i, j int) bool { return rs[i].Workers < rs[j].Workers })

		fmt.Fprintf(w, "**%s** — worker scaling comparison (batch=%d):\n\n", k.scenario, k.batch)
		fmt.Fprintf(w, "| Workers | CPU Ratio | Rows/s | Speedup | Peak RAM |\n")
		fmt.Fprintf(w, "|--------:|----------:|-------:|--------:|---------:|\n")
		for _, r := range rs {
			ratio := float64(r.Workers) / float64(cpuCores)
			fmt.Fprintf(w, "| %d | %.2fx | %s |", r.Workers, ratio, formatFloat(r.RowsPerSec))
			if len(rs) > 0 && rs[0].Workers != r.Workers && rs[0].RowsPerSec > 0 {
				speedup := r.RowsPerSec / rs[0].RowsPerSec
				fmt.Fprintf(w, " %.2fx |", speedup)
			} else {
				fmt.Fprintf(w, " - |")
			}
			fmt.Fprintf(w, " %s |\n", formatRAM(r.PeakRAMMB))
		}
		fmt.Fprintf(w, "\n")

		// CPU scaling analysis
		maxWorkers := rs[len(rs)-1].Workers
		if maxWorkers <= cpuCores {
			fmt.Fprintf(w, "> With %d CPU cores available: all tested worker counts fit within CPU capacity.\n", cpuCores)
		} else {
			fmt.Fprintf(w, "> With %d CPU cores available: workers > %d will contend for CPU time, diminishing returns expected.\n", cpuCores, cpuCores)
		}
		fmt.Fprintf(w, "\n")
	}
}

func analyzeVerify(w io.Writer, results []BenchmarkResult) {
	withVerify := filterVerify(results, true)
	withoutVerify := filterVerify(results, false)
	if len(withVerify) == 0 || len(withoutVerify) == 0 {
		return
	}

	fmt.Fprintf(w, "Verification overhead:\n\n")
	fmt.Fprintf(w, "| Size | Without Verify | With Verify | Overhead |\n")
	fmt.Fprintf(w, "|------|---------------:|------------:|--------:|\n")
	for _, wv := range withVerify {
		for _, wov := range withoutVerify {
			if wv.SizeName == wov.SizeName && wv.BatchSize == wov.BatchSize && wv.Workers == wov.Workers {
				overhead := time.Duration(0)
				if wov.TotalDuration > 0 {
					overhead = wv.TotalDuration - wov.TotalDuration
				}
				fmt.Fprintf(w, "| %s | %v | %v | +%v |\n",
					wv.SizeName,
					wov.TotalDuration.Round(time.Millisecond),
					wv.TotalDuration.Round(time.Millisecond),
					overhead.Round(time.Millisecond),
				)
			}
		}
	}
	fmt.Fprintf(w, "\n")
}

func filterVerify(results []BenchmarkResult, verify bool) []BenchmarkResult {
	var out []BenchmarkResult
	for _, r := range results {
		if r.Verify == verify {
			out = append(out, r)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Full report writer
// ---------------------------------------------------------------------------

func writeFullReport(w io.Writer, hw HardwareInfo, results []BenchmarkResult) {
	fmt.Fprintf(w, "# Bridge-DB Benchmark Report\n\n")
	fmt.Fprintf(w, "Generated: %s\n\n", time.Now().UTC().Format(time.RFC3339))

	printMarkdownTable(w, hw, results)

	// Phase breakdown
	fmt.Fprintf(w, "### Phase Breakdown\n\n")
	fmt.Fprintf(w, "| Scenario | Size | Schema | Transfer | Verify |\n")
	fmt.Fprintf(w, "|----------|------|-------:|---------:|-------:|\n")
	for _, r := range results {
		if r.IsResumeTest {
			continue
		}
		fmt.Fprintf(w, "| %s | %s | %v | %v | %v |\n",
			r.Label,
			r.SizeName,
			r.SchemaTime.Round(time.Millisecond),
			r.TransferTime.Round(time.Millisecond),
			r.VerifyTime.Round(time.Millisecond),
		)
	}
	fmt.Fprintf(w, "\n")

	// Interpretation guide
	fmt.Fprintf(w, "### Interpreting Results\n\n")
	fmt.Fprintf(w, "- **CPU impact**: Workers beyond %d cores will compete for CPU time.\n", hw.CPUCores)
	fmt.Fprintf(w, "  Diminishing returns are expected when workers >> cores.\n")
	fmt.Fprintf(w, "- **RAM impact**: Peak RAM should stay well below %s total system memory.\n", hw.RAMGB)
	fmt.Fprintf(w, "  If Peak RAM approaches system total, expect swapping and degraded performance.\n")
	fmt.Fprintf(w, "- **Batch size**: Larger batches reduce per-row overhead but increase memory usage.\n")
	fmt.Fprintf(w, "  The optimal size depends on row width and available memory.\n")
	fmt.Fprintf(w, "- **Verification**: Adds a full pass over destination data; overhead is proportional\n")
	fmt.Fprintf(w, "  to dataset size and verification depth.\n")
	fmt.Fprintf(w, "- **Disk I/O**: SQLite uses file-based I/O. SSD results will differ significantly from HDD.\n")
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

func formatInt(n int) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%dM", n/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%dK", n/1_000)
	}
	return fmt.Sprintf("%d", n)
}

func formatInt64(n int64) string {
	if n >= 1_000_000 {
		return fmt.Sprintf("%dM", n/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%dK", n/1_000)
	}
	return fmt.Sprintf("%d", n)
}

func formatBytes(b int64) string {
	const mb = 1024 * 1024
	if b >= mb {
		return fmt.Sprintf("%.0fMB", float64(b)/float64(mb))
	}
	if b >= 1024 {
		return fmt.Sprintf("%.0fKB", float64(b)/1024)
	}
	return fmt.Sprintf("%dB", b)
}

func formatFloat(f float64) string {
	if f >= 10000 {
		return fmt.Sprintf("%.0f", f)
	}
	if f >= 100 {
		return fmt.Sprintf("%.1f", f)
	}
	return fmt.Sprintf("%.2f", f)
}

func formatRAM(mb float64) string {
	if mb >= 1024 {
		return fmt.Sprintf("%.1fGB", mb/1024)
	}
	return fmt.Sprintf("%.0fMB", mb)
}

func sortedKeys(m map[string][]BenchmarkResult) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---------------------------------------------------------------------------
// Tabular output for console
// ---------------------------------------------------------------------------

func printTable(w io.Writer, results []BenchmarkResult) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "SCENARIO\tSIZE\tRECORDS\tBATCH\tWORKERS\tVERIFY\tTIME\tROWS/S\tMB/S\tRAM\n")
	fmt.Fprintf(tw, "--------\t----\t-------\t-----\t-------\t------\t----\t------\t-----\t---\n")
	for _, r := range results {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%v\t%v\t%.0f\t%.1f\t%.0fMB\n",
			r.Scenario,
			r.SizeName,
			r.Records,
			r.BatchSize,
			r.Workers,
			r.Verify,
			r.TotalDuration.Round(time.Millisecond),
			r.RowsPerSec,
			r.MBPerSec,
			r.PeakRAMMB,
		)
	}
	tw.Flush()
}

// ---------------------------------------------------------------------------
// JSON checkpoint unmarshal (avoids importing bridge internals)
// ---------------------------------------------------------------------------

func unmarshalCheckpoint(data []byte, v any) error {
	return sonic.Unmarshal(data, v)
}
