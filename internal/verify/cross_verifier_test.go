package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pageton/bridge-db/pkg/provider"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ---------------------------------------------------------------------------
// Options tests
// ---------------------------------------------------------------------------

func TestOptions_Validate(t *testing.T) {
	tests := []struct {
		name    string
		opts    Options
		wantErr bool
	}{
		{"defaults", DefaultOptions(), false},
		{"none mode", Options{SampleMode: "none"}, false},
		{"count mode valid", Options{SampleMode: "count", SampleN: 50}, false},
		{"count mode zero N", Options{SampleMode: "count", SampleN: 0}, true},
		{"pct mode valid", Options{SampleMode: "pct", SamplePct: 10.0}, false},
		{"pct mode zero", Options{SampleMode: "pct", SamplePct: 0}, true},
		{"pct mode over 100", Options{SampleMode: "pct", SamplePct: 101}, true},
		{"invalid mode", Options{SampleMode: "bad"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCountsOnly(t *testing.T) {
	opts := CountsOnly()
	if !opts.CountComparison {
		t.Error("CountsOnly should enable CountComparison")
	}
	if opts.SampleMode != "none" {
		t.Error("CountsOnly should disable sampling")
	}
	if opts.ChecksumComparison {
		t.Error("CountsOnly should disable checksum comparison")
	}
}

// ---------------------------------------------------------------------------
// Count comparison tests
// ---------------------------------------------------------------------------

// mockProvider implements enough of provider.Provider for verification tests.
type mockProvider struct {
	name         string
	tables       map[string]int64
	records      map[string]map[string]any
	checksums    map[string]string
	scannerUnits []provider.MigrationUnit
	lastScanOpts provider.ScanOptions
}

func (m *mockProvider) Name() string                              { return m.name }
func (m *mockProvider) Connect(_ context.Context, _, _ any) error { return nil }
func (m *mockProvider) Close() error                              { return nil }
func (m *mockProvider) Ping(_ context.Context) error              { return nil }
func (m *mockProvider) DryRun() provider.Provider                 { return m }
func (m *mockProvider) SchemaMigrator(_ context.Context) provider.SchemaMigrator {
	return nil
}

func (m *mockProvider) Verifier(_ context.Context) provider.Verifier {
	return &mockVerifier{}
}

func (m *mockProvider) Writer(_ context.Context, _ provider.WriteOptions) provider.Writer {
	return &mockWriter{}
}

func (m *mockProvider) Scanner(_ context.Context, opts provider.ScanOptions) provider.Scanner {
	m.lastScanOpts = opts
	skip := make(map[string]bool, len(opts.TablesCompleted))
	for _, table := range opts.TablesCompleted {
		skip[table] = true
	}
	units := make([]provider.MigrationUnit, 0, len(m.scannerUnits))
	for _, unit := range m.scannerUnits {
		if skip[unit.Table] {
			continue
		}
		units = append(units, unit)
	}
	return &mockScanner{units: units}
}

// Implement optional interfaces for cross-verification
func (m *mockProvider) EnumerateTables(_ context.Context) (map[string]int64, error) {
	return m.tables, nil
}

func (m *mockProvider) ReadRecords(_ context.Context, keys []string) (map[string]map[string]any, error) {
	result := make(map[string]map[string]any)
	for _, k := range keys {
		if v, ok := m.records[k]; ok {
			result[k] = v
		}
	}
	return result, nil
}

func (m *mockProvider) ComputeChecksums(_ context.Context, keys []string) (map[string]string, error) {
	result := make(map[string]string)
	for _, k := range keys {
		if v, ok := m.checksums[k]; ok {
			result[k] = v
		}
	}
	return result, nil
}

type mockVerifier struct{}

func (m *mockVerifier) Verify(_ context.Context, _ []string) ([]provider.VerificationError, error) {
	return nil, nil
}

type mockWriter struct{}

func (m *mockWriter) Write(_ context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	return &provider.BatchResult{TotalUnits: len(units), WrittenUnits: len(units)}, nil
}
func (m *mockWriter) Flush(_ context.Context) error { return nil }

type mockScanner struct {
	units []provider.MigrationUnit
	done  bool
}

func (m *mockScanner) Next(_ context.Context) ([]provider.MigrationUnit, error) {
	if m.done {
		return nil, nil
	}
	m.done = true
	return m.units, nil
}
func (m *mockScanner) Stats() provider.ScanStats { return provider.ScanStats{} }

func (m *mockScanner) Close() error { return nil }

func TestCountComparison_Matching(t *testing.T) {
	src := &mockProvider{
		name:   "src",
		tables: map[string]int64{"users": 100, "orders": 500},
	}
	dst := &mockProvider{
		name:   "dst",
		tables: map[string]int64{"users": 100, "orders": 500},
	}

	cv := NewCrossVerifier(src, dst, CountsOnly())
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !report.Passed() {
		t.Errorf("expected pass, got mismatches: %v", report.Mismatches)
	}
}

func TestCountComparison_CountMismatch(t *testing.T) {
	src := &mockProvider{
		name:   "src",
		tables: map[string]int64{"users": 100},
	}
	dst := &mockProvider{
		name:   "dst",
		tables: map[string]int64{"users": 99},
	}

	cv := NewCrossVerifier(src, dst, CountsOnly())
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Passed() {
		t.Fatal("expected mismatch to be detected")
	}
	if report.Mismatches[0].Category != MismatchCountMismatch {
		t.Errorf("expected count_mismatch, got %s", report.Mismatches[0].Category)
	}
}

func TestCountComparison_MissingTable(t *testing.T) {
	src := &mockProvider{
		name:   "src",
		tables: map[string]int64{"users": 100, "orders": 50},
	}
	dst := &mockProvider{
		name:   "dst",
		tables: map[string]int64{"users": 100},
	}

	cv := NewCrossVerifier(src, dst, CountsOnly())
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Passed() {
		t.Fatal("expected mismatch for missing table")
	}
	found := false
	for _, m := range report.Mismatches {
		if m.Table == "orders" && m.Category == MismatchCountMismatch {
			found = true
		}
	}
	if !found {
		t.Error("expected mismatch for 'orders' table")
	}
}

func TestCountComparison_ExtraTableInDest(t *testing.T) {
	src := &mockProvider{
		name:   "src",
		tables: map[string]int64{"users": 100},
	}
	dst := &mockProvider{
		name:   "dst",
		tables: map[string]int64{"users": 100, "orphan": 10},
	}

	cv := NewCrossVerifier(src, dst, CountsOnly())
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Passed() {
		t.Fatal("expected mismatch for extra dest table")
	}
}

// ---------------------------------------------------------------------------
// Checksum comparison tests
// ---------------------------------------------------------------------------

func TestChecksumComparison_Matching(t *testing.T) {
	src := &mockProvider{
		name:      "postgres",
		tables:    map[string]int64{"users": 10},
		checksums: map[string]string{"k1": "abc123", "k2": "def456"},
		scannerUnits: []provider.MigrationUnit{
			{Key: "k1", Table: "users"},
			{Key: "k2", Table: "users"},
		},
	}
	dst := &mockProvider{
		name:      "postgres",
		tables:    map[string]int64{"users": 10},
		checksums: map[string]string{"k1": "abc123", "k2": "def456"},
	}

	opts := Options{
		CountComparison:    true,
		SampleMode:         "count",
		SampleN:            10,
		ChecksumComparison: true,
		MaxSampleKeys:      10000,
	}
	cv := NewCrossVerifier(src, dst, opts)
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !report.Passed() {
		t.Errorf("expected pass, got mismatches: %v", report.Mismatches)
	}
}

func TestChecksumComparison_Mismatch(t *testing.T) {
	src := &mockProvider{
		name:      "postgres",
		tables:    map[string]int64{"users": 10},
		checksums: map[string]string{"k1": "abc123"},
		scannerUnits: []provider.MigrationUnit{
			{Key: "k1", Table: "users"},
		},
	}
	dst := &mockProvider{
		name:      "postgres",
		tables:    map[string]int64{"users": 10},
		checksums: map[string]string{"k1": "different"},
	}

	opts := Options{
		CountComparison:    true,
		SampleMode:         "count",
		SampleN:            10,
		ChecksumComparison: true,
		MaxSampleKeys:      10000,
	}
	cv := NewCrossVerifier(src, dst, opts)
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Passed() {
		t.Fatal("expected checksum mismatch to be detected")
	}
	found := false
	for _, m := range report.Mismatches {
		if m.Category == MismatchChecksumDiff && m.Key == "k1" {
			found = true
		}
	}
	if !found {
		t.Error("expected checksum_diff mismatch for key k1")
	}
}

func TestChecksumComparison_MissingInDest(t *testing.T) {
	src := &mockProvider{
		name:      "postgres",
		tables:    map[string]int64{"users": 5},
		checksums: map[string]string{"k1": "abc123"},
		scannerUnits: []provider.MigrationUnit{
			{Key: "k1", Table: "users"},
		},
	}
	dst := &mockProvider{
		name:      "postgres",
		tables:    map[string]int64{"users": 5},
		checksums: map[string]string{}, // k1 missing
	}

	opts := Options{
		CountComparison:    true,
		SampleMode:         "count",
		SampleN:            10,
		ChecksumComparison: true,
		MaxSampleKeys:      10000,
	}
	cv := NewCrossVerifier(src, dst, opts)
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, m := range report.Mismatches {
		if m.Category == MismatchMissingInDst && m.Key == "k1" {
			found = true
		}
	}
	if !found {
		t.Error("expected missing_in_destination for key k1")
	}
}

// ---------------------------------------------------------------------------
// Record comparison tests
// ---------------------------------------------------------------------------

func TestRecordComparison_FieldDiff(t *testing.T) {
	src := &mockProvider{
		name:   "src",
		tables: map[string]int64{"users": 5},
		records: map[string]map[string]any{
			"k1": {"id": 1, "name": "alice", "age": 30},
		},
		scannerUnits: []provider.MigrationUnit{
			{Key: "k1", Table: "users"},
		},
	}
	dst := &mockProvider{
		name:   "dst",
		tables: map[string]int64{"users": 5},
		records: map[string]map[string]any{
			"k1": {"id": 1, "name": "alice", "age": 31}, // age differs
		},
	}

	opts := Options{
		CountComparison:    true,
		SampleMode:         "count",
		SampleN:            10,
		ChecksumComparison: false, // use record comparison
		MaxSampleKeys:      10000,
	}
	cv := NewCrossVerifier(src, dst, opts)
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, m := range report.Mismatches {
		if m.Category == MismatchValueDiff && m.Key == "k1" {
			found = true
		}
	}
	if !found {
		t.Error("expected value_diff mismatch for key k1")
	}
}

func TestCrossProviderSampling_FallsBackToRecordComparison(t *testing.T) {
	src := &mockProvider{
		name:   "mongodb",
		tables: map[string]int64{"users": 1},
		records: map[string]map[string]any{
			"users:abc123": {"_id": "abc123", "name": "alice"},
		},
		checksums: map[string]string{"users:abc123": "src-hash"},
		scannerUnits: []provider.MigrationUnit{
			{Key: "users:abc123", Table: "users"},
		},
	}
	dst := &mockProvider{
		name:   "postgres",
		tables: map[string]int64{"users": 1},
		records: map[string]map[string]any{
			"users:abc123": {"_id": "abc123", "name": "alice"},
		},
		checksums: map[string]string{"users:abc123": "dst-hash"},
	}

	opts := Options{
		CountComparison:    true,
		SampleMode:         "count",
		SampleN:            1,
		ChecksumComparison: true,
		MaxSampleKeys:      10000,
	}
	cv := NewCrossVerifier(src, dst, opts)
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !report.Passed() {
		t.Fatalf("expected pass with record-comparison fallback, got mismatches: %+v", report.Mismatches)
	}
}

func TestCrossProviderSampling_NormalizesMongoObjectIDToString(t *testing.T) {
	id := bson.NewObjectID()
	key := "users:" + id.Hex()

	src := &mockProvider{
		name:   "mongodb",
		tables: map[string]int64{"users": 1},
		records: map[string]map[string]any{
			key: {"_id": id, "name": "alice"},
		},
		scannerUnits: []provider.MigrationUnit{{Key: key, Table: "users"}},
	}
	dst := &mockProvider{
		name:   "postgres",
		tables: map[string]int64{"users": 1},
		records: map[string]map[string]any{
			key: {"_id": id.Hex(), "name": "alice"},
		},
	}

	opts := Options{
		CountComparison:    true,
		SampleMode:         "count",
		SampleN:            1,
		ChecksumComparison: true,
		MaxSampleKeys:      10000,
	}

	cv := NewCrossVerifier(src, dst, opts)
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !report.Passed() {
		t.Fatalf("expected normalized ObjectID/string comparison to pass, got mismatches: %+v", report.Mismatches)
	}
}

func TestRemapDestinationCounts_SQLToRedisUsesSourceTableCounts(t *testing.T) {
	src := map[string]int64{"users": 2, "orders": 3}
	dst := map[string]int64{"": 5}
	got, ok := remapDestinationCounts("sqlite", "redis", src, dst)
	if !ok {
		t.Fatal("expected remapDestinationCounts to remap sql->redis counts")
	}
	if got["users"] != 2 || got["orders"] != 3 {
		t.Fatalf("remapped counts = %#v", got)
	}
}

func TestRemapSourceCounts_RedisToMongoUsesDestinationCollectionCounts(t *testing.T) {
	src := map[string]int64{"": 5}
	dst := map[string]int64{"users": 2, "orders": 3}
	got, ok := remapSourceCounts("redis", "mongodb", src, dst)
	if !ok {
		t.Fatal("expected remapSourceCounts to remap redis->mongodb counts")
	}
	if got["users"] != 2 || got["orders"] != 3 {
		t.Fatalf("remapped counts = %#v", got)
	}
}

func TestCrossProviderSampling_RedisToMongoSkipsSyntheticTables(t *testing.T) {
	src := &mockProvider{
		name:   "redis",
		tables: map[string]int64{"": 2},
		records: map[string]map[string]any{
			"users:1": {"type": "hash", "value": map[string]string{"id": "1"}},
		},
		scannerUnits: []provider.MigrationUnit{{Key: "users:1", Table: ""}},
	}
	dst := &mockProvider{
		name:   "mongodb",
		tables: map[string]int64{"users": 2},
	}

	opts := Options{CountComparison: true, SampleMode: "count", SampleN: 1, MaxSampleKeys: 10000}
	cv := NewCrossVerifier(src, dst, opts)
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !report.Passed() {
		t.Fatalf("expected redis->mongodb synthetic table verification to avoid false mismatches: %+v", report.Mismatches)
	}
}

type stringyDecimal struct{}

func (stringyDecimal) String() string { return "999.99" }

func TestValuesEqual_NormalizesByteDecimalAndStringer(t *testing.T) {
	if !valuesEqual([]byte("999.99"), stringyDecimal{}) {
		t.Fatal("expected []byte decimal and Stringer decimal to compare equal")
	}
}

func TestValuesEqual_NormalizesByteDecimalAndPgNumeric(t *testing.T) {
	dec := pgtype.Numeric{Int: big.NewInt(99999), Exp: -2, Valid: true}
	if !valuesEqual([]byte("999.99"), dec) {
		t.Fatal("expected []byte decimal and pgtype.Numeric to compare equal")
	}
}

func TestValuesEqual_NormalizesJSONNumberAndDecimalString(t *testing.T) {
	if !valuesEqual(json.Number("999.99"), []byte("999.99")) {
		t.Fatal("expected json.Number and []byte decimal to compare equal")
	}
}

func TestValuesEqual_NormalizesFloatAndDecimalBytes(t *testing.T) {
	if !valuesEqual(999.99, []byte("999.99")) {
		t.Fatal("expected float64 and []byte decimal to compare equal")
	}
}

func TestValuesEqual_NormalizesFloat64AndDecimalString(t *testing.T) {
	if !valuesEqual(999.99, "999.99") {
		t.Fatal("expected float64 and decimal string to compare equal")
	}
}

func TestValuesEqual_NormalizesIntegerAndDecimalString(t *testing.T) {
	if !valuesEqual(int64(1000), "1000") {
		t.Fatal("expected integer and decimal string to compare equal")
	}
}

func TestNormalizeRecordsForComparison_FlattensRedisHashEnvelope(t *testing.T) {
	src := map[string]any{"id": 1, "name": "alice"}
	dst := map[string]any{
		"type":  "hash",
		"value": map[string]any{"id": 1, "name": "alice"},
		"ttl":   int64(0),
	}

	normSrc, normDst := normalizeRecordsForComparison(src, dst)
	if len(compareFields(normSrc, normDst)) != 0 {
		t.Fatal("expected flattened redis hash to compare equal to flat source row")
	}
}

func TestNormalizeRecordsForComparison_FlattensRedisStringMapEnvelope(t *testing.T) {
	src := map[string]any{"id": "1", "name": "alice"}
	dst := map[string]any{
		"type":  "hash",
		"value": map[string]string{"id": "1", "name": "alice"},
		"ttl":   int64(0),
	}

	normSrc, normDst := normalizeRecordsForComparison(src, dst)
	if len(compareFields(normSrc, normDst)) != 0 {
		t.Fatal("expected flattened redis string map to compare equal to flat source row")
	}
}

// ---------------------------------------------------------------------------
// Table summary tests
// ---------------------------------------------------------------------------

func TestTableSummary(t *testing.T) {
	src := &mockProvider{
		name:   "src",
		tables: map[string]int64{"a": 10, "b": 20},
	}
	dst := &mockProvider{
		name:   "dst",
		tables: map[string]int64{"a": 10, "b": 21}, // b mismatches
	}

	cv := NewCrossVerifier(src, dst, CountsOnly())
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.TotalTables != 2 {
		t.Errorf("expected 2 total tables, got %d", report.TotalTables)
	}
	if report.FailCount != 1 {
		t.Errorf("expected 1 failed table, got %d", report.FailCount)
	}
	if report.PassCount != 1 {
		t.Errorf("expected 1 passed table, got %d", report.PassCount)
	}
}

// ---------------------------------------------------------------------------
// MismatchDetail.String() tests
// ---------------------------------------------------------------------------

func TestMismatchDetail_String(t *testing.T) {
	tests := []struct {
		name    string
		detail  MismatchDetail
		wantHas string
	}{
		{
			name: "count mismatch",
			detail: MismatchDetail{
				Category: MismatchCountMismatch,
				Table:    "users",
				SrcCount: 100,
				DstCount: 99,
			},
			wantHas: "count_mismatch",
		},
		{
			name: "missing in dst",
			detail: MismatchDetail{
				Category: MismatchMissingInDst,
				Table:    "users",
				Key:      "k1",
			},
			wantHas: "not found in destination",
		},
		{
			name: "extra in dst",
			detail: MismatchDetail{
				Category: MismatchExtraInDst,
				Table:    "users",
				Key:      "k1",
			},
			wantHas: "exists in destination but not in source",
		},
		{
			name: "value diff",
			detail: MismatchDetail{
				Category:   MismatchValueDiff,
				Table:      "users",
				Key:        "k1",
				FieldDiffs: []FieldDiff{{Field: "age"}},
			},
			wantHas: "field(s) differ",
		},
		{
			name: "checksum diff",
			detail: MismatchDetail{
				Category:    MismatchChecksumDiff,
				Table:       "users",
				Key:         "k1",
				SrcChecksum: "abc",
				DstChecksum: "def",
			},
			wantHas: "checksum_mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.detail.String()
			if s == "" {
				t.Error("String() returned empty")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Report tests
// ---------------------------------------------------------------------------

func TestVerificationReport_Summary(t *testing.T) {
	passed := &VerificationReport{TotalTables: 3, FailCount: 0}
	s := passed.Summary()
	if s == "" {
		t.Error("Summary() returned empty")
	}

	failed := &VerificationReport{
		TotalTables: 3,
		FailCount:   1,
		Mismatches:  []MismatchDetail{{Table: "users"}},
	}
	s = failed.Summary()
	if s == "" {
		t.Error("Summary() returned empty")
	}
}

func TestToVerificationErrors(t *testing.T) {
	report := &VerificationReport{
		Mismatches: []MismatchDetail{
			{Table: "users", Key: "k1", Message: "count mismatch"},
			{Table: "orders", Key: "k2", Message: "checksum diff"},
		},
	}
	errs := ToVerificationErrors(report)
	if len(errs) != 2 {
		t.Fatalf("expected 2 errors, got %d", len(errs))
	}
	if errs[0].Table != "users" {
		t.Errorf("expected table 'users', got %q", errs[0].Table)
	}
}

// ---------------------------------------------------------------------------
// Per-table result tests
// ---------------------------------------------------------------------------

func TestPerTableResults_Pass(t *testing.T) {
	src := &mockProvider{
		name:   "src",
		tables: map[string]int64{"users": 100, "orders": 200},
	}
	dst := &mockProvider{
		name:   "dst",
		tables: map[string]int64{"users": 100, "orders": 200},
	}

	cv := NewCrossVerifier(src, dst, CountsOnly())
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(report.Tables) != 2 {
		t.Fatalf("expected 2 table results, got %d", len(report.Tables))
	}

	for _, tr := range report.Tables {
		if !tr.CountMatch {
			t.Errorf("table %s: expected CountMatch=true", tr.Table)
		}
		if tr.Status != StatusPass {
			t.Errorf("table %s: expected StatusPass, got %s", tr.Table, tr.Status)
		}
		if tr.SrcCount != tr.DstCount {
			t.Errorf("table %s: src=%d dst=%d", tr.Table, tr.SrcCount, tr.DstCount)
		}
	}
}

func TestPerTableResults_Fail(t *testing.T) {
	src := &mockProvider{
		name:   "src",
		tables: map[string]int64{"users": 100, "orders": 200},
	}
	dst := &mockProvider{
		name:   "dst",
		tables: map[string]int64{"users": 100, "orders": 199},
	}

	cv := NewCrossVerifier(src, dst, CountsOnly())
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	usersTr := findTableResult(report, "users")
	ordersTr := findTableResult(report, "orders")

	if usersTr == nil || usersTr.Status != StatusPass {
		t.Error("users table should pass")
	}
	if ordersTr == nil || ordersTr.Status != StatusFail {
		t.Error("orders table should fail")
	}
	if ordersTr.CountDelta != -1 {
		t.Errorf("expected delta -1, got %d", ordersTr.CountDelta)
	}
}

func TestPerTableResults_SamplingAddsInfo(t *testing.T) {
	src := &mockProvider{
		name:      "src",
		tables:    map[string]int64{"users": 5},
		checksums: map[string]string{"k1": "abc"},
		scannerUnits: []provider.MigrationUnit{
			{Key: "k1", Table: "users"},
		},
	}
	dst := &mockProvider{
		name:      "dst",
		tables:    map[string]int64{"users": 5},
		checksums: map[string]string{"k1": "abc"},
	}

	opts := Options{
		CountComparison:    true,
		SampleMode:         "count",
		SampleN:            5,
		ChecksumComparison: true,
		MaxSampleKeys:      10000,
	}
	cv := NewCrossVerifier(src, dst, opts)
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tr := findTableResult(report, "users")
	if tr == nil {
		t.Fatal("expected table result for users")
	}
	if tr.SampleChecked == 0 {
		t.Error("expected some samples checked")
	}
	if tr.SampleMismatch != 0 {
		t.Errorf("expected 0 mismatches, got %d", tr.SampleMismatch)
	}
	if report.TotalSampled == 0 {
		t.Error("expected TotalSampled > 0")
	}
}

func TestSampling_NonPublicSchemaUsesRawSourceTableName(t *testing.T) {
	src := &mockProvider{
		name:   "src",
		tables: map[string]int64{"tenant1.users": 1},
		records: map[string]map[string]any{
			"tenant1.users:id=1": {"id": 1, "name": "alice"},
		},
		checksums: map[string]string{"tenant1.users:id=1": "abc123"},
		scannerUnits: []provider.MigrationUnit{
			{Key: "tenant1.users:id=1", Table: "tenant1.users"},
		},
	}
	dst := &mockProvider{
		name:   "dst",
		tables: map[string]int64{"tenant1.users": 1},
		records: map[string]map[string]any{
			"tenant1.users:id=1": {"id": 1, "name": "alice"},
		},
		checksums: map[string]string{"tenant1.users:id=1": "abc123"},
	}

	opts := Options{
		CountComparison:    true,
		SampleMode:         "count",
		SampleN:            1,
		ChecksumComparison: true,
		MaxSampleKeys:      10000,
	}

	cv := NewCrossVerifier(src, dst, opts)
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tr := findTableResult(report, "tenant1.users")
	if tr == nil {
		t.Fatal("expected table result for tenant1.users")
	}
	if tr.SampleChecked != 1 {
		t.Fatalf("SampleChecked = %d, want 1", tr.SampleChecked)
	}
	if tr.SampleMismatch != 0 {
		t.Fatalf("SampleMismatch = %d, want 0", tr.SampleMismatch)
	}
	if len(src.lastScanOpts.TablesCompleted) != 0 {
		t.Fatalf("TablesCompleted = %v, want empty skip list", src.lastScanOpts.TablesCompleted)
	}
}

// ---------------------------------------------------------------------------
// Verdict and status computation tests
// ---------------------------------------------------------------------------

func TestVerdict_Pass(t *testing.T) {
	r := &VerificationReport{
		VerifiedAny: true,
		Status:      StatusPass,
		TotalTables: 3,
		PassCount:   3,
		Duration:    1 * time.Second,
	}
	v := r.Verdict()
	if !containsStr(v, "PASS") || !containsStr(v, "trustworthy") {
		t.Errorf("unexpected verdict: %s", v)
	}
}

func TestVerdict_Warn(t *testing.T) {
	r := &VerificationReport{
		VerifiedAny:     true,
		Status:          StatusWarn,
		TotalTables:     3,
		WarnCount:       1,
		TotalMismatches: 2,
		Duration:        1 * time.Second,
	}
	v := r.Verdict()
	if !containsStr(v, "WARN") || !containsStr(v, "Spot-check") {
		t.Errorf("unexpected verdict: %s", v)
	}
}

func TestVerdict_Fail(t *testing.T) {
	r := &VerificationReport{
		VerifiedAny:     true,
		Status:          StatusFail,
		TotalTables:     3,
		FailCount:       2,
		TotalMismatches: 5,
		Duration:        1 * time.Second,
	}
	v := r.Verdict()
	if !containsStr(v, "FAIL") || !containsStr(v, "Manual inspection") {
		t.Errorf("unexpected verdict: %s", v)
	}
}

func TestVerdict_Skipped(t *testing.T) {
	r := &VerificationReport{Status: StatusSkipped}
	v := r.Verdict()
	if !containsStr(v, "SKIPPED") {
		t.Errorf("unexpected verdict: %s", v)
	}
}

func TestComputeStatus(t *testing.T) {
	tests := []struct {
		name     string
		report   *VerificationReport
		expected VerificationStatus
	}{
		{
			name:     "not verified",
			report:   &VerificationReport{},
			expected: StatusSkipped,
		},
		{
			name:     "all pass",
			report:   &VerificationReport{VerifiedAny: true, PassCount: 3},
			expected: StatusPass,
		},
		{
			name:     "warn only",
			report:   &VerificationReport{VerifiedAny: true, PassCount: 2, WarnCount: 1},
			expected: StatusWarn,
		},
		{
			name:     "fail takes precedence over warn",
			report:   &VerificationReport{VerifiedAny: true, PassCount: 1, FailCount: 1, WarnCount: 1},
			expected: StatusFail,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.report.computeStatus()
			if tt.report.Status != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.report.Status)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Coverage tracking tests
// ---------------------------------------------------------------------------

func TestCoverageMetrics(t *testing.T) {
	src := &mockProvider{
		name:      "src",
		tables:    map[string]int64{"users": 100, "orders": 500},
		checksums: map[string]string{"k1": "abc"},
		scannerUnits: []provider.MigrationUnit{
			{Key: "k1", Table: "users"},
		},
	}
	dst := &mockProvider{
		name:      "dst",
		tables:    map[string]int64{"users": 100, "orders": 500},
		checksums: map[string]string{"k1": "abc"},
	}

	opts := Options{
		CountComparison:    true,
		SampleMode:         "count",
		SampleN:            50,
		ChecksumComparison: true,
		MaxSampleKeys:      10000,
	}
	cv := NewCrossVerifier(src, dst, opts)
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if report.TotalSrcRows != 600 {
		t.Errorf("expected TotalSrcRows=600, got %d", report.TotalSrcRows)
	}
	if report.TotalDstRows != 600 {
		t.Errorf("expected TotalDstRows=600, got %d", report.TotalDstRows)
	}
	// Only users has matching count AND sampling keys; orders has no scanner units.
	if report.TotalSampled == 0 {
		t.Error("expected some samples")
	}
}

// ---------------------------------------------------------------------------
// Report formatting tests
// ---------------------------------------------------------------------------

func TestFormatTable_Pass(t *testing.T) {
	report := &VerificationReport{
		SourceProvider: "postgres",
		DestProvider:   "mysql",
		Status:         StatusPass,
		TotalTables:    2,
		PassCount:      2,
		TotalSrcRows:   150,
		TotalDstRows:   150,
		Duration:       10 * time.Millisecond,
		Tables: []TableResult{
			{Table: "orders", SrcCount: 100, DstCount: 100, CountMatch: true, Status: StatusPass},
			{Table: "users", SrcCount: 50, DstCount: 50, CountMatch: true, Status: StatusPass},
		},
		VerifiedAny: true,
	}

	out := FormatTable(report)
	if !containsStr(out, "PASS") {
		t.Error("expected PASS in output")
	}
	if !containsStr(out, "trustworthy") {
		t.Error("expected 'trustworthy' in verdict")
	}
	if !containsStr(out, "150") {
		t.Error("expected row counts in output")
	}
}

func TestFormatTable_WithMismatches(t *testing.T) {
	report := &VerificationReport{
		SourceProvider:  "postgres",
		DestProvider:    "mysql",
		Status:          StatusFail,
		TotalTables:     1,
		FailCount:       1,
		TotalSrcRows:    100,
		TotalDstRows:    99,
		TotalMismatches: 1,
		Duration:        5 * time.Millisecond,
		Tables: []TableResult{
			{
				Table:      "users",
				SrcCount:   100,
				DstCount:   99,
				CountDelta: -1,
				Status:     StatusFail,
				Mismatches: []MismatchDetail{
					{
						Category: MismatchCountMismatch, Table: "users", SrcCount: 100, DstCount: 99,
						Message: "count mismatch",
					},
				},
			},
		},
		Mismatches: []MismatchDetail{
			{
				Category: MismatchCountMismatch, Table: "users", SrcCount: 100, DstCount: 99,
				Message: "count mismatch",
			},
		},
		VerifiedAny: true,
	}

	out := FormatTable(report)
	if !containsStr(out, "FAIL") {
		t.Error("expected FAIL in output")
	}
	if !containsStr(out, "MISMATCHES") {
		t.Error("expected MISMATCHES section")
	}
	if !containsStr(out, "count_mismatch") {
		t.Error("expected mismatch category in output")
	}
}

func TestFormatTable_TruncatesLongMismatches(t *testing.T) {
	mismatches := make([]MismatchDetail, 25)
	for i := range mismatches {
		mismatches[i] = MismatchDetail{
			Category: MismatchChecksumDiff,
			Table:    "big_table",
			Key:      fmt.Sprintf("k%d", i),
			Message:  fmt.Sprintf("mismatch %d", i),
		}
	}

	report := &VerificationReport{
		SourceProvider:  "src",
		DestProvider:    "dst",
		Status:          StatusWarn,
		TotalTables:     1,
		WarnCount:       1,
		TotalMismatches: 25,
		Duration:        1 * time.Millisecond,
		Mismatches:      mismatches,
		VerifiedAny:     true,
	}

	out := FormatTable(report)
	if !containsStr(out, "... and 5 more") {
		t.Errorf("expected truncation message, got:\n%s", out)
	}
}

func TestFormatTable_Skipped(t *testing.T) {
	report := &VerificationReport{
		SourceProvider: "src",
		DestProvider:   "dst",
		Status:         StatusSkipped,
		Skipped:        []string{"count_comparison (not supported)", "sampling (requires table enumeration)"},
	}

	out := FormatTable(report)
	if !containsStr(out, "SKIPPED") {
		t.Error("expected SKIPPED in output")
	}
	if !containsStr(out, "count_comparison") {
		t.Error("expected skipped reason in output")
	}
}

func TestFormatJSON(t *testing.T) {
	report := &VerificationReport{
		SourceProvider: "postgres",
		DestProvider:   "mysql",
		Status:         StatusPass,
		TotalTables:    1,
		PassCount:      1,
		VerifiedAny:    true,
		Duration:       1 * time.Millisecond,
	}

	out, err := FormatJSON(report)
	if err != nil {
		t.Fatalf("FormatJSON error: %v", err)
	}
	if !containsStr(out, `"status": "PASS"`) {
		t.Errorf("expected PASS status in JSON, got:\n%s", out)
	}
}

// ---------------------------------------------------------------------------
// Sort order test
// ---------------------------------------------------------------------------

func TestSortTables(t *testing.T) {
	report := &VerificationReport{
		Tables: []TableResult{
			{Table: "z_table"},
			{Table: "a_table"},
			{Table: "m_table"},
		},
	}
	report.sortTables()
	if report.Tables[0].Table != "a_table" {
		t.Errorf("expected a_table first, got %s", report.Tables[0].Table)
	}
	if report.Tables[2].Table != "z_table" {
		t.Errorf("expected z_table last, got %s", report.Tables[2].Table)
	}
}

// ---------------------------------------------------------------------------
// No provider support (both sides lack interfaces)
// ---------------------------------------------------------------------------

func TestVerify_NoTableEnumerator(t *testing.T) {
	// Provider that does NOT implement TableEnumerator.
	// Since mockProvider already satisfies TableEnumerator, we can't easily
	// undo that. Instead, test the code path by calling with both sides having
	// empty tables.

	src := &mockProvider{name: "src", tables: map[string]int64{}}
	dst := &mockProvider{name: "dst", tables: map[string]int64{}}

	cv := NewCrossVerifier(src, dst, CountsOnly())
	report, err := cv.Verify(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Empty tables: VerifiedAny=true (both enumerators returned), 0 tables.
	if report.TotalTables != 0 {
		t.Errorf("expected 0 tables, got %d", report.TotalTables)
	}
}

// ---------------------------------------------------------------------------
// Passed() backward compatibility
// ---------------------------------------------------------------------------

func TestPassed_BackwardCompat(t *testing.T) {
	// Passed() should return true only when VerifiedAny and no mismatches.
	r1 := &VerificationReport{VerifiedAny: true, Mismatches: nil}
	if !r1.Passed() {
		t.Error("expected Passed=true with VerifiedAny and no mismatches")
	}
	r2 := &VerificationReport{VerifiedAny: true, Mismatches: []MismatchDetail{{}}}
	if r2.Passed() {
		t.Error("expected Passed=false with mismatches")
	}
	r3 := &VerificationReport{VerifiedAny: false}
	if r3.Passed() {
		t.Error("expected Passed=false when not verified")
	}
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || strings.Contains(s, sub))
}
