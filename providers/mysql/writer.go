package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// mysqlWriter persists MigrationUnits to a MySQL destination using batch
// operations. It supports INSERT ON DUPLICATE KEY UPDATE for upsert.
type mysqlWriter struct {
	db      *sql.DB
	opts    provider.WriteOptions
	written int64
	failed  int64
	skipped int64
	bytes   int64
	log     interface {
		Info(msg string, args ...any)
		Debug(msg string, args ...any)
	}
}

const (
	// maxPacketSize is a conservative estimate of MySQL's max_allowed_packet.
	// The MySQL default is 64MB (67108864). We use 16MB to leave room for
	// overhead (column names, query framing, charset conversion, etc.).
	maxPacketSize = 16 * 1024 * 1024

	// maxRowsPerInsert caps the number of rows in a single INSERT to avoid
	// excessive memory allocation for the args slice.
	maxRowsPerInsert = 10000
)

func newMySQLWriter(db *sql.DB, opts provider.WriteOptions) *mysqlWriter {
	return &mysqlWriter{
		db:   db,
		opts: opts,
		log:  logger.L().With("component", "mysql-writer"),
	}
}

// Write persists a batch of MigrationUnits to the destination MySQL.
// It groups rows by table and uses batch operations for performance.
func (w *mysqlWriter) Write(ctx context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	start := time.Now()
	prevWritten, prevFailed, prevSkipped, prevBytes := w.written, w.failed, w.skipped, w.bytes

	if len(units) == 0 {
		return &provider.BatchResult{
			TotalUnits: 0,
			Duration:   time.Since(start),
		}, nil
	}

	// Group rows by table
	tableRows := make(map[string][]mysqlRow)
	for i := range units {
		unit := &units[i]

		// Decode the row
		row, err := decodeMySQLRow(unit.Data)
		if err != nil {
			w.failed++
			w.log.Debug("failed to decode row", "key", unit.Key, "error", err)
			continue
		}

		tableRows[row.Table] = append(tableRows[row.Table], *row)
	}

	var failedKeys []string
	var errors []error

	// Write each table's rows
	for table, rows := range tableRows {
		if err := w.writeTable(ctx, table, rows, &failedKeys, &errors); err != nil {
			w.failed += int64(len(rows))
			for _, row := range rows {
				failedKeys = append(failedKeys, buildRowKey(table, row.PrimaryKey))
			}
			errors = append(errors, fmt.Errorf("write table %s: %w", table, err))
		}
	}

	return &provider.BatchResult{
		TotalUnits:   len(units),
		WrittenUnits: int(w.written - prevWritten),
		FailedUnits:  int(w.failed - prevFailed),
		SkippedUnits: int(w.skipped - prevSkipped),
		FailedKeys:   failedKeys,
		BytesWritten: w.bytes - prevBytes,
		Duration:     time.Since(start),
		Errors:       errors,
	}, nil
}

// Flush is a no-op for MySQL since writes are immediate.
func (w *mysqlWriter) Flush(_ context.Context) error {
	return nil
}

// writeTable writes rows to a specific table.
func (w *mysqlWriter) writeTable(ctx context.Context, table string, rows []mysqlRow, failedKeys *[]string, errors *[]error) error {
	if len(rows) == 0 {
		return nil
	}

	// Compute the union of all columns across all rows in the batch.
	// NoSQL sources (e.g. Redis hashes) can have heterogeneous fields,
	// so different rows may carry different columns.
	colSet := make(map[string]string) // col → column type
	for _, row := range rows {
		for col, typ := range row.ColumnTypes {
			if _, exists := colSet[col]; !exists {
				colSet[col] = typ
			}
		}
		for col := range row.Data {
			if _, exists := colSet[col]; !exists {
				colSet[col] = "TEXT"
			}
		}
	}
	columns := make([]string, 0, len(colSet))
	for col := range colSet {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	// Build a merged column-types map from all rows for table creation.
	mergedTypes := make(map[string]string, len(colSet))
	for col, typ := range colSet {
		mergedTypes[col] = typ
	}
	mergedRow := mysqlRow{
		Table:       table,
		PrimaryKey:  rows[0].PrimaryKey,
		Data:        rows[0].Data,
		ColumnTypes: mergedTypes,
	}

	if err := w.ensureTableExists(ctx, table, mergedRow, columns); err != nil {
		return fmt.Errorf("ensure table exists: %w", err)
	}

	// Check conflict strategy
	if w.opts.OnConflict == provider.ConflictSkip {
		return w.writeWithSkip(ctx, table, columns, rows, failedKeys, errors)
	}

	// Default: overwrite (upsert)
	return w.writeWithUpsert(ctx, table, columns, rows, failedKeys, errors)
}

func (w *mysqlWriter) ensureTableExists(ctx context.Context, table string, row mysqlRow, columns []string) error {
	// Build a set of PK columns for fast lookup.
	isPK := make(map[string]bool, len(row.PrimaryKey))
	for col := range row.PrimaryKey {
		isPK[col] = true
	}

	defs := make([]string, 0, len(columns)+1)
	for _, col := range columns {
		colType := "LONGTEXT"
		if mapped, ok := row.ColumnTypes[col]; ok && mapped != "" {
			colType = normalizeMySQLColumnType(mapped)
		}
		// MySQL does not allow TEXT types in PRIMARY KEY without a key
		// length prefix. Use VARCHAR(768) instead so that auto-created
		// tables from NoSQL sources (which default to TEXT) succeed.
		if isPK[col] && isMySQLTextType(colType) {
			colType = "VARCHAR(768)"
		}
		defs = append(defs, fmt.Sprintf("%s %s", quoteIdentifier(col), colType))
	}

	if len(row.PrimaryKey) > 0 {
		pkCols := make([]string, 0, len(row.PrimaryKey))
		for col := range row.PrimaryKey {
			pkCols = append(pkCols, quoteIdentifier(col))
		}
		sort.Strings(pkCols)
		defs = append(defs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
	}

	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		quoteIdentifier(table), strings.Join(defs, ",\n  "))

	_, err := w.db.ExecContext(ctx, stmt)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// For NoSQL sources with heterogeneous fields, the first CREATE TABLE
	// may not cover all columns. Add any missing columns via ALTER TABLE.
	return w.addMissingColumns(ctx, table, columns, row, isPK)
}

// addMissingColumns checks which columns already exist in the table and
// adds any that are missing. This handles heterogeneous data from NoSQL
// sources where each batch may introduce new fields.
func (w *mysqlWriter) addMissingColumns(ctx context.Context, table string, columns []string, row mysqlRow, isPK map[string]bool) error {
	rows, err := w.db.QueryContext(ctx,
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", table)
	if err != nil {
		return nil // best-effort
	}
	existing := make(map[string]bool)
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err == nil {
			existing[strings.ToLower(colName)] = true
		}
	}
	_ = rows.Close()

	for _, col := range columns {
		if existing[strings.ToLower(col)] {
			continue
		}
		colType := "LONGTEXT"
		if mapped, ok := row.ColumnTypes[col]; ok && mapped != "" {
			colType = normalizeMySQLColumnType(mapped)
		}
		if isPK[col] && isMySQLTextType(colType) {
			colType = "VARCHAR(768)"
		}
		alterStmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
			quoteIdentifier(table), quoteIdentifier(col), colType)
		if _, err := w.db.ExecContext(ctx, alterStmt); err != nil {
			if !isDuplicateColumnError(err) {
				return fmt.Errorf("add column %s: %w", col, err)
			}
		}
	}
	return nil
}

// isDuplicateColumnError checks if a MySQL error is a "duplicate column" error.
func isDuplicateColumnError(err error) bool {
	s := err.Error()
	return strings.Contains(s, "Duplicate column") ||
		strings.Contains(s, "1060")
}

// isMySQLTextType returns true for MySQL text-like types that cannot be used
// directly as PRIMARY KEY columns.
func isMySQLTextType(colType string) bool {
	switch strings.ToUpper(colType) {
	case "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return true
	}
	return false
}

func normalizeMySQLColumnType(colType string) string {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	switch {
	case upper == "":
		return "LONGTEXT"
	case strings.HasPrefix(upper, "INT"), strings.HasPrefix(upper, "TINYINT"),
		strings.HasPrefix(upper, "SMALLINT"), strings.HasPrefix(upper, "MEDIUMINT"),
		strings.HasPrefix(upper, "BIGINT"):
		return upper
	case strings.HasPrefix(upper, "FLOAT"), strings.HasPrefix(upper, "DOUBLE"),
		strings.HasPrefix(upper, "DECIMAL"), strings.HasPrefix(upper, "NUMERIC"):
		return upper
	case strings.HasPrefix(upper, "VARCHAR"), strings.HasPrefix(upper, "CHAR"):
		return upper
	case strings.HasPrefix(upper, "TEXT"), strings.HasPrefix(upper, "TINYTEXT"),
		strings.HasPrefix(upper, "MEDIUMTEXT"), strings.HasPrefix(upper, "LONGTEXT"):
		return upper
	case strings.HasPrefix(upper, "BLOB"), strings.HasPrefix(upper, "TINYBLOB"),
		strings.HasPrefix(upper, "MEDIUMBLOB"), strings.HasPrefix(upper, "LONGBLOB"):
		return upper
	case strings.HasPrefix(upper, "DATETIME"), strings.HasPrefix(upper, "TIMESTAMP"),
		upper == "DATE", upper == "TIME":
		return upper
	case strings.HasPrefix(upper, "BOOL"):
		return "TINYINT(1)"
	case strings.HasPrefix(upper, "JSON"):
		return "JSON"
	default:
		return "LONGTEXT"
	}
}

// writeWithUpsert uses chunked INSERT ON DUPLICATE KEY UPDATE for upsert operations.
// Rows are split into chunks that fit within max_allowed_packet.
func (w *mysqlWriter) writeWithUpsert(ctx context.Context, table string, columns []string, rows []mysqlRow, failedKeys *[]string, errors *[]error) error {
	// Build the INSERT query prefix (everything before VALUES).
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}

	updateClauses := make([]string, len(columns))
	for i, col := range columns {
		updateClauses[i] = fmt.Sprintf("%s = VALUES(%s)", quoteIdentifier(col), quoteIdentifier(col))
	}

	queryPrefix := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES ",
		quoteIdentifier(table),
		strings.Join(quotedColumns, ", "),
	)
	querySuffix := fmt.Sprintf(" ON DUPLICATE KEY UPDATE %s", strings.Join(updateClauses, ", "))

	// Estimate per-row size for chunking.
	rowPlaceholder := "(" + buildPlaceholders(len(columns)) + ")"

	// Split rows into chunks that fit within maxPacketSize.
	for i := 0; i < len(rows); {
		chunkSize := w.estimateChunkSize(rows[i:], columns, rowPlaceholder, queryPrefix, querySuffix)
		if chunkSize == 0 {
			chunkSize = 1
		}
		end := min(i+chunkSize, len(rows))

		chunk := rows[i:end]
		if err := w.execChunkedUpsert(ctx, table, chunk, columns, queryPrefix, rowPlaceholder, querySuffix, failedKeys, errors); err != nil {
			return err
		}

		i = end
	}

	return nil
}

// execChunkedUpsert executes a single chunked INSERT ON DUPLICATE KEY UPDATE.
func (w *mysqlWriter) execChunkedUpsert(ctx context.Context, table string, rows []mysqlRow, columns []string, queryPrefix, rowPlaceholder, querySuffix string, failedKeys *[]string, errors *[]error) error {
	placeholders := make([]string, len(rows))
	for i := range rows {
		placeholders[i] = rowPlaceholder
	}

	query := queryPrefix + strings.Join(placeholders, ", ") + querySuffix

	args := make([]any, 0, len(rows)*len(columns))
	for _, row := range rows {
		for _, col := range columns {
			args = append(args, w.prepareValue(col, row))
		}
	}

	_, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		w.failed += int64(len(rows))
		for _, row := range rows {
			key := buildRowKey(table, row.PrimaryKey)
			*failedKeys = append(*failedKeys, key)
		}
		*errors = append(*errors, fmt.Errorf("insert batch: %w", err))
		return err
	}

	w.written += int64(len(rows))
	for _, row := range rows {
		w.bytes += int64(len(row.Data))
	}

	return nil
}

// estimateChunkSize estimates how many rows can fit in a single INSERT
// without exceeding maxPacketSize.
func (w *mysqlWriter) estimateChunkSize(rows []mysqlRow, columns []string, rowPlaceholder, queryPrefix, querySuffix string) int {
	if len(rows) == 0 {
		return 0
	}

	// Base overhead: query prefix + suffix + separators.
	overhead := len(queryPrefix) + len(querySuffix)

	// Estimate max row data size from first few rows. Using max instead
	// of average avoids underestimating when small rows appear before
	// large BLOBs, which could cause chunks to exceed max_allowed_packet.
	sampleSize := min(10, len(rows))
	maxRowDataSize := 64 // minimum baseline for non-string types
	for i := range sampleSize {
		var rowSize int
		for _, col := range columns {
			if v, ok := rows[i].Data[col]; ok {
				switch val := v.(type) {
				case string:
					rowSize += len(val)
				case []byte:
					rowSize += len(val)
				default:
					rowSize += 64
				}
			}
		}
		if rowSize > maxRowDataSize {
			maxRowDataSize = rowSize
		}
	}

	// Per-row overhead: placeholder string + comma separator + escaped data overhead.
	perRow := len(rowPlaceholder) + 1 + int(float64(maxRowDataSize)*1.2) // 20% escape overhead

	available := maxPacketSize - overhead
	if available <= 0 {
		return 1
	}

	chunkSize := available / perRow
	chunkSize = max(min(chunkSize, maxRowsPerInsert), 1)

	return min(chunkSize, len(rows))
}

// writeWithSkip uses INSERT IGNORE for efficient skip operations.
// This is much faster than per-row SELECT + INSERT.
func (w *mysqlWriter) writeWithSkip(ctx context.Context, table string, columns []string, rows []mysqlRow, failedKeys *[]string, errors *[]error) error {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}

	queryPrefix := fmt.Sprintf(
		"INSERT IGNORE INTO %s (%s) VALUES ",
		quoteIdentifier(table),
		strings.Join(quotedColumns, ", "),
	)

	rowPlaceholder := "(" + buildPlaceholders(len(columns)) + ")"

	// Split rows into chunks that fit within maxPacketSize.
	for i := 0; i < len(rows); {
		chunkSize := w.estimateChunkSize(rows[i:], columns, rowPlaceholder, queryPrefix, "")
		if chunkSize == 0 {
			chunkSize = 1
		}
		end := min(i+chunkSize, len(rows))

		chunk := rows[i:end]
		if err := w.execChunkedSkip(ctx, table, chunk, columns, queryPrefix, rowPlaceholder, failedKeys, errors); err != nil {
			return err
		}

		i = end
	}

	return nil
}

// execChunkedSkip executes a single chunked INSERT IGNORE.
func (w *mysqlWriter) execChunkedSkip(ctx context.Context, table string, rows []mysqlRow, columns []string, queryPrefix, rowPlaceholder string, failedKeys *[]string, errors *[]error) error {
	placeholders := make([]string, len(rows))
	for i := range rows {
		placeholders[i] = rowPlaceholder
	}

	query := queryPrefix + strings.Join(placeholders, ", ")

	args := make([]any, 0, len(rows)*len(columns))
	for _, row := range rows {
		for _, col := range columns {
			args = append(args, w.prepareValue(col, row))
		}
	}

	result, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		w.failed += int64(len(rows))
		for _, row := range rows {
			key := buildRowKey(table, row.PrimaryKey)
			*failedKeys = append(*failedKeys, key)
		}
		*errors = append(*errors, fmt.Errorf("insert ignore batch: %w", err))
		return err
	}

	affected, _ := result.RowsAffected()
	w.written += affected
	w.skipped += int64(len(rows)) - affected
	for _, row := range rows {
		w.bytes += int64(len(row.Data))
	}

	return nil
}

// buildPlaceholders builds a placeholder string like "?, ?, ?".
func buildPlaceholders(count int) string {
	placeholders := make([]string, count)
	for i := range count {
		placeholders[i] = "?"
	}
	return strings.Join(placeholders, ", ")
}

// buildPKWhere builds a WHERE clause for primary key matching.
func buildPKWhere(pk map[string]any, _ int) (string, []any) {
	var clauses []string
	var args []any

	for col, val := range pk {
		clauses = append(clauses, fmt.Sprintf("%s = ?", quoteIdentifier(col)))
		args = append(args, val)
	}

	return strings.Join(clauses, " AND "), args
}

var dateTimeTypes = map[string]bool{
	"datetime":  true,
	"timestamp": true,
	"date":      true,
	"time":      true,
}

func (w *mysqlWriter) prepareValue(col string, row mysqlRow) any {
	val := row.Data[col]
	if val == nil {
		return nil
	}

	colType := strings.ToLower(row.ColumnTypes[col])
	if !dateTimeTypes[colType] {
		return val
	}

	s, ok := val.(string)
	if !ok {
		return val
	}

	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return val
		}
	}
	return t
}
