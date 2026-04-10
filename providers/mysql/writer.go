package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
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
			w.log.Debug("failed to write table", "table", table, "error", err)
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

	// Get column names from first row
	firstRow := rows[0]
	columns := make([]string, 0, len(firstRow.Data))
	for col := range firstRow.Data {
		columns = append(columns, col)
	}

	// Check conflict strategy
	if w.opts.OnConflict == provider.ConflictSkip {
		return w.writeWithSkip(ctx, table, columns, rows, failedKeys, errors)
	}

	// Default: overwrite (upsert)
	return w.writeWithUpsert(ctx, table, columns, rows, failedKeys, errors)
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
		end := i + chunkSize
		if end > len(rows) {
			end = len(rows)
		}

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

	// Estimate average row data size from first few rows.
	sampleSize := min(10, len(rows))
	var avgRowDataSize int
	for i := 0; i < sampleSize; i++ {
		for _, col := range columns {
			if v, ok := rows[i].Data[col]; ok {
				switch val := v.(type) {
				case string:
					avgRowDataSize += len(val)
				case []byte:
					avgRowDataSize += len(val)
				default:
					avgRowDataSize += 64 // estimate for non-string types
				}
			}
		}
	}
	avgRowDataSize /= sampleSize

	// Per-row overhead: placeholder string + comma separator + escaped data overhead.
	perRow := len(rowPlaceholder) + 1 + int(float64(avgRowDataSize)*1.2) // 20% escape overhead

	available := maxPacketSize - overhead
	if available <= 0 {
		return 1
	}

	chunkSize := available / perRow
	if chunkSize > maxRowsPerInsert {
		chunkSize = maxRowsPerInsert
	}
	if chunkSize < 1 {
		chunkSize = 1
	}

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
		end := i + chunkSize
		if end > len(rows) {
			end = len(rows)
		}

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
	for i := 0; i < count; i++ {
		placeholders[i] = "?"
	}
	return strings.Join(placeholders, ", ")
}

// buildPKWhere builds a WHERE clause for primary key matching.
func buildPKWhere(pk map[string]any, startIdx int) (string, []any) {
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

	switch x := val.(type) {
	case pgtype.Numeric:
		if x.Valid {
			b, err := x.MarshalJSON()
			if err == nil {
				return string(b)
			}
		}
		return nil
	case json.Number:
		return x.String()
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
