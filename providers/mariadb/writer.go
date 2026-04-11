package mariadb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

type mariaDBWriter struct {
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
	maxPacketSize    = 16 * 1024 * 1024
	maxRowsPerInsert = 10000
)

func newMariaDBWriter(db *sql.DB, opts provider.WriteOptions) *mariaDBWriter {
	return &mariaDBWriter{
		db:   db,
		opts: opts,
		log:  logger.L().With("component", "mariadb-writer"),
	}
}

func (w *mariaDBWriter) Write(ctx context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	start := time.Now()
	prevWritten, prevFailed, prevSkipped, prevBytes := w.written, w.failed, w.skipped, w.bytes

	if len(units) == 0 {
		return &provider.BatchResult{TotalUnits: 0, Duration: time.Since(start)}, nil
	}

	tableRows := make(map[string][]mariaDBRow)
	for i := range units {
		unit := &units[i]
		row, err := decodeMariaDBRow(unit.Data)
		if err != nil {
			w.failed++
			w.log.Debug("failed to decode row", "key", unit.Key, "error", err)
			continue
		}
		tableRows[row.Table] = append(tableRows[row.Table], *row)
	}

	var failedKeys []string
	var errors []error
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

func (w *mariaDBWriter) Flush(_ context.Context) error { return nil }

func (w *mariaDBWriter) writeTable(ctx context.Context, table string, rows []mariaDBRow, failedKeys *[]string, errors *[]error) error {
	if len(rows) == 0 {
		return nil
	}
	firstRow := rows[0]
	columns := make([]string, 0, len(firstRow.Data))
	for col := range firstRow.Data {
		columns = append(columns, col)
	}
	if w.opts.OnConflict == provider.ConflictSkip {
		return w.writeWithSkip(ctx, table, columns, rows, failedKeys, errors)
	}
	return w.writeWithUpsert(ctx, table, columns, rows, failedKeys, errors)
}

func (w *mariaDBWriter) writeWithUpsert(ctx context.Context, table string, columns []string, rows []mariaDBRow, failedKeys *[]string, errors *[]error) error {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}
	updateClauses := make([]string, len(columns))
	for i, col := range columns {
		updateClauses[i] = fmt.Sprintf("%s = VALUES(%s)", quoteIdentifier(col), quoteIdentifier(col))
	}
	queryPrefix := fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		quoteIdentifier(table), strings.Join(quotedColumns, ", "))
	querySuffix := fmt.Sprintf(" ON DUPLICATE KEY UPDATE %s", strings.Join(updateClauses, ", "))
	rowPlaceholder := "(" + buildPlaceholders(len(columns)) + ")"

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

func (w *mariaDBWriter) execChunkedUpsert(ctx context.Context, table string, rows []mariaDBRow, columns []string, queryPrefix, rowPlaceholder, querySuffix string, failedKeys *[]string, errors *[]error) error {
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
			*failedKeys = append(*failedKeys, buildRowKey(table, row.PrimaryKey))
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

func (w *mariaDBWriter) estimateChunkSize(rows []mariaDBRow, columns []string, rowPlaceholder, queryPrefix, querySuffix string) int {
	if len(rows) == 0 {
		return 0
	}
	overhead := len(queryPrefix) + len(querySuffix)
	sampleSize := min(10, len(rows))
	maxRowDataSize := 64
	for i := 0; i < sampleSize; i++ {
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
	perRow := len(rowPlaceholder) + 1 + int(float64(maxRowDataSize)*1.2)
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

func (w *mariaDBWriter) writeWithSkip(ctx context.Context, table string, columns []string, rows []mariaDBRow, failedKeys *[]string, errors *[]error) error {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}
	queryPrefix := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES ",
		quoteIdentifier(table), strings.Join(quotedColumns, ", "))
	rowPlaceholder := "(" + buildPlaceholders(len(columns)) + ")"

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

func (w *mariaDBWriter) execChunkedSkip(ctx context.Context, table string, rows []mariaDBRow, columns []string, queryPrefix, rowPlaceholder string, failedKeys *[]string, errors *[]error) error {
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
			*failedKeys = append(*failedKeys, buildRowKey(table, row.PrimaryKey))
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

func buildPlaceholders(count int) string {
	p := make([]string, count)
	for i := range p {
		p[i] = "?"
	}
	return strings.Join(p, ", ")
}

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
	"datetime": true, "timestamp": true, "date": true, "time": true,
}

func (w *mariaDBWriter) prepareValue(col string, row mariaDBRow) any {
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
