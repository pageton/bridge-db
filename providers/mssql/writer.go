//go:build mssql

package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

type mssqlWriter struct {
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

func newMSSQLWriter(db *sql.DB, opts provider.WriteOptions) *mssqlWriter {
	return &mssqlWriter{
		db:   db,
		opts: opts,
		log:  logger.L().With("component", "mssql-writer"),
	}
}

func (w *mssqlWriter) Write(ctx context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	start := time.Now()
	prevWritten, prevFailed, prevSkipped, prevBytes := w.written, w.failed, w.skipped, w.bytes

	if len(units) == 0 {
		return &provider.BatchResult{
			TotalUnits: 0,
			Duration:   time.Since(start),
		}, nil
	}

	tableRows := make(map[string][]mssqlRow)
	for i := range units {
		unit := &units[i]
		row, err := decodeMSSQLRow(unit.Data)
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

func (w *mssqlWriter) Flush(_ context.Context) error {
	return nil
}

func (w *mssqlWriter) writeTable(ctx context.Context, table string, rows []mssqlRow, failedKeys *[]string, errors *[]error) error {
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

// hasIdentityColumn checks if a table has an IDENTITY column.
func (w *mssqlWriter) hasIdentityColumn(ctx context.Context, table string) bool {
	var count int
	err := w.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sys.identity_columns WHERE OBJECT_NAME(object_id) = @p1",
		table,
	).Scan(&count)
	if err != nil {
		w.log.Debug("failed to check identity columns", "table", table, "error", err)
		return false
	}
	return count > 0
}

// setIdentityInsert wraps fn with SET IDENTITY_INSERT ON/OFF for tables
// that have identity columns. If the table has no identity column, fn runs
// unchanged. Uses a transaction to guarantee the SET and fn execute on the
// same connection (IDENTITY_INSERT is per-connection in MSSQL).
func (w *mssqlWriter) setIdentityInsert(ctx context.Context, table string, hasIdentity bool, fn func(tx *sql.Tx) error) error {
	if !hasIdentity {
		return fn(nil)
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin identity insert tx: %w", err)
	}
	defer tx.Rollback()

	ident := fmt.Sprintf("SET IDENTITY_INSERT %s ON", quoteIdentifier(table))
	if _, err := tx.ExecContext(ctx, ident); err != nil {
		return fmt.Errorf("enable identity insert: %w", err)
	}
	if err := fn(tx); err != nil {
		return err
	}
	ident = fmt.Sprintf("SET IDENTITY_INSERT %s OFF", quoteIdentifier(table))
	if _, err := tx.ExecContext(ctx, ident); err != nil {
		w.log.Debug("failed to disable identity insert", "table", table, "error", err)
	}
	return tx.Commit()
}

func (w *mssqlWriter) writeWithUpsert(ctx context.Context, table string, columns []string, rows []mssqlRow, failedKeys *[]string, errors *[]error) error {
	hasIdentity := w.hasIdentityColumn(ctx, table)
	for i := 0; i < len(rows); {
		chunkSize := w.estimateChunkSize(rows[i:], columns)
		if chunkSize == 0 {
			chunkSize = 1
		}
		end := i + chunkSize
		if end > len(rows) {
			end = len(rows)
		}

		chunk := rows[i:end]
		if err := w.setIdentityInsert(ctx, table, hasIdentity, func(tx *sql.Tx) error {
			return w.execChunkedMerge(ctx, tx, table, chunk, columns, failedKeys, errors)
		}); err != nil {
			return err
		}

		i = end
	}

	return nil
}

func (w *mssqlWriter) execChunkedMerge(ctx context.Context, tx *sql.Tx, table string, rows []mssqlRow, columns []string, failedKeys *[]string, errors *[]error) error {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}

	pkCols := extractPKColumns(rows[0])
	if len(pkCols) == 0 {
		pkCols = columns[:1]
	}

	srcCols := make([]string, len(columns))
	for i, col := range columns {
		srcCols[i] = fmt.Sprintf("src.%s", quoteIdentifier(col))
	}

	onClauses := make([]string, len(pkCols))
	for i, col := range pkCols {
		onClauses[i] = fmt.Sprintf("target.%s = src.%s", quoteIdentifier(col), quoteIdentifier(col))
	}

	updateClauses := make([]string, 0, len(columns))
	for _, col := range columns {
		isPK := false
		for _, pkCol := range pkCols {
			if col == pkCol {
				isPK = true
				break
			}
		}
		if !isPK {
			updateClauses = append(updateClauses, fmt.Sprintf("target.%s = src.%s", quoteIdentifier(col), quoteIdentifier(col)))
		}
	}

	rowPlaceholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*len(columns))
	for i, row := range rows {
		valPlaceholders := make([]string, len(columns))
		for j, col := range columns {
			valPlaceholders[j] = fmt.Sprintf("@p%d", i*len(columns)+j+1)
			args = append(args, w.prepareValue(col, row))
		}
		rowPlaceholders[i] = "(" + strings.Join(valPlaceholders, ", ") + ")"
	}

	onStr := strings.Join(onClauses, " AND ")
	query := fmt.Sprintf(
		"MERGE INTO %s AS target USING (VALUES %s) AS src (%s) ON (%s) "+
			"WHEN MATCHED THEN UPDATE SET %s "+
			"WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
		quoteIdentifier(table),
		strings.Join(rowPlaceholders, ", "),
		strings.Join(quotedColumns, ", "),
		onStr,
		strings.Join(updateClauses, ", "),
		strings.Join(quotedColumns, ", "),
		strings.Join(srcCols, ", "),
	)
	if len(updateClauses) == 0 {
		query = fmt.Sprintf(
			"MERGE INTO %s AS target USING (VALUES %s) AS src (%s) ON (%s) "+
				"WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
			quoteIdentifier(table),
			strings.Join(rowPlaceholders, ", "),
			strings.Join(quotedColumns, ", "),
			onStr,
			strings.Join(quotedColumns, ", "),
			strings.Join(srcCols, ", "),
		)
	}
	var executor interface{ ExecContext(context.Context, string, ...any) (sql.Result, error) }
	if tx != nil {
		executor = tx
	} else {
		executor = w.db
	}
	_, err := executor.ExecContext(ctx, query, args...)
	if err != nil {
		w.failed += int64(len(rows))
		for _, row := range rows {
			key := buildRowKey(table, row.PrimaryKey)
			*failedKeys = append(*failedKeys, key)
		}
		*errors = append(*errors, fmt.Errorf("merge batch: %w", err))
		return err
	}

	w.written += int64(len(rows))
	for _, row := range rows {
		w.bytes += int64(len(row.Data))
	}

	return nil
}

func (w *mssqlWriter) writeWithSkip(ctx context.Context, table string, columns []string, rows []mssqlRow, failedKeys *[]string, errors *[]error) error {
	hasIdentity := w.hasIdentityColumn(ctx, table)
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}

	for i := 0; i < len(rows); {
		chunkSize := w.estimateChunkSize(rows[i:], columns)
		if chunkSize == 0 {
			chunkSize = 1
		}
		end := i + chunkSize
		if end > len(rows) {
			end = len(rows)
		}

		chunk := rows[i:end]
		if err := w.setIdentityInsert(ctx, table, hasIdentity, func(tx *sql.Tx) error {
			return w.execChunkedInsertSkip(ctx, tx, table, chunk, columns, quotedColumns, failedKeys, errors)
		}); err != nil {
			return err
		}

		i = end
	}

	return nil
}

func (w *mssqlWriter) execChunkedInsertSkip(ctx context.Context, tx *sql.Tx, table string, rows []mssqlRow, columns []string, quotedColumns []string, failedKeys *[]string, errors *[]error) error {
	pkCols := extractPKColumns(rows[0])
	if len(pkCols) == 0 {
		pkCols = columns[:1]
	}

	onClauses := make([]string, len(pkCols))
	for i, col := range pkCols {
		onClauses[i] = fmt.Sprintf("target.%s = src.%s", quoteIdentifier(col), quoteIdentifier(col))
	}

	srcCols := make([]string, len(columns))
	for i, col := range columns {
		srcCols[i] = fmt.Sprintf("src.%s", quoteIdentifier(col))
	}

	rowPlaceholders := make([]string, len(rows))
	args := make([]any, 0, len(rows)*len(columns))
	for i, row := range rows {
		valPlaceholders := make([]string, len(columns))
		for j, col := range columns {
			valPlaceholders[j] = fmt.Sprintf("@p%d", i*len(columns)+j+1)
			args = append(args, w.prepareValue(col, row))
		}
		rowPlaceholders[i] = "(" + strings.Join(valPlaceholders, ", ") + ")"
	}

	query := fmt.Sprintf(
		"MERGE INTO %s AS target USING (VALUES %s) AS src (%s) ON (%s "+
			"WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
		quoteIdentifier(table),
		strings.Join(rowPlaceholders, ", "),
		strings.Join(quotedColumns, ", "),
		strings.Join(onClauses, " AND "),
		strings.Join(quotedColumns, ", "),
		strings.Join(srcCols, ", "),
	)

	var executor interface{ ExecContext(context.Context, string, ...any) (sql.Result, error) }
	if tx != nil {
		executor = tx
	} else {
		executor = w.db
	}
	result, err := executor.ExecContext(ctx, query, args...)
	if err != nil {
		w.failed += int64(len(rows))
		for _, row := range rows {
			key := buildRowKey(table, row.PrimaryKey)
			*failedKeys = append(*failedKeys, key)
		}
		*errors = append(*errors, fmt.Errorf("insert skip batch: %w", err))
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

func (w *mssqlWriter) estimateChunkSize(rows []mssqlRow, columns []string) int {
	if len(rows) == 0 {
		return 0
	}

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

	perRow := 20 + int(float64(maxRowDataSize)*1.2)
	available := maxPacketSize / 2
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

func extractPKColumns(row mssqlRow) []string {
	if len(row.PrimaryKey) == 0 {
		return nil
	}
	cols := make([]string, 0, len(row.PrimaryKey))
	for col := range row.PrimaryKey {
		cols = append(cols, col)
	}
	return cols
}

var mssqlDateTimeTypes = map[string]bool{
	"datetime":       true,
	"datetime2":      true,
	"smalldatetime":  true,
	"datetimeoffset": true,
	"date":           true,
	"time":           true,
}

func (w *mssqlWriter) prepareValue(col string, row mssqlRow) any {
	val := row.Data[col]
	if val == nil {
		return nil
	}

	colType := strings.ToLower(row.ColumnTypes[col])
	if !mssqlDateTimeTypes[colType] {
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
