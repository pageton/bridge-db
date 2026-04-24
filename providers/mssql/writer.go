//go:build mssql

package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
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
	pkColumnsCache map[string][]string
}

const (
	maxPacketSize    = 16 * 1024 * 1024
	maxRowsPerInsert = 10000
	maxSQLParams     = 2100
)

func newMSSQLWriter(db *sql.DB, opts provider.WriteOptions) *mssqlWriter {
	return &mssqlWriter{
		db:             db,
		opts:           opts,
		log:            logger.L().With("component", "mssql-writer"),
		pkColumnsCache: make(map[string][]string),
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
			// Retryable errors (deadlocks) must propagate so the batch-level
			// retry in the pipeline can re-drive the entire Write() call.
			// Non-retryable errors are recorded as partial failures.
			if isRetryableMSSQLError(strings.ToLower(err.Error())) {
				return nil, fmt.Errorf("write table %s: %w", table, err)
			}
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

func (w *mssqlWriter) Flush(_ context.Context) error {
	return nil
}

func (w *mssqlWriter) writeTable(ctx context.Context, table string, rows []mssqlRow, failedKeys *[]string, errors *[]error) error {
	if len(rows) == 0 {
		return nil
	}

	// Compute the union of all columns across all rows in the batch.
	colSet := make(map[string]string)
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
	slices.Sort(columns)

	if err := w.ensureTableExists(ctx, table, colSet); err != nil {
		return fmt.Errorf("ensure table exists: %w", err)
	}

	pkCols := choosePKColumns(w.getPrimaryKeyColumns(ctx, table), rows[0], columns)
	rows = normalizeMSSQLRows(rows, pkCols, columns)
	rows = dedupMSSQLRows(rows)

	if w.opts.OnConflict == provider.ConflictSkip {
		return w.writeWithSkip(ctx, table, columns, rows, failedKeys, errors)
	}

	return w.writeWithUpsert(ctx, table, columns, rows, failedKeys, errors)
}

func (w *mssqlWriter) ensureTableExists(ctx context.Context, table string, colTypes map[string]string) error {
	var count int
	err := w.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @p1", table).Scan(&count)
	if err != nil {
		return err
	}

	if count == 0 {
		colDefs := make([]string, 0, len(colTypes))
		for _, col := range sortedKeys(colTypes) {
			colType := normalizeMSSQLAutoCreateType(colTypes[col])
			colDefs = append(colDefs, fmt.Sprintf("%s %s", quoteIdentifier(col), colType))
		}

		query := fmt.Sprintf("CREATE TABLE %s (\n  %s\n)",
			quoteIdentifier(table), strings.Join(colDefs, ",\n  "))

		if _, err := w.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("create table %s: %w", table, err)
		}
		w.log.Debug("auto-created table", "table", table)
		return nil
	}

	// Table exists — add any missing columns for heterogeneous NoSQL sources.
	return w.addMissingColumns(ctx, table, colTypes)
}

func (w *mssqlWriter) addMissingColumns(ctx context.Context, table string, colTypes map[string]string) error {
	rows, err := w.db.QueryContext(ctx,
		"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @p1", table)
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
	rows.Close()

	for _, col := range sortedKeys(colTypes) {
		if existing[strings.ToLower(col)] {
			continue
		}
		colType := normalizeMSSQLAutoCreateType(colTypes[col])
		alterStmt := fmt.Sprintf("ALTER TABLE %s ADD %s %s",
			quoteIdentifier(table), quoteIdentifier(col), colType)
		if _, err := w.db.ExecContext(ctx, alterStmt); err != nil {
			msg := strings.ToLower(err.Error())
			if !strings.Contains(msg, "column names in each table must be unique") {
				return fmt.Errorf("add column %s to %s: %w", col, table, err)
			}
		}
	}
	return nil
}

// sortedKeys returns the map keys in sorted order.
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
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

func (w *mssqlWriter) getPrimaryKeyColumns(ctx context.Context, table string) []string {
	if cols, ok := w.pkColumnsCache[table]; ok {
		return cols
	}

	rows, err := w.db.QueryContext(ctx, "SELECT c.[name] FROM sys.indexes i "+
		"JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "+
		"JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "+
		"JOIN sys.tables t ON i.object_id = t.object_id "+
		"WHERE t.[name] = @p1 AND i.is_primary_key = 1 ORDER BY ic.key_ordinal", table)
	if err != nil {
		w.log.Debug("failed to load primary key columns", "table", table, "error", err)
		return nil
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			w.log.Debug("failed to scan primary key column", "table", table, "error", err)
			return nil
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		w.log.Debug("primary key column rows error", "table", table, "error", err)
		return nil
	}

	w.pkColumnsCache[table] = cols
	return cols
}

func choosePKColumns(destPK []string, row mssqlRow, columns []string) []string {
	if len(destPK) > 0 {
		return destPK
	}

	pkCols := extractPKColumns(row)
	if len(pkCols) > 0 {
		return pkCols
	}

	if len(columns) > 0 {
		return columns[:1]
	}

	return nil
}

func normalizeMSSQLRows(rows []mssqlRow, pkCols []string, columns []string) []mssqlRow {
	if len(rows) == 0 {
		return rows
	}
	normalized := make([]mssqlRow, len(rows))
	for i, row := range rows {
		row.PrimaryKey = effectivePrimaryKey(pkCols, row, columns)
		normalized[i] = row
	}
	return normalized
}

func effectivePrimaryKey(pkCols []string, row mssqlRow, columns []string) map[string]any {
	if len(pkCols) > 0 {
		pk := make(map[string]any, len(pkCols))
		for _, col := range pkCols {
			if val, ok := row.Data[col]; ok {
				pk[col] = val
				continue
			}
			if val, ok := row.PrimaryKey[col]; ok {
				pk[col] = val
			}
		}
		if len(pk) == len(pkCols) {
			return pk
		}
	}

	if len(row.PrimaryKey) > 0 {
		return row.PrimaryKey
	}

	fallbackCols := columns
	if len(fallbackCols) == 0 {
		fallbackCols = extractOrderedColumns(row.Data)
	}
	if len(fallbackCols) == 0 {
		return nil
	}
	return map[string]any{fallbackCols[0]: row.Data[fallbackCols[0]]}
}

func extractOrderedColumns(data map[string]any) []string {
	cols := make([]string, 0, len(data))
	for col := range data {
		cols = append(cols, col)
	}
	slices.Sort(cols)
	return cols
}

func (w *mssqlWriter) execChunkedMerge(ctx context.Context, tx *sql.Tx, table string, rows []mssqlRow, columns []string, failedKeys *[]string, errors *[]error) error {
	if len(rows) == 0 || len(columns) == 0 {
		return nil
	}

	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}

	pkCols := choosePKColumns(w.getPrimaryKeyColumns(ctx, table), rows[0], columns)

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
	var executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}
	if tx != nil {
		executor = tx
	} else {
		executor = w.db
	}
	_, err := executor.ExecContext(ctx, query, args...)
	if err != nil {
		msg := strings.ToLower(err.Error())
		if isDuplicateKeyError(msg) {
			w.log.Debug("merge batch hit duplicate key; falling back to row upsert",
				"table", table,
				"rows", len(rows),
				"operation", "merge->update_then_insert",
			)
			return w.execRowByRowUpsert(ctx, tx, table, rows, columns, failedKeys, errors)
		}
		if isRetryableMSSQLError(msg) {
			return err
		}
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

func (w *mssqlWriter) execRowByRowUpsert(ctx context.Context, tx *sql.Tx, table string, rows []mssqlRow, columns []string, failedKeys *[]string, errors *[]error) error {
	pkCols := choosePKColumns(w.getPrimaryKeyColumns(ctx, table), rows[0], columns)

	var executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}
	if tx != nil {
		executor = tx
	} else {
		executor = w.db
	}

	for _, row := range rows {
		if err := w.execUpdateThenInsertUpsert(ctx, executor, table, row, columns, pkCols); err != nil {
			if isRetryableMSSQLError(strings.ToLower(err.Error())) {
				return err
			}
			w.failed++
			key := buildRowKey(table, row.PrimaryKey)
			*failedKeys = append(*failedKeys, key)
			*errors = append(*errors, fmt.Errorf("row upsert %s: %w", key, err))
			continue
		}
		w.written++
		w.bytes += int64(len(row.Data))
	}

	return nil
}

// execUpdateThenInsertUpsert implements an idempotent single-row upsert for
// MSSQL without relying on MERGE matching semantics. It tries UPDATE first,
// then INSERT if the row does not exist, and finally retries UPDATE if the
// INSERT races or hits a duplicate key against an existing row.
func (w *mssqlWriter) execUpdateThenInsertUpsert(
	ctx context.Context,
	executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	},
	table string,
	row mssqlRow,
	columns []string,
	pkCols []string,
) error {
	if err := w.execUpdateFallback(ctx, executor, table, row, columns, pkCols); err == nil {
		return nil
	} else if err != sql.ErrNoRows {
		return err
	}

	if err := w.execInsertRow(ctx, executor, table, row, columns); err != nil {
		msg := strings.ToLower(err.Error())
		if !isDuplicateKeyError(msg) {
			return err
		}

		w.log.Debug("insert hit duplicate key after update miss; retrying direct update",
			"table", table,
			"key", buildRowKey(table, row.PrimaryKey),
			"operation", "update_then_insert->update",
		)

		return w.execUpdateFallback(ctx, executor, table, row, columns, pkCols)
	}

	return nil
}

func (w *mssqlWriter) execInsertRow(
	ctx context.Context,
	executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	},
	table string,
	row mssqlRow,
	columns []string,
) error {
	quotedColumns := make([]string, len(columns))
	placeholders := make([]string, len(columns))
	args := make([]any, 0, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
		placeholders[i] = fmt.Sprintf("@p%d", i+1)
		args = append(args, w.prepareValue(col, row))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(table),
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err := executor.ExecContext(ctx, query, args...)
	return err
}

// execUpdateFallback runs a plain UPDATE for a single row, used as a fallback
// when MERGE fails with a duplicate-key error (the ON clause didn't match an
// existing row due to a type coercion edge case).
func (w *mssqlWriter) execUpdateFallback(
	ctx context.Context,
	executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	},
	table string,
	row mssqlRow,
	columns []string,
	pkCols []string,
) error {
	pkSet := make(map[string]struct{}, len(pkCols))
	for _, pkCol := range pkCols {
		pkSet[pkCol] = struct{}{}
	}

	setParts := make([]string, 0, len(columns))
	args := make([]any, 0, len(columns))
	paramIdx := 1
	for _, col := range columns {
		if _, isPK := pkSet[col]; isPK {
			continue
		}
		setParts = append(setParts, fmt.Sprintf("%s = @p%d", quoteIdentifier(col), paramIdx))
		args = append(args, w.prepareValue(col, row))
		paramIdx++
	}
	if len(setParts) == 0 {
		return nil
	}

	whereParts := make([]string, 0, len(pkCols))
	for _, pkCol := range pkCols {
		whereParts = append(whereParts, fmt.Sprintf("%s = @p%d", quoteIdentifier(pkCol), paramIdx))
		args = append(args, w.prepareValue(pkCol, row))
		paramIdx++
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		quoteIdentifier(table),
		strings.Join(setParts, ", "),
		strings.Join(whereParts, " AND "),
	)
	result, err := executor.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// isDuplicateKeyError checks whether a (lowercased) MSSQL error message
// indicates a primary key or unique constraint violation.
func isDuplicateKeyError(msg string) bool {
	return strings.Contains(msg, "primary key constraint") ||
		strings.Contains(msg, "cannot insert duplicate key") ||
		strings.Contains(msg, "unique constraint") ||
		strings.Contains(msg, "the merge statement attempted to update or delete the same row more than once")
}

func isRetryableMSSQLError(msg string) bool {
	return strings.Contains(msg, "deadlocked on lock resources") ||
		strings.Contains(msg, "deadlock victim") ||
		strings.Contains(msg, "error 1205")
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
	pkCols := choosePKColumns(w.getPrimaryKeyColumns(ctx, table), rows[0], columns)

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

	var executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}
	if tx != nil {
		executor = tx
	} else {
		executor = w.db
	}
	result, err := executor.ExecContext(ctx, query, args...)
	if err != nil {
		if isRetryableMSSQLError(strings.ToLower(err.Error())) {
			return err
		}
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
	if len(columns) == 0 {
		return 1
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

	paramLimitedChunkSize := (maxSQLParams - 1) / len(columns)
	if paramLimitedChunkSize < 1 {
		paramLimitedChunkSize = 1
	}
	if chunkSize > paramLimitedChunkSize {
		chunkSize = paramLimitedChunkSize
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
	slices.Sort(cols)
	return cols
}

func dedupMSSQLRows(rows []mssqlRow) []mssqlRow {
	if len(rows) <= 1 {
		return rows
	}

	seen := make(map[string]int, len(rows))
	out := make([]mssqlRow, 0, len(rows))

	for _, row := range rows {
		key := buildRowKey(row.Table, row.PrimaryKey)
		if idx, ok := seen[key]; ok {
			out[idx] = row
			continue
		}
		seen[key] = len(out)
		out = append(out, row)
	}

	return out
}

var mssqlDateTimeTypes = map[string]bool{
	"datetime":       true,
	"datetime2":      true,
	"smalldatetime":  true,
	"datetimeoffset": true,
	"date":           true,
	"time":           true,
}

// mssqlIntegerTypes maps source column type names (lowercase) that should be
// treated as integer columns. Covers postgres, mysql, mariadb, sqlite, and
// mssql native type names.
var mssqlIntegerTypes = map[string]bool{
	// PostgreSQL
	"integer": true, "int": true, "bigint": true, "smallint": true,
	"serial": true, "bigserial": true, "smallserial": true,
	"int2": true, "int4": true, "int8": true,
	// MySQL / MariaDB
	"tinyint": true, "mediumint": true,
	// SQLite
	// (sqlite types are dynamic, handled by the generic float64→int64 coercion)
	// MSSQL native
	// (redundant with postgres entries but keeps the mapping complete)
}

func normalizeColumnType(colType string) string {
	colType = strings.ToLower(strings.TrimSpace(colType))
	if idx := strings.Index(colType, "("); idx >= 0 {
		colType = colType[:idx]
	}
	return strings.TrimSpace(colType)
}

// normalizeMSSQLAutoCreateType maps generic/foreign column types to
// MSSQL-compatible types for auto-created tables. This avoids issues like
// "text and nvarchar are incompatible in the equal to operator" when MERGE
// compares a TEXT column against NVARCHAR parameters.
func normalizeMSSQLAutoCreateType(colType string) string {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	switch upper {
	case "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "CLOB":
		return "NVARCHAR(MAX)"
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BYTEA", "BINARY",
		"VARBINARY", "IMAGE":
		return "VARBINARY(MAX)"
	case "":
		return "NVARCHAR(MAX)"
	}
	// Already a valid MSSQL type or something specific — pass through.
	return upper
}

func (w *mssqlWriter) prepareValue(col string, row mssqlRow) any {
	val := row.Data[col]
	if val == nil {
		return nil
	}

	colType := normalizeColumnType(row.ColumnTypes[col])

	// Coerce float64 to int64 for integer columns. After JSON round-trip,
	// all numbers become float64. The go-mssqldb driver infers SQL types
	// from Go types: float64 → float(53), which can cause MERGE ON clause
	// mismatches when the target column is INT/BIGINT. Converting to int64
	// ensures the driver sends the correct integer SQL type.
	if mssqlIntegerTypes[colType] {
		if f, ok := val.(float64); ok {
			return int64(f)
		}
		return val
	}

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
