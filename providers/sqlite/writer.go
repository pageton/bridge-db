//go:build sqlite

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

type sqliteWriter struct {
	db      *sql.DB
	opts    provider.WriteOptions
	written int64
	failed  int64
	skipped int64
	bytes   int64
	log     interface{ Info(msg string, args ...any) }
}

func newSQLiteWriter(db *sql.DB, opts provider.WriteOptions) *sqliteWriter {
	return &sqliteWriter{
		db:   db,
		opts: opts,
		log:  logger.L().With("component", "sqlite-writer"),
	}
}

func (w *sqliteWriter) Write(ctx context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	start := time.Now()
	prevWritten, prevFailed, prevSkipped, prevBytes := w.written, w.failed, w.skipped, w.bytes

	if len(units) == 0 {
		return &provider.BatchResult{
			TotalUnits: 0,
			Duration:   time.Since(start),
		}, nil
	}

	tableRows := make(map[string][]sqliteRow)
	for i := range units {
		unit := &units[i]
		row, err := decodeSQLiteRow(unit.Data)
		if err != nil {
			w.failed++
			w.log.Info("failed to decode row", "key", unit.Key, "error", err)
			continue
		}
		tableRows[row.Table] = append(tableRows[row.Table], *row)
	}

	var failedKeys []string
	var errors []error

	for table, rows := range tableRows {
		if err := w.writeTable(ctx, table, rows, &failedKeys, &errors); err != nil {
			w.log.Info("failed to write table", "table", table, "error", err)
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

func (w *sqliteWriter) Flush(_ context.Context) error {
	return nil
}

func (w *sqliteWriter) writeTable(ctx context.Context, table string, rows []sqliteRow, failedKeys *[]string, errors *[]error) error {
	if len(rows) == 0 {
		return nil
	}

	firstRow := rows[0]
	columns := make([]string, 0, len(firstRow.Data))
	for col := range firstRow.Data {
		columns = append(columns, col)
	}

	if err := w.ensureTable(ctx, table, firstRow); err != nil {
		return fmt.Errorf("ensure table %s: %w", table, err)
	}

	if w.opts.OnConflict == provider.ConflictSkip {
		return w.writeWithSkip(ctx, table, columns, rows, failedKeys, errors)
	}

	return w.writeWithUpsert(ctx, table, columns, rows, failedKeys, errors)
}

func (w *sqliteWriter) ensureTable(ctx context.Context, table string, sampleRow sqliteRow) error {
	var count int
	err := w.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	colDefs := make([]string, 0, len(sampleRow.Data)+1)
	colDefs = append(colDefs, "\"_rowid\" INTEGER PRIMARY KEY AUTOINCREMENT")
	for col, colType := range sampleRow.ColumnTypes {
		if colType == "" {
			colType = "TEXT"
		}
		colDefs = append(colDefs, fmt.Sprintf("%s %s", quoteIdentifier(col), colType))
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		quoteIdentifier(table), strings.Join(colDefs, ",\n  "))

	if _, err := w.db.ExecContext(ctx, query); err != nil {
		return err
	}

	w.log.Info("auto-created table", "table", table)
	return nil
}

func (w *sqliteWriter) writeWithUpsert(ctx context.Context, table string, columns []string, rows []sqliteRow, failedKeys *[]string, errors *[]error) error {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}

	updateClauses := make([]string, len(columns))
	for i, col := range columns {
		updateClauses[i] = fmt.Sprintf("%s = excluded.%s", quoteIdentifier(col), quoteIdentifier(col))
	}

	query := fmt.Sprintf(
		"INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		quoteIdentifier(table),
		strings.Join(quotedColumns, ", "),
		buildPlaceholders(len(columns)),
	)

	for _, row := range rows {
		args := make([]any, len(columns))
		for i, col := range columns {
			args[i] = row.Data[col]
		}

		if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
			w.failed++
			key := buildRowKey(table, row.PrimaryKey)
			*failedKeys = append(*failedKeys, key)
			*errors = append(*errors, fmt.Errorf("insert row %s: %w", key, err))
			continue
		}

		w.written++
		w.bytes += int64(len(row.Data))
	}

	return nil
}

func (w *sqliteWriter) writeWithSkip(ctx context.Context, table string, columns []string, rows []sqliteRow, failedKeys *[]string, errors *[]error) error {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}

	query := fmt.Sprintf(
		"INSERT OR IGNORE INTO %s (%s) VALUES (%s)",
		quoteIdentifier(table),
		strings.Join(quotedColumns, ", "),
		buildPlaceholders(len(columns)),
	)

	for _, row := range rows {
		args := make([]any, len(columns))
		for i, col := range columns {
			args[i] = row.Data[col]
		}

		result, err := w.db.ExecContext(ctx, query, args...)
		if err != nil {
			w.failed++
			key := buildRowKey(table, row.PrimaryKey)
			*failedKeys = append(*failedKeys, key)
			*errors = append(*errors, fmt.Errorf("insert ignore row %s: %w", key, err))
			continue
		}

		affected, _ := result.RowsAffected()
		if affected > 0 {
			w.written++
		} else {
			w.skipped++
		}
		w.bytes += int64(len(row.Data))
	}

	return nil
}

func buildPlaceholders(count int) string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "?"
	}
	return strings.Join(placeholders, ", ")
}
