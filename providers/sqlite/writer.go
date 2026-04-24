//go:build sqlite

package sqlite

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

type sqliteWriter struct {
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
			w.log.Debug("failed to decode row", "key", unit.Key, "error", err)
			continue
		}
		tableRows[row.Table] = append(tableRows[row.Table], *row)
	}

	var failedKeys []string
	var errors []error

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

func (w *sqliteWriter) Flush(_ context.Context) error {
	return nil
}

func (w *sqliteWriter) writeTable(ctx context.Context, table string, rows []sqliteRow, failedKeys *[]string, errors *[]error) error {
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
	sort.Strings(columns)

	if err := w.ensureTable(ctx, table, colSet); err != nil {
		return fmt.Errorf("ensure table %s: %w", table, err)
	}

	if w.opts.OnConflict == provider.ConflictSkip {
		return w.writeWithSkip(ctx, table, columns, rows, failedKeys, errors)
	}

	return w.writeWithUpsert(ctx, table, columns, rows, failedKeys, errors)
}

func (w *sqliteWriter) ensureTable(ctx context.Context, table string, colTypes map[string]string) error {
	var count int
	err := w.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count)
	if err != nil {
		return err
	}

	if count == 0 {
		colDefs := make([]string, 0, len(colTypes)+1)
		colDefs = append(colDefs, "\"_rowid\" INTEGER PRIMARY KEY AUTOINCREMENT")
		for col, colType := range colTypes {
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
		w.log.Debug("auto-created table", "table", table)
		return nil
	}

	// Table already exists — add any missing columns for heterogeneous
	// NoSQL sources where each batch may introduce new fields.
	existing, err := w.existingColumns(ctx, table)
	if err != nil {
		return nil // best-effort
	}
	for col, colType := range colTypes {
		if existing[col] {
			continue
		}
		if colType == "" {
			colType = "TEXT"
		}
		alterStmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
			quoteIdentifier(table), quoteIdentifier(col), colType)
		if _, err := w.db.ExecContext(ctx, alterStmt); err != nil {
			// SQLite doesn't have IF NOT EXISTS for ADD COLUMN; ignore
			// duplicate column errors from concurrent writers.
			if !strings.Contains(err.Error(), "duplicate column") {
				return fmt.Errorf("add column %s to %s: %w", col, table, err)
			}
		}
	}
	return nil
}

func (w *sqliteWriter) existingColumns(ctx context.Context, table string) (map[string]bool, error) {
	rows, err := w.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", quoteIdentifier(table)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	existing := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultVal sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultVal, &pk); err == nil {
			existing[name] = true
		}
	}
	return existing, nil
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

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, row := range rows {
		args := make([]any, len(columns))
		for i, col := range columns {
			args[i] = row.Data[col]
		}

		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			w.failed++
			key := buildRowKey(table, row.PrimaryKey)
			*failedKeys = append(*failedKeys, key)
			*errors = append(*errors, fmt.Errorf("insert row %s: %w", key, err))
			continue
		}

		w.written++
		w.bytes += int64(len(row.Data))
	}

	return tx.Commit()
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

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, row := range rows {
		args := make([]any, len(columns))
		for i, col := range columns {
			args[i] = row.Data[col]
		}

		result, err := tx.ExecContext(ctx, query, args...)
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

	return tx.Commit()
}

func buildPlaceholders(count int) string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "?"
	}
	return strings.Join(placeholders, ", ")
}
