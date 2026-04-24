package cockroachdb

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

type cockroachDBWriter struct {
	pool    *pgxpool.Pool
	opts    provider.WriteOptions
	written int64
	failed  int64
	skipped int64
	bytes   int64
	log     interface {
		Info(msg string, args ...any)
		Warn(msg string, args ...any)
		Debug(msg string, args ...any)
	}
}

func newCockroachDBWriter(pool *pgxpool.Pool, opts provider.WriteOptions) *cockroachDBWriter {
	return &cockroachDBWriter{
		pool: pool,
		opts: opts,
		log:  logger.L().With("component", "cockroachdb-writer"),
	}
}

func (w *cockroachDBWriter) Write(ctx context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	start := time.Now()
	prevWritten, prevFailed, prevSkipped, prevBytes := w.written, w.failed, w.skipped, w.bytes

	if len(units) == 0 {
		return &provider.BatchResult{
			TotalUnits: 0,
			Duration:   time.Since(start),
		}, nil
	}

	tableRows := make(map[string][]cockroachDBRow)
	for i := range units {
		unit := &units[i]
		row, err := decodeCockroachDBRow(unit.Data)
		if err != nil {
			w.failed++
			w.log.Debug("failed to decode row", "key", unit.Key, "error", err)
			continue
		}
		tableKey := row.Schema + "." + row.Table
		tableRows[tableKey] = append(tableRows[tableKey], *row)
	}

	var failedKeys []string
	var errors []error

	for tableKey, rows := range tableRows {
		if err := w.writeTable(ctx, tableKey, rows, &failedKeys, &errors); err != nil {
			w.log.Debug("failed to write table", "table", tableKey, "error", err)
			w.failed += int64(len(rows))
			for _, row := range rows {
				failedKeys = append(failedKeys, buildRowKey(row.Schema, row.Table, row.PrimaryKey))
			}
			errors = append(errors, fmt.Errorf("write table %s: %w", tableKey, err))
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

func (w *cockroachDBWriter) Flush(_ context.Context) error {
	return nil
}

func (w *cockroachDBWriter) writeTable(ctx context.Context, tableKey string, rows []cockroachDBRow, failedKeys *[]string, errors *[]error) error {
	if len(rows) == 0 {
		return nil
	}
	schema, table := parseTableKey(tableKey)

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

	// Build a merged row for table creation using the union column types.
	mergedRow := cockroachDBRow{
		Table:       rows[0].Table,
		Schema:      rows[0].Schema,
		PrimaryKey:  rows[0].PrimaryKey,
		Data:        rows[0].Data,
		ColumnTypes: colSet,
	}

	if err := w.ensureTableExists(ctx, schema, table, mergedRow, columns); err != nil {
		return fmt.Errorf("ensure table exists: %w", err)
	}

	if w.opts.OnConflict == provider.ConflictSkip {
		return w.writeWithSkip(ctx, schema, table, columns, rows, failedKeys, errors)
	}
	return w.writeWithUpsert(ctx, schema, table, columns, rows, failedKeys, errors)
}

func (w *cockroachDBWriter) writeWithUpsert(ctx context.Context, schema, table string, columns []string, rows []cockroachDBRow, failedKeys *[]string, errors *[]error) error {
	src := &cockroachDBRowSource{
		rows:    rows,
		columns: columns,
		idx:     0,
	}
	tableName := []string{schema, table}
	columnNames := make([]string, len(columns))
	copy(columnNames, columns)

	copied, err := w.pool.CopyFrom(ctx, tableName, columnNames, src)
	if err != nil {
		w.log.Debug("COPY failed, falling back to batch INSERT", "error", err)
		return w.writeWithBatchUpsert(ctx, schema, table, columns, rows, failedKeys, errors)
	}

	w.written += copied
	for i := range rows {
		w.bytes += int64(len(rows[i].Data))
	}
	return nil
}

func (w *cockroachDBWriter) writeWithBatchUpsert(ctx context.Context, schema, table string, columns []string, rows []cockroachDBRow, failedKeys *[]string, errors *[]error) error {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}
	updateClauses := make([]string, len(columns))
	for i, col := range columns {
		updateClauses[i] = fmt.Sprintf("%s = EXCLUDED.%s", quoteIdentifier(col), quoteIdentifier(col))
	}

	pkColumns := w.extractPKColumns(rows)
	onConflict := ""
	if len(pkColumns) > 0 {
		quotedPK := make([]string, len(pkColumns))
		for i, col := range pkColumns {
			quotedPK[i] = quoteIdentifier(col)
		}
		onConflict = fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s",
			strings.Join(quotedPK, ", "),
			strings.Join(updateClauses, ", "),
		)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s)%s",
		quoteIdentifier(schema),
		quoteIdentifier(table),
		strings.Join(quotedColumns, ", "),
		buildPlaceholders(len(columns)),
		onConflict,
	)

	batch := &pgx.Batch{}
	for _, row := range rows {
		values := make([]any, len(columns))
		for i, col := range columns {
			values[i] = row.Data[col]
		}
		batch.Queue(query, values...)
	}

	results := w.pool.SendBatch(ctx, batch)
	defer func() { _ = results.Close() }()

	for i := 0; i < len(rows); i++ {
		_, err := results.Exec()
		if err != nil {
			w.failed++
			key := buildRowKey(schema, table, rows[i].PrimaryKey)
			*failedKeys = append(*failedKeys, key)
			*errors = append(*errors, fmt.Errorf("insert row: %w", err))
			continue
		}
		w.written++
		w.bytes += int64(len(rows[i].Data))
	}
	return nil
}

func (w *cockroachDBWriter) writeWithSkip(ctx context.Context, schema, table string, columns []string, rows []cockroachDBRow, failedKeys *[]string, errors *[]error) error {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}

	pkColumns := w.extractPKColumns(rows)
	onConflict := ""
	if len(pkColumns) > 0 {
		quotedPK := make([]string, len(pkColumns))
		for i, col := range pkColumns {
			quotedPK[i] = quoteIdentifier(col)
		}
		onConflict = fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", strings.Join(quotedPK, ", "))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s)%s",
		quoteIdentifier(schema),
		quoteIdentifier(table),
		strings.Join(quotedColumns, ", "),
		buildPlaceholders(len(columns)),
		onConflict,
	)

	batch := &pgx.Batch{}
	for _, row := range rows {
		values := make([]any, len(columns))
		for i, col := range columns {
			values[i] = row.Data[col]
		}
		batch.Queue(query, values...)
	}

	results := w.pool.SendBatch(ctx, batch)
	defer func() { _ = results.Close() }()

	for i := 0; i < len(rows); i++ {
		ct, err := results.Exec()
		if err != nil {
			w.failed++
			key := buildRowKey(schema, table, rows[i].PrimaryKey)
			*failedKeys = append(*failedKeys, key)
			*errors = append(*errors, fmt.Errorf("insert: %w", err))
			continue
		}
		if ct.RowsAffected() == 0 {
			w.skipped++
		} else {
			w.written++
			w.bytes += int64(len(rows[i].Data))
		}
	}
	return nil
}

func (w *cockroachDBWriter) extractPKColumns(rows []cockroachDBRow) []string {
	if len(rows) == 0 || len(rows[0].PrimaryKey) == 0 {
		return nil
	}
	pkCols := make([]string, 0, len(rows[0].PrimaryKey))
	for col := range rows[0].PrimaryKey {
		pkCols = append(pkCols, col)
	}
	return pkCols
}

type cockroachDBRowSource struct {
	rows    []cockroachDBRow
	columns []string
	idx     int
}

func (s *cockroachDBRowSource) Next() bool {
	s.idx++
	return s.idx <= len(s.rows)
}

func (s *cockroachDBRowSource) Values() ([]any, error) {
	row := s.rows[s.idx-1]
	values := make([]any, len(s.columns))
	for i, col := range s.columns {
		v := row.Data[col]
		if str, ok := v.(string); ok {
			ct := strings.ToUpper(row.ColumnTypes[col])
			if strings.HasPrefix(ct, "TIMESTAMP") || strings.HasPrefix(ct, "TIMESTAMPTZ") {
				for _, layout := range []string{
					time.RFC3339Nano,
					time.RFC3339,
					"2006-01-02 15:04:05.999999",
					"2006-01-02 15:04:05",
				} {
					if t, err := time.Parse(layout, str); err == nil {
						v = t
						break
					}
				}
			}
		}
		values[i] = v
	}
	return values, nil
}

func (s *cockroachDBRowSource) Err() error {
	return nil
}

func (w *cockroachDBWriter) ensureTableExists(ctx context.Context, schema, table string, row cockroachDBRow, columns []string) error {
	stmts := w.buildCreateTableStatements(schema, table, row, columns)
	for _, stmt := range stmts {
		if _, err := w.pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return w.addMissingColumns(ctx, schema, table, columns, row)
}

func (w *cockroachDBWriter) addMissingColumns(ctx context.Context, schema, table string, columns []string, row cockroachDBRow) error {
	rows, err := w.pool.Query(ctx,
		"SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2",
		schema, table)
	if err != nil {
		return nil // best-effort
	}
	existing := make(map[string]bool)
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err == nil {
			existing[colName] = true
		}
	}
	rows.Close()

	for _, col := range columns {
		if existing[col] {
			continue
		}
		colType := "TEXT"
		if mapped, ok := row.ColumnTypes[col]; ok && mapped != "" {
			colType = normalizeCockroachDBColumnType(mapped)
		}
		alterStmt := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s %s",
			quoteIdentifier(schema), quoteIdentifier(table),
			quoteIdentifier(col), colType)
		if _, err := w.pool.Exec(ctx, alterStmt); err != nil {
			return fmt.Errorf("add column %s: %w", col, err)
		}
	}
	return nil
}

func (w *cockroachDBWriter) buildCreateTableStatements(schema, table string, row cockroachDBRow, columns []string) []string {
	stmts := []string{
		fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quoteIdentifier(schema)),
	}

	defs := make([]string, 0, len(columns)+1)
	for _, col := range columns {
		colType := "TEXT"
		if mapped, ok := row.ColumnTypes[col]; ok && mapped != "" {
			colType = normalizeCockroachDBColumnType(mapped)
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

	stmts = append(stmts, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s (\n  %s\n)",
		quoteIdentifier(schema),
		quoteIdentifier(table),
		strings.Join(defs, ",\n  "),
	))

	return stmts
}

// normalizeCockroachDBColumnType maps generic column types to CockroachDB-compatible types.
func normalizeCockroachDBColumnType(colType string) string {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	switch {
	case upper == "":
		return "TEXT"
	case strings.HasPrefix(upper, "INT"):
		return "INT"
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT"
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT"
	case strings.HasPrefix(upper, "TINYINT"):
		return "SMALLINT"
	case strings.HasPrefix(upper, "FLOAT"):
		return "FLOAT4"
	case strings.HasPrefix(upper, "DOUBLE"):
		return "FLOAT8"
	case strings.HasPrefix(upper, "REAL"):
		return "FLOAT4"
	case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
		return "DECIMAL"
	case strings.HasPrefix(upper, "VARCHAR"):
		return "STRING"
	case strings.HasPrefix(upper, "CHAR"):
		return "STRING"
	case strings.HasPrefix(upper, "TEXT"):
		return "STRING"
	case strings.HasPrefix(upper, "JSON"):
		return "JSONB"
	case strings.HasPrefix(upper, "BLOB") || strings.HasPrefix(upper, "BYTEA") || strings.HasPrefix(upper, "BINARY"):
		return "BYTES"
	case strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMPTZ"):
		return "TIMESTAMPTZ"
	case strings.HasPrefix(upper, "TIMESTAMP"):
		return "TIMESTAMP"
	case upper == "DATE":
		return "DATE"
	case upper == "TIME":
		return "TIME"
	case strings.HasPrefix(upper, "BOOL"):
		return "BOOL"
	default:
		return colType
	}
}

func parseTableKey(key string) (schema, table string) {
	parts := strings.SplitN(key, ".", 2)
	if len(parts) == 2 {
		if parts[0] == "" {
			return "public", parts[1]
		}
		return parts[0], parts[1]
	}
	return "public", key
}

func buildPlaceholders(count int) string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return strings.Join(placeholders, ", ")
}

func buildPKWhere(pk map[string]any, startIdx int) (string, []any) {
	var clauses []string
	var args []any
	idx := startIdx
	for col, val := range pk {
		clauses = append(clauses, fmt.Sprintf("%s = $%d", quoteIdentifier(col), idx))
		args = append(args, val)
		idx++
	}
	return strings.Join(clauses, " AND "), args
}
