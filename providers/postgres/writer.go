package postgres

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// postgresWriter persists MigrationUnits to a PostgreSQL destination using batch
// operations. It supports INSERT ON CONFLICT for upsert and batch inserts.
type postgresWriter struct {
	pool    *pgxpool.Pool
	opts    provider.WriteOptions
	written int64
	failed  int64
	skipped int64
	bytes   int64
	log     interface{ Info(msg string, args ...any) }
}

func newPostgresWriter(pool *pgxpool.Pool, opts provider.WriteOptions) *postgresWriter {
	return &postgresWriter{
		pool: pool,
		opts: opts,
		log:  logger.L().With("component", "postgres-writer"),
	}
}

// Write persists a batch of MigrationUnits to the destination PostgreSQL.
// It groups rows by table and uses batch operations for performance.
func (w *postgresWriter) Write(ctx context.Context, units []provider.MigrationUnit) (*provider.BatchResult, error) {
	start := time.Now()
	prevWritten, prevFailed, prevSkipped, prevBytes := w.written, w.failed, w.skipped, w.bytes

	if len(units) == 0 {
		return &provider.BatchResult{
			TotalUnits: 0,
			Duration:   time.Since(start),
		}, nil
	}

	// Group rows by table
	tableRows := make(map[string][]postgresRow)
	for i := range units {
		unit := &units[i]

		// Decode the row
		row, err := decodePostgresRow(unit.Data)
		if err != nil {
			w.failed++
			w.log.Info("failed to decode row", "key", unit.Key, "error", err)
			continue
		}

		tableKey := row.Schema + "." + row.Table
		tableRows[tableKey] = append(tableRows[tableKey], *row)
	}

	var failedKeys []string
	var errors []error

	// Write each table's rows
	for tableKey, rows := range tableRows {
		if err := w.writeTable(ctx, tableKey, rows, &failedKeys, &errors); err != nil {
			w.log.Info("failed to write table", "table", tableKey, "error", err)
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

// Flush is a no-op for PostgreSQL since writes are immediate.
func (w *postgresWriter) Flush(_ context.Context) error {
	return nil
}

// writeTable writes rows to a specific table.
func (w *postgresWriter) writeTable(ctx context.Context, tableKey string, rows []postgresRow, failedKeys *[]string, errors *[]error) error {
	if len(rows) == 0 {
		return nil
	}

	// Parse schema and table name
	schema, table := parseTableKey(tableKey)

	// Get column names from first row
	firstRow := rows[0]
	columns := make([]string, 0, len(firstRow.Data))
	for col := range firstRow.Data {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	if err := w.ensureTableExists(ctx, schema, table, firstRow, columns); err != nil {
		return fmt.Errorf("ensure table exists: %w", err)
	}

	// Check conflict strategy
	if w.opts.OnConflict == provider.ConflictSkip {
		return w.writeWithSkip(ctx, schema, table, columns, rows, failedKeys, errors)
	}

	// Default: overwrite (upsert)
	return w.writeWithUpsert(ctx, schema, table, columns, rows, failedKeys, errors)
}

func (w *postgresWriter) ensureTableExists(ctx context.Context, schema, table string, row postgresRow, columns []string) error {
	stmts := buildCreateTableStatements(schema, table, row, columns)
	for _, stmt := range stmts {
		if _, err := w.pool.Exec(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func buildCreateTableStatements(schema, table string, row postgresRow, columns []string) []string {
	stmts := []string{
		fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quoteIdentifier(schema)),
	}

	defs := make([]string, 0, len(columns)+1)
	for _, col := range columns {
		colType := "TEXT"
		if mapped, ok := row.ColumnTypes[col]; ok && mapped != "" {
			colType = mapped
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

// writeWithUpsert uses COPY for bulk insert. Falls back to batch INSERT ON CONFLICT
// if COPY fails (e.g., due to constraints). This is significantly faster than
// individual INSERTs.
func (w *postgresWriter) writeWithUpsert(ctx context.Context, schema, table string, columns []string, rows []postgresRow, failedKeys *[]string, errors *[]error) error {
	// Build a pgx.CopyFromSource from the rows.
	src := &postgresRowSource{
		rows:    rows,
		columns: columns,
		idx:     0,
	}

	tableName := []string{schema, table}
	columnNames := make([]string, len(columns))
	copy(columnNames, columns)

	// Try COPY first for maximum throughput. If the table has constraints
	// that prevent COPY (e.g., NOT NULL without defaults, unique constraints),
	// fall back to batch INSERT with ON CONFLICT.
	copied, err := w.pool.CopyFrom(ctx, tableName, columnNames, src)
	if err != nil {
		// COPY failed — fall back to batch INSERT with ON CONFLICT.
		w.log.Info("COPY failed, falling back to batch INSERT", "error", err)
		return w.writeWithBatchUpsert(ctx, schema, table, columns, rows, failedKeys, errors)
	}

	w.written += copied
	for i := range rows {
		w.bytes += int64(len(rows[i].Data))
	}

	return nil
}

// writeWithBatchUpsert uses batch INSERT ON CONFLICT as a fallback when COPY fails.
func (w *postgresWriter) writeWithBatchUpsert(ctx context.Context, schema, table string, columns []string, rows []postgresRow, failedKeys *[]string, errors *[]error) error {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}

	// Build conflict update clause
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
			values[i] = coercePostgresValue(row.Data[col], row.ColumnTypes[col])
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

// writeWithSkip uses INSERT ON CONFLICT DO NOTHING for efficient skip operations.
func (w *postgresWriter) writeWithSkip(ctx context.Context, schema, table string, columns []string, rows []postgresRow, failedKeys *[]string, errors *[]error) error {
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
			values[i] = coercePostgresValue(row.Data[col], row.ColumnTypes[col])
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

// extractPKColumns gets primary key column names from the first row.
func (w *postgresWriter) extractPKColumns(rows []postgresRow) []string {
	if len(rows) == 0 || len(rows[0].PrimaryKey) == 0 {
		return nil
	}
	pkCols := make([]string, 0, len(rows[0].PrimaryKey))
	for col := range rows[0].PrimaryKey {
		pkCols = append(pkCols, col)
	}
	return pkCols
}

// postgresRowSource implements pgx.CopyFromSource for batch COPY operations.
type postgresRowSource struct {
	rows    []postgresRow
	columns []string
	idx     int
}

func (s *postgresRowSource) Next() bool {
	s.idx++
	return s.idx <= len(s.rows)
}

func (s *postgresRowSource) Values() ([]any, error) {
	row := s.rows[s.idx-1]
	values := make([]any, len(s.columns))
	for i, col := range s.columns {
		values[i] = coercePostgresValue(row.Data[col], row.ColumnTypes[col])
	}
	return values, nil
}

func (s *postgresRowSource) Err() error {
	return nil
}

// parseTableKey parses a "schema.table" key.
func parseTableKey(key string) (schema, table string) {
	parts := strings.SplitN(key, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "public", key
}

// buildPlaceholders builds a placeholder string like "$1, $2, $3".
func buildPlaceholders(count int) string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return strings.Join(placeholders, ", ")
}

// buildPKWhere builds a WHERE clause for primary key matching.
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

func coercePostgresValue(v any, columnType string) any {
	ct := strings.ToUpper(columnType)
	if s, ok := v.(string); ok {
		if strings.HasPrefix(ct, "TIMESTAMP") || strings.HasPrefix(ct, "TIMESTAMPTZ") {
			for _, layout := range []string{
				time.RFC3339Nano,
				time.RFC3339,
				"2006-01-02 15:04:05.999999",
				"2006-01-02 15:04:05",
			} {
				if t, err := time.Parse(layout, s); err == nil {
					return t
				}
			}
		}
		return s
	}

	if ct == "TEXT" || ct == "VARCHAR" || strings.HasPrefix(ct, "VARCHAR(") {
		switch x := v.(type) {
		case nil:
			return nil
		case fmt.Stringer:
			return x.String()
		case float64:
			return strconv.FormatFloat(x, 'f', -1, 64)
		case float32:
			return strconv.FormatFloat(float64(x), 'f', -1, 32)
		case int:
			return strconv.Itoa(x)
		case int8, int16, int32, int64:
			return fmt.Sprintf("%d", x)
		case uint, uint8, uint16, uint32, uint64:
			return fmt.Sprintf("%d", x)
		case bool:
			return strconv.FormatBool(x)
		default:
			return fmt.Sprintf("%v", x)
		}
	}

	return v
}
