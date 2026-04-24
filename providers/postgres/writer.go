package postgres

import (
	"context"
	"encoding/json"
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
	log     interface {
		Info(msg string, args ...any)
		Warn(msg string, args ...any)
		Debug(msg string, args ...any)
	}
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
			w.log.Debug("failed to decode row", "key", unit.Key, "error", err)
			continue
		}
		row.Schema = normalizePostgresSchema(row.Schema)

		tableKey := row.Schema + "." + row.Table
		tableRows[tableKey] = append(tableRows[tableKey], *row)
	}

	var failedKeys []string
	var errors []error

	// Write each table's rows
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
	mergedRow := postgresRow{
		Table:       rows[0].Table,
		Schema:      rows[0].Schema,
		PrimaryKey:  rows[0].PrimaryKey,
		Data:        rows[0].Data,
		ColumnTypes: colSet,
	}

	if err := w.ensureTableExists(ctx, schema, table, mergedRow, columns); err != nil {
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
	// Add any missing columns for heterogeneous NoSQL sources.
	return w.addMissingColumns(ctx, schema, table, columns, row)
}

func (w *postgresWriter) addMissingColumns(ctx context.Context, schema, table string, columns []string, row postgresRow) error {
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
			colType = normalizePostgresColumnType(mapped)
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

func buildCreateTableStatements(schema, table string, row postgresRow, columns []string) []string {
	schema = normalizePostgresSchema(schema)
	stmts := []string{
		fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", quoteIdentifier(schema)),
	}

	defs := make([]string, 0, len(columns)+1)
	for _, col := range columns {
		colType := "TEXT"
		if mapped, ok := row.ColumnTypes[col]; ok && mapped != "" {
			colType = normalizePostgresColumnType(mapped)
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
		w.log.Debug("COPY failed, falling back to batch INSERT", "error", err)
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
		return normalizePostgresSchema(parts[0]), parts[1]
	}
	return "public", key
}

func normalizePostgresSchema(schema string) string {
	if schema == "" {
		return "public"
	}
	return schema
}

func normalizePostgresColumnType(colType string) string {
	upper := strings.ToUpper(strings.TrimSpace(colType))
	switch {
	case upper == "":
		return "TEXT"
	case upper == "INT2":
		return "SMALLINT"
	case upper == "INT8":
		return "BIGINT"
	case strings.HasPrefix(upper, "BIGINT"):
		return "BIGINT"
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT"
	case strings.HasPrefix(upper, "INTEGER") || strings.HasPrefix(upper, "INT"):
		return "INTEGER"
	case strings.HasPrefix(upper, "SMALLINT"):
		return "SMALLINT"
	case strings.HasPrefix(upper, "TINYINT"):
		return "SMALLINT"
	case strings.HasPrefix(upper, "MEDIUMINT"):
		return "INTEGER"
	case strings.HasPrefix(upper, "FLOAT"):
		return "REAL"
	case strings.HasPrefix(upper, "DOUBLE"):
		return "DOUBLE PRECISION"
	case strings.HasPrefix(upper, "REAL"):
		return "REAL"
	case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
		return "NUMERIC"
	case strings.HasPrefix(upper, "VARCHAR"):
		return "VARCHAR"
	case strings.HasPrefix(upper, "CHAR"):
		return "CHAR"
	case strings.HasPrefix(upper, "LONGTEXT") || strings.HasPrefix(upper, "MEDIUMTEXT") || strings.HasPrefix(upper, "TINYTEXT") || strings.HasPrefix(upper, "TEXT"):
		return "TEXT"
	case strings.HasPrefix(upper, "JSON"):
		return "JSONB"
	case strings.HasPrefix(upper, "BLOB") || strings.HasPrefix(upper, "LONGBLOB") || strings.HasPrefix(upper, "MEDIUMBLOB") || strings.HasPrefix(upper, "TINYBLOB") || strings.HasPrefix(upper, "BINARY") || strings.HasPrefix(upper, "VARBINARY"):
		return "BYTEA"
	case strings.HasPrefix(upper, "DATETIME") || strings.HasPrefix(upper, "TIMESTAMP"):
		return "TIMESTAMP"
	case upper == "DATE":
		return "DATE"
	case upper == "TIME":
		return "TIME"
	case strings.HasPrefix(upper, "BOOL") || strings.HasPrefix(upper, "BOOLEAN"):
		return "BOOLEAN"
	default:
		return colType
	}
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
	if n, ok := v.(json.Number); ok {
		switch {
		case strings.Contains(ct, "INT") || strings.Contains(ct, "SERIAL"):
			if i, err := n.Int64(); err == nil {
				return i
			}
		case strings.HasPrefix(ct, "NUMERIC") || strings.HasPrefix(ct, "DECIMAL"):
			return n.String()
		case strings.HasPrefix(ct, "REAL") || strings.HasPrefix(ct, "DOUBLE"):
			if f, err := n.Float64(); err == nil {
				return f
			}
		}
		return n.String()
	}
	if s, ok := v.(string); ok {
		switch {
		case strings.Contains(ct, "INT") || strings.Contains(ct, "SERIAL"):
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return i
			}
		case strings.HasPrefix(ct, "NUMERIC") || strings.HasPrefix(ct, "DECIMAL"):
			return s
		case strings.HasPrefix(ct, "REAL") || strings.HasPrefix(ct, "DOUBLE") || strings.HasPrefix(ct, "FLOAT"):
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f
			}
		case ct == "BOOLEAN" || ct == "BOOL":
			if b, err := strconv.ParseBool(s); err == nil {
				return b
			}
			if s == "0" {
				return false
			}
			if s == "1" {
				return true
			}
		}
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
