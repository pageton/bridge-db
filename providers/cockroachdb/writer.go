package cockroachdb

import (
	"context"
	"fmt"
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
	firstRow := rows[0]
	columns := make([]string, 0, len(firstRow.Data))
	for col := range firstRow.Data {
		columns = append(columns, col)
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

func parseTableKey(key string) (schema, table string) {
	parts := strings.SplitN(key, ".", 2)
	if len(parts) == 2 {
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
