package cockroachdb

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// cockroachDBScanner enumerates rows from a CockroachDB database using
// LIMIT/OFFSET pagination. It lists all tables, then scans each table
// in batches, yielding batches of MigrationUnits.
type cockroachDBScanner struct {
	pool            *pgxpool.Pool
	opts            provider.ScanOptions
	stats           provider.ScanStats
	tables          []tableInfo
	currentTable    int
	rows            pgx.Rows
	columns         []columnInfo
	pkColumns       []string
	done            bool
	offset          int
	tablesCompleted map[string]bool
	log             interface {
		Debug(msg string, args ...any)
	}
}

type tableInfo struct {
	Schema string
	Name   string
}

type columnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

func newCockroachDBScanner(pool *pgxpool.Pool, opts provider.ScanOptions) *cockroachDBScanner {
	s := &cockroachDBScanner{
		pool: pool,
		opts: opts,
		log:  logger.L().With("component", "cockroachdb-scanner"),
	}
	// Parse resume token to restore stats for logging. Table skipping is
	// handled by TablesCompleted filtering below; we do NOT use TablesDone
	// for index-based skipping (conflicts with name-based filtering).
	if len(opts.ResumeToken) > 0 {
		if stats, err := provider.UnmarshalScanToken(opts.ResumeToken); err == nil {
			s.stats = stats
			s.log.Debug("resuming from checkpoint",
				"tables_done", stats.TablesDone,
				"tables_total", stats.TablesTotal,
				"rows_scanned", stats.TotalScanned,
			)
		}
	}
	if len(opts.TablesCompleted) > 0 {
		s.tablesCompleted = make(map[string]bool, len(opts.TablesCompleted))
		for _, t := range opts.TablesCompleted {
			s.tablesCompleted[t] = true
		}
	}
	return s
}

func (s *cockroachDBScanner) Next(ctx context.Context) ([]provider.MigrationUnit, error) {
	if s.done {
		return nil, io.EOF
	}
	batchSize := s.opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	if s.tables == nil {
		if err := s.listTables(ctx); err != nil {
			return nil, err
		}
		if len(s.tables) == 0 {
			s.done = true
			return nil, io.EOF
		}
	}

	// Drain leftover rows from a previous fetch.
	if s.rows != nil {
		units := make([]provider.MigrationUnit, 0, batchSize)
		for s.rows.Next() && len(units) < batchSize {
			unit, err := s.readRow(ctx)
			if err != nil {
				s.log.Debug("failed to read row, skipping",
					"table", s.tables[s.currentTable].Name, "error", err)
				continue
			}
			units = append(units, *unit)
			s.stats.TotalScanned++
			s.stats.TotalBytes += unit.Size
		}
		if err := s.rows.Err(); err != nil {
			s.log.Debug("row error", "table", s.tables[s.currentTable].Name, "error", err)
		}
		s.rows.Close()
		s.rows = nil
		if len(units) > 0 {
			s.offset += len(units)
			return units, nil
		}
		s.offset = 0
		s.currentTable++
		s.stats.TablesDone++
	}

	// Advance to the next table that has data.
	for s.currentTable < len(s.tables) {
		table := s.tables[s.currentTable]
		s.log.Debug("scanning table", "schema", table.Schema, "table", table.Name, "offset", s.offset)

		if err := s.getTableInfo(ctx, table); err != nil {
			s.log.Debug("failed to get table info", "table", table.Name, "error", err)
			s.currentTable++
			s.stats.TablesDone++
			s.offset = 0
			continue
		}

		query := s.buildPaginatedQuery(table, batchSize, s.offset)
		rows, err := s.pool.Query(ctx, query)
		if err != nil {
			s.log.Debug("failed to query table", "table", table.Name, "error", err)
			s.currentTable++
			s.stats.TablesDone++
			s.offset = 0
			continue
		}

		units := make([]provider.MigrationUnit, 0, batchSize)
		for rows.Next() {
			unit, err := s.readRowDirect(ctx, table, rows)
			if err != nil {
				s.log.Debug("failed to read row, skipping",
					"table", table.Name, "error", err)
				continue
			}
			units = append(units, *unit)
			s.stats.TotalScanned++
			s.stats.TotalBytes += unit.Size
		}
		if err := rows.Err(); err != nil {
			s.log.Debug("row error", "table", table.Name, "error", err)
		}
		rows.Close()

		if len(units) == 0 {
			s.offset = 0
			s.currentTable++
			s.stats.TablesDone++
			continue
		}

		s.offset += len(units)

		if len(units) < batchSize {
			s.offset = 0
			s.currentTable++
			s.stats.TablesDone++
		}

		return units, nil
	}

	s.done = true
	return nil, io.EOF
}

func (s *cockroachDBScanner) Stats() provider.ScanStats { return s.stats }

func (s *cockroachDBScanner) Close() error {
	if s.rows != nil {
		s.rows.Close()
		s.rows = nil
	}
	s.done = true
	return nil
}

func (s *cockroachDBScanner) listTables(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'crdb_internal')
		AND table_type = 'BASE TABLE'
		ORDER BY table_schema, table_name`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var tables []tableInfo
	for rows.Next() {
		var t tableInfo
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			continue
		}
		tables = append(tables, t)
	}
	s.tables = tables
	s.stats.TablesTotal = len(tables)

	if len(s.tablesCompleted) > 0 {
		filtered := tables[:0]
		for _, t := range tables {
			if !s.tablesCompleted[t.Schema+"."+t.Name] {
				filtered = append(filtered, t)
			}
		}
		s.tables = filtered
		s.stats.TablesTotal = len(filtered)
		if s.currentTable > len(s.tables) {
			s.currentTable = len(s.tables)
		}
	}
	s.log.Debug("found tables", "count", len(s.tables))
	return nil
}

func (s *cockroachDBScanner) getTableInfo(ctx context.Context, table tableInfo) error {
	rows, err := s.pool.Query(ctx, `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	var columns []columnInfo
	for rows.Next() {
		var col columnInfo
		var nullable string
		if err := rows.Scan(&col.Name, &col.Type, &nullable); err != nil {
			continue
		}
		// CockroachDB adds a hidden "rowid" column to tables. It is not a
		// real user column and should not be migrated.
		if col.Name == "rowid" {
			continue
		}
		col.Nullable = nullable == "YES"
		columns = append(columns, col)
	}
	s.columns = columns

	pkRows, err := s.pool.Query(ctx, `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = $1
			AND tc.table_name = $2
		ORDER BY kcu.ordinal_position`, table.Schema, table.Name)
	if err != nil {
		return err
	}
	defer pkRows.Close()

	var pkColumns []string
	for pkRows.Next() {
		var col string
		if err := pkRows.Scan(&col); err != nil {
			continue
		}
		if col == "rowid" {
			continue
		}
		pkColumns = append(pkColumns, col)
	}
	if len(pkColumns) == 0 {
		for _, col := range columns {
			pkColumns = append(pkColumns, col.Name)
		}
	}
	s.pkColumns = pkColumns
	return nil
}

func (s *cockroachDBScanner) buildPaginatedQuery(table tableInfo, limit, offset int) string {
	colNames := make([]string, len(s.columns))
	for i, col := range s.columns {
		colNames[i] = quoteIdentifier(col.Name)
	}
	orderBy := make([]string, len(s.pkColumns))
	for i, col := range s.pkColumns {
		orderBy[i] = quoteIdentifier(col)
	}
	return fmt.Sprintf(
		"SELECT %s FROM %s.%s ORDER BY %s LIMIT %d OFFSET %d",
		strings.Join(colNames, ", "),
		quoteIdentifier(table.Schema), quoteIdentifier(table.Name),
		strings.Join(orderBy, ", "),
		limit, offset,
	)
}

func (s *cockroachDBScanner) readRowDirect(_ context.Context, table tableInfo, rows pgx.Rows) (*provider.MigrationUnit, error) {
	values, err := rows.Values()
	if err != nil {
		return nil, err
	}
	data := make(map[string]any)
	pk := make(map[string]any)
	columnTypes := make(map[string]string)
	for i, col := range s.columns {
		val := convertValue(values[i], col.Type)
		data[col.Name] = val
		columnTypes[col.Name] = col.Type
		for _, pkCol := range s.pkColumns {
			if col.Name == pkCol {
				pk[col.Name] = val
				break
			}
		}
	}
	row := &cockroachDBRow{
		Table: table.Name, Schema: table.Schema,
		PrimaryKey: pk, Data: data, ColumnTypes: columnTypes,
	}
	rowData, err := encodeCockroachDBRow(row)
	if err != nil {
		return nil, err
	}
	key := buildRowKey(table.Schema, table.Name, pk)
	return &provider.MigrationUnit{
		Key: key, Table: table.Name, DataType: provider.DataTypeRow,
		Data: rowData, Meta: provider.UnitMeta{Schema: table.Schema, PrimaryKey: pk, ColumnTypes: columnTypes},
		Size: int64(len(rowData)),
	}, nil
}

func (s *cockroachDBScanner) readRow(ctx context.Context) (*provider.MigrationUnit, error) {
	table := s.tables[s.currentTable]
	return s.readRowDirect(ctx, table, s.rows)
}

func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
