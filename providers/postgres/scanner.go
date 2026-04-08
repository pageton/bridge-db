package postgres

import (
	"context"
	"fmt"
	"github.com/bytedance/sonic"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// postgresScanner enumerates rows from a PostgreSQL database using cursor-based
// pagination. It first lists all tables, then scans each table with
// a cursor, yielding batches of MigrationUnits.
type postgresScanner struct {
	pool            *pgxpool.Pool
	opts            provider.ScanOptions
	stats           provider.ScanStats
	tables          []tableInfo
	currentTable    int
	rows            pgx.Rows
	columns         []columnInfo
	pkColumns       []string
	done            bool
	tablesCompleted map[string]bool // tables to skip on resume
	log             interface{ Info(msg string, args ...any) }
}

// tableInfo holds information about a table to scan.
type tableInfo struct {
	Schema string
	Name   string
}

// columnInfo holds information about a column.
type columnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

func newPostgresScanner(pool *pgxpool.Pool, opts provider.ScanOptions) *postgresScanner {
	s := &postgresScanner{
		pool: pool,
		opts: opts,
		log:  logger.L().With("component", "postgres-scanner"),
	}

	// Parse resume token to skip already-scanned tables and rows.
	if len(opts.ResumeToken) > 0 {
		if stats, err := unmarshalScanToken(opts.ResumeToken); err == nil {
			s.stats = stats
			s.currentTable = stats.TablesDone
			s.log.Info("resuming from checkpoint",
				"tables_done", stats.TablesDone,
				"tables_total", stats.TablesTotal,
				"rows_scanned", stats.TotalScanned,
			)
		}
	}

	// Track tables to skip on resume.
	if len(opts.TablesCompleted) > 0 {
		s.tablesCompleted = make(map[string]bool, len(opts.TablesCompleted))
		for _, t := range opts.TablesCompleted {
			s.tablesCompleted[t] = true
		}
	}

	return s
}

// Next returns the next batch of MigrationUnits. It returns io.EOF when
// all tables have been scanned.
func (s *postgresScanner) Next(ctx context.Context) ([]provider.MigrationUnit, error) {
	if s.done {
		return nil, io.EOF
	}

	batchSize := s.opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	// If we don't have tables listed yet, do that first
	if s.tables == nil {
		if err := s.listTables(ctx); err != nil {
			return nil, err
		}
		if len(s.tables) == 0 {
			s.done = true
			return nil, io.EOF
		}
	}

	// Collect units until we have a full batch or run out of data
	units := make([]provider.MigrationUnit, 0, batchSize)

	for len(units) < batchSize && !s.done {
		// If we have rows, get next row
		if s.rows != nil && s.rows.Next() {
			unit, err := s.readRow(ctx)
			if err != nil {
				s.log.Info("failed to read row, skipping",
					"table", s.tables[s.currentTable].Name,
					"error", err)
				continue
			}
			units = append(units, *unit)
			s.stats.TotalScanned++
			s.stats.TotalBytes += unit.Size
			continue
		}

		// Check for row error
		if s.rows != nil {
			if err := s.rows.Err(); err != nil {
				s.log.Info("row error", "table", s.tables[s.currentTable].Name, "error", err)
			}
			s.rows.Close()
			s.rows = nil
			s.currentTable++
			s.stats.TablesDone++
		}

		// Move to next table or finish
		if s.currentTable >= len(s.tables) {
			s.done = true
			break
		}

		// Open cursor for next table
		table := s.tables[s.currentTable]
		s.log.Info("scanning table", "schema", table.Schema, "table", table.Name)

		// Get table columns and primary key
		if err := s.getTableInfo(ctx, table); err != nil {
			s.log.Info("failed to get table info", "table", table.Name, "error", err)
			s.currentTable++
			s.stats.TablesDone++
			continue
		}

		// Build query with cursor-based pagination
		query := s.buildScanQuery(table)
		rows, err := s.pool.Query(ctx, query)
		if err != nil {
			s.log.Info("failed to open cursor for table", "table", table.Name, "error", err)
			s.currentTable++
			s.stats.TablesDone++
			continue
		}
		s.rows = rows
	}

	if len(units) == 0 {
		return nil, io.EOF
	}

	return units, nil
}

// Stats returns current scan statistics.
func (s *postgresScanner) Stats() provider.ScanStats {
	return s.stats
}

// listTables enumerates all tables in the database.
func (s *postgresScanner) listTables(ctx context.Context) error {
	query := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		AND table_type = 'BASE TABLE'
		ORDER BY table_schema, table_name
	`

	rows, err := s.pool.Query(ctx, query)
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

	// Filter out completed tables on resume.
	if len(s.tablesCompleted) > 0 {
		filtered := tables[:0]
		for _, t := range tables {
			if !s.tablesCompleted[t.Schema+"."+t.Name] {
				filtered = append(filtered, t)
			}
		}
		s.tables = filtered
		s.stats.TablesTotal = len(filtered)
		// Adjust currentTable since we removed tables from the front.
		if s.currentTable > len(s.tables) {
			s.currentTable = len(s.tables)
		}
	}

	s.log.Info("found tables", "count", len(s.tables))

	return nil
}

// getTableInfo retrieves column information and primary key for a table.
func (s *postgresScanner) getTableInfo(ctx context.Context, table tableInfo) error {
	// Get columns
	colQuery := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := s.pool.Query(ctx, colQuery, table.Schema, table.Name)
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
		col.Nullable = nullable == "YES"
		columns = append(columns, col)
	}
	s.columns = columns

	// Get primary key columns
	pkQuery := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.table_schema = $1
			AND tc.table_name = $2
		ORDER BY kcu.ordinal_position
	`

	pkRows, err := s.pool.Query(ctx, pkQuery, table.Schema, table.Name)
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
		pkColumns = append(pkColumns, col)
	}

	// If no primary key, use all columns as key (not ideal but workable)
	if len(pkColumns) == 0 {
		for _, col := range columns {
			pkColumns = append(pkColumns, col.Name)
		}
	}
	s.pkColumns = pkColumns

	return nil
}

// buildScanQuery builds a SELECT query for scanning a table.
func (s *postgresScanner) buildScanQuery(table tableInfo) string {
	// Build column list
	colNames := make([]string, len(s.columns))
	for i, col := range s.columns {
		colNames[i] = quoteIdentifier(col.Name)
	}

	// Build ORDER BY using primary key columns
	orderBy := make([]string, len(s.pkColumns))
	for i, col := range s.pkColumns {
		orderBy[i] = quoteIdentifier(col)
	}

	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s ORDER BY %s",
		strings.Join(colNames, ", "),
		quoteIdentifier(table.Schema),
		quoteIdentifier(table.Name),
		strings.Join(orderBy, ", "),
	)

	return query
}

// readRow reads a single row and returns it as a MigrationUnit.
func (s *postgresScanner) readRow(ctx context.Context) (*provider.MigrationUnit, error) {
	table := s.tables[s.currentTable]

	// Scan row values
	values := make([]any, len(s.columns))
	valuePtrs := make([]any, len(s.columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := s.rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	// Build data map and extract primary key
	data := make(map[string]any)
	pk := make(map[string]any)
	columnTypes := make(map[string]string)

	for i, col := range s.columns {
		val := convertValue(values[i], col.Type)
		data[col.Name] = val
		columnTypes[col.Name] = col.Type

		// Check if this column is part of the primary key
		for _, pkCol := range s.pkColumns {
			if col.Name == pkCol {
				pk[col.Name] = val
				break
			}
		}
	}

	// Create the row envelope
	row := &postgresRow{
		Table:       table.Name,
		Schema:      table.Schema,
		PrimaryKey:  pk,
		Data:        data,
		ColumnTypes: columnTypes,
	}

	// Encode to JSON for the MigrationUnit
	rowData, err := encodePostgresRow(row)
	if err != nil {
		return nil, err
	}

	// Create row key
	key := buildRowKey(table.Schema, table.Name, pk)

	// Estimate size
	size := int64(len(rowData))

	return &provider.MigrationUnit{
		Key:      key,
		Table:    table.Name,
		DataType: provider.DataTypeRow,
		Data:     rowData,
		Metadata: map[string]any{
			"schema":      table.Schema,
			"table":       table.Name,
			"primary_key": pk,
		},
		Size: size,
	}, nil
}

// quoteIdentifier quotes a PostgreSQL identifier.
func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// unmarshalScanToken deserializes a resume token.
func unmarshalScanToken(token []byte) (provider.ScanStats, error) {
	if len(token) == 0 {
		return provider.ScanStats{}, nil
	}
	var m map[string]int64
	if err := sonic.Unmarshal(token, &m); err != nil {
		return provider.ScanStats{}, err
	}
	return provider.ScanStats{
		TotalScanned: m["total_scanned"],
		TotalBytes:   m["total_bytes"],
		TablesDone:   int(m["tables_done"]),
		TablesTotal:  int(m["tables_total"]),
	}, nil
}
