package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/bytedance/sonic"
	"io"
	"strings"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// mysqlScanner enumerates rows from a MySQL database using cursor-based
// pagination. It first lists all tables, then scans each table with
// a cursor, yielding batches of MigrationUnits.
type mysqlScanner struct {
	db              *sql.DB
	opts            provider.ScanOptions
	stats           provider.ScanStats
	tables          []string
	currentTable    int
	rows            *sql.Rows
	columns         []columnInfo
	pkColumns       []string
	done            bool
	tablesCompleted map[string]bool // tables to skip on resume
	log             interface{ Info(msg string, args ...any) }
}

// columnInfo holds information about a column.
type columnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

func newMySQLScanner(db *sql.DB, opts provider.ScanOptions) *mysqlScanner {
	s := &mysqlScanner{
		db:   db,
		opts: opts,
		log:  logger.L().With("component", "mysql-scanner"),
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
func (s *mysqlScanner) Next(ctx context.Context) ([]provider.MigrationUnit, error) {
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
					"table", s.tables[s.currentTable],
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
				s.log.Info("row error", "table", s.tables[s.currentTable], "error", err)
			}
			_ = s.rows.Close()
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
		s.log.Info("scanning table", "table", table)

		// Get table columns and primary key
		if err := s.getTableInfo(ctx, table); err != nil {
			s.log.Info("failed to get table info", "table", table, "error", err)
			s.currentTable++
			s.stats.TablesDone++
			continue
		}

		// Build query with cursor-based pagination
		query := s.buildScanQuery(table)
		rows, err := s.db.QueryContext(ctx, query)
		if err != nil {
			s.log.Info("failed to open cursor for table", "table", table, "error", err)
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
func (s *mysqlScanner) Stats() provider.ScanStats {
	return s.stats
}

// listTables enumerates all tables in the database.
func (s *mysqlScanner) listTables(ctx context.Context) error {
	query := "SHOW TABLES"

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			continue
		}
		tables = append(tables, table)
	}

	s.tables = tables
	s.stats.TablesTotal = len(tables)

	// Filter out completed tables on resume.
	if len(s.tablesCompleted) > 0 {
		filtered := tables[:0]
		for _, t := range tables {
			if !s.tablesCompleted[t] {
				filtered = append(filtered, t)
			}
		}
		s.tables = filtered
		s.stats.TablesTotal = len(filtered)
		if s.currentTable > len(s.tables) {
			s.currentTable = len(s.tables)
		}
	}

	s.log.Info("found tables", "count", len(s.tables))

	return nil
}

// getTableInfo retrieves column information and primary key for a table.
func (s *mysqlScanner) getTableInfo(ctx context.Context, table string) error {
	// Get columns
	colQuery := fmt.Sprintf("DESCRIBE %s", quoteIdentifier(table))

	rows, err := s.db.QueryContext(ctx, colQuery)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	var columns []columnInfo
	for rows.Next() {
		var col columnInfo
		var null string
		var key, defaultVal, extra sql.NullString

		if err := rows.Scan(&col.Name, &col.Type, &null, &key, &defaultVal, &extra); err != nil {
			continue
		}
		col.Nullable = null == "YES"
		columns = append(columns, col)

		// Check if this is a primary key column
		if key.Valid && key.String == "PRI" {
			s.pkColumns = append(s.pkColumns, col.Name)
		}
	}
	s.columns = columns

	// If no primary key, use all columns as key (not ideal but workable)
	if len(s.pkColumns) == 0 {
		for _, col := range columns {
			s.pkColumns = append(s.pkColumns, col.Name)
		}
	}

	return nil
}

// buildScanQuery builds a SELECT query for scanning a table.
func (s *mysqlScanner) buildScanQuery(table string) string {
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
		"SELECT %s FROM %s ORDER BY %s",
		strings.Join(colNames, ", "),
		quoteIdentifier(table),
		strings.Join(orderBy, ", "),
	)

	return query
}

// readRow reads a single row and returns it as a MigrationUnit.
func (s *mysqlScanner) readRow(ctx context.Context) (*provider.MigrationUnit, error) {
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
		val := convertValue(values[i])
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
	row := &mysqlRow{
		Table:       table,
		PrimaryKey:  pk,
		Data:        data,
		ColumnTypes: columnTypes,
	}

	// Encode to JSON for the MigrationUnit
	rowData, err := encodeMySQLRow(row)
	if err != nil {
		return nil, err
	}

	// Create row key
	key := buildRowKey(table, pk)

	// Estimate size
	size := int64(len(rowData))

	return &provider.MigrationUnit{
		Key:      key,
		Table:    table,
		DataType: provider.DataTypeRow,
		Data:     rowData,
		Metadata: map[string]any{
			"table":       table,
			"primary_key": pk,
		},
		Size: size,
	}, nil
}

// convertValue converts a MySQL value to a Go type suitable for JSON serialization.
func convertValue(value any) any {
	if value == nil {
		return nil
	}

	switch v := value.(type) {
	case []byte:
		// Return as string for most cases
		return string(v)
	default:
		return v
	}
}

// quoteIdentifier quotes a MySQL identifier.
func quoteIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
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
