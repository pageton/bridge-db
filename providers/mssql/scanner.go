//go:build mssql

package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

type mssqlScanner struct {
	db              *sql.DB
	opts            provider.ScanOptions
	stats           provider.ScanStats
	tables          []string
	currentTable    int
	rows            *sql.Rows
	columns         []columnInfo
	pkColumns       []string
	done            bool
	tablesCompleted map[string]bool
	log             interface{ Info(msg string, args ...any) }
}

type columnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

func newMSSQLScanner(db *sql.DB, opts provider.ScanOptions) *mssqlScanner {
	s := &mssqlScanner{
		db:   db,
		opts: opts,
		log:  logger.L().With("component", "mssql-scanner"),
	}

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

	if len(opts.TablesCompleted) > 0 {
		s.tablesCompleted = make(map[string]bool, len(opts.TablesCompleted))
		for _, t := range opts.TablesCompleted {
			s.tablesCompleted[t] = true
		}
	}

	return s
}

func (s *mssqlScanner) Next(ctx context.Context) ([]provider.MigrationUnit, error) {
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

	units := make([]provider.MigrationUnit, 0, batchSize)

	for len(units) < batchSize && !s.done {
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

		if s.rows != nil {
			if err := s.rows.Err(); err != nil {
				s.log.Info("row error", "table", s.tables[s.currentTable], "error", err)
			}
			_ = s.rows.Close()
			s.rows = nil
			s.currentTable++
			s.stats.TablesDone++
		}

		if s.currentTable >= len(s.tables) {
			s.done = true
			break
		}

		table := s.tables[s.currentTable]
		s.log.Info("scanning table", "table", table)

		if err := s.getTableInfo(ctx, table); err != nil {
			s.log.Info("failed to get table info", "table", table, "error", err)
			s.currentTable++
			s.stats.TablesDone++
			continue
		}

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

func (s *mssqlScanner) Stats() provider.ScanStats {
	return s.stats
}

func (s *mssqlScanner) listTables(ctx context.Context) error {
	query := "SELECT [name] FROM sys.tables WHERE [type] = 'U' ORDER BY [name]"

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

func (s *mssqlScanner) getTableInfo(ctx context.Context, table string) error {
	colQuery := "SELECT c.[name], t.[name], c.is_nullable FROM sys.columns c " +
		"JOIN sys.types t ON c.user_type_id = t.user_type_id " +
		"JOIN sys.tables tb ON c.object_id = tb.object_id " +
		"WHERE tb.[name] = @p1 ORDER BY c.column_id"

	rows, err := s.db.QueryContext(ctx, colQuery, table)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	s.pkColumns = nil
	var columns []columnInfo
	for rows.Next() {
		var col columnInfo
		var nullable bool
		if err := rows.Scan(&col.Name, &col.Type, &nullable); err != nil {
			continue
		}
		col.Nullable = nullable
		columns = append(columns, col)
	}
	s.columns = columns

	pkQuery := "SELECT col.[name] FROM sys.index_columns ic " +
		"JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = ic.index_id " +
		"JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id " +
		"JOIN sys.tables t ON i.object_id = t.object_id " +
		"WHERE t.[name] = @p1 AND i.is_primary_key = 1 ORDER BY ic.key_ordinal"

	pkRows, err := s.db.QueryContext(ctx, pkQuery, table)
	if err != nil {
		s.log.Info("failed to get primary key columns", "table", table, "error", err)
	} else {
		defer func() { _ = pkRows.Close() }()
		for pkRows.Next() {
			var col string
			if err := pkRows.Scan(&col); err != nil {
				continue
			}
			s.pkColumns = append(s.pkColumns, col)
		}
	}

	if len(s.pkColumns) == 0 {
		for _, col := range columns {
			s.pkColumns = append(s.pkColumns, col.Name)
		}
	}

	return nil
}

func (s *mssqlScanner) buildScanQuery(table string) string {
	colNames := make([]string, len(s.columns))
	for i, col := range s.columns {
		colNames[i] = quoteIdentifier(col.Name)
	}

	orderBy := make([]string, len(s.pkColumns))
	for i, col := range s.pkColumns {
		orderBy[i] = quoteIdentifier(col)
	}

	return fmt.Sprintf(
		"SELECT %s FROM %s ORDER BY %s",
		strings.Join(colNames, ", "),
		quoteIdentifier(table),
		strings.Join(orderBy, ", "),
	)
}

func (s *mssqlScanner) readRow(ctx context.Context) (*provider.MigrationUnit, error) {
	table := s.tables[s.currentTable]

	values := make([]any, len(s.columns))
	valuePtrs := make([]any, len(s.columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := s.rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	data := make(map[string]any)
	pk := make(map[string]any)
	columnTypes := make(map[string]string)

	for i, col := range s.columns {
		val := convertValue(values[i])
		data[col.Name] = val
		columnTypes[col.Name] = col.Type

		for _, pkCol := range s.pkColumns {
			if col.Name == pkCol {
				pk[col.Name] = val
				break
			}
		}
	}

	row := &mssqlRow{
		Table:       table,
		PrimaryKey:  pk,
		Data:        data,
		ColumnTypes: columnTypes,
	}

	rowData, err := encodeMSSQLRow(row)
	if err != nil {
		return nil, err
	}

	key := buildRowKey(table, pk)
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

func convertValue(value any) any {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}

func quoteIdentifier(s string) string {
	return "[" + strings.ReplaceAll(s, "]", "]]") + "]"
}

func quoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

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
