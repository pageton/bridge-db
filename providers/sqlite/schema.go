//go:build sqlite

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

func sqliteSafeDefault(defaultVal string) (string, bool) {
	if defaultVal == "" {
		return "", false
	}
	upper := strings.ToUpper(defaultVal)
	if strings.Contains(upper, "NEXTVAL") ||
		strings.Contains(upper, "CURRVAL") ||
		strings.Contains(upper, "SETVAL") ||
		strings.Contains(upper, "::REGCLASS") {
		return "", false
	}
	if strings.Contains(upper, "()") && !strings.HasPrefix(upper, "'") {
		langFns := []string{"NOW()", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME"}
		for _, fn := range langFns {
			if upper == fn {
				return "CURRENT_TIMESTAMP", true
			}
		}
		return "", false
	}
	// SQLite function calls like datetime('now') need wrapping in parens
	// for DEFAULT clauses: DEFAULT (datetime('now'))
	if strings.ContainsRune(defaultVal, '(') && strings.ContainsRune(defaultVal, ')') {
		return "(" + defaultVal + ")", true
	}
	return defaultVal, true
}

type sqliteSchemaMigrator struct {
	db  *sql.DB
	log interface {
		Debug(msg string, args ...any)
	}
}

func newSQLiteSchemaMigrator(db *sql.DB) *sqliteSchemaMigrator {
	return &sqliteSchemaMigrator{
		db:  db,
		log: logger.L().With("component", "sqlite-schema"),
	}
}

func (m *sqliteSchemaMigrator) Inspect(ctx context.Context) (*provider.Schema, error) {
	schema := &provider.Schema{}

	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		tableNames = append(tableNames, tableName)
	}
	_ = rows.Close()

	for _, tableName := range tableNames {
		columns, err := m.getTableColumns(ctx, tableName)
		if err != nil {
			m.log.Debug("failed to get columns", "table", tableName, "error", err)
			continue
		}

		indexes, err := m.getTableIndexes(ctx, tableName)
		if err != nil {
			m.log.Debug("failed to get indexes", "table", tableName, "error", err)
			continue
		}

		schema.Tables = append(schema.Tables, provider.TableSchema{
			Name:    tableName,
			Columns: columns,
			Indexes: indexes,
		})
	}

	return schema, nil
}

func (m *sqliteSchemaMigrator) Create(ctx context.Context, schema *provider.Schema, mapper provider.TypeMapper) error {
	for _, table := range schema.Tables {
		if err := m.createTable(ctx, table, mapper); err != nil {
			return fmt.Errorf("create table %s: %w", table.Name, err)
		}
	}
	return nil
}

func (m *sqliteSchemaMigrator) getTableColumns(ctx context.Context, table string) ([]provider.ColumnSchema, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", quoteIdentifier(table))
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var columns []provider.ColumnSchema
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var defaultVal sql.NullString
		var pk int

		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultVal, &pk); err != nil {
			continue
		}

		col := provider.ColumnSchema{
			Name:     name,
			Type:     colType,
			Nullable: notNull == 0,
			AutoInc:  pk == 1 && strings.Contains(strings.ToUpper(colType), "INTEGER"),
		}

		if defaultVal.Valid {
			col.Default = defaultVal.String
		}

		columns = append(columns, col)
	}

	return columns, nil
}

func (m *sqliteSchemaMigrator) getTableIndexes(ctx context.Context, table string) ([]provider.IndexSchema, error) {
	query := fmt.Sprintf("PRAGMA index_list(%s)", quoteIdentifier(table))
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	var indexes []struct {
		name    string
		unique  bool
		primary bool
	}

	for rows.Next() {
		var seq int
		var name string
		var unique int
		var origin string
		var partial int

		if err := rows.Scan(&seq, &name, &unique, &origin, &partial); err != nil {
			continue
		}

		indexes = append(indexes, struct {
			name    string
			unique  bool
			primary bool
		}{
			name:    name,
			unique:  unique == 1,
			primary: origin == "pk",
		})
	}
	_ = rows.Close()

	pkColumns, err := m.getPrimaryKeyColumns(ctx, table)
	if err != nil {
		return nil, err
	}

	var result []provider.IndexSchema
	for _, idx := range indexes {
		colQuery := fmt.Sprintf("PRAGMA index_info(%s)", quoteIdentifier(idx.name))
		colRows, err := m.db.QueryContext(ctx, colQuery)
		if err != nil {
			continue
		}

		var cols []string
		for colRows.Next() {
			var seq int
			var rank int
			var col sql.NullString

			if err := colRows.Scan(&seq, &rank, &col); err != nil {
				continue
			}
			if col.Valid {
				cols = append(cols, col.String)
			}
		}
		_ = colRows.Close()

		if len(cols) > 0 {
			result = append(result, provider.IndexSchema{
				Name:    idx.name,
				Columns: cols,
				Unique:  idx.unique,
				Primary: idx.primary,
			})
		}
	}

	if len(pkColumns) > 0 {
		hasPrimary := false
		for _, idx := range result {
			if idx.Primary {
				hasPrimary = true
				break
			}
		}
		if !hasPrimary {
			result = append(result, provider.IndexSchema{
				Name:    table + "_pk",
				Columns: pkColumns,
				Unique:  true,
				Primary: true,
			})
		}
	}

	return result, nil
}

func (m *sqliteSchemaMigrator) getPrimaryKeyColumns(ctx context.Context, table string) ([]string, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", quoteIdentifier(table))
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	type pkCol struct {
		order int
		name  string
	}
	var cols []pkCol
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var defaultVal sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultVal, &pk); err != nil {
			continue
		}
		if pk > 0 {
			cols = append(cols, pkCol{order: pk, name: name})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(cols, func(a, b pkCol) int {
		return a.order - b.order
	})
	out := make([]string, 0, len(cols))
	for _, col := range cols {
		out = append(out, col.name)
	}
	return out, nil
}

func (m *sqliteSchemaMigrator) createTable(ctx context.Context, table provider.TableSchema, mapper provider.TypeMapper) error {
	if len(table.Columns) == 0 {
		return fmt.Errorf("create table %s: no column definitions in schema (source may be schemaless)", table.Name)
	}

	columnDefs := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		colType := col.Type
		if mapper != nil {
			if mappedType, ok := mapper.MapType(col.Type); ok {
				colType = mappedType
			}
		}

		def := fmt.Sprintf("%s %s", quoteIdentifier(col.Name), colType)

		if !col.Nullable {
			def += " NOT NULL"
		}

		if col.AutoInc {
			def += " PRIMARY KEY AUTOINCREMENT"
			columnDefs[i] = def
			continue
		}

		if safeDefault, ok := sqliteSafeDefault(col.Default); ok && safeDefault != "" {
			def += " DEFAULT " + safeDefault
		}

		columnDefs[i] = def
	}

	var pkColumns []string
	for _, idx := range table.Indexes {
		if idx.Primary {
			hasAutoInc := false
			for _, col := range table.Columns {
				if col.AutoInc {
					hasAutoInc = true
					break
				}
			}
			if hasAutoInc {
				continue
			}
			for _, col := range idx.Columns {
				pkColumns = append(pkColumns, quoteIdentifier(col))
			}
			break
		}
	}

	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s",
		quoteIdentifier(table.Name),
		strings.Join(columnDefs, ",\n  "),
	)

	if len(pkColumns) > 0 {
		query += fmt.Sprintf(",\n  PRIMARY KEY (%s)", strings.Join(pkColumns, ", "))
	}

	query += "\n)"

	if _, err := m.db.ExecContext(ctx, query); err != nil {
		return err
	}

	for _, idx := range table.Indexes {
		if idx.Primary {
			continue
		}

		if err := m.createIndex(ctx, table.Name, idx); err != nil {
			m.log.Debug("failed to create index", "table", table.Name, "index", idx.Name, "error", err)
		}
	}

	m.log.Debug("created table", "table", table.Name)
	return nil
}

func (m *sqliteSchemaMigrator) createIndex(ctx context.Context, table string, idx provider.IndexSchema) error {
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}

	columns := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		columns[i] = quoteIdentifier(col)
	}

	query := fmt.Sprintf(
		"CREATE %sINDEX IF NOT EXISTS %s ON %s (%s)",
		unique,
		quoteIdentifier(idx.Name),
		quoteIdentifier(table),
		strings.Join(columns, ", "),
	)

	_, err := m.db.ExecContext(ctx, query)
	return err
}
