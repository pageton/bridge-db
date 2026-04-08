//go:build mssql

package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

type mssqlSchemaMigrator struct {
	db  *sql.DB
	log interface{ Info(msg string, args ...any) }
}

func newMSSQLSchemaMigrator(db *sql.DB) *mssqlSchemaMigrator {
	return &mssqlSchemaMigrator{
		db:  db,
		log: logger.L().With("component", "mssql-schema"),
	}
}

func (m *mssqlSchemaMigrator) Inspect(ctx context.Context) (*provider.Schema, error) {
	schema := &provider.Schema{}

	tableQuery := "SELECT [name] FROM sys.tables WHERE [type] = 'U' ORDER BY [name]"

	rows, err := m.db.QueryContext(ctx, tableQuery)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}

		columns, err := m.getTableColumns(ctx, tableName)
		if err != nil {
			m.log.Info("failed to get columns", "table", tableName, "error", err)
			continue
		}

		indexes, err := m.getTableIndexes(ctx, tableName)
		if err != nil {
			m.log.Info("failed to get indexes", "table", tableName, "error", err)
			continue
		}

		tableSchema := provider.TableSchema{
			Name:    tableName,
			Columns: columns,
			Indexes: indexes,
		}
		schema.Tables = append(schema.Tables, tableSchema)
	}

	return schema, nil
}

func (m *mssqlSchemaMigrator) Create(ctx context.Context, schema *provider.Schema, mapper provider.TypeMapper) error {
	for _, table := range schema.Tables {
		if err := m.createTable(ctx, table, mapper); err != nil {
			return fmt.Errorf("create table %s: %w", table.Name, err)
		}
	}
	return nil
}

func (m *mssqlSchemaMigrator) Drop(ctx context.Context, schema *provider.Schema) error {
	if _, err := m.db.ExecContext(ctx, "EXEC sp_msforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT all'"); err != nil {
		m.log.Info("warning: could not disable constraints", "error", err)
	}

	for _, table := range schema.Tables {
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(table.Name))
		if _, err := m.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("drop table %s: %w", table.Name, err)
		}
	}

	return nil
}

func (m *mssqlSchemaMigrator) getTableColumns(ctx context.Context, table string) ([]provider.ColumnSchema, error) {
	query := "SELECT c.[name], t.[name], c.is_nullable, " +
		"OBJECT_DEFINITION(c.default_object_id), " +
		"COLUMNPROPERTY(c.object_id, c.[name], 'IsIdentity') " +
		"FROM sys.columns c " +
		"JOIN sys.types t ON c.user_type_id = t.user_type_id " +
		"JOIN sys.tables tb ON c.object_id = tb.object_id " +
		"WHERE tb.[name] = @p1 ORDER BY c.column_id"

	rows, err := m.db.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var columns []provider.ColumnSchema
	for rows.Next() {
		var col provider.ColumnSchema
		var nullable bool
		var defaultDef sql.NullString
		var isIdentity int

		if err := rows.Scan(&col.Name, &col.Type, &nullable, &defaultDef, &isIdentity); err != nil {
			continue
		}

		col.Nullable = nullable
		col.AutoInc = isIdentity == 1

		if defaultDef.Valid && defaultDef.String != "" {
			col.Default = defaultDef.String
		}

		columns = append(columns, col)
	}

	return columns, nil
}

func (m *mssqlSchemaMigrator) getTableIndexes(ctx context.Context, table string) ([]provider.IndexSchema, error) {
	query := "SELECT i.[name], i.is_unique, i.is_primary_key, col.[name] " +
		"FROM sys.indexes i " +
		"JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id " +
		"JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id " +
		"JOIN sys.tables t ON i.object_id = t.object_id " +
		"WHERE t.[name] = @p1 AND i.[name] IS NOT NULL " +
		"ORDER BY i.[name], ic.key_ordinal"

	rows, err := m.db.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	indexMap := make(map[string]*provider.IndexSchema)

	for rows.Next() {
		var indexName, columnName string
		var isUnique, isPrimaryKey bool

		if err := rows.Scan(&indexName, &isUnique, &isPrimaryKey, &columnName); err != nil {
			m.log.Info("scan index row failed", "error", err)
			continue
		}

		idx, exists := indexMap[indexName]
		if !exists {
			idx = &provider.IndexSchema{
				Name:    indexName,
				Unique:  isUnique,
				Primary: isPrimaryKey,
			}
			indexMap[indexName] = idx
		}

		idx.Columns = append(idx.Columns, columnName)
	}

	var indexes []provider.IndexSchema
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

func (m *mssqlSchemaMigrator) createTable(ctx context.Context, table provider.TableSchema, mapper provider.TypeMapper) error {
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
			def += " IDENTITY(1,1)"
		}

		if col.Default != "" && !col.AutoInc {
			upper := strings.ToUpper(col.Default)
			if !strings.Contains(upper, "IDENTITY") && !strings.Contains(upper, "AUTOINCREMENT") {
				def += " DEFAULT " + col.Default
			}
		}

		columnDefs[i] = def
	}

	var pkColumns []string
	for _, idx := range table.Indexes {
		if idx.Primary {
			for _, col := range idx.Columns {
				pkColumns = append(pkColumns, quoteIdentifier(col))
			}
			break
		}
	}

	if len(pkColumns) == 0 {
		for _, col := range table.Columns {
			if col.AutoInc {
				pkColumns = []string{quoteIdentifier(col.Name)}
				break
			}
		}
	}

	query := fmt.Sprintf(
		"CREATE TABLE %s (\n  %s",
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
			m.log.Info("failed to create index", "table", table.Name, "index", idx.Name, "error", err)
		}
	}

	m.log.Info("created table", "table", table.Name)
	return nil
}

func (m *mssqlSchemaMigrator) createIndex(ctx context.Context, table string, idx provider.IndexSchema) error {
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}

	columns := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		columns[i] = quoteIdentifier(col)
	}

	query := fmt.Sprintf(
		"CREATE %sINDEX %s ON %s (%s)",
		unique,
		quoteIdentifier(idx.Name),
		quoteIdentifier(table),
		strings.Join(columns, ", "),
	)

	_, err := m.db.ExecContext(ctx, query)
	return err
}
