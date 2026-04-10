package mariadb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

type mariaDBSchemaMigrator struct {
	db  *sql.DB
	log interface {
		Debug(msg string, args ...any)
	}
}

func newMariaDBSchemaMigrator(db *sql.DB) *mariaDBSchemaMigrator {
	return &mariaDBSchemaMigrator{
		db:  db,
		log: logger.L().With("component", "mariadb-schema"),
	}
}

func (m *mariaDBSchemaMigrator) Inspect(ctx context.Context) (*provider.Schema, error) {
	schema := &provider.Schema{}
	rows, err := m.db.QueryContext(ctx, "SHOW TABLES")
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

func (m *mariaDBSchemaMigrator) Create(ctx context.Context, schema *provider.Schema, mapper provider.TypeMapper) error {
	for _, table := range schema.Tables {
		if err := m.createTable(ctx, table, mapper); err != nil {
			return fmt.Errorf("create table %s: %w", table.Name, err)
		}
	}
	return nil
}

func (m *mariaDBSchemaMigrator) Drop(ctx context.Context, schema *provider.Schema) error {
	if _, err := m.db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return fmt.Errorf("disable foreign key checks: %w", err)
	}
	for _, table := range schema.Tables {
		if _, err := m.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(table.Name))); err != nil {
			return fmt.Errorf("drop table %s: %w", table.Name, err)
		}
	}
	if _, err := m.db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		return fmt.Errorf("enable foreign key checks: %w", err)
	}
	return nil
}

func (m *mariaDBSchemaMigrator) getTableColumns(ctx context.Context, table string) ([]provider.ColumnSchema, error) {
	rows, err := m.db.QueryContext(ctx, fmt.Sprintf("DESCRIBE %s", quoteIdentifier(table)))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var columns []provider.ColumnSchema
	for rows.Next() {
		var col provider.ColumnSchema
		var null string
		var key, defaultVal, extra sql.NullString
		if err := rows.Scan(&col.Name, &col.Type, &null, &key, &defaultVal, &extra); err != nil {
			continue
		}
		col.Nullable = null == "YES"
		col.AutoInc = extra.Valid && strings.Contains(extra.String, "auto_increment")
		if defaultVal.Valid {
			col.Default = defaultVal.String
		}
		columns = append(columns, col)
	}
	return columns, nil
}

func (m *mariaDBSchemaMigrator) getTableIndexes(ctx context.Context, table string) ([]provider.IndexSchema, error) {
	rows, err := m.db.QueryContext(ctx, fmt.Sprintf("SHOW INDEX FROM %s", quoteIdentifier(table)))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	indexMap := make(map[string]*provider.IndexSchema)
	for rows.Next() {
		var nonUnique int
		var indexName, columnName sql.NullString
		var seqInIndex int
		var collation, cardinality, subPart, packed, nullVal sql.NullString
		var indexType, comment, indexComment, visible, expression sql.NullString
		if err := rows.Scan(
			new(sql.NullString), &nonUnique, &indexName, &seqInIndex, &columnName,
			&collation, &cardinality, &subPart, &packed, &nullVal,
			&indexType, &comment, &indexComment, &visible, &expression,
		); err != nil {
			continue
		}
		if !indexName.Valid || !columnName.Valid {
			continue
		}
		idx, exists := indexMap[indexName.String]
		if !exists {
			idx = &provider.IndexSchema{
				Name:    indexName.String,
				Unique:  nonUnique == 0,
				Primary: indexName.String == "PRIMARY",
			}
			indexMap[indexName.String] = idx
		}
		idx.Columns = append(idx.Columns, columnName.String)
	}

	var indexes []provider.IndexSchema
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}
	return indexes, nil
}

func (m *mariaDBSchemaMigrator) createTable(ctx context.Context, table provider.TableSchema, mapper provider.TypeMapper) error {
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
			def += " AUTO_INCREMENT"
		}
		if col.Default != "" && !col.AutoInc {
			upper := strings.ToUpper(col.Default)
			if !strings.Contains(upper, "DATETIME") && !strings.Contains(upper, "SQLITE_") && !strings.Contains(upper, "AUTOINCREMENT") {
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

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s",
		quoteIdentifier(table.Name), strings.Join(columnDefs, ",\n  "))
	if len(pkColumns) > 0 {
		query += fmt.Sprintf(",\n  PRIMARY KEY (%s)", strings.Join(pkColumns, ", "))
	}
	query += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"

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

func (m *mariaDBSchemaMigrator) createIndex(ctx context.Context, table string, idx provider.IndexSchema) error {
	unique := ""
	if idx.Unique {
		unique = "UNIQUE "
	}
	columns := make([]string, len(idx.Columns))
	for i, col := range idx.Columns {
		columns[i] = quoteIdentifier(col)
	}
	query := fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		unique, quoteIdentifier(idx.Name), quoteIdentifier(table), strings.Join(columns, ", "))
	_, err := m.db.ExecContext(ctx, query)
	return err
}
