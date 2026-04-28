package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// mysqlSchemaMigrator handles schema migration for MySQL databases.
// It can inspect source schemas and create them on the destination.
type mysqlSchemaMigrator struct {
	db  *sql.DB
	log interface {
		Warn(msg string, args ...any)
		Debug(msg string, args ...any)
	}
}

func newMySQLSchemaMigrator(db *sql.DB) *mysqlSchemaMigrator {
	return &mysqlSchemaMigrator{
		db:  db,
		log: logger.L().With("component", "mysql-schema"),
	}
}

// Inspect reads the schema from the source database.
func (m *mysqlSchemaMigrator) Inspect(ctx context.Context) (*provider.Schema, error) {
	schema := &provider.Schema{}

	// List tables
	tableQuery := "SHOW TABLES"

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

		// Get columns for this table
		columns, err := m.getTableColumns(ctx, tableName)
		if err != nil {
			m.log.Debug("failed to get columns", "table", tableName, "error", err)
			continue
		}

		// Get indexes for this table
		indexes, err := m.getTableIndexes(ctx, tableName)
		if err != nil {
			m.log.Debug("failed to get indexes", "table", tableName, "error", err)
			continue
		}

		// Get foreign keys for this table
		fks, err := m.getTableForeignKeys(ctx, tableName)
		if err != nil {
			m.log.Debug("failed to get foreign keys", "table", tableName, "error", err)
		}

		tableSchema := provider.TableSchema{
			Name:        tableName,
			Columns:     columns,
			Indexes:     indexes,
			ForeignKeys: fks,
		}
		schema.Tables = append(schema.Tables, tableSchema)
	}

	return schema, nil
}

// Create applies the given schema to the destination database.
func (m *mysqlSchemaMigrator) Create(ctx context.Context, schema *provider.Schema, mapper provider.TypeMapper) error {
	for _, table := range schema.Tables {
		if err := m.createTable(ctx, table, mapper); err != nil {
			return fmt.Errorf("create table %s: %w", table.Name, err)
		}
	}
	return nil
}

// Drop removes all tables from the database.
func (m *mysqlSchemaMigrator) Drop(ctx context.Context, schema *provider.Schema) error {
	// Disable foreign key checks
	if _, err := m.db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return fmt.Errorf("disable foreign key checks: %w", err)
	}

	for _, table := range schema.Tables {
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(table.Name))
		if _, err := m.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("drop table %s: %w", table.Name, err)
		}
	}

	// Re-enable foreign key checks
	if _, err := m.db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		return fmt.Errorf("enable foreign key checks: %w", err)
	}

	return nil
}

// getTableColumns retrieves column information for a table.
func (m *mysqlSchemaMigrator) getTableColumns(ctx context.Context, table string) ([]provider.ColumnSchema, error) {
	query := fmt.Sprintf("DESCRIBE %s", quoteIdentifier(table))

	rows, err := m.db.QueryContext(ctx, query)
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

// getTableIndexes retrieves index information for a table.
func (m *mysqlSchemaMigrator) getTableIndexes(ctx context.Context, table string) ([]provider.IndexSchema, error) {
	query := fmt.Sprintf("SHOW INDEX FROM %s", quoteIdentifier(table))

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(columnNames) < 5 {
		return nil, fmt.Errorf("show index returned too few columns: %d", len(columnNames))
	}

	// Group by index name
	indexMap := make(map[string]*provider.IndexSchema)

	for rows.Next() {
		values := make([]sql.NullString, len(columnNames))
		scanArgs := make([]any, 0, len(values))
		for i := range values {
			scanArgs = append(scanArgs, &values[i])
		}

		if err := rows.Scan(scanArgs...); err != nil {
			m.log.Warn("scan index row failed", "error", err)
			continue
		}

		var nonUnique int
		if !values[1].Valid {
			continue
		}
		if _, err := fmt.Sscanf(values[1].String, "%d", &nonUnique); err != nil {
			m.log.Warn("parse index uniqueness failed", "table", table, "value", values[1].String, "error", err)
			continue
		}

		indexName := values[2]
		columnName := values[4]

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

// getTableForeignKeys retrieves foreign key relationships for a table.
func (m *mysqlSchemaMigrator) getTableForeignKeys(ctx context.Context, table string) ([]provider.ForeignKey, error) {
	query := `
		SELECT
			ccu.CONSTRAINT_NAME,
			ccu.COLUMN_NAME,
			kcu.TABLE_NAME AS REFERENCED_TABLE,
			kcu.COLUMN_NAME AS REFERENCED_COLUMN
		FROM information_schema.KEY_COLUMN_USAGE ccu
		JOIN information_schema.TABLE_CONSTRAINTS tc
			ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			AND ccu.TABLE_SCHEMA = tc.TABLE_SCHEMA
		LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu
			ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
		WHERE ccu.TABLE_SCHEMA = DATABASE()
			AND ccu.TABLE_NAME = ?
			AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
		ORDER BY ccu.ORDINAL_POSITION
	`

	rows, err := m.db.QueryContext(ctx, query, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	// Group by constraint name
	fkMap := make(map[string]*provider.ForeignKey)

	for rows.Next() {
		var fk provider.ForeignKey
		var refTable, refCol, col sql.NullString

		if err := rows.Scan(&fk.Name, &col, &refTable, &refCol); err != nil {
			continue
		}

		if !col.Valid || !refTable.Valid {
			continue
		}

		existing, exists := fkMap[fk.Name]
		if !exists {
			fk.ReferencedTable = refTable.String
			fkMap[fk.Name] = &fk
			existing = &fk
		}

		existing.Columns = append(existing.Columns, col.String)
		if refCol.Valid {
			existing.ReferencedColumns = append(existing.ReferencedColumns, refCol.String)
		}
	}

	var fks []provider.ForeignKey
	for _, fk := range fkMap {
		fks = append(fks, *fk)
	}

	return fks, nil
}

// createTable creates a table with the given schema.
func (m *mysqlSchemaMigrator) createTable(ctx context.Context, table provider.TableSchema, mapper provider.TypeMapper) error {
	// Build column definitions
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

	// Build primary key constraint
	var pkColumns []string
	for _, idx := range table.Indexes {
		if idx.Primary {
			for _, col := range idx.Columns {
				pkColumns = append(pkColumns, quoteIdentifier(col))
			}
			break
		}
	}

	// MySQL requires AUTO_INCREMENT columns to be a key.
	// If no PRIMARY KEY was found from indexes but a column has AutoInc,
	// add it as PRIMARY KEY.
	if len(pkColumns) == 0 {
		for _, col := range table.Columns {
			if col.AutoInc {
				pkColumns = []string{quoteIdentifier(col.Name)}
				break
			}
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

	query += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"

	if _, err := m.db.ExecContext(ctx, query); err != nil {
		return err
	}

	// Create indexes
	for _, idx := range table.Indexes {
		if idx.Primary {
			continue // Already created with table
		}

		if err := m.createIndex(ctx, table.Name, idx); err != nil {
			m.log.Debug("failed to create index", "table", table.Name, "index", idx.Name, "error", err)
		}
	}

	m.log.Debug("created table", "table", table.Name)
	return nil
}

// createIndex creates an index on a table.
func (m *mysqlSchemaMigrator) createIndex(ctx context.Context, table string, idx provider.IndexSchema) error {
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
