package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

func pgSafeDefault(defaultVal string) (string, bool) {
	if defaultVal == "" {
		return "", false
	}
	upper := strings.ToUpper(defaultVal)
	if strings.Contains(upper, "DATETIME") ||
		strings.Contains(upper, "SQLITE_") ||
		strings.Contains(upper, "AUTOINCREMENT") {
		return "", false
	}
	allowed := []string{"NOW()", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "GEN_RANDOM_UUID()", "UUID_GENERATE_V4()", "NEXTVAL("}
	for _, fn := range allowed {
		if strings.Contains(upper, fn) {
			return defaultVal, true
		}
	}
	if strings.Contains(upper, "()") && !strings.HasPrefix(upper, "'") {
		return "", false
	}
	return defaultVal, true
}

// postgresSchemaMigrator handles schema migration for PostgreSQL databases.
// It can inspect source schemas and create them on the destination.
type postgresSchemaMigrator struct {
	pool *pgxpool.Pool
	log  interface {
		Debug(msg string, args ...any)
	}
}

func newPostgresSchemaMigrator(pool *pgxpool.Pool) *postgresSchemaMigrator {
	return &postgresSchemaMigrator{
		pool: pool,
		log:  logger.L().With("component", "postgres-schema"),
	}
}

// Inspect reads the schema from the source database.
func (m *postgresSchemaMigrator) Inspect(ctx context.Context) (*provider.Schema, error) {
	schema := &provider.Schema{}

	// List tables
	tableQuery := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		AND table_type = 'BASE TABLE'
		ORDER BY table_schema, table_name
	`

	rows, err := m.pool.Query(ctx, tableQuery)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableSchema, tableName string
		if err := rows.Scan(&tableSchema, &tableName); err != nil {
			continue
		}

		// Get columns for this table
		columns, err := m.getTableColumns(ctx, tableSchema, tableName)
		if err != nil {
			m.log.Debug("failed to get columns", "table", tableName, "error", err)
			continue
		}

		// Get indexes for this table
		indexes, err := m.getTableIndexes(ctx, tableSchema, tableName)
		if err != nil {
			m.log.Debug("failed to get indexes", "table", tableName, "error", err)
			continue
		}

		tableSchemaObj := provider.TableSchema{
			Name:    tableName,
			Columns: columns,
			Indexes: indexes,
		}
		schema.Tables = append(schema.Tables, tableSchemaObj)
	}

	return schema, nil
}

// Create applies the given schema to the destination database.
func (m *postgresSchemaMigrator) Create(ctx context.Context, schema *provider.Schema, mapper provider.TypeMapper) error {
	for _, table := range schema.Tables {
		if err := m.createTable(ctx, table, mapper); err != nil {
			return fmt.Errorf("create table %s: %w", table.Name, err)
		}
	}
	return nil
}

// Drop removes all tables from the database.
func (m *postgresSchemaMigrator) Drop(ctx context.Context, schema *provider.Schema) error {
	for _, table := range schema.Tables {
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", quoteIdentifier(table.Name))
		if _, err := m.pool.Exec(ctx, query); err != nil {
			return fmt.Errorf("drop table %s: %w", table.Name, err)
		}
	}
	return nil
}

// getTableColumns retrieves column information for a table.
func (m *postgresSchemaMigrator) getTableColumns(ctx context.Context, schema, table string) ([]provider.ColumnSchema, error) {
	query := `
		SELECT 
			c.column_name,
			CASE
				WHEN c.data_type IN ('numeric', 'decimal') AND c.numeric_precision IS NOT NULL THEN
					upper(c.data_type) || '(' || c.numeric_precision || ',' || COALESCE(c.numeric_scale, 0) || ')'
				WHEN c.data_type IN ('character varying', 'character') AND c.character_maximum_length IS NOT NULL THEN
					CASE
						WHEN c.data_type = 'character varying' THEN 'VARCHAR(' || c.character_maximum_length || ')'
						ELSE 'CHAR(' || c.character_maximum_length || ')'
					END
				ELSE upper(c.data_type)
			END AS column_type,
			c.is_nullable,
			c.column_default,
			CASE WHEN c.column_default LIKE 'nextval%' THEN true ELSE false END as is_auto_increment
		FROM information_schema.columns c
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position
	`

	rows, err := m.pool.Query(ctx, query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []provider.ColumnSchema
	for rows.Next() {
		var col provider.ColumnSchema
		var nullable string
		var defaultVal *string

		if err := rows.Scan(&col.Name, &col.Type, &nullable, &defaultVal, &col.AutoInc); err != nil {
			continue
		}

		col.Nullable = nullable == "YES"
		if defaultVal != nil {
			col.Default = *defaultVal
		}

		columns = append(columns, col)
	}

	return columns, nil
}

// getTableIndexes retrieves index information for a table.
func (m *postgresSchemaMigrator) getTableIndexes(ctx context.Context, schema, table string) ([]provider.IndexSchema, error) {
	query := `
		SELECT
			i.relname as index_name,
			array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns,
			ix.indisunique as is_unique,
			ix.indisprimary as is_primary
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
		WHERE n.nspname = $1 AND t.relname = $2
		GROUP BY i.relname, ix.indisunique, ix.indisprimary
	`

	rows, err := m.pool.Query(ctx, query, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []provider.IndexSchema
	for rows.Next() {
		var idx provider.IndexSchema
		var columns []string

		if err := rows.Scan(&idx.Name, &columns, &idx.Unique, &idx.Primary); err != nil {
			continue
		}

		idx.Columns = columns
		indexes = append(indexes, idx)
	}

	return indexes, nil
}

// createTable creates a table with the given schema.
func (m *postgresSchemaMigrator) createTable(ctx context.Context, table provider.TableSchema, mapper provider.TypeMapper) error {
	// Build column definitions
	columnDefs := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		colType := col.Type
		if mapper != nil {
			if mappedType, ok := mapper.MapType(col.Type); ok {
				colType = mappedType
			}
		}

		if col.AutoInc {
			switch strings.ToLower(colType) {
			case "bigint", "bigserial":
				colType = "BIGSERIAL"
			case "smallint", "smallserial":
				colType = "SMALLSERIAL"
			default:
				colType = "SERIAL"
			}
		}

		def := fmt.Sprintf("%s %s", quoteIdentifier(col.Name), colType)

		if !col.Nullable && !col.AutoInc {
			def += " NOT NULL"
		}

		if col.Default != "" && !col.AutoInc {
			if safeDefault, ok := pgSafeDefault(col.Default); ok {
				def += " DEFAULT " + safeDefault
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

	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (\n  %s",
		quoteIdentifier(table.Name),
		strings.Join(columnDefs, ",\n  "),
	)

	if len(pkColumns) > 0 {
		query += fmt.Sprintf(",\n  PRIMARY KEY (%s)", strings.Join(pkColumns, ", "))
	}

	query += "\n)"

	if _, err := m.pool.Exec(ctx, query); err != nil {
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
func (m *postgresSchemaMigrator) createIndex(ctx context.Context, table string, idx provider.IndexSchema) error {
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

	_, err := m.pool.Exec(ctx, query)
	return err
}
