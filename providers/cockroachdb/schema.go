package cockroachdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

func crdbSafeDefault(defaultVal string) (string, bool) {
	if defaultVal == "" {
		return "", false
	}
	upper := strings.ToUpper(defaultVal)
	if strings.Contains(upper, "DATETIME") ||
		strings.Contains(upper, "SQLITE_") ||
		strings.Contains(upper, "AUTOINCREMENT") {
		return "", false
	}
	allowed := []string{"NOW()", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "GEN_RANDOM_UUID()", "UUID_GENERATE_V4()", "NEXTVAL(", "UNIQUE_ROWID"}
	for _, fn := range allowed {
		if strings.Contains(upper, fn) {
			switch upper {
			case "CURRENT_TIMESTAMP()":
				return "CURRENT_TIMESTAMP", true
			case "NOW()":
				return "CURRENT_TIMESTAMP", true
			}
			return defaultVal, true
		}
	}
	if strings.Contains(upper, "()") && !strings.HasPrefix(upper, "'") {
		return "", false
	}
	return defaultVal, true
}

type cockroachDBSchemaMigrator struct {
	pool *pgxpool.Pool
	log  interface {
		Debug(msg string, args ...any)
	}
}

func newCockroachDBSchemaMigrator(pool *pgxpool.Pool) *cockroachDBSchemaMigrator {
	return &cockroachDBSchemaMigrator{
		pool: pool,
		log:  logger.L().With("component", "cockroachdb-schema"),
	}
}

func (m *cockroachDBSchemaMigrator) Inspect(ctx context.Context) (*provider.Schema, error) {
	schema := &provider.Schema{}

	tableQuery := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'crdb_internal')
		AND table_type = 'BASE TABLE'
		ORDER BY table_schema, table_name
	`

	rows, err := m.pool.Query(ctx, tableQuery)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tblSchema, tableName string
		if err := rows.Scan(&tblSchema, &tableName); err != nil {
			continue
		}

		columns, err := m.getTableColumns(ctx, tblSchema, tableName)
		if err != nil {
			m.log.Debug("failed to get columns", "table", tableName, "error", err)
			continue
		}

		indexes, err := m.getTableIndexes(ctx, tblSchema, tableName)
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

func (m *cockroachDBSchemaMigrator) Create(ctx context.Context, schema *provider.Schema, mapper provider.TypeMapper) error {
	for _, table := range schema.Tables {
		if err := m.createTable(ctx, table, mapper); err != nil {
			return fmt.Errorf("create table %s: %w", table.Name, err)
		}
	}
	return nil
}

func (m *cockroachDBSchemaMigrator) Drop(ctx context.Context, schema *provider.Schema) error {
	for _, table := range schema.Tables {
		query := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", quoteIdentifier(table.Name))
		if _, err := m.pool.Exec(ctx, query); err != nil {
			return fmt.Errorf("drop table %s: %w", table.Name, err)
		}
	}
	return nil
}

func (m *cockroachDBSchemaMigrator) getTableColumns(ctx context.Context, schema, table string) ([]provider.ColumnSchema, error) {
	query := `
		SELECT
			c.column_name,
			c.data_type,
			c.is_nullable,
			c.column_default,
			CASE WHEN c.column_default LIKE 'nextval%' OR c.column_default LIKE 'unique_rowid%' THEN true ELSE false END as is_auto_increment
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

		// CockroachDB adds a hidden "rowid" column to tables. It is not a
		// real user column and should not be migrated.
		if col.Name == "rowid" {
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

func (m *cockroachDBSchemaMigrator) getTableIndexes(ctx context.Context, schema, table string) ([]provider.IndexSchema, error) {
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
		// Filter out CockroachDB's hidden "rowid" column from index definitions.
		filtered := make([]string, 0, len(columns))
		for _, c := range columns {
			if c != "rowid" {
				filtered = append(filtered, c)
			}
		}
		if len(filtered) == 0 {
			continue
		}
		idx.Columns = filtered
		indexes = append(indexes, idx)
	}

	return indexes, nil
}

func (m *cockroachDBSchemaMigrator) createTable(ctx context.Context, table provider.TableSchema, mapper provider.TypeMapper) error {
	columnDefs := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		colType := col.Type
		if mapper != nil {
			if mappedType, ok := mapper.MapType(col.Type); ok {
				colType = mappedType
			}
		}

		if col.AutoInc {
			switch colType {
			case "bigint", "bigserial":
				colType = "BIGINT"
			case "smallint", "smallserial":
				colType = "SMALLINT"
			default:
				colType = "INT"
			}
		}

		def := fmt.Sprintf("%s %s", quoteIdentifier(col.Name), colType)

		if !col.Nullable && !col.AutoInc {
			def += " NOT NULL"
		}

		if col.Default != "" && !col.AutoInc {
			if safeDefault, ok := crdbSafeDefault(col.Default); ok {
				def += " DEFAULT " + safeDefault
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
	if err := m.waitForTable(ctx, table.Name, len(table.Columns)); err != nil {
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

func (m *cockroachDBSchemaMigrator) waitForTable(ctx context.Context, table string, wantColumns int) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		var count int
		err := m.pool.QueryRow(ctx, `
			SELECT count(*)
			FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = $1
		`, table).Scan(&count)
		if err == nil && count >= wantColumns {
			return nil
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("wait for table %s visibility: %w", table, err)
			}
			return fmt.Errorf("wait for table %s visibility: have %d columns, want at least %d", table, count, wantColumns)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (m *cockroachDBSchemaMigrator) createIndex(ctx context.Context, table string, idx provider.IndexSchema) error {
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
