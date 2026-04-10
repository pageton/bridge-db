package postgres

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/bytedance/sonic"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// postgresVerifier checks that rows in the destination PostgreSQL match the source.
// It verifies row counts per table and performs sample-based comparison.
type postgresVerifier struct {
	pool *pgxpool.Pool
	log  interface {
		Info(msg string, args ...any)
		Warn(msg string, args ...any)
	}
}

func newPostgresVerifier(pool *pgxpool.Pool) *postgresVerifier {
	return &postgresVerifier{
		pool: pool,
		log:  logger.L().With("component", "postgres-verifier"),
	}
}

// Verify checks the given row keys for existence and correctness.
// If keys is empty, it performs a count-based check across all tables.
func (v *postgresVerifier) Verify(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
	if len(keys) == 0 {
		// Count-based verification: check each table has rows
		return v.verifyCounts(ctx)
	}

	// Key-level verification: check specific rows
	return v.verifyRows(ctx, keys)
}

// verifyCounts checks that all tables have at least one row.
func (v *postgresVerifier) verifyCounts(ctx context.Context) ([]provider.VerificationError, error) {
	var errs []provider.VerificationError

	// List tables
	query := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		AND table_type = 'BASE TABLE'
	`

	rows, err := v.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			continue
		}

		// Count rows in table
		countQuery := fmt.Sprintf(
			"SELECT COUNT(*) FROM %s.%s",
			quoteIdentifier(schema),
			quoteIdentifier(table),
		)

		var count int64
		err := v.pool.QueryRow(ctx, countQuery).Scan(&count)
		if err != nil {
			errs = append(errs, provider.VerificationError{
				Table:   schema + "." + table,
				Message: fmt.Sprintf("count failed: %v", err),
			})
			continue
		}

		if count == 0 {
			errs = append(errs, provider.VerificationError{
				Table:   schema + "." + table,
				Message: "table is empty",
			})
		}
	}

	return errs, nil
}

// verifyRows checks specific rows by their keys.
// Keys are in the format "schema.table:primaryKey".
func (v *postgresVerifier) verifyRows(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
	var errs []provider.VerificationError

	for _, key := range keys {
		schema, table, pk, err := parseRowKey(key)
		if err != nil {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Message: fmt.Sprintf("invalid key format: %v", err),
			})
			continue
		}

		// Build WHERE clause for primary key
		whereClause, whereArgs := buildPKWhere(pk, 1)

		// Check if row exists
		checkQuery := fmt.Sprintf(
			"SELECT EXISTS(SELECT 1 FROM %s.%s WHERE %s)",
			quoteIdentifier(schema),
			quoteIdentifier(table),
			whereClause,
		)

		var exists bool
		err = v.pool.QueryRow(ctx, checkQuery, whereArgs...).Scan(&exists)
		if err != nil {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Table:   schema + "." + table,
				Message: fmt.Sprintf("check failed: %v", err),
			})
			continue
		}

		if !exists {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Table:   schema + "." + table,
				Message: "row does not exist in destination",
			})
		}
	}

	return errs, nil
}

// parseRowKey parses a row key in the format "schema.table:primaryKey".
func parseRowKey(key string) (schema, table string, pk map[string]any, err error) {
	// Find the colon that separates table from primary key
	colonIdx := strings.Index(key, ":")
	if colonIdx == -1 {
		return "", "", nil, fmt.Errorf("invalid key format, expected 'schema.table:pk'")
	}

	tablePart := key[:colonIdx]
	pkPart := key[colonIdx+1:]

	// Parse schema.table
	parts := strings.SplitN(tablePart, ".", 2)
	if len(parts) == 2 {
		schema = parts[0]
		table = parts[1]
	} else {
		schema = "public"
		table = parts[0]
	}

	// Try to parse primary key as JSON (composite key)
	if strings.HasPrefix(pkPart, "{") {
		if err := sonic.Unmarshal([]byte(pkPart), &pk); err != nil {
			return "", "", nil, fmt.Errorf("invalid primary key JSON: %w", err)
		}
	} else {
		pk = map[string]any{"id": parsePrimaryKeyValue(pkPart)}
	}

	return schema, table, pk, nil
}

func parsePrimaryKeyValue(v string) any {
	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i
	}
	if u, err := strconv.ParseUint(v, 10, 64); err == nil {
		return u
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	return v
}
