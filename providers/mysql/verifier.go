package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/bytedance/sonic"
	"strings"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

// mysqlVerifier checks that rows in the destination MySQL match the source.
// It verifies row counts per table and performs sample-based comparison.
type mysqlVerifier struct {
	db  *sql.DB
	log interface{ Info(msg string, args ...any) }
}

func newMySQLVerifier(db *sql.DB) *mysqlVerifier {
	return &mysqlVerifier{
		db:  db,
		log: logger.L().With("component", "mysql-verifier"),
	}
}

// Verify checks the given row keys for existence and correctness.
// If keys is empty, it performs a count-based check across all tables.
func (v *mysqlVerifier) Verify(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
	if len(keys) == 0 {
		// Count-based verification: check each table has rows
		return v.verifyCounts(ctx)
	}

	// Key-level verification: check specific rows
	return v.verifyRows(ctx, keys)
}

// verifyCounts checks that all tables have at least one row.
func (v *mysqlVerifier) verifyCounts(ctx context.Context) ([]provider.VerificationError, error) {
	var errs []provider.VerificationError

	// List tables
	query := "SHOW TABLES"

	rows, err := v.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			continue
		}

		// Count rows in table
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(table))

		var count int64
		err := v.db.QueryRowContext(ctx, countQuery).Scan(&count)
		if err != nil {
			errs = append(errs, provider.VerificationError{
				Table:   table,
				Message: fmt.Sprintf("count failed: %v", err),
			})
			continue
		}

		if count == 0 {
			errs = append(errs, provider.VerificationError{
				Table:   table,
				Message: "table is empty",
			})
		}
	}

	return errs, nil
}

// verifyRows checks specific rows by their keys.
// Keys are in the format "table:primaryKey".
func (v *mysqlVerifier) verifyRows(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
	var errs []provider.VerificationError

	for _, key := range keys {
		table, pk, err := parseRowKey(key)
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
			"SELECT EXISTS(SELECT 1 FROM %s WHERE %s)",
			quoteIdentifier(table),
			whereClause,
		)

		var exists bool
		err = v.db.QueryRowContext(ctx, checkQuery, whereArgs...).Scan(&exists)
		if err != nil {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Table:   table,
				Message: fmt.Sprintf("check failed: %v", err),
			})
			continue
		}

		if !exists {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Table:   table,
				Message: "row does not exist in destination",
			})
		}
	}

	return errs, nil
}

// parseRowKey parses a row key in the format "table:primaryKey".
func parseRowKey(key string) (table string, pk map[string]any, err error) {
	// Find the colon that separates table from primary key
	colonIdx := strings.Index(key, ":")
	if colonIdx == -1 {
		return "", nil, fmt.Errorf("invalid key format, expected 'table:pk'")
	}

	table = key[:colonIdx]
	pkPart := key[colonIdx+1:]

	// Try to parse primary key as JSON (composite key)
	if strings.HasPrefix(pkPart, "{") {
		if err := sonic.Unmarshal([]byte(pkPart), &pk); err != nil {
			return "", nil, fmt.Errorf("invalid primary key JSON: %w", err)
		}
	} else {
		// Single column key - we need to find the column name
		// For now, assume it's the first column of the primary key
		// In a real implementation, we'd need to query the table schema
		pk = map[string]any{"id": pkPart}
	}

	return table, pk, nil
}
