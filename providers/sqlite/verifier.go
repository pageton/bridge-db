//go:build sqlite

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

type sqliteVerifier struct {
	db  *sql.DB
	log interface {
		Info(msg string, args ...any)
		Warn(msg string, args ...any)
	}
}

func newSQLiteVerifier(db *sql.DB) *sqliteVerifier {
	return &sqliteVerifier{
		db:  db,
		log: logger.L().With("component", "sqlite-verifier"),
	}
}

func (v *sqliteVerifier) Verify(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
	if len(keys) == 0 {
		return v.verifyCounts(ctx)
	}
	return v.verifyRows(ctx, keys)
}

func (v *sqliteVerifier) verifyCounts(ctx context.Context) ([]provider.VerificationError, error) {
	var errs []provider.VerificationError

	query := "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
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

		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(table))
		var count int64
		if err := v.db.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
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

func (v *sqliteVerifier) verifyRows(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
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

		var clauses []string
		var args []any
		for col, val := range pk {
			clauses = append(clauses, fmt.Sprintf("%s = ?", quoteIdentifier(col)))
			args = append(args, val)
		}

		checkQuery := fmt.Sprintf(
			"SELECT EXISTS(SELECT 1 FROM %s WHERE %s)",
			quoteIdentifier(table),
			strings.Join(clauses, " AND "),
		)

		var exists bool
		if err := v.db.QueryRowContext(ctx, checkQuery, args...).Scan(&exists); err != nil {
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

func parseRowKey(key string) (table string, pk map[string]any, err error) {
	colonIdx := strings.Index(key, ":")
	if colonIdx == -1 {
		return "", nil, fmt.Errorf("invalid key format, expected 'table:pk'")
	}

	table = key[:colonIdx]
	pkPart := key[colonIdx+1:]

	if strings.HasPrefix(pkPart, "{") {
		if err := sonic.Unmarshal([]byte(pkPart), &pk); err != nil {
			return "", nil, fmt.Errorf("invalid primary key JSON: %w", err)
		}
	} else {
		pk = map[string]any{"id": pkPart}
	}

	return table, pk, nil
}
