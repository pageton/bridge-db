//go:build mssql

package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

type mssqlVerifier struct {
	db  *sql.DB
	log interface{ Info(msg string, args ...any) }
}

func newMSSQLVerifier(db *sql.DB) *mssqlVerifier {
	return &mssqlVerifier{
		db:  db,
		log: logger.L().With("component", "mssql-verifier"),
	}
}

func (v *mssqlVerifier) Verify(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
	if len(keys) == 0 {
		return v.verifyCounts(ctx)
	}
	return v.verifyRows(ctx, keys)
}

func (v *mssqlVerifier) verifyCounts(ctx context.Context) ([]provider.VerificationError, error) {
	var errs []provider.VerificationError

	query := "SELECT [name] FROM sys.tables WHERE [type] = 'U' ORDER BY [name]"

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

func (v *mssqlVerifier) verifyRows(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
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

		whereClauses := make([]string, 0, len(pk))
		whereArgs := make([]any, 0, len(pk))
		for col, val := range pk {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = @p%d", quoteIdentifier(col), len(whereArgs)+1))
			whereArgs = append(whereArgs, val)
		}

		checkQuery := fmt.Sprintf(
			"SELECT CASE WHEN EXISTS(SELECT 1 FROM %s WHERE %s) THEN 1 ELSE 0 END",
			quoteIdentifier(table),
			strings.Join(whereClauses, " AND "),
		)

		var exists int
		err = v.db.QueryRowContext(ctx, checkQuery, whereArgs...).Scan(&exists)
		if err != nil {
			errs = append(errs, provider.VerificationError{
				Key:     key,
				Table:   table,
				Message: fmt.Sprintf("check failed: %v", err),
			})
			continue
		}

		if exists == 0 {
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
