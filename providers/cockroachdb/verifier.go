package cockroachdb

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

type cockroachDBVerifier struct {
	pool *pgxpool.Pool
	log  interface {
		Info(msg string, args ...any)
		Warn(msg string, args ...any)
	}
}

func newCockroachDBVerifier(pool *pgxpool.Pool) *cockroachDBVerifier {
	return &cockroachDBVerifier{
		pool: pool,
		log:  logger.L().With("component", "cockroachdb-verifier"),
	}
}

func (v *cockroachDBVerifier) Verify(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
	if len(keys) == 0 {
		return v.verifyCounts(ctx)
	}
	return v.verifyRows(ctx, keys)
}

func (v *cockroachDBVerifier) verifyCounts(ctx context.Context) ([]provider.VerificationError, error) {
	var errs []provider.VerificationError

	query := `
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'crdb_internal')
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

func (v *cockroachDBVerifier) verifyRows(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
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

		whereClause, whereArgs := buildPKWhere(pk, 1)

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

func parseRowKey(key string) (schema, table string, pk map[string]any, err error) {
	colonIdx := strings.Index(key, ":")
	if colonIdx == -1 {
		return "", "", nil, fmt.Errorf("invalid key format, expected 'schema.table:pk'")
	}

	tablePart := key[:colonIdx]
	pkPart := key[colonIdx+1:]

	parts := strings.SplitN(tablePart, ".", 2)
	if len(parts) == 2 {
		schema = parts[0]
		table = parts[1]
	} else {
		schema = "public"
		table = parts[0]
	}

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
