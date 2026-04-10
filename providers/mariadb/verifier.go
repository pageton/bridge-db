package mariadb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bytedance/sonic"

	"github.com/pageton/bridge-db/internal/logger"
	"github.com/pageton/bridge-db/pkg/provider"
)

type mariaDBVerifier struct {
	db  *sql.DB
	log interface {
		Info(msg string, args ...any)
		Warn(msg string, args ...any)
	}
}

func newMariaDBVerifier(db *sql.DB) *mariaDBVerifier {
	return &mariaDBVerifier{
		db:  db,
		log: logger.L().With("component", "mariadb-verifier"),
	}
}

func (v *mariaDBVerifier) Verify(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
	if len(keys) == 0 {
		return v.verifyCounts(ctx)
	}
	return v.verifyRows(ctx, keys)
}

func (v *mariaDBVerifier) verifyCounts(ctx context.Context) ([]provider.VerificationError, error) {
	var errs []provider.VerificationError
	rows, err := v.db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			continue
		}
		var count int64
		err := v.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdentifier(table))).Scan(&count)
		if err != nil {
			errs = append(errs, provider.VerificationError{Table: table, Message: fmt.Sprintf("count failed: %v", err)})
			continue
		}
		if count == 0 {
			errs = append(errs, provider.VerificationError{Table: table, Message: "table is empty"})
		}
	}
	return errs, nil
}

func (v *mariaDBVerifier) verifyRows(ctx context.Context, keys []string) ([]provider.VerificationError, error) {
	var errs []provider.VerificationError
	for _, key := range keys {
		table, pk, err := parseRowKey(key)
		if err != nil {
			errs = append(errs, provider.VerificationError{Key: key, Message: fmt.Sprintf("invalid key format: %v", err)})
			continue
		}
		whereClause, whereArgs := buildPKWhere(pk, 1)
		checkQuery := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s)", quoteIdentifier(table), whereClause)
		var exists bool
		err = v.db.QueryRowContext(ctx, checkQuery, whereArgs...).Scan(&exists)
		if err != nil {
			errs = append(errs, provider.VerificationError{Key: key, Table: table, Message: fmt.Sprintf("check failed: %v", err)})
			continue
		}
		if !exists {
			errs = append(errs, provider.VerificationError{Key: key, Table: table, Message: "row does not exist in destination"})
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
