package transform

import (
	"strings"
	"time"
)

// Dialect identifies a SQL dialect for timestamp and type formatting.
type Dialect string

const (
	DialectMySQL       Dialect = "mysql"
	DialectPostgres    Dialect = "postgres"
	DialectSQLite      Dialect = "sqlite"
	DialectMariaDB     Dialect = "mariadb"
	DialectCockroachDB Dialect = "cockroachdb"
	DialectMSSQL       Dialect = "mssql"
	DialectMongoDB     Dialect = "mongodb"
	DialectRedis       Dialect = "redis"
)

// TimestampFormats returns the Go time layout strings used by a dialect.
// The primary format is canonical; alternates are tried during parsing.
func TimestampFormats(d Dialect) (primary string, alternates []string) {
	switch d {
	case DialectMySQL, DialectMariaDB:
		return "2006-01-02 15:04:05", []string{time.RFC3339Nano, "2006-01-02T15:04:05Z07:00"}
	case DialectPostgres, DialectCockroachDB:
		return time.RFC3339Nano, []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05Z07:00"}
	case DialectSQLite:
		return time.RFC3339Nano, []string{"2006-01-02 15:04:05", "2006-01-02T15:04:05Z07:00"}
	case DialectMSSQL:
		return "2006-01-02T15:04:05.9999999Z07:00", []string{time.RFC3339Nano, "2006-01-02 15:04:05"}
	case DialectMongoDB:
		return time.RFC3339Nano, []string{"2006-01-02 15:04:05"}
	default:
		return time.RFC3339Nano, []string{"2006-01-02 15:04:05"}
	}
}

// ConvertTimestamp parses a timestamp string using the source dialect's formats
// and reformats it for the destination dialect. Returns the original string
// unchanged if parsing fails (graceful degradation, no error).
func ConvertTimestamp(value string, srcDialect, dstDialect Dialect) string {
	if value == "" {
		return value
	}

	parsed, ok := parseTimestamp(value, srcDialect)
	if !ok {
		return value
	}

	dstPrimary, _ := TimestampFormats(dstDialect)
	return parsed.Format(dstPrimary)
}

// parseTimestamp tries the source dialect's primary and alternate formats.
func parseTimestamp(value string, dialect Dialect) (time.Time, bool) {
	primary, alternates := TimestampFormats(dialect)

	for _, layout := range append([]string{primary}, alternates...) {
		if t, err := time.Parse(layout, value); err == nil {
			return t, true
		}
	}

	// Try common layouts as a last resort.
	extra := []string{
		time.RFC3339,
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.DateOnly,
	}
	for _, layout := range extra {
		if t, err := time.Parse(layout, value); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

// IsTimestampColumn returns true if the column type looks like a datetime/timestamp type.
func IsTimestampColumn(colType string) bool {
	upper := strings.ToUpper(colType)
	switch {
	case strings.HasPrefix(upper, "DATETIME"),
		strings.HasPrefix(upper, "TIMESTAMP"),
		strings.HasPrefix(upper, "TIMESTAMPTZ"),
		strings.HasPrefix(upper, "SMALLDATETIME"):
		return true
	case upper == "DATE", upper == "TIME":
		return false
	}
	return false
}

// CategorizeProvider returns the general category of a provider.
func CategorizeProvider(provider string) string {
	switch provider {
	case "redis":
		return "redis"
	case "mongodb":
		return "mongodb"
	default:
		return "sql"
	}
}

// NeedsSchemaField returns true if the provider uses a schema namespace (Postgres, CockroachDB).
func NeedsSchemaField(provider string) bool {
	return provider == "postgres" || provider == "cockroachdb"
}
