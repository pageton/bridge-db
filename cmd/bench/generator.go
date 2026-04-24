//go:build sqlite

package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// ---------------------------------------------------------------------------
// Table schemas for generated data
// ---------------------------------------------------------------------------

// tableSchema defines a table to be generated.
type tableSchema struct {
	name    string
	columns []columnDef
	pkCol   string
}

// columnDef defines a single column.
type columnDef struct {
	name     string
	sqlType  string
	nullable bool
	genType  valueGenType // how to generate values
}

type valueGenType int

const (
	genInt       valueGenType = iota // sequential or random int
	genFloat                         // random float
	genBool                          // random bool
	genString                        // variable-length string
	genTimestamp                     // time within a range
	genNullable                      // 15% chance of NULL
	genEmail                         // email-like string
	genJSON                          // small JSON blob
	genEnum                          // one of a fixed set
)

// tableSchemas returns realistic table definitions for the benchmark.
func tableSchemas(wideRows bool) []tableSchema {
	schemas := []tableSchema{
		{
			name: "users",
			columns: []columnDef{
				{name: "id", sqlType: "INTEGER", genType: genInt},
				{name: "username", sqlType: "TEXT", genType: genString},
				{name: "email", sqlType: "TEXT", genType: genEmail},
				{name: "full_name", sqlType: "TEXT", genType: genString},
				{name: "age", sqlType: "INTEGER", genType: genInt},
				{name: "balance", sqlType: "REAL", genType: genFloat},
				{name: "is_active", sqlType: "INTEGER", genType: genBool},
				{name: "created_at", sqlType: "TEXT", genType: genTimestamp},
				{name: "bio", sqlType: "TEXT", nullable: true, genType: genNullable},
				{name: "avatar_url", sqlType: "TEXT", nullable: true, genType: genNullable},
			},
			pkCol: "id",
		},
		{
			name: "orders",
			columns: []columnDef{
				{name: "id", sqlType: "INTEGER", genType: genInt},
				{name: "user_id", sqlType: "INTEGER", genType: genInt},
				{name: "status", sqlType: "TEXT", genType: genEnum},
				{name: "total_amount", sqlType: "REAL", genType: genFloat},
				{name: "item_count", sqlType: "INTEGER", genType: genInt},
				{name: "shipping_address", sqlType: "TEXT", genType: genString},
				{name: "notes", sqlType: "TEXT", nullable: true, genType: genNullable},
				{name: "ordered_at", sqlType: "TEXT", genType: genTimestamp},
				{name: "shipped_at", sqlType: "TEXT", nullable: true, genType: genNullable},
				{name: "metadata", sqlType: "TEXT", genType: genJSON},
			},
			pkCol: "id",
		},
		{
			name: "products",
			columns: []columnDef{
				{name: "id", sqlType: "INTEGER", genType: genInt},
				{name: "sku", sqlType: "TEXT", genType: genString},
				{name: "name", sqlType: "TEXT", genType: genString},
				{name: "description", sqlType: "TEXT", genType: genString},
				{name: "price", sqlType: "REAL", genType: genFloat},
				{name: "stock", sqlType: "INTEGER", genType: genInt},
				{name: "category", sqlType: "TEXT", genType: genEnum},
				{name: "weight_kg", sqlType: "REAL", genType: genFloat},
				{name: "is_available", sqlType: "INTEGER", genType: genBool},
				{name: "created_at", sqlType: "TEXT", genType: genTimestamp},
			},
			pkCol: "id",
		},
	}

	if wideRows {
		// Add wider tables for the large dataset
		schemas = append(schemas, tableSchema{
			name: "audit_log",
			columns: []columnDef{
				{name: "id", sqlType: "INTEGER", genType: genInt},
				{name: "user_id", sqlType: "INTEGER", genType: genInt},
				{name: "action", sqlType: "TEXT", genType: genEnum},
				{name: "resource_type", sqlType: "TEXT", genType: genEnum},
				{name: "resource_id", sqlType: "INTEGER", genType: genInt},
				{name: "ip_address", sqlType: "TEXT", genType: genString},
				{name: "user_agent", sqlType: "TEXT", genType: genString},
				{name: "request_body", sqlType: "TEXT", genType: genJSON},
				{name: "response_code", sqlType: "INTEGER", genType: genInt},
				{name: "duration_ms", sqlType: "INTEGER", genType: genInt},
				{name: "error_message", sqlType: "TEXT", nullable: true, genType: genNullable},
				{name: "created_at", sqlType: "TEXT", genType: genTimestamp},
			},
			pkCol: "id",
		}, tableSchema{
			name: "events",
			columns: []columnDef{
				{name: "id", sqlType: "INTEGER", genType: genInt},
				{name: "event_type", sqlType: "TEXT", genType: genEnum},
				{name: "source", sqlType: "TEXT", genType: genEnum},
				{name: "payload", sqlType: "TEXT", genType: genJSON},
				{name: "correlation_id", sqlType: "TEXT", genType: genString},
				{name: "session_id", sqlType: "TEXT", genType: genString},
				{name: "user_id", sqlType: "INTEGER", nullable: true, genType: genNullable},
				{name: "timestamp", sqlType: "TEXT", genType: genTimestamp},
				{name: "processed", sqlType: "INTEGER", genType: genBool},
				{name: "retry_count", sqlType: "INTEGER", genType: genInt},
			},
			pkCol: "id",
		})
	}

	return schemas
}

// ---------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------

// enumValues provides realistic categorical data.
var (
	orderStatuses = []string{"pending", "confirmed", "shipped", "delivered", "cancelled", "refunded"}
	productCats   = []string{"electronics", "clothing", "books", "home", "sports", "food", "toys", "health"}
	auditActions  = []string{"create", "update", "delete", "login", "logout", "export", "import"}
	resourceTypes = []string{"user", "order", "product", "settings", "report"}
	eventTypes    = []string{"page_view", "click", "purchase", "signup", "error", "api_call", "webhook"}
	eventSources  = []string{"web", "mobile", "api", "cron", "system"}

	firstNames = []string{"James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth", "David", "Susan", "Richard", "Karen", "Joseph", "Nancy", "Thomas", "Lisa", "Charles", "Sarah"}
	lastNames  = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"}
	domains    = []string{"gmail.com", "yahoo.com", "outlook.com", "example.com", "company.io", "test.org"}
)

// Generator creates realistic test data.
type Generator struct {
	rng *rand.Rand
}

// NewGenerator creates a seeded generator.
func NewGenerator(seed int64) *Generator {
	return &Generator{rng: rand.New(rand.NewSource(seed))}
}

// GenerateDatabase creates a SQLite database with realistic data.
func (g *Generator) GenerateDatabase(dbPath string, cfg DatasetSizeConfig) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	// Optimize for bulk inserts.
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("set WAL: %w", err)
	}
	if _, err := db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
		return fmt.Errorf("set synchronous: %w", err)
	}
	if _, err := db.Exec("PRAGMA cache_size=-64000"); err != nil {
		return fmt.Errorf("set cache: %w", err)
	}
	if _, err := db.Exec("PRAGMA temp_store=MEMORY"); err != nil {
		return fmt.Errorf("set temp_store: %w", err)
	}

	schemas := tableSchemas(cfg.WideRows)
	// Limit to cfg.Tables
	if len(schemas) > cfg.Tables {
		schemas = schemas[:cfg.Tables]
	}

	// Create tables
	for _, tbl := range schemas {
		if err := g.createTable(db, tbl); err != nil {
			return fmt.Errorf("create table %s: %w", tbl.name, err)
		}
	}

	// Generate data — distribute records across tables using a skewed distribution
	// to simulate realistic data (not uniform).
	tableWeights := make([]float64, len(schemas))
	for i := range schemas {
		// Skew: first table gets more data, last table gets less
		tableWeights[i] = 1.0 / float64(i+1)
	}
	totalWeight := 0.0
	for _, w := range tableWeights {
		totalWeight += w
	}

	recordsPerTable := make([]int, len(schemas))
	remaining := cfg.Records
	for i, w := range tableWeights {
		if i == len(schemas)-1 {
			recordsPerTable[i] = remaining
		} else {
			recordsPerTable[i] = int(float64(cfg.Records) * w / totalWeight)
			remaining -= recordsPerTable[i]
		}
	}

	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	yearDuration := 365 * 24 * time.Hour

	// Generate data for each table
	for i, tbl := range schemas {
		count := recordsPerTable[i]
		if err := g.generateTableData(db, tbl, count, baseTime, yearDuration); err != nil {
			return fmt.Errorf("generate data for %s: %w", tbl.name, err)
		}
	}

	return nil
}

func (g *Generator) createTable(db *sql.DB, tbl tableSchema) error {
	var colDefs []string
	for _, col := range tbl.columns {
		def := fmt.Sprintf(`"%s" %s`, col.name, col.sqlType)
		if col.name == tbl.pkCol {
			def += " PRIMARY KEY"
		}
		if !col.nullable && col.name != tbl.pkCol {
			def += " NOT NULL"
		}
		colDefs = append(colDefs, def)
	}

	query := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS "%s" (%s)`,
		tbl.name,
		strings.Join(colDefs, ", "),
	)

	_, err := db.Exec(query)
	return err
}

func (g *Generator) generateTableData(db *sql.DB, tbl tableSchema, count int, baseTime time.Time, timeRange time.Duration) error {
	if count == 0 {
		return nil
	}

	// Build INSERT statement with placeholders
	colNames := make([]string, len(tbl.columns))
	placeholders := make([]string, len(tbl.columns))
	for i, col := range tbl.columns {
		colNames[i] = fmt.Sprintf(`"%s"`, col.name)
		placeholders[i] = "?"
	}

	query := fmt.Sprintf(
		`INSERT INTO "%s" (%s) VALUES (%s)`,
		tbl.name,
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
	)

	// Use transaction for speed
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for i := 0; i < count; i++ {
		args := make([]any, len(tbl.columns))
		for j, col := range tbl.columns {
			args[j] = g.generateValue(col, tbl.pkCol, i, baseTime, timeRange)
		}

		if _, err := stmt.Exec(args...); err != nil {
			return fmt.Errorf("insert row %d: %w", i, err)
		}
	}

	return tx.Commit()
}

func (g *Generator) generateValue(col columnDef, pkCol string, rowIdx int, baseTime time.Time, timeRange time.Duration) any {
	switch col.genType {
	case genInt:
		// Primary key column: always sequential to avoid UNIQUE constraint failures
		if col.name == pkCol {
			return rowIdx + 1
		}
		// Skewed distribution: most values small, some large
		if g.rng.Float64() < 0.1 {
			return g.rng.Intn(1_000_000)
		}
		return rowIdx + 1

	case genFloat:
		// Prices/balances: realistic range
		if g.rng.Float64() < 0.05 {
			return g.rng.Float64() * 100000 // occasional large values
		}
		return g.rng.Float64() * 1000

	case genBool:
		// 80% true, 20% false (skewed)
		if g.rng.Float64() < 0.8 {
			return 1
		}
		return 0

	case genString:
		return g.randomString(g.rng.Intn(50) + 10) // 10-60 chars

	case genEmail:
		return strings.ToLower(g.randomString(g.rng.Intn(8)+4)) +
			"." + strings.ToLower(g.randomString(g.rng.Intn(6)+3)) +
			"@" + domains[g.rng.Intn(len(domains))]

	case genTimestamp:
		offset := time.Duration(g.rng.Int63n(int64(timeRange)))
		return baseTime.Add(offset).UTC().Format(time.RFC3339)

	case genNullable:
		// 15% NULL, 85% value
		if g.rng.Float64() < 0.15 {
			return nil
		}
		if strings.Contains(strings.ToUpper(col.sqlType), "INT") {
			// Preserve integer affinity for nullable numeric columns so stricter
			// destinations like Postgres accept benchmark data seeded via SQLite.
			if g.rng.Float64() < 0.1 {
				return g.rng.Intn(1_000_000)
			}
			return rowIdx + 1
		}
		return g.randomString(g.rng.Intn(100) + 10)

	case genJSON:
		// Small realistic JSON payload
		return fmt.Sprintf(
			`{"key":"%s","value":%d,"tags":["%s","%s"]}`,
			g.randomString(8),
			g.rng.Intn(1000),
			g.randomString(5),
			g.randomString(5),
		)

	case genEnum:
		// Table-name-aware enum selection
		switch col.name {
		case "status":
			return orderStatuses[g.rng.Intn(len(orderStatuses))]
		case "category":
			return productCats[g.rng.Intn(len(productCats))]
		case "action":
			return auditActions[g.rng.Intn(len(auditActions))]
		case "resource_type":
			return resourceTypes[g.rng.Intn(len(resourceTypes))]
		case "event_type":
			return eventTypes[g.rng.Intn(len(eventTypes))]
		case "source":
			return eventSources[g.rng.Intn(len(eventSources))]
		default:
			return g.randomString(10)
		}

	default:
		return g.randomString(10)
	}
}

// randomString generates a random alphanumeric string of length n.
func (g *Generator) randomString(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[g.rng.Intn(len(charset))]
	}
	return string(b)
}

// randomName generates a realistic person name.
func (g *Generator) randomName() string {
	return firstNames[g.rng.Intn(len(firstNames))] + " " +
		lastNames[g.rng.Intn(len(lastNames))]
}
