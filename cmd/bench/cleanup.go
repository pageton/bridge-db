package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pageton/bridge-db/internal/config"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var benchmarkTables = []string{"users", "orders", "products", "audit_log", "events"}

func cleanupEndpoint(ctx context.Context, ep ProviderEndpoint) error {
	if ep.IsSQLite() || ep.URL == "" {
		return nil
	}

	cc := config.ConnectionConfig{Provider: ep.Provider, URL: ep.URL}
	if err := cc.Resolve(); err != nil {
		return fmt.Errorf("resolve %s cleanup config: %w", ep.Provider, err)
	}

	switch ep.Provider {
	case "postgres":
		cfg := cc.Postgres
		pool, err := pgxpool.New(ctx, cfg.DSNWithPassword())
		if err != nil {
			return err
		}
		defer pool.Close()
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS public CASCADE"); err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, "CREATE SCHEMA public"); err != nil {
			return err
		}
	case "cockroachdb":
		cfg := cc.CockroachDB
		pool, err := pgxpool.New(ctx, cfg.DSNWithPassword())
		if err != nil {
			return err
		}
		defer pool.Close()
		rows, err := pool.Query(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
		if err != nil {
			return err
		}
		var tables []string
		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err == nil {
				tables = append(tables, t)
			}
		}
		rows.Close()
		for _, t := range tables {
			if _, err := pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS public.%s CASCADE", t)); err != nil {
				return err
			}
		}
		if err := waitForCockroachTablesDropped(ctx, pool, tables); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	case "mysql":
		cfg := cc.MySQL
		return cleanupSQL(ctx, "mysql", cfg.DSNWithPassword(), nil)
	case "mariadb":
		cfg := cc.MariaDB
		return cleanupSQL(ctx, "mysql", cfg.DSNWithPassword(), nil)
	case "mssql":
		cfg := cc.MSSQL
		return cleanupMSSQL(ctx, cfg.DSNWithPassword(), nil)
	case "mongodb":
		client, err := mongo.Connect(options.Client().ApplyURI(ep.URL))
		if err != nil {
			return err
		}
		defer client.Disconnect(ctx)
		dbName := cc.MongoDB.Database
		db := client.Database(dbName)
		for _, c := range benchmarkTables {
			_ = db.Collection(c).Drop(ctx)
		}
	case "redis":
		client, err := redis.ParseURL(ep.URL)
		if err != nil {
			return err
		}
		rdb := redis.NewClient(client)
		defer rdb.Close()
		if err := rdb.FlushDB(ctx).Err(); err != nil {
			return err
		}
	}

	return nil
}

func cleanupSQL(ctx context.Context, driverName, dsn string, tables []string) error {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetConnMaxLifetime(30 * time.Second)
	if driverName == "mysql" {
		if _, err := db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
			return err
		}
		defer db.ExecContext(context.Background(), "SET FOREIGN_KEY_CHECKS = 1")
	}
	if len(tables) == 0 {
		rows, err := db.QueryContext(ctx, "SHOW TABLES")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var t string
			if err := rows.Scan(&t); err == nil {
				tables = append(tables, t)
			}
		}
	}
	for _, t := range tables {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", t)); err != nil {
			return err
		}
	}
	return nil
}

func cleanupMSSQL(ctx context.Context, dsn string, tables []string) error {
	var lastErr error
	for i := 0; i < 3; i++ {
		db, err := sql.Open("sqlserver", dsn)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}

		func() {
			defer db.Close()

			localTables := append([]string(nil), tables...)
			if len(localTables) == 0 {
				rows, err := db.QueryContext(ctx, `
					SELECT TABLE_NAME
					FROM INFORMATION_SCHEMA.TABLES
					WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
				`)
				if err != nil {
					lastErr = err
					return
				}
				defer rows.Close()
				for rows.Next() {
					var t string
					if err := rows.Scan(&t); err == nil {
						localTables = append(localTables, t)
					}
				}
				if err := rows.Err(); err != nil {
					lastErr = err
					return
				}
			}
			for _, t := range localTables {
				q := fmt.Sprintf("IF OBJECT_ID('dbo.%s', 'U') IS NOT NULL DROP TABLE dbo.%s", t, t)
				if _, err := db.ExecContext(ctx, q); err != nil {
					lastErr = err
					return
				}
			}
			lastErr = nil
		}()

		if lastErr == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return lastErr
}

func waitForCockroachTablesDropped(ctx context.Context, pool *pgxpool.Pool, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		remaining := 0
		for _, t := range tables {
			var count int
			err := pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1", t).Scan(&count)
			if err != nil {
				return err
			}
			remaining += count
		}
		if remaining == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cockroach cleanup timeout: %d tables still visible", remaining)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
