package config

// OverrideFields holds CLI flag values that override provider-specific config fields.
type OverrideFields struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// ApplyOverrides applies non-zero override fields to the appropriate
// provider-specific config within the ConnectionConfig.
func ApplyOverrides(cfg *ConnectionConfig, ov OverrideFields) {
	switch cfg.Provider {
	case "postgres":
		if cfg.Postgres == nil {
			cfg.Postgres = &PostgresConfig{}
		}
		if ov.Host != "" {
			cfg.Postgres.Host = ov.Host
		}
		if ov.Port != 0 {
			cfg.Postgres.Port = ov.Port
		}
		if ov.User != "" {
			cfg.Postgres.Username = ov.User
		}
		if ov.Password != "" {
			cfg.Postgres.Password = ov.Password
		}
		if ov.Database != "" {
			cfg.Postgres.Database = ov.Database
		}
	case "mysql":
		if cfg.MySQL == nil {
			cfg.MySQL = &MySQLConfig{}
		}
		if ov.Host != "" {
			cfg.MySQL.Host = ov.Host
		}
		if ov.Port != 0 {
			cfg.MySQL.Port = ov.Port
		}
		if ov.User != "" {
			cfg.MySQL.Username = ov.User
		}
		if ov.Password != "" {
			cfg.MySQL.Password = ov.Password
		}
		if ov.Database != "" {
			cfg.MySQL.Database = ov.Database
		}
	case "redis":
		if cfg.Redis == nil {
			cfg.Redis = &RedisConfig{}
		}
		if ov.Host != "" {
			cfg.Redis.Host = ov.Host
		}
		if ov.Port != 0 {
			cfg.Redis.Port = ov.Port
		}
		if ov.Password != "" {
			cfg.Redis.Password = ov.Password
		}
	case "mongodb":
		if cfg.MongoDB == nil {
			cfg.MongoDB = &MongoDBConfig{}
		}
		if ov.Host != "" {
			cfg.MongoDB.Host = ov.Host
		}
		if ov.Port != 0 {
			cfg.MongoDB.Port = ov.Port
		}
		if ov.User != "" {
			cfg.MongoDB.Username = ov.User
		}
		if ov.Password != "" {
			cfg.MongoDB.Password = ov.Password
		}
		if ov.Database != "" {
			cfg.MongoDB.Database = ov.Database
		}
	case "sqlite":
		if cfg.SQLite == nil {
			cfg.SQLite = &SQLiteConfig{}
		}
		if ov.Database != "" {
			cfg.SQLite.Path = ov.Database
		}
		if ov.Host != "" {
			cfg.SQLite.Path = ov.Host
		}
	case "mariadb":
		if cfg.MariaDB == nil {
			cfg.MariaDB = &MariaDBConfig{}
		}
		if ov.Host != "" {
			cfg.MariaDB.Host = ov.Host
		}
		if ov.Port != 0 {
			cfg.MariaDB.Port = ov.Port
		}
		if ov.User != "" {
			cfg.MariaDB.Username = ov.User
		}
		if ov.Password != "" {
			cfg.MariaDB.Password = ov.Password
		}
		if ov.Database != "" {
			cfg.MariaDB.Database = ov.Database
		}
	case "cockroachdb":
		if cfg.CockroachDB == nil {
			cfg.CockroachDB = &CockroachDBConfig{}
		}
		if ov.Host != "" {
			cfg.CockroachDB.Host = ov.Host
		}
		if ov.Port != 0 {
			cfg.CockroachDB.Port = ov.Port
		}
		if ov.User != "" {
			cfg.CockroachDB.Username = ov.User
		}
		if ov.Password != "" {
			cfg.CockroachDB.Password = ov.Password
		}
		if ov.Database != "" {
			cfg.CockroachDB.Database = ov.Database
		}
	case "mssql":
		if cfg.MSSQL == nil {
			cfg.MSSQL = &MSSQLConfig{}
		}
		if ov.Host != "" {
			cfg.MSSQL.Host = ov.Host
		}
		if ov.Port != 0 {
			cfg.MSSQL.Port = ov.Port
		}
		if ov.User != "" {
			cfg.MSSQL.Username = ov.User
		}
		if ov.Password != "" {
			cfg.MSSQL.Password = ov.Password
		}
		if ov.Database != "" {
			cfg.MSSQL.Database = ov.Database
		}
	}
}
