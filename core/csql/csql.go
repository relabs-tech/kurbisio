// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package csql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/relabs-tech/kurbisio/core/logger"
)

// DBConfig holds the configuration for the database
type DBConfig struct {
	PostgresPassword string `env:"POSTGRES_PASSWORD,default=docker" description:"password to the Postgres DB"`
	PostgresHOST     string `env:"POSTGRES_HOST,default=localhost" description:"URL of the DB"`
	PostgresUser     string `env:"POSTGRES_USER,default=postgres" description:"user of the DB"`
	PostgresDBName   string `env:"POSTGRES_DBNAME,default=postgres" description:"database name"`
	PostgresPort     string `env:"POSTGRES_PORT,default=5432" description:"database port"`
	PostgresSSLMode  string `env:"POSTGRES_SSLMODE,default=disable" description:"sslmode status for timescale (disable, enable)"`
}

// PostgresConfig composes the configuration string for connecting to the Postgres DB
func (s DBConfig) PostgresConfigString() string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=%s&search_path=%s,public",
		s.PostgresUser, s.PostgresPassword, s.PostgresHOST, s.PostgresPort, s.PostgresDBName, s.PostgresSSLMode, "public")
}

// DB encapsulates a pgxpool.Pool with a schema
type DB struct {
	*pgxpool.Pool
	Schema string
}

// ErrNoRows is returned by Scan when QueryRow doesn't return a
// row. In such a case, QueryRow returns a placeholder *Row value that
// defers this error until a Scan.
var ErrNoRows = pgx.ErrNoRows

// PoolConfig contains configuration options for the connection pool
type PoolConfig struct {
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
}

// DefaultPoolConfig returns default pool configuration
func DefaultPoolConfig() *PoolConfig {
	return &PoolConfig{
		MaxConns:        5,
		MinConns:        0,
		MaxConnLifetime: 15 * time.Minute,
		MaxConnIdleTime: 5 * time.Minute,
	}
}

// OpenWithSchema opens a kurbisio postgres database with a schema.
// The schema gets created if it does not exist yet.
// The returned database also has the uuid-ossp extension loaded.
// Connection string format: postgresql://user:password@host:port/dbname?sslmode=disable
func OpenWithSchema(dataSourceName, schema string) *DB {

	return OpenWithSchemaAndConfig(dataSourceName, schema, nil)
}

// OpenWithSchemaAndConfig opens a kurbisio postgres database with a schema and custom pool config.
// The schema gets created if it does not exist yet.
// The returned database also has the uuid-ossp extension loaded.
// Connection string format: postgresql://user:password@host:port/dbname?sslmode=disable
func OpenWithSchemaAndConfig(connString, schema string, poolConfig *PoolConfig) *DB {
	logger.Default().Infoln("connecting to postgres database with pgx")

	ctx := context.Background()

	if poolConfig == nil {
		poolConfig = DefaultPoolConfig()
	}

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		panic(err)
	}

	config.MaxConns = poolConfig.MaxConns
	config.MinConns = poolConfig.MinConns
	config.MaxConnLifetime = poolConfig.MaxConnLifetime
	config.MaxConnIdleTime = poolConfig.MaxConnIdleTime

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		panic(err)
	}

	err = pool.Ping(ctx)
	if err != nil {
		panic(err)
	}

	if len(schema) == 0 {
		schema = "public"
	} else {
		logger.Default().Infoln("selected database schema:", schema)
		_, err = pool.Exec(ctx, `CREATE extension IF NOT EXISTS "uuid-ossp";`)
		if err != nil {
			if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"pg_extension_name_index\"") {
				logger.Default().Error("installing uuid-ossp extension failed, this should not happen except in CI")
			} else {
				panic(err)
			}
		}

		_, err = pool.Exec(ctx, `CREATE schema IF NOT EXISTS `+schema+`;`)
		if err != nil {
			panic(err)
		}
	}
	return &DB{Pool: pool, Schema: schema}
}

// ClearSchema clears all the data contained in the database's schema
// Technically this is done by dropping the schema and then recreating it
func (db *DB) ClearSchema() {
	if db.Schema == "public" {
		panic("refuse to drop public schema")
	}
	ctx := context.Background()
	_, err := db.Exec(ctx, `DROP SCHEMA `+db.Schema+` CASCADE;
	CREATE schema IF NOT EXISTS `+db.Schema+`;`)
	if err != nil {
		logger.Default().Infoln("clear schema error:", db.Schema, err.Error())
	}
}
