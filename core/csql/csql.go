package csql

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq" // load database driver for postgres
	"github.com/relabs-tech/backends/core/logger"
)

// DB encapsulates a standard sql.DB with a schema
type DB struct {
	*sql.DB
	Schema string
}

// ErrNoRows is returned by Scan when QueryRow doesn't return a
// row. In such a case, QueryRow returns a placeholder *Row value that
// defers this error until a Scan.
var ErrNoRows = sql.ErrNoRows

// OpenWithSchema opens a kurbisio postgres database with a schema.
// The schema gets created if it does not exist yet.
// The returned database also has the uuid-ossp extension loaded.
func OpenWithSchema(dataSourceName, dataSourcePassword, schema string) *DB {
	logger.Default().Infoln("connecting to postgres database: ", dataSourceName)
	db, err := sql.Open("postgres", fmt.Sprintf("%s password=%s", dataSourceName, dataSourcePassword))
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err)
	}
	if len(schema) == 0 {
		schema = "public"
	} else {
		logger.Default().Infoln("selected database schema:", schema)
		_, err = db.Exec(`CREATE extension IF NOT EXISTS "uuid-ossp";`)
		if err != nil {
			if strings.Contains(err.Error(), "duplicate key value violates unique constraint \"pg_extension_name_index\"") {
				logger.Default().Error("installing uuid-ossp extension failed, this should not happen except in CI")
			} else {
				panic(err)
			}
		}

		_, err = db.Exec(`CREATE schema IF NOT EXISTS ` + schema + `;`)
		if err != nil {
			panic(err)
		}
	}
	return &DB{DB: db, Schema: schema}
}

// ClearSchema clears all the data contained in the database's schema
// Technically this is done by dropping the schema and then recreating it
func (db *DB) ClearSchema() {
	if db.Schema == "public" {
		panic("refuse to drop public schema")
	}
	_, err := db.Exec(`DROP SCHEMA ` + db.Schema + ` CASCADE;
	CREATE schema IF NOT EXISTS ` + db.Schema + `;`)
	if err != nil {
		logger.Default().Infoln("clear schema error:", db.Schema, err.Error())
	}
}
