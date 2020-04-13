package csql

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq" // load database driver for postgres
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
func OpenWithSchema(dataSourceName, schema string) *DB {
	log.Println("connecting to postgres database: ", dataSourceName)
	db, err := sql.Open("postgres", dataSourceName)
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
		log.Println("selected database schema:", schema)
		_, err = db.Exec(`CREATE extension IF NOT EXISTS "uuid-ossp";
CREATE schema IF NOT EXISTS ` + schema + `;
`)
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
		log.Println("clear schema error:", db.Schema, err.Error())
	}
}
