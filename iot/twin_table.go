package iot

import (
	"database/sql"

	_ "github.com/lib/pq" // for the postgres database
)

// MustCreateTwinTableIfNotExists creates the SQL table for the
// device twin under the requested schema. Use "public" if you do
// not want a custom schema.
//
// The function requires that the database manages a resource "device"
func MustCreateTwinTableIfNotExists(db *sql.DB, schema string) {
	// poor man's database migrations
	_, err := db.Query(
		`CREATE extension IF NOT EXISTS "uuid-ossp";
CREATE table IF NOT EXISTS ` + schema + `.twin 
(device_id uuid references ` + schema + `.device(device_id) ON DELETE CASCADE, 
key varchar NOT NULL, 
request json NOT NULL, 
report json NOT NULL, 
requested_at timestamp NOT NULL, 
reported_at timestamp NOT NULL,
PRIMARY KEY(device_id, key)
);`)

	if err != nil {
		panic(err)
	}

}
