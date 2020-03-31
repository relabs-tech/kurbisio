/*Package registry provides a persistent registry of objects in a SQL database

The package uses JSON to serialize the data.
*/
package registry

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/relabs-tech/backends/core/sql"
)

// MustNew creates a new registry for the specified database
func MustNew(db *sql.DB) *Registry {
	_, err := db.Exec(`CREATE table IF NOT EXISTS ` + db.Schema + `."_registry_" 
(key varchar NOT NULL, 
value json NOT NULL, 
created_at timestamp NOT NULL, 
PRIMARY KEY(key)
);`)

	if err != nil {
		panic(err)
	}
	return &Registry{db: db}
}

// Registry provides a persistent registry of objects in a sql database.
type Registry struct {
	db *sql.DB
}

// Accessor is an accessor with optional prefix
type Accessor struct {
	Prefix   string
	Registry *Registry
}

// Accessor returns a registry accessor with prefix
func (r *Registry) Accessor(prefix string) Accessor {
	return Accessor{
		Prefix:   prefix,
		Registry: r,
	}
}

// Read reads a value from the registry. It returns the
// time when the value was written.
//
// If the accessor has a prefix, the key is prepended with "{prefix}:"
func (r *Accessor) Read(key string, value interface{}) (time.Time, error) {
	var (
		rawValue  json.RawMessage
		createdAt time.Time
	)
	if len(r.Prefix) > 0 {
		key = r.Prefix + ":" + key
	}

	err := r.Registry.db.QueryRow(
		`SELECT value, created_at FROM `+r.Registry.db.Schema+`."_registry_" WHERE key=$1;`,
		key).Scan(&rawValue, &createdAt)
	if err == sql.ErrNoRows {
		return createdAt, nil
	}
	if err != nil {
		return createdAt, fmt.Errorf("cannot read key '%s': %s", key, err.Error())
	}
	err = json.Unmarshal(rawValue, &value)

	return createdAt, err
}

// Write writes a value into the registry.
//
// If the accessor has a prefix, the key is prepended with "{prefix}:"
func (r *Accessor) Write(key string, value interface{}) error {

	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if len(r.Prefix) > 0 {
		key = r.Prefix + ":" + key
	}
	now := time.Now().UTC()
	res, err := r.Registry.db.Exec(
		`INSERT INTO `+r.Registry.db.Schema+`."_registry_"(key,value,created_at)
VALUES($1,$2,$3)
ON CONFLICT (key) DO UPDATE SET value=$2,created_at=$3;`,
		key, string(body), now)

	if err != nil {
		return err
	}
	count, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("could not write key %s", key)
	}
	return nil
}
