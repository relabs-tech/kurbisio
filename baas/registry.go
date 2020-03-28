package baas

import (
	// "context"
	// "crypto/tls"
	// "crypto/x509"
	// "database/sql"

	// "fmt"

	// "log"
	// "net"

	// _ "net/http/pprof"

	// "os"
	// "os/signal"
	// "strings"
	// "sync"
	// "syscall"
	// "time"

	// "github.com/DrmagicE/gmqtt"
	// "github.com/DrmagicE/gmqtt/pkg/packets"

	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq" // load database driver for postgres
)

func (b *Backend) mustInitializeRegistry() {
	_, err := b.db.Exec(
		`CREATE extension IF NOT EXISTS "uuid-ossp";
CREATE schema IF NOT EXISTS ` + b.schema + `;
CREATE table IF NOT EXISTS ` + b.schema + `."_registry_" 
(key varchar NOT NULL, 
value json NOT NULL, 
created_at timestamp NOT NULL, 
PRIMARY KEY(key)
);`)

	if err != nil {
		panic(err)
	}
}

// Registry is a simple JSON registry for reading and writing persisted values
type Registry struct {
	Group   string
	Backend *Backend
}

// NewRegistry return a registry accessor for a group
func (b *Backend) NewRegistry(group string) *Registry {
	return &Registry{
		Group:   group,
		Backend: b,
	}
}

// Read reads a key from the registry
func (r *Registry) Read(key string, value interface{}) (time.Time, error) {
	var (
		rawValue  json.RawMessage
		createdAt time.Time
	)
	fullKey := r.Group + ":" + key
	err := r.Backend.db.QueryRow(
		`SELECT value, created_at FROM `+r.Backend.schema+`."_registry_" WHERE key=$1;`,
		fullKey).Scan(&rawValue, &createdAt)
	if err == sql.ErrNoRows {
		return createdAt, nil
	}
	if err != nil {
		return createdAt, fmt.Errorf("cannot read key '%s': %s", fullKey, err.Error())
	}
	err = json.Unmarshal(rawValue, &value)

	return createdAt, err
}

// Write writes a key into the registry
func (r *Registry) Write(key string, value interface{}) error {

	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	fullKey := r.Group + ":" + key
	now := time.Now().UTC()
	res, err := r.Backend.db.Exec(
		`INSERT INTO `+r.Backend.schema+`."_registry_"(key,value,created_at)
VALUES($1,$2,$3)
ON CONFLICT (key) DO UPDATE SET value=$2,created_at=$3;`,
		fullKey, string(body), now)

	if err != nil {
		return err
	}
	count, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("could not write key %s", fullKey)
	}
	return nil
}
