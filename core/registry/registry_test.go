package registry

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/joeshaw/envdecode"

	_ "github.com/lib/pq"
)

// TestService holds the configuration for this service
//
// use POSTGRES="host=localhost port=5432 user=postgres password=docker dbname=postgres sslmode=disable"
type TestService struct {
	Postgres string `env:"POSTGRES,required" description:"the connection string for the Postgres DB"`
	registry *Registry
}

var testService TestService

func TestMain(m *testing.M) {
	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db, err := sql.Open("postgres", testService.Postgres)
	if err != nil {
		panic(err)
	}

	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	schema := "_core_unit_test_"
	db.Exec("drop schema " + schema + " cascade;")

	testService.registry = MustNew(db, schema)

	code := m.Run()
	os.Exit(code)
}

func TestRegistry(t *testing.T) {

	type foo struct {
		A string
		B string
	}

	write := foo{
		A: "Hello",
		B: "World",
	}

	testRegistry := testService.registry.Accessor("_test_")

	// test non-existing key
	var something interface{}
	createdAt, err := testRegistry.Read("key does not exist", something)
	if err != nil {
		t.Fatal(err)
	}
	if !createdAt.IsZero() {
		t.Fatal("non existing key seems to exist")
	}

	now := time.Now()
	err = testRegistry.Write("test", write)
	if err != nil {
		t.Fatal(err)
	}
	var read foo
	createdAt, err = testRegistry.Read("test", &read)
	if err != nil {
		t.Fatal(err)
	}

	if read.A != write.A || read.B != read.B {
		t.Fatal("could not read what I wrote")
	}
	if createdAt.Sub(now) > time.Second {
		t.Fatal("created at is off")
	}

}

func asJSON(object interface{}) string {
	j, _ := json.MarshalIndent(object, "", "  ")
	return string(j)
}
