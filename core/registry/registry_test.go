package registry

import (
	"os"
	"testing"
	"time"

	"github.com/goccy/go-json"

	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/backends/core/csql"

	_ "github.com/lib/pq"
)

// TestService holds the configuration for this service
//
// use POSTGRES="host=localhost port=5432 user=postgres dbname=postgres sslmode=disable"
// and POSTRGRES_PASSWORD="docker"
type TestService struct {
	Postgres         string `env:"POSTGRES,required" description:"the connection string for the Postgres DB without password"`
	PostgresPassword string `env:"POSTGRES_PASSWORD,optional" description:"password to the Postgres DB"`
	registry         Registry
}

var testService TestService

func TestMain(m *testing.M) {
	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_core_registry_unit_test_")
	defer db.Close()
	db.ClearSchema()

	testService.registry = New(db)

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
	timestamp, err := testRegistry.Read("key does not exist", something)
	if err != nil {
		t.Fatal(err)
	}
	if !timestamp.IsZero() {
		t.Fatal("non existing key seems to exist")
	}

	now := time.Now()
	err = testRegistry.Write("test", write)
	if err != nil {
		t.Fatal(err)
	}
	var read foo
	timestamp, err = testRegistry.Read("test", &read)
	if err != nil {
		t.Fatal(err)
	}

	if read.A != write.A || read.B != read.B {
		t.Fatal("could not read what I wrote")
	}
	if timestamp.Sub(now) > time.Second {
		t.Fatal("created at is off")
	}

	// test that we can delete
	testRegistry.Delete("test")
	if err != nil {
		t.Fatal(err)
	}
	timestamp, err = testRegistry.Read("test", something)
	if err != nil {
		t.Fatal(err)
	}
	if !timestamp.IsZero() {
		t.Fatal("Deleted key still exists")
	}

}

func asJSON(object interface{}) string {
	j, _ := json.Marshal(object)
	return string(j)
}
