package backend_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/joeshaw/envdecode"
	_ "github.com/lib/pq"
	"github.com/relabs-tech/backends/core/backend"
	"github.com/relabs-tech/backends/core/client"
	"github.com/relabs-tech/backends/core/csql"
)

func TestRelation(t *testing.T) {
	// Create a relation and verifies that the relation can be listed

	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_backend_relation_unit_test_")
	defer db.Close()
	db.ClearSchema()

	var configurationJSON string = `{
		"collections": [
		  {
			"resource": "a"
		  },
		  {
			"resource": "b"
		  }
		],
		"relations": [
			{
				"left": "a",
				"right": "b"
			}
		]
	  }
	`
	router := mux.NewRouter()
	testService.backend = backend.New(&backend.Builder{
		Config:       configurationJSON,
		DB:           db,
		Router:       router,
		UpdateSchema: true,
	})
	cl := client.NewWithRouter(router)

	type A struct {
		AID       uuid.UUID `json:"a_id"`
		Timestamp time.Time `json:"timestamp,omitempty"`
	}
	type B struct {
		BID       uuid.UUID `json:"b_id"`
		Timestamp time.Time `json:"timestamp,omitempty"`
	}

	// First we create an A and a B
	a := A{AID: uuid.New()}
	_, err := cl.RawPut("/as", &a, &a)
	if err != nil {
		t.Fatal(err)
	}
	b := B{BID: uuid.New()}
	_, err = cl.RawPut("/bs", &b, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Then we create the relation
	_, err = cl.RawPut(fmt.Sprintf("/as/%s/bs/%s", a.AID, b.BID), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// We verify that we can list the relation in both directions
	bs := []B{}
	as := []A{}
	_, err = cl.RawGet(fmt.Sprintf("/as/%s/bs", a.AID), &bs)
	if err != nil {
		t.Fatal(err)
	}
	if len(bs) != 1 {
		t.Fatalf("Execting one relation, got %d", len(bs))
	}
	if bs[0].BID != b.BID {
		t.Fatalf("Execting %s, got %s", b.BID, bs[0].BID)
	}

	_, err = cl.RawGet(fmt.Sprintf("/bs/%s/as", b.BID), &as)
	if err != nil {
		t.Fatal(err)
	}
	if len(as) != 1 {
		t.Fatalf("Execting one relation, got %d", len(as))
	}
	if as[0].AID != a.AID {
		t.Fatalf("Execting %s, got %s", a.AID, as[0].AID)
	}

}

// use POSTGRES="host=localhost port=5432 user=postgres dbname=postgres sslmode=disable"
// and POSTRGRES_PASSWORD="docker"
type TestService struct {
	Postgres         string `env:"POSTGRES,required" description:"the connection string for the Postgres DB without password"`
	PostgresPassword string `env:"POSTGRES_PASSWORD,optional" description:"password to the Postgres DB"`
	backend          *backend.Backend
}

var testService TestService
