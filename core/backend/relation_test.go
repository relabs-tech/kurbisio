package backend_test

import (
	"fmt"
	"net/http"
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
			"resource": "a",
			"permits": [
				{
					"role": "role1",
					"operations": [
						"create",
						"update"
					]
				}
			]	
		  },
		  {
			"resource": "b",
			"permits": [
				{
					"role": "role1",
					"operations": [
						"create",
						"update"
					]
				}
			]
		  }
		],
		"relations": [
			{
				"left": "a",
				"right": "b",
				"left_permits": [
					{
						"role": "role2",
						"operations": [
							"read",
							"create",							
							"list",
							"delete"
						]
					}
				],
				"right_permits": [
					{
						"role": "role2",
						"operations": [
							"read",				
							"list"
						]
					}
				]
			}
		]
	  }
	`
	router := mux.NewRouter()
	testService.backend = backend.New(&backend.Builder{
		Config:               configurationJSON,
		DB:                   db,
		Router:               router,
		UpdateSchema:         true,
		AuthorizationEnabled: true,
	})
	role1Client := client.NewWithRouter(router).WithRole("role1")

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
	_, err := role1Client.RawPut("/as", &a, &a)
	if err != nil {
		t.Fatal(err)
	}
	b := B{BID: uuid.New()}
	_, err = role1Client.RawPut("/bs", &b, nil)
	if err != nil {
		t.Fatal(err)
	}

	role2Client := client.NewWithRouter(router).WithRole("role2")

	// Check that role2 does not allow to create a relation b/a (role2 lacks create on right permits)
	status, _ := role2Client.RawPut(fmt.Sprintf("/bs/%s/as/%s", b.BID, a.AID), nil, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got %v", status)
	}

	// Then we create the relation a/b
	_, err = role2Client.RawPut(fmt.Sprintf("/as/%s/bs/%s", a.AID, b.BID), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// We verify that we can list the relation in both directions
	bs := []B{}
	_, err = role2Client.RawGet(fmt.Sprintf("/as/%s/bs", a.AID), &bs)
	if err != nil {
		t.Fatal(err)
	}
	if len(bs) != 1 {
		t.Fatalf("Execting one relation, got %d", len(bs))
	}
	if bs[0].BID != b.BID {
		t.Fatalf("Execting %s, got %s", b.BID, bs[0].BID)
	}

	as := []A{}
	_, err = role2Client.RawGet(fmt.Sprintf("/bs/%s/as", b.BID), &as)
	if err != nil {
		t.Fatal(err)
	}
	if len(as) != 1 {
		t.Fatalf("Execting one relation, got %d", len(as))
	}
	if as[0].AID != a.AID {
		t.Fatalf("Execting %s, got %s", a.AID, as[0].AID)
	}

	// Check that role2 does not allow to list nor read bs and as
	status, _ = role2Client.RawGet(fmt.Sprintf("/bs/%s", b.BID), nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got %v", status)
	}
	status, _ = role2Client.RawGet(fmt.Sprintf("/as/%s", b.BID), nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got %v", status)
	}
	status, _ = role2Client.RawGet("/bs", nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got %v", status)
	}
	status, _ = role2Client.RawGet("/as", nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got %v", status)
	}

	// Check that role2 does not allow to delete b/a
	status, _ = role2Client.RawDelete(fmt.Sprintf("/bs/%s/as/%s", b.BID, a.AID))
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting unauthorized access, got %v", status)
	}

	// Check that role2 allows to delete a/b
	status, err = role2Client.RawDelete(fmt.Sprintf("/as/%s/bs/%s", a.AID, b.BID))
	if err != nil {
		t.Fatal(err)
	}
	_, err = role2Client.RawGet(fmt.Sprintf("/bs/%s/as", b.BID), &as)
	if err != nil {
		t.Fatal(err)
	}
	if len(as) != 0 {
		t.Fatalf("Execting 0 relation, got %d", len(as))
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
