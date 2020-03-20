package baas

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/joeshaw/envdecode"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

var configurationJSON string = `{
	"resources": [
	  {
		"resource": "a",
		"external_indices": ["external_id"],
		"extra_properties": ["extra_prop"]
	  },
	  {
		"resource": "b"
	  },
	  {
		"resource": "b/c"
	  },
	  {
		"resource": "b/c/d"
	  }

	],
	"relations": [
	]
  }
`

// TestService holds the configuration for this service
//
// use POSTGRES="host=localhost port=5432 user=postgres password=docker dbname=postgres sslmode=disable"
type TestService struct {
	Postgres string `env:"POSTGRES,required" description:"the connection string for the Postgres DB"`
	backend  *Backend
	admin    *Admin
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

	schema := "baas_unit_test"
	db.Exec("drop schema " + schema + " cascade;")

	router := mux.NewRouter()
	testService.backend = MustNewBackend(configurationJSON).WithSchema(schema).Create(db, router)
	testService.admin = testService.backend.Admin()

	code := m.Run()
	os.Exit(code)
}

func asJSON(object interface{}) string {
	j, _ := json.MarshalIndent(object, "", "  ")
	return string(j)
}

type ANew struct {
	Properties map[string]string `json:"properties"`
	ExternalID string            `json:"external_id"`
	ExtraProp  string            `json:"extra_prop"`
	CreatedAt  time.Time         `json:"created_at"`
}

type A struct {
	ANew
	AID uuid.UUID `json:"a_id"`
}

func TestResourceA(t *testing.T) {

	someJSON := map[string]string{
		"foo": "bar",
	}

	aNew := ANew{
		Properties: someJSON,
		ExternalID: "external",
		ExtraProp:  "extra",
		CreatedAt:  time.Now().UTC().Round(time.Millisecond), // round to postgres precision
	}

	a := A{}

	_, err := testService.admin.Post("/as", &aNew, &a)
	if err != nil {
		t.Fatal(err)
	}

	u := uuid.UUID{}
	if a.AID == u {
		t.Fatal("no id")
	}

	fmt.Println(asJSON(aNew.Properties))
	if asJSON(a.Properties) != asJSON(aNew.Properties) ||
		a.ExternalID != aNew.ExternalID ||
		a.ExtraProp != aNew.ExtraProp ||
		a.CreatedAt != aNew.CreatedAt {
		t.Fatal("unexpected result:", asJSON(a), "expected:", asJSON(aNew))
	}

	aGet := A{}
	_, err = testService.admin.Get("/as/"+a.AID.String(), &aGet)
	if err != nil {
		t.Fatal(err)
	}
	if asJSON(a.Properties) != asJSON(aGet.Properties) ||
		a.ExternalID != aGet.ExternalID ||
		a.ExtraProp != aGet.ExtraProp ||
		a.CreatedAt != aGet.CreatedAt {
		t.Fatal("unexpected result:", asJSON(aGet))
	}

	aPut := aGet
	aRes := A{}
	aPut.ExtraProp = "new value for extra"
	_, err = testService.admin.Put("/as", &aPut, &aRes)
	if err != nil {
		t.Fatal(err)
	}
	if asJSON(aPut.Properties) != asJSON(aRes.Properties) ||
		aPut.ExternalID != aRes.ExternalID ||
		aPut.ExtraProp != aRes.ExtraProp ||
		aPut.CreatedAt != aRes.CreatedAt {
		t.Fatal("unexpected result:", asJSON(aGet))
	}

	_, err = testService.admin.Delete("/as/" + a.AID.String())
	if err != nil {
		t.Fatal(err)
	}
	status, err := testService.admin.Get("/as/"+a.AID.String(), &aGet)
	if status != http.StatusNotFound {
		t.Fatal("not deleted")
	}
}
