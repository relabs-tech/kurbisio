package client_test

import (
	"encoding/json"
	"net/url"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/joeshaw/envdecode"
	_ "github.com/lib/pq"
	"github.com/relabs-tech/backends/core/backend"
	"github.com/relabs-tech/backends/core/client"
	"github.com/relabs-tech/backends/core/csql"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func TestCient(t *testing.T) {

	client := client.NewWithRouter(nil)

	parentID := uuid.MustParse("4f1638da-861e-4a81-8cc7-e6847b6fdf9b")
	childID := uuid.MustParse("c46da255-eb72-4cc6-8835-1b34a9917826")

	collection := client.Collection("parent/child")
	if p := collection.CollectionPath(); p != "/parents/all/children" {
		t.Fatal("unexpected collection path:", p)
	}

	item := collection.Item(childID)
	if p := item.Path(); p != "/parents/all/children/"+childID.String() {
		t.Fatal("unexpected item path:", p)
	}

	collection = client.Collection("parent/child").WithParent(parentID)
	if p := collection.CollectionPath(); p != "/parents/"+parentID.String()+"/children" {
		t.Fatal("unexpected collection path:", p)
	}

	item = collection.Singleton()
	if p := item.Path(); p != "/parents/"+parentID.String()+"/child" {
		t.Fatal("unexpected item path:", p)
	}

	collection = client.Collection("parent/child").WithFilter("email", "maybe@yes.no").WithParameter("something", "else")
	if p := collection.CollectionPath(); p != "/parents/all/children?filter="+url.QueryEscape("email=maybe@yes.no")+"&something=else" {
		t.Fatal("unexpected collection path:", p)
	}

	// filter really is a only a shortcut for WithParameter
	collection = client.Collection("parent/child").WithParameter("filter", "email=maybe@yes.no").WithParameter("something", "else")
	if p := collection.CollectionPath(); p != "/parents/all/children?filter="+url.QueryEscape("email=maybe@yes.no")+"&something=else" {
		t.Fatal("unexpected collection path:", p)
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

func asJSON(object interface{}) string {
	j, _ := json.Marshal(object)
	return string(j)
}

func TestUpsert(t *testing.T) {
	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_client_unit_test_")
	defer db.Close()
	db.ClearSchema()

	var configurationJSON string = `{
		"collections": [
		  {
			"resource": "aaa"
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
		AID      uuid.UUID `json:"aaa_id"`
		Revision *int64    `json:"revision,omitempty"`
		Foo      string    `json:"foo"`
	}
	var a A
	_, err := cl.Collection("aaa").Create(&a, &a)
	if err != nil {
		t.Fatal(err)
	}
	aOld := a
	a.Foo = "blablabla"
	var result A
	_, err = cl.Collection("aaa").Item(a.AID).Upsert(&a, &result)
	if err != nil {
		t.Fatal(err)
	}
	if result.Foo != "blablabla" {
		t.Fatalf("Expecting blablabla, got %s", result.Foo)
	}

	_, err = cl.Collection("aaa").Upsert(&aOld, &result)
	if err == nil {
		t.Fatalf("Expecting conflict")
	}
	if result.Foo != "blablabla" {
		t.Fatalf("Expecting blablabla, got %s. This means that we were not given back the conflicting object from the DB", result.Foo)
	}

}
