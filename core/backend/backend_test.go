package backend

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/backends/core/access"

	"github.com/relabs-tech/backends/core/client"
	"github.com/relabs-tech/backends/core/sql"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

var configurationJSON string = `{
	"collections": [
	  {
		"resource": "a",
		"external_index": "external_id",
		"static_properties": ["static_prop"],
		"searchable_properties": ["searchable_prop"]
	  },
	  {
		"resource": "b",
		"logged_in_routes": true
	  },
	  {
		"resource": "b/c"
	  },
	  {
		"resource": "b/c/d"
	  },
	  {
		"resource": "o"
	  },
	  {
		"resource": "zero_time"
	  }
	],
	"singletons": [
	  {
		"resource": "o/s"
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
	client   client.Client
}

var testService TestService

func TestMain(m *testing.M) {
	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := sql.MustOpenWithSchema(testService.Postgres, "_core_unit_test_")
	defer db.Close()
	db.ClearSchema()

	router := mux.NewRouter()
	testService.backend = MustNew(&Builder{
		Config: configurationJSON,
		DB:     db,
		Router: router,
	})
	testService.client = client.New(router)

	code := m.Run()
	os.Exit(code)
}

func TestCollectionA(t *testing.T) {

	type ANew struct {
		Properties     map[string]string `json:"properties"`
		ExternalID     string            `json:"external_id"`
		StaticProp     string            `json:"static_prop"`
		SearchableProp string            `json:"searchable_prop"`
		CreatedAt      time.Time         `json:"created_at"`
	}

	type A struct {
		ANew
		AID uuid.UUID `json:"a_id"`
	}
	someJSON := map[string]string{
		"foo": "bar",
	}

	aNew := ANew{
		Properties:     someJSON,
		ExternalID:     "external",
		StaticProp:     "static",
		SearchableProp: "searchable",
		CreatedAt:      time.Now().UTC().Round(time.Millisecond), // round to postgres precision
	}

	a := A{}

	_, err := testService.client.Post("/as", &aNew, &a)
	if err != nil {
		t.Fatal(err)
	}

	u := uuid.UUID{}
	if a.AID == u {
		t.Fatal("no id")
	}

	if asJSON(a.Properties) != asJSON(aNew.Properties) ||
		a.ExternalID != aNew.ExternalID ||
		a.StaticProp != aNew.StaticProp ||
		a.SearchableProp != aNew.SearchableProp ||
		a.CreatedAt != aNew.CreatedAt {
		t.Fatal("unexpected result:", asJSON(a), "expected:", asJSON(aNew))
	}

	aGet := A{}
	_, err = testService.client.Get("/as/"+a.AID.String(), &aGet)
	if err != nil {
		t.Fatal(err)
	}
	if asJSON(a.Properties) != asJSON(aGet.Properties) ||
		a.ExternalID != aGet.ExternalID ||
		a.StaticProp != aGet.StaticProp ||
		a.CreatedAt != aGet.CreatedAt {
		t.Fatal("unexpected result:", asJSON(aGet))
	}

	aPut := aGet
	aRes := A{}
	aPut.StaticProp = "new value for static property"
	_, err = testService.client.Put("/as", &aPut, &aRes)
	if err != nil {
		t.Fatal(err)
	}
	if asJSON(aPut.Properties) != asJSON(aRes.Properties) ||
		aPut.ExternalID != aRes.ExternalID ||
		aPut.StaticProp != aRes.StaticProp ||
		aPut.CreatedAt != aRes.CreatedAt {
		t.Fatal("unexpected result:", asJSON(aGet))
	}

	_, err = testService.client.Delete("/as/" + a.AID.String())
	if err != nil {
		t.Fatal(err)
	}
	status, err := testService.client.Get("/as/"+a.AID.String(), &aGet)
	if status != http.StatusNotFound {
		t.Fatal("not deleted")
	}
}

type Empty struct{}

type B struct {
	BID uuid.UUID `json:"b_id"`
}

type C struct {
	B
	CID uuid.UUID `json:"c_id"`
}

type D struct {
	C
	DID uuid.UUID `json:"d_id"`
}

func TestResourceBCD(t *testing.T) {

	empty := Empty{}
	b := B{}

	_, err := testService.client.Post("/bs", &empty, &b)
	if err != nil {
		t.Fatal(err)
	}

	c := C{}
	_, err = testService.client.Post("/bs/"+b.BID.String()+"/cs", &empty, &c)
	if err != nil {
		t.Fatal(err)
	}

	d := D{}
	_, err = testService.client.Post("/bs/"+b.BID.String()+"/cs/"+c.CID.String()+"/ds", &empty, &d)
	if err != nil {
		t.Fatal(err)
	}

	if d.BID != b.BID || d.CID != c.CID {
		t.Fatal("properties do not match:", asJSON(d))
	}

	// delete the root object b, this should cascade to all child objects
	status, err := testService.client.Delete("/bs/" + b.BID.String())
	if status != http.StatusNoContent {
		t.Fatal("delete failed")
	}
	if err != nil {
		t.Fatal(err)
	}
	// cross check that the cascade worked: deleting b has also deleted c and d
	dGet := D{}
	status, err = testService.client.Get("/bs/"+b.BID.String()+"/cs/"+c.CID.String()+"/ds/"+d.DID.String(), &dGet)
	if status != http.StatusNotFound {
		t.Fatal("cascade delete failed")
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestResourceBCD_LoggedInRoutes(t *testing.T) {

	empty := Empty{}
	b := B{}

	_, err := testService.client.Post("/bs", &empty, &b)
	if err != nil {
		t.Fatal(err)
	}

	auth := access.Authorization{
		Resources: map[string]uuid.UUID{"b_id": b.BID},
	}

	loggedInClient := testService.client.WithAuthorization(&auth)

	bl := B{}
	_, err = loggedInClient.Get("/b", &bl)
	if err != nil {
		t.Fatal(err)
	}
	if bl.BID != b.BID {
		t.Fatal("id does not match:", asJSON(bl))
	}

	c := C{}
	_, err = loggedInClient.Post("/b/cs", &empty, &c)
	if err != nil {
		t.Fatal(err)
	}

	d := D{}
	_, err = loggedInClient.Post("/b/cs/"+c.CID.String()+"/ds", &empty, &d)
	if err != nil {
		t.Fatal(err)
	}

	if d.BID != b.BID || d.CID != c.CID {
		t.Fatal("properties do not match:", asJSON(d))
	}

	// delete the root object b, this should cascade to all child objects
	status, err := loggedInClient.Delete("/b")
	if status != http.StatusNoContent {
		t.Fatal("delete failed")
	}
	if err != nil {
		t.Fatal(err)
	}
	// cross check that the cascade worked: deleting b has also deleted c and d
	dGet := D{}
	status, err = testService.client.Get("/bs/"+b.BID.String()+"/cs/"+c.CID.String()+"/ds/"+d.DID.String(), &dGet)
	if status != http.StatusNotFound {
		t.Fatal("cascade delete failed")
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestSingletonOS(t *testing.T) {
	type O struct {
		OID uuid.UUID `json:"o_id"`
	}

	type S struct {
		SID        uuid.UUID         `json:"s_id"`
		Properties map[string]string `json:"properties"`
	}

	empty := Empty{}

	// create o instance
	o := O{}
	_, err := testService.client.Post("/os", &empty, &o)
	if err != nil {
		t.Fatal(err)
	}

	// create single s with initial name
	s := S{
		Properties: map[string]string{
			"name": "initial",
		},
	}
	sResult := S{}
	_, err = testService.client.Put("/os/"+o.OID.String()+"/s", &s, &sResult)
	if err != nil {
		t.Fatal(err)
	}

	if name, ok := sResult.Properties["name"]; !ok || name != "initial" {
		t.Fatal("properties not as expected:", asJSON(sResult))
	}

	// update single s to have updated name, the object's id (sid) remains the same
	sUpdate := S{
		Properties: map[string]string{
			"name": "updated",
		},
	}
	sUpdateResult := S{}

	status, err := testService.client.Put("/os/"+o.OID.String()+"/s", &sUpdate, &sUpdateResult)
	if err != nil {
		t.Fatal(err)
	}
	if name, ok := sUpdateResult.Properties["name"]; !ok || name != "updated" {
		t.Fatal("properties not as expected:", asJSON(sUpdateResult))
	}
	if sUpdateResult.SID != sResult.SID {
		t.Fatal("got a new object, should have gotten the same object")
	}

	oldUID := sResult.SID
	newUID := uuid.New()

	// put another update to s and try to give it a new id. This will ingore the new
	// uid and simply update the rest of the object
	sUpdate.SID = newUID
	status, err = testService.client.Put("/os/"+o.OID.String()+"/s", &sUpdate, &sUpdateResult)
	if err != nil {
		t.Fatal(err)
	}

	if sUpdateResult.SID != oldUID {
		t.Fatal("singleton id changed")
	}

	// delete single s
	status, err = testService.client.Delete("/os/" + o.OID.String() + "/s")
	if status != http.StatusNoContent {
		t.Fatal("delete failed")
	}
	if err != nil {
		t.Fatal(err)
	}

	// cross check that the delete worked
	sGet := S{}
	status, err = testService.client.Get("/os/"+o.OID.String()+"/s", &sGet)
	if status != http.StatusNoContent {
		t.Fatal("delete failed")
		if err != nil {
			t.Fatal(err)
		}
	}

	// re-create single s with new uuid, now the sid should be the new one we set
	sResult2 := S{}

	s.SID = newUID
	_, err = testService.client.Put("/os/"+o.OID.String()+"/s", &s, &sResult2)
	if err != nil {
		t.Fatal(err)
	}
	if sResult2.SID == sResult.SID {
		t.Fatal("recreation did not work, still same ID")
	}
	if sResult2.SID != newUID {
		t.Fatal("recreation did not work, could not choose ID")
	}

	// delete the owner o, this should also delete the single s
	status, err = testService.client.Delete("/os/" + o.OID.String())
	if status != http.StatusNoContent {
		t.Fatal("delete failed")
	}
	if err != nil {
		t.Fatal(err)
	}

	// cross check that the cascade worked: deleting o has also deleted s
	status, err = testService.client.Get("/os/"+o.OID.String()+"/s", &sGet)
	if status != http.StatusNoContent {
		t.Fatal("cascade delete failed")
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestZeroTimeAndNullID(t *testing.T) {

	type ZeroTime struct {
		ZeroTimeID uuid.UUID `json:"zero_time_id"`
		CreatedAt  time.Time `json:"created_at"`
	}

	zNew := ZeroTime{}
	var z ZeroTime
	_, err := testService.client.Post("/zero_times", &zNew, &z)
	if err != nil {
		t.Fatal(err)
	}
	if z.ZeroTimeID == zNew.ZeroTimeID {
		t.Fatal("null id was not replaced")
	}
	if z.CreatedAt != zNew.CreatedAt {
		t.Fatal("zero created_at was not kept")
	}

	// the zero created_at should be hidden from the collection list
	var collection []ZeroTime
	_, err = testService.client.Get("/zero_times", &collection)
	if err != nil {
		t.Fatal(err)
	}
	if len(collection) != 0 {
		t.Fatal("collection not empty as expected")
	}

	// update created_at from the hidden object and try again
	now := time.Now().Round(time.Millisecond) // round to postgres precision
	z.CreatedAt = now
	var z2 ZeroTime
	_, err = testService.client.Put("/zero_times", &z, &z2)
	if err != nil {
		t.Fatal(err)
	}
	// the timestamp should come back as UTC
	if z2.CreatedAt != now.UTC() {
		t.Fatal("created_at timestamp was not properly updated ")
	}

	// now the item should be visible in the collection
	_, err = testService.client.Get("/zero_times", &collection)
	if err != nil {
		t.Fatal(err)
	}
	if len(collection) == 0 {
		t.Fatal("collection is empty, unexpected")
	}

	// check that we actually got the right object back
	if collection[0].ZeroTimeID != z.ZeroTimeID {
		t.Fatal("wrong object id in collection")
	}

	// an empty created_at string should also result in a zero time
	emptyString := struct {
		CreatedAt string `json:"created_at"`
	}{
		CreatedAt: "",
	}
	_, err = testService.client.Post("/zero_times", &emptyString, &z)
	if err != nil {
		t.Fatal(err)
	}
	if !z.CreatedAt.IsZero() {
		t.Fatal("empty string did not produce a zero time")
	}

}

func asJSON(object interface{}) string {
	j, _ := json.MarshalIndent(object, "", "  ")
	return string(j)
}
