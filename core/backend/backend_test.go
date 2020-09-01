package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/google/uuid"
	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/backends/core"
	"github.com/relabs-tech/backends/core/access"

	"github.com/relabs-tech/backends/core/client"
	"github.com/relabs-tech/backends/core/csql"

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
		"resource": "b"
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
		"resource": "created_time"
	  },
	  {
		"resource": "state"
	  },
	  {
		"resource":"notification"
	  },
	  {
		"resource":"notification/normal"
	  },
	  {
		"resource":"interception"
	  },
	  {
		"resource": "with_schema",
		"schema_id": "http://some_host.com/workout.json"
	  },
	  {
		  "resource":"order"
	  }
	],
	"singletons": [
	  {
		"resource": "o/s",
		"static_properties":["name"]
	  },
	  {
		"resource":"notification/single"
	  }
	],
	"blobs": [
	  {
		"resource": "blob",
		"searchable_properties":["content_type"],
		"mutable": true
	  },
	  {
		"resource": "blob2",
		"searchable_properties":["content_type"],
		"mutable": true
	  },
	  {
		"resource": "blob3",
		"searchable_properties":["content_type"],
		"mutable": true,
		"external_index": "external_id"
	  }
	],
	"relations": [],
	"shortcuts": [
		{
			"shortcut" : "b",
			"target" : "b",
			"roles" : ["beerole"]
		}
	]
  }
`
var schemaRefString = `{ "type" : "string" ,
                         "$id" : "http://some_host.com/string.json"}`

var schemaWorkoutString = `{ "$id": "http://some_host.com/workout.json",
                             "type": "object",
                             "required": [
								"workouts"
								],
								"properties": {
									"workouts": {
										"$ref": "http://some_host.com/string.json"
									}
								}
							}`

// TestService holds the configuration for this service
//
// use POSTGRES="host=localhost port=5432 user=postgres dbname=postgres sslmode=disable"
// and POSTRGRES_PASSWORD="docker"
type TestService struct {
	Postgres         string `env:"POSTGRES,required" description:"the connection string for the Postgres DB without password"`
	PostgresPassword string `env:"POSTGRES_PASSWORD,optional" description:"password to the Postgres DB"`
	backend          *Backend
	client           client.Client
}

var testService TestService

func asJSON(object interface{}) string {
	j, _ := json.Marshal(object)
	return string(j)
}

func TestMain(m *testing.M) {
	if err := envdecode.Decode(&testService); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(testService.Postgres, testService.PostgresPassword, "_core_unit_test_")
	defer db.Close()
	db.ClearSchema()

	router := mux.NewRouter()
	testService.backend = New(&Builder{
		Config:          configurationJSON,
		DB:              db,
		Router:          router,
		JSONSchemas:     []string{schemaWorkoutString},
		JSONSchemasRefs: []string{schemaRefString},
	})
	testService.client = client.NewWithRouter(router)

	code := m.Run()
	os.Exit(code)
}

type A struct {
	AID            uuid.UUID `json:"a_id"`
	ExternalID     string    `json:"external_id"`
	StaticProp     string    `json:"static_prop"`
	SearchableProp string    `json:"searchable_prop"`
	CreatedAt      time.Time `json:"created_at"`
	Foo            string    `json:"foo"`
}

func TestCollectionA(t *testing.T) {

	aNew := A{
		Foo:            "bar",
		ExternalID:     "external",
		StaticProp:     "static",
		SearchableProp: "searchable",
		CreatedAt:      time.Now().UTC().Round(time.Millisecond), // round to postgres precision
	}

	a := A{}

	_, err := testService.client.RawPost("/as", &aNew, &a)
	if err != nil {
		t.Fatal(err)
	}

	u := uuid.UUID{}
	if a.AID == u {
		t.Fatal("no id")
	}

	if a.Foo != aNew.Foo ||
		a.ExternalID != aNew.ExternalID ||
		a.StaticProp != aNew.StaticProp ||
		a.SearchableProp != aNew.SearchableProp ||
		a.CreatedAt != aNew.CreatedAt {
		t.Fatal("unexpected result:", asJSON(a), "expected:", asJSON(aNew))
	}

	aGet := A{}
	_, err = testService.client.RawGet("/as/"+a.AID.String(), &aGet)
	if err != nil {
		t.Fatal(err)
	}
	if aNew.Foo != aGet.Foo ||
		a.ExternalID != aGet.ExternalID ||
		a.StaticProp != aGet.StaticProp ||
		a.CreatedAt != aGet.CreatedAt {
		t.Fatal("unexpected result:", asJSON(aGet))
	}

	aPut := aGet
	aRes := A{}
	aPut.StaticProp = "new value for static property"
	_, err = testService.client.RawPut("/as", &aPut, &aRes)
	if err != nil {
		t.Fatal(err)
	}
	if aPut.Foo != aRes.Foo ||
		aPut.ExternalID != aRes.ExternalID ||
		aPut.StaticProp != aRes.StaticProp ||
		aPut.CreatedAt != aRes.CreatedAt {
		t.Fatal("unexpected result:", asJSON(aGet))
	}

	// test the fast put for static properties
	aPut.StaticProp = "another new value for static property"
	_, err = testService.client.RawPut("/as/"+aRes.AID.String()+"/static_prop/"+aPut.StaticProp, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = testService.client.RawGet("/as/"+aRes.AID.String(), &aRes)
	if err != nil {
		t.Fatal(err)
	}
	if aPut.Foo != aRes.Foo ||
		aPut.ExternalID != aRes.ExternalID ||
		aPut.StaticProp != aRes.StaticProp ||
		aPut.CreatedAt != aRes.CreatedAt {
		t.Fatal("unexpected result:", asJSON(aGet))
	}

	// create another object with a different searchable property
	anotherNew := A{
		Foo:            "bar",
		ExternalID:     "another_external",
		StaticProp:     "static",
		SearchableProp: "not_searchable",
		CreatedAt:      time.Now().UTC().Round(time.Millisecond), // round to postgres precision
	}

	_, err = testService.client.RawPost("/as", &anotherNew, nil)
	if err != nil {
		t.Fatal(err)
	}

	// get the entire collection: 2 items
	var collectionResult []A
	_, err = testService.client.RawGet("/as", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 2 {
		t.Fatal("unexpected number of items in collection, expected only 2:", asJSON(collectionResult))
	}

	// we now search for the searachable property and should only find our single item a
	_, err = testService.client.RawGet("/as?filter=searchable_prop=searchable", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 1 {
		t.Fatal("unexpected number of items in collection, expected only 1:", asJSON(collectionResult))
	}
	if collectionResult[0].AID != a.AID {
		t.Fatal("wrong item in collection:", asJSON(collectionResult))
	}

	_, err = testService.client.RawDelete("/as") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}
	status, err := testService.client.RawGet("/as/"+a.AID.String(), &aGet)
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

	_, err := testService.client.RawPost("/bs", &empty, &b)
	if err != nil {
		t.Fatal(err)
	}

	c := C{}
	_, err = testService.client.RawPost("/bs/"+b.BID.String()+"/cs", &empty, &c)
	if err != nil {
		t.Fatal(err)
	}

	d := D{}
	_, err = testService.client.RawPost("/bs/"+b.BID.String()+"/cs/"+c.CID.String()+"/ds", &empty, &d)
	if err != nil {
		t.Fatal(err)
	}

	if d.BID != b.BID || d.CID != c.CID {
		t.Fatal("properties do not match:", asJSON(d))
	}

	// delete the root object b, this should cascade to all child objects
	status, err := testService.client.RawDelete("/bs/" + b.BID.String())
	if status != http.StatusNoContent {
		t.Fatal("delete failed")
	}
	if err != nil {
		t.Fatal(err)
	}
	// cross check that the cascade worked: deleting b has also deleted c and d
	dGet := D{}
	status, err = testService.client.RawGet("/bs/"+b.BID.String()+"/cs/"+c.CID.String()+"/ds/"+d.DID.String(), &dGet)
	if status != http.StatusNotFound {
		t.Fatal("cascade delete failed")
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestResourceBCD_Shortcuts(t *testing.T) {

	empty := Empty{}
	b := B{}

	_, err := testService.client.RawPost("/bs", &empty, &b)
	if err != nil {
		t.Fatal(err)
	}

	auth := access.Authorization{
		Roles:     []string{"beerole"},
		Selectors: map[string]string{"b_id": b.BID.String()},
	}

	authenticatedClient := testService.client.WithAuthorization(&auth)

	bl := B{}
	_, err = authenticatedClient.RawGet("/b", &bl)
	if err != nil {
		t.Fatal(err)
	}
	if bl.BID != b.BID {
		t.Fatal("id does not match:", asJSON(bl))
	}

	c := C{}
	_, err = authenticatedClient.RawPost("/b/cs", &empty, &c)
	if err != nil {
		t.Fatal(err)
	}

	d := D{}
	_, err = authenticatedClient.RawPost("/b/cs/"+c.CID.String()+"/ds", &empty, &d)
	if err != nil {
		t.Fatal(err)
	}

	if d.BID != b.BID || d.CID != c.CID {
		t.Fatal("properties do not match:", asJSON(d))
	}

	// delete the root object b, this should cascade to all child objects
	status, err := authenticatedClient.RawDelete("/b")
	if status != http.StatusNoContent {
		t.Fatal("delete failed")
	}
	if err != nil {
		t.Fatal(err)
	}
	// cross check that the cascade worked: deleting b has also deleted c and d
	dGet := D{}
	status, err = testService.client.RawGet("/bs/"+b.BID.String()+"/cs/"+c.CID.String()+"/ds/"+d.DID.String(), &dGet)
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
		OID  uuid.UUID `json:"o_id"`
		Name string    `json:"name"`
	}

	empty := Empty{}

	// create o instance
	o := O{}
	_, err := testService.client.RawPost("/os", &empty, &o)
	if err != nil {
		t.Fatal(err)
	}

	// create single s with initial name
	s := S{
		Name: "initial",
	}
	sResult := S{}
	_, err = testService.client.RawPut("/os/"+o.OID.String()+"/s", &s, &sResult)
	if err != nil {
		t.Fatal(err)
	}

	if sResult.Name != "initial" {
		t.Fatal("properties not as expected:", asJSON(sResult))
	}

	// update single s to have updated name, the object's id (sid) remains the same
	sUpdate := S{
		Name: "updated",
	}
	sUpdateResult := S{}

	status, err := testService.client.RawPut("/os/"+o.OID.String()+"/s", &sUpdate, &sUpdateResult)
	if err != nil {
		t.Fatal(err)
	}
	if sUpdateResult.Name != "updated" {
		t.Fatal("properties not as expected:", asJSON(sUpdateResult))
	}
	if sUpdateResult.OID != sResult.OID {
		t.Fatal("got a new object, should have gotten the same object")
	}

	// now update with direct property update
	status, err = testService.client.RawPut("/os/"+o.OID.String()+"/s/name/updated_again", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	status, err = testService.client.RawGet("/os/"+o.OID.String()+"/s", &sUpdateResult)
	if err != nil {
		t.Fatal(err)
	}
	if sUpdateResult.Name != "updated_again" {
		t.Fatal("properties not as expected:", asJSON(sUpdateResult))
	}
	if sUpdateResult.OID != sResult.OID {
		t.Fatal("got a new object, should have gotten the same object")
	}

	// now update with direct property update, but flip the ids
	status, err = testService.client.RawPut("/os/all/ss/"+o.OID.String()+"/name/third_update", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	status, err = testService.client.RawGet("/os/all/ss/"+o.OID.String(), &sUpdateResult)
	if err != nil {
		t.Fatal(err)
	}
	if sUpdateResult.Name != "third_update" {
		t.Fatal("properties not as expected:", asJSON(sUpdateResult))
	}
	if sUpdateResult.OID != sResult.OID {
		t.Fatal("got a new object, should have gotten the same object")
	}

	newUID := uuid.New()

	// // put another update to s and try to give it a new id. This will fail.
	sUpdate.OID = newUID
	status, err = testService.client.RawPut("/os/"+o.OID.String()+"/s", &sUpdate, &sUpdateResult)
	if err == nil {
		t.Fatal("was allowed to change the primary id")
	}

	// delete single s
	status, err = testService.client.RawDelete("/os/" + o.OID.String() + "/s")
	if status != http.StatusNoContent {
		t.Fatal("delete failed")
	}
	if err != nil {
		t.Fatal(err)
	}

	// cross check that the delete worked
	sGet := S{}
	status, err = testService.client.RawGet("/os/"+o.OID.String()+"/s", &sGet)
	if status != http.StatusNoContent {
		t.Fatal("delete failed, got status ", status)
		if err != nil {
			t.Fatal(err)
		}
	}

	// re-create single s with no uuid, we will nonetheless receive the owner id
	sResult2 := S{}
	_, err = testService.client.RawPut("/os/"+o.OID.String()+"/s", &s, &sResult2)
	if err != nil {
		t.Fatal(err)
	}
	if sResult2.OID != o.OID {
		t.Fatal("recreation did not work, wrong owner id")
	}

	// delete the owner o, this should also delete the single s
	status, err = testService.client.RawDelete("/os/" + o.OID.String())
	if status != http.StatusNoContent {
		t.Fatal("delete failed")
	}
	if err != nil {
		t.Fatal(err)
	}

	// cross check that the cascade worked: deleting o has also deleted s
	status, err = testService.client.RawGet("/os/"+o.OID.String()+"/s", &sGet)
	if status != http.StatusNoContent {
		t.Fatal("cascade delete failed")
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestCreatedTimeAndNullID(t *testing.T) {

	type CreatedTime struct {
		CreatedTimeID uuid.UUID `json:"created_time_id"`
		CreatedAt     time.Time `json:"created_at"`
	}

	now := time.Now().UTC().Round(time.Millisecond) // round to postgres precision
	cNew := CreatedTime{CreatedAt: now}
	var c CreatedTime
	_, err := testService.client.RawPost("/created_times", &cNew, &c)
	if err != nil {
		t.Fatal(err)
	}
	if c.CreatedTimeID == cNew.CreatedTimeID {
		t.Fatal("null id was not replaced")
	}
	if c.CreatedAt != cNew.CreatedAt {
		t.Fatal("created_at was not kept")
	}

	// an empty created_at string should produce an error
	emptyString := struct {
		CreatedAt string `json:"created_at"`
	}{
		CreatedAt: "",
	}
	_, err = testService.client.RawPost("/created_times", &emptyString, &c)
	if err == nil {
		t.Fatal("eerror expected")
	}

	// This should also work with Struct
	a := CreatedTime{}
	if _, err := testService.client.Collection("created_time").Create(a, &a); err != nil {
		t.Fatal(err)
	}
	if a.CreatedAt.IsZero() {
		t.Fatal("CreatedAt was not expected to be Zero")
	}
}

func TestCollectionOrder(t *testing.T) {

	type Order struct {
		CreatedAt time.Time `json:"created_at"`
		Serial    int64     `json:"serial"`
	}

	t0 := time.Now().UTC().Add(-time.Hour).Round(time.Millisecond) // round to postgres precision

	_, err := testService.client.RawDelete("/orders")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		newOrder := Order{CreatedAt: t0.Add(time.Duration(i) * time.Minute), Serial: int64(i)}
		_, err := testService.client.RawPost("/orders", &newOrder, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	isAscending := func(list []Order) bool {
		for i, order := range list {
			if order.Serial != int64(i) {
				return false
			}
		}
		return true
	}

	isDescending := func(list []Order) bool {
		for i, order := range list {
			if order.Serial != int64(len(list)-1-i) {
				return false
			}
		}
		return true
	}

	list := []Order{}
	// default is descending
	_, err = testService.client.RawGet("/orders", &list)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 10 {
		t.Fatalf("unexpected size %d", len(list))
	}
	if !isDescending(list) {
		t.Fatalf("list is not descending %v", list)
	}

	// explicit descending
	_, err = testService.client.RawGet("/orders?order=desc", &list)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 10 {
		t.Fatalf("unexpected size %d", len(list))
	}
	if !isDescending(list) {
		t.Fatalf("list is not descending %v", list)
	}

	// explicit ascending
	_, err = testService.client.RawGet("/orders?order=asc", &list)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 10 {
		t.Fatalf("unexpected size %d", len(list))
	}
	if !isAscending(list) {
		t.Fatalf("list is not ascending %v", list)
	}
}

type Blob struct {
	BlobID      uuid.UUID `json:"blob_id"`
	CreatedAt   time.Time `json:"created_at"`
	ContentType string    `json:"content_type"`
}

func TestBlob(t *testing.T) {
	data, err := ioutil.ReadFile("./testdata/dalarubettrich.png")
	if err != nil {
		t.Fatal(err)
	}

	var br Blob
	header := map[string]string{
		"Content-Type":       "image/png",
		"Kurbisio-Meta-Data": `{"hello":"world"}`,
	}
	_, err = testService.client.RawPostBlob("/blobs", header, data, &br)
	if err != nil {
		t.Fatal(err)
	}

	list := []Blob{}
	_, err = testService.client.RawGet("/blobs", &list)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("unexpected blob list size %d", len(list))
	}
	if list[0].BlobID != br.BlobID {
		t.Fatal("missing my blob")
	}
	if list[0].ContentType != "image/png" {
		t.Fatal("wrong content type")
	}

	var dataReturn []byte
	_, headerReturn, err := testService.client.RawGetBlobWithHeader("/blobs/"+br.BlobID.String(), map[string]string{}, &dataReturn)

	if err != nil {
		t.Fatal(err)
	}
	if headerReturn.Get("Content-Type") != header["Content-Type"] {
		t.Fatal("wrong Content-Type in return header")
	}

	if headerReturn.Get("Kurbisio-Meta-Data") != header["Kurbisio-Meta-Data"] {
		t.Fatal("wrong meta data in return header")
	}

	if bytes.Compare(data, dataReturn) != 0 {
		t.Fatal("returned binary data is not equal")
	}

	// now update the  blob with something completely different
	var ubr Blob
	uHeader := map[string]string{
		"Content-Type": "something weird",
	}
	uData := []byte("binary stuff")
	_, err = testService.client.RawPutBlob("/blobs/"+br.BlobID.String(), uHeader, uData, &ubr)
	if err != nil {
		t.Fatal(err)
	}

	var uDataReturn []byte
	_, uHeaderReturn, err := testService.client.RawGetBlobWithHeader("/blobs/"+br.BlobID.String(), map[string]string{}, &uDataReturn)

	if err != nil {
		t.Fatal(err)
	}
	if uHeaderReturn.Get("Content-Type") != uHeader["Content-Type"] {
		t.Fatal("wrong Content-Type in return header")
	}

	if uHeaderReturn.Get("Kurbisio-Meta-Data") != "{}" {
		t.Fatal("got meta data, but should have been cleared")
	}

	if bytes.Compare(uData, uDataReturn) != 0 {
		t.Fatal("returned binary data is not equal")
	}

}

func TestNotifications(t *testing.T) {

	var (
		createCount, updateCount, deleteCount, pointsCount int
	)
	var lock sync.Mutex
	createHandler := func(ctx context.Context, n Notification) error {
		lock.Lock()
		defer lock.Unlock()
		createCount++
		return nil
	}
	updateHandler := func(ctx context.Context, n Notification) error {
		lock.Lock()
		defer lock.Unlock()
		updateCount++
		var object map[string]interface{}
		err := json.Unmarshal(n.Payload, &object)
		if err != nil {
			return err
		}
		if points, ok := object["points"].(float64); ok {
			pointsCount += int(points)
		}
		return nil
	}
	deleteHandler := func(ctx context.Context, n Notification) error {
		lock.Lock()
		defer lock.Unlock()
		deleteCount++
		return nil
	}

	backend := testService.backend

	backend.HandleResourceNotification("notification", createHandler, core.OperationCreate)
	backend.HandleResourceNotification("notification/normal", createHandler, core.OperationCreate)
	backend.HandleResourceNotification("notification/single", createHandler, core.OperationCreate)

	backend.HandleResourceNotification("notification", updateHandler, core.OperationUpdate)
	backend.HandleResourceNotification("notification/normal", updateHandler, core.OperationUpdate)
	backend.HandleResourceNotification("notification/single", updateHandler, core.OperationUpdate)

	backend.HandleResourceNotification("notification", deleteHandler, core.OperationDelete)
	backend.HandleResourceNotification("notification/normal", deleteHandler, core.OperationDelete)
	backend.HandleResourceNotification("notification/single", deleteHandler, core.OperationDelete)

	client := testService.client

	// create root object
	type G map[string]interface{}
	nreq := G{}
	var nres G
	_, err := client.RawPost("/notifications", &nreq, &nres)
	if err != nil {
		t.Fatal(err)
	}
	nid, _ := nres["notification_id"].(string)

	// update root object with 1 point. First create
	nres["points"] = int64(1)
	_, err = client.RawPut("/notifications", &nres, &nres)
	if err != nil {
		t.Fatal(err)
	}

	// create child collection object. Second create.
	nnreq := G{"notification_id": nid}
	var nnres G
	_, err = client.RawPost("/notifications/"+nid+"/normals", &nnreq, &nnres)
	if err != nil {
		t.Fatal(err)
	}

	// update child collection object with 5 points
	nnres["points"] = int64(5)
	_, err = client.RawPut("/notifications/"+nid+"/normals", &nnres, &nnres)
	if err != nil {
		t.Fatal(err)
	}

	// delete child collection object
	_, err = client.RawDelete("/notifications/" + nid + "/normals/" + nnres["normal_id"].(string))
	if err != nil {
		t.Fatal(err)
	}

	// create child singleton object with collection path. Third create.
	nsreq := G{"notification_id": nid}
	var nsres G
	_, err = client.RawPost("/notifications/"+nid+"/singles", &nsreq, &nsres)
	if err != nil {
		t.Fatal(err)
	}

	// update child collection object with collection path and 2 points
	nsres["points"] = int64(2)
	_, err = client.RawPut("/notifications/"+nid+"/singles", &nsres, &nsres)
	if err != nil {
		t.Fatal(err)
	}

	// do notification processing
	backend.ProcessJobsSync(0)

	// delete child singleton object with wildcard path
	_, err = client.RawDelete("/notifications/all/singles/" + nid)
	if err != nil {
		t.Fatal(err)
	}

	// re-create child singleton object with singleton path. Fourth create.
	nsreq = G{}
	_, err = client.RawPut("/notifications/"+nid+"/single", &nsreq, &nsres)
	if err != nil {
		t.Fatal(err)
	}

	// do notification processing
	backend.ProcessJobsSync(0)

	// update child collection object with singleton path and 3 points
	nsres["points"] = int64(3)
	_, err = client.RawPut("/notifications/"+nid+"/single", &nsres, &nsres)
	if err != nil {
		t.Fatal(err)
	}

	// do notification processing
	backend.ProcessJobsSync(0)

	// delete child singleton object with singleton path
	_, err = client.RawDelete("/notifications/" + nid + "/single")
	if err != nil {
		t.Fatal(err)
	}

	// delete root object
	_, err = client.RawDelete("/notifications/" + nid)
	if err != nil {
		t.Fatal(err)
	}

	// do notification processing
	backend.ProcessJobsSync(0)

	if createCount != 4 {
		t.Fatalf("unexpected number of creates: %d", createCount)
	}
	if updateCount != 4 {
		t.Fatalf("unexpected number of updates: %d", updateCount)
	}
	if deleteCount != 4 {
		t.Fatalf("unexpected number of deletes: %d", deleteCount)
	}
	if pointsCount != 11 {
		t.Fatalf("unexpected number of points: %d", pointsCount)
	}

}

func TestRequestInterceptors(t *testing.T) {
	backend := testService.backend

	backend.HandleResourceRequest("interception", func(ctx context.Context, request Request, data []byte) ([]byte, error) {
		var object map[string]interface{}
		json.Unmarshal(data, &object)
		object["interceptor_create"] = "Kilroy was here!"
		return json.Marshal(object)
	}, core.OperationCreate)

	backend.HandleResourceRequest("interception", func(ctx context.Context, request Request, data []byte) ([]byte, error) {
		var object map[string]interface{}
		json.Unmarshal(data, &object)
		object["interceptor_update"] = "Kilroy was here!"
		return json.Marshal(object)
	}, core.OperationUpdate)

	backend.HandleResourceRequest("interception", func(ctx context.Context, request Request, data []byte) ([]byte, error) {
		var object map[string]interface{}
		json.Unmarshal(data, &object)
		object["interceptor_read"] = "Kilroy was here!"
		return json.Marshal(object)
	}, core.OperationRead)

	backend.HandleResourceRequest("interception", func(ctx context.Context, request Request, data []byte) ([]byte, error) {
		return nil, errors.New("Kilroy does not want this to be deleted")
	}, core.OperationDelete)

	backend.HandleResourceRequest("interception", func(ctx context.Context, request Request, data []byte) ([]byte, error) {
		return nil, errors.New("Kilroy does not want the entire list to be cleared")
	}, core.OperationClear)

	backend.HandleResourceRequest("interception", func(ctx context.Context, request Request, data []byte) ([]byte, error) {
		var list []map[string]interface{}
		json.Unmarshal(data, &list)
		for i := range list {
			list[i]["interceptor_list"] = "Kilroy was here!"
		}
		return json.Marshal(list)
	}, core.OperationList)

	client := testService.client

	// create root object
	type Interception map[string]interface{}
	nreq := Interception{"secret": "pssst!"}
	var nres Interception
	_, err := client.RawPost("/interceptions", &nreq, &nres)
	if err != nil {
		t.Fatal(err)
	}
	// check that we got the newly created object back
	assert.Equal(t, nres["secret"], "pssst!")
	// check that the create interceptor did its work
	assert.Equal(t, "Kilroy was here!", nres["interceptor_create"])
	// check that the read interceptor was NOT called
	assert.NotEqual(t, "Kilroy was here!", nres["interceptor_read"])

	id, _ := nres["interception_id"].(string)
	_, err = client.RawPut("/interceptions/"+id, &nreq, &nres)
	if err != nil {
		t.Fatal(err)
	}
	// check that we got the newly created objecttback
	assert.Equal(t, "pssst!", nres["secret"])
	// check that the work of the create interceptor was persisted in the database
	assert.Equal(t, "Kilroy was here!", nres["interceptor_create"])
	// check that the update interceptor did its work
	assert.Equal(t, "Kilroy was here!", nres["interceptor_update"])
	// check that the read interceptor was NOT called
	assert.NotEqual(t, "Kilroy was here!", nres["interceptor_read"])

	_, err = client.RawGet("/interceptions/"+id, &nres)
	if err != nil {
		t.Fatal(err)
	}
	// check that we got the newly created objecttback
	assert.Equal(t, nres["secret"], "pssst!")
	// check that the work of the create interceptor was persisted in the database
	assert.Equal(t, "Kilroy was here!", nres["interceptor_create"])
	// check that the work of the update interceptor was persisted in the database
	assert.Equal(t, "Kilroy was here!", nres["interceptor_update"])
	// check that the read interceptor was called
	assert.Equal(t, "Kilroy was here!", nres["interceptor_read"])

	status, err := client.RawDelete("/interceptions/" + id)
	assert.Equal(t, http.StatusBadRequest, status)
	if err == nil || !strings.HasSuffix(err.Error(), "Kilroy does not want this to be deleted") {
		t.Fatal("missing Kilroy's message for deletion:", err)
	}

	status, err = client.RawDelete("/interceptions")
	assert.Equal(t, http.StatusBadRequest, status)
	if err == nil || !strings.HasSuffix(err.Error(), "Kilroy does not want the entire list to be cleared") {
		t.Fatal("missing Kilroy's message for clear:", err)
	}

	var list []Interception
	_, err = client.RawGet("/interceptions", &list)
	if err != nil {
		t.Fatal(err)
	}
	for i := range list {
		// check that the list interceptor was called
		assert.Equal(t, "Kilroy was here!", list[i]["interceptor_list"])

	}

}

func TestPaginationCollection(t *testing.T) {
	// Populate the DB with elements created at two created_at times
	numberOfElements := 210
	createdAtFirst50 := time.Now().UTC().Round(time.Millisecond)
	createdAtRemaining := time.Now().UTC().Round(time.Millisecond).Add(time.Minute)
	for i := 1; i <= numberOfElements; i++ {
		aNew := A{
			ExternalID: fmt.Sprint(i),
			CreatedAt:  createdAtFirst50,
		}
		if i > 50 {
			aNew.CreatedAt = createdAtRemaining
		}

		if _, err := testService.client.RawPost("/as", &aNew, &A{}); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []struct {
		path           string
		expectedStatus int
		expectedLength int
		expectedError  bool
		valid          func(*testing.T, A)
	}{
		{"/as", http.StatusOK, 100, false, nil},
		{"/as?limit=10", http.StatusOK, 10, false, nil},
		{"/as?limit=10&page=1", http.StatusOK, 10, false, nil},
		{"/as?limit=10&page=10", http.StatusOK, 10, false, nil},
		{"/as?page=0", http.StatusBadRequest, 0, true, nil},
		{"/as?until=" + createdAtFirst50.Add(time.Second).Format(time.RFC3339), http.StatusOK, 50, false, func(tc *testing.T, a A) {
			if a.CreatedAt.After(createdAtFirst50) {
				tc.Fatal("Got too recent record")
			}
		}},
		{"/as?limit=45&from=" + createdAtRemaining.Format(time.RFC3339), http.StatusOK, 45, false, func(tc *testing.T, a A) {
			if a.CreatedAt.Before(createdAtRemaining) {
				tc.Fatal("Got too old record:", a.CreatedAt)
			}
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			var as []A
			status, err := testService.client.RawGet(tc.path, &as)
			if !tc.expectedError && err != nil {
				t.Fatal(err)
			}
			if status != tc.expectedStatus {
				t.Fatalf("Expected status %d, got status: %d", tc.expectedStatus, status)
			}
			if len(as) != tc.expectedLength {
				t.Fatalf("The expected returned size is %d, but %d were received", tc.expectedLength, len(as))
			}
			if tc.valid != nil {
				for _, a := range as {
					tc.valid(t, a)
				}
			}
		})
	}

	// Verify that we can get all elements by iterating through pages
	limit := 10
	var received = make(map[uuid.UUID]A)
	for page := 1; page <= (numberOfElements-1)/limit+1; page++ {
		path := fmt.Sprintf("/as?limit=%d&page=%d", limit, page)
		var as []A
		status, h, err := testService.client.RawGetWithHeader(path, map[string]string{}, &as)
		if err != nil || status != http.StatusOK {
			t.Fatal("error: ", err, "status: ", status)
		}
		assert.Equal(t, strconv.Itoa(limit), h.Get("Pagination-Limit"))
		assert.Equal(t, strconv.Itoa(numberOfElements), h.Get("Pagination-Total-Count"))
		assert.Equal(t, strconv.Itoa((numberOfElements-1)/limit+1), h.Get("Pagination-Page-Count"))
		assert.Equal(t, strconv.Itoa(page), h.Get("Pagination-Current-Page"))

		for _, a := range as {
			if _, ok := received[a.AID]; ok {
				t.Fatalf("Received the same UUID: %s multiple times", a.AID)
			}
			received[a.AID] = a
		}
	}
	if len(received) != numberOfElements {
		t.Fatalf("Did not get %d elements, only got %d", numberOfElements, len(received))
	}

}

func TestPaginationBlob(t *testing.T) {
	numberOfElements := 10
	beforeCreation := time.Now().UTC().Add(-time.Second)
	blobData, err := ioutil.ReadFile("./testdata/dalarubettrich.png")
	header := map[string]string{
		"Content-Type":       "image/png",
		"Kurbisio-Meta-Data": `{"hello":"world"}`,
	}
	for i := 1; i <= numberOfElements; i++ {
		if _, err = testService.client.RawPostBlob("/blob2s", header, blobData, &Blob{}); err != nil {
			t.Fatal(err)
		}

	}
	afterCreation := time.Now().UTC().Add(time.Second)

	testCases := []struct {
		path           string
		expectedStatus int
		expectedLength int
		expectedError  bool
	}{
		{"/blob2s", http.StatusOK, 10, false},
		{"/blob2s?limit=5", http.StatusOK, 5, false},
		{"/blob2s?limit=4&page=1", http.StatusOK, 4, false},
		{"/blob2s?limit=4&page=3", http.StatusOK, 2, false},
		{"/blob2s?page=0", http.StatusBadRequest, 0, true},
		{"/blob2s?until=" + afterCreation.Format(time.RFC3339), http.StatusOK, 10, false},
		{"/blob2s?from=" + beforeCreation.Format(time.RFC3339), http.StatusOK, 10, false},
		{"/blob2s?limit=4&until=" + afterCreation.Format(time.RFC3339), http.StatusOK, 4, false},
		{"/blob2s?limit=4&from=" + beforeCreation.Format(time.RFC3339), http.StatusOK, 4, false},
	}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			var blobs []Blob
			status, h, err := testService.client.RawGetWithHeader(tc.path, map[string]string{}, &blobs)
			if !tc.expectedError {
				if err != nil {
					t.Fatal(err)
				} else {
					assert.Equal(t, strconv.Itoa(numberOfElements), h.Get("Pagination-Total-Count"))
				}
			}
			if status != tc.expectedStatus {
				t.Fatalf("Expected status %d, got status: %d", tc.expectedStatus, status)
			}
			if len(blobs) != tc.expectedLength {
				t.Fatalf("The expected returned size is %d, but %d were received", tc.expectedLength, len(blobs))
			}

		})
	}
}

func TestInvalidPaths(t *testing.T) {
	testCases := []struct {
		path           string
		expectedStatus int
		expectedError  bool
	}{
		{"/as/invalid-uuid", http.StatusBadRequest, true},
		{"/blobs/invalid-uuid", http.StatusBadRequest, true},
		{"/as/273cf448-b8e0-4e7b-9f80-e378050eb719", http.StatusNotFound, true},
		{"/blobs/273cf448-b8e0-4e7b-9f80-e378050eb719", http.StatusNotFound, true},
	}

	for _, tc := range testCases {
		t.Run(tc.path, func(t *testing.T) {
			var blobs []Blob
			status, err := testService.client.RawGet(tc.path, &blobs)
			if !tc.expectedError && err != nil {
				t.Fatal(err)
			}
			if status != tc.expectedStatus {
				t.Fatalf("Expected status %d, got status: %d", tc.expectedStatus, status)
			}
		})
	}
}
