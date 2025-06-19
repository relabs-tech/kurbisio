// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend_test

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/relabs-tech/kurbisio/core/backend"

	"github.com/stretchr/testify/assert"
)

func TestEtagGet(t *testing.T) {
	a := A{ExternalID: t.Name()}
	if _, err := testService.client.RawPost("/as", a, &a); err != nil {
		t.Fatal(err)
	}

	_, firstHeader, err := testService.client.RawGetWithHeader("/as/"+a.AID.String(), map[string]string{}, &A{})
	if err != nil {
		t.Fatal(err)
	}

	etag := firstHeader.Get("ETag")
	if etag == "" {
		t.Fatal("ETag is not present in response's header from Get header")
	}

	// Check that another Get with If-None-Match and ETag header returns 304
	testCases := []struct {
		etag           string
		expectedStatus int
	}{
		{etag, http.StatusNotModified},
		{"\"" + etag + "\", \"1234\"", http.StatusNotModified},
		{"*", http.StatusNotModified},
		{"", http.StatusOK},
		{"\"54637\", \"1234\"", http.StatusOK},
	}
	for _, tc := range testCases {
		t.Run(tc.etag, func(t *testing.T) {
			header := make(map[string]string)
			if etag != "" {
				header["If-None-Match"] = tc.etag
			}
			var receivedBuffer []byte
			status, h, _ := testService.client.RawGetWithHeader(
				"/as/"+a.AID.String(), header, &A{})

			if status != tc.expectedStatus {
				t.Fatalf("Expected return status %d, got: %d", tc.expectedStatus, status)
			}
			if status == http.StatusNotModified && len(receivedBuffer) > 0 {
				t.Fatal("Expected 0 data length, got: ", len(receivedBuffer))
			}

			// As per https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match, the header
			// returned when using If-None-Match shall return the same header fields that would have been
			// sent in a 200 (OK) response to the same request: Cache-Control, Content-Location, Date, ETag,
			// Expires, and Vary.
			fields := []string{"Cache-Control", "Content-Location", "Date", "ETag", "Expires", "Vary"}
			for _, f := range fields {
				if firstHeader.Get(f) != h.Get(f) {
					t.Fatalf("Expected same headers field for %s, got: '%s' instead of '%s'", f, h.Get(f), firstHeader.Get(f))
				}
			}
		})
	}
}

func TestEtagGetCollection(t *testing.T) {
	a := A{ExternalID: t.Name()}
	if _, err := testService.client.RawPost("/as", a, &a); err != nil {
		t.Fatal(err)
	}

	_, h1, err := testService.client.RawGetWithHeader("/as", map[string]string{}, &[]A{})
	if err != nil {
		t.Fatal(err)
	}

	etag := h1.Get("ETag")
	if etag == "" {
		t.Fatal("ETag is not present in response's header from Get header")
	}

	// Check that another Get with If-None-Match and ETag header returns 304
	testCases := []struct {
		etag           string
		expectedStatus int
	}{
		{etag, http.StatusNotModified},
		{"\"" + etag + "\", \"1234\"", http.StatusNotModified},
		{"*", http.StatusNotModified},
		{"", http.StatusOK},
		{"\"54637\", \"1234\"", http.StatusOK},
	}
	for _, tc := range testCases {
		t.Run(tc.etag, func(t *testing.T) {
			header := make(map[string]string)
			if etag != "" {
				header["If-None-Match"] = tc.etag
			}
			var receivedBuffer []byte
			status, h, _ := testService.client.RawGetWithHeader("/as", header, &[]A{})

			if status != tc.expectedStatus {
				t.Fatalf("Expected return status %d, got: %d", tc.expectedStatus, status)
			}
			if status == http.StatusNotModified && len(receivedBuffer) > 0 {
				t.Fatal("Expected 0 data length, got: ", len(receivedBuffer))
			}

			// As per https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match, the header
			// returned when using If-None-Match shall return the same header fields that would have been
			// sent in a 200 (OK) response to the same request: Cache-Control, Content-Location, Date, ETag,
			// Expires, and Vary.
			fields := []string{"Cache-Control", "Content-Location", "Date", "ETag", "Expires", "Vary"}
			for _, f := range fields {
				if h1.Get(f) != h.Get(f) {
					t.Fatalf("Expected same headers field for %s, got: '%s' instead of '%s'", f, h.Get(f), h1.Get(f))
				}
			}
		})
	}
}

func TestDeleteCollection(t *testing.T) {
	b := B{}
	c := C{}
	if _, err := testService.client.RawPost("/bs", b, &b); err != nil {
		t.Fatal(err)
	}
	if _, err := testService.client.RawPost("/bs/"+b.BID.String()+"/cs", c, &c); err != nil {
		t.Fatal(err)
	}
	if _, err := testService.client.RawDelete("/bs/" + "all" + "/cs/" + c.CID.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := testService.client.RawDelete("/bs/" + b.BID.String()); err != nil {
		t.Fatal(err)
	}

}

// TestEtagRegenerated checks that if a property of an element is modified through a PUT request,
// the ETag is modified
func TestEtagRegenerated(t *testing.T) {
	a := A{
		ExternalID: t.Name(),
		StaticProp: "a property",
		Timestamp:  time.Now().Add(time.Hour),
	}
	if _, err := testService.client.RawPost("/as", a, &a); err != nil {
		t.Fatal(err)
	}

	_, h1, err := testService.client.RawGetWithHeader("/as/"+a.AID.String(), map[string]string{}, &A{})
	if err != nil {
		t.Fatal(err)
	}

	a.StaticProp = "a new property"
	if _, err = testService.client.RawPut("/as/"+a.AID.String(), a, &A{}); err != nil {
		t.Fatal(err)
	}

	_, h2, err := testService.client.RawGetWithHeader("/as/"+a.AID.String(), map[string]string{}, &A{})
	if err != nil {
		t.Fatal(err)
	}

	if h1.Get("ETag") == h2.Get("ETag") {
		t.Fatal("ETag was not updated: ", h2.Get("ETag"))
	}
}

// TestEtagCollectionRegenerated checks that if another element is added to a collection through
// a POST request, then ETag is modified
func TestEtagCollectionRegenerated(t *testing.T) {

	// Clear the collection first
	if _, err := testService.client.RawDelete("/as"); err != nil {
		t.Fatal(err)
	}

	// Create 10 elements
	for i := 0; i < 10; i++ {
		if _, err := testService.client.RawPost("/as", A{ExternalID: fmt.Sprintf("element_%d", i)}, &A{}); err != nil {
			t.Fatal(err)
		}
	}

	l := []A{}
	_, h1, err := testService.client.RawGetWithHeader("/as?limit=10", map[string]string{}, &l)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("Found before %d\n", len(l))
	etag := h1.Get("ETag")
	if etag == "" {
		t.Fatal("ETag is not present in response's header from Get header")
	}

	if _, err := testService.client.RawPost("/as", A{ExternalID: t.Name() + "2"}, &A{}); err != nil {
		t.Fatal(err)
	}

	_, h2, err := testService.client.RawGetWithHeader("/as?limit=10", map[string]string{}, &l)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("Found after %d\n", len(l))

	if h1.Get("ETag") == h2.Get("ETag") {
		t.Fatal("ETag was not updated: ", h2.Get("ETag"))
	}
}

// TestCollectionExternalID verifies that if we try to create twice an element with the same
// external id, we get a 422 error
func TestCollectionExternalID(t *testing.T) {
	a := A{ExternalID: "an external id"}
	if _, err := testService.client.RawPost("/as", a, &a); err != nil {
		t.Fatal(err)
	}
	status, err := testService.client.RawPost("/as", a, &a)
	assert.Equal(t, http.StatusConflict, status, err)
}

func TestCollectionWithSchemaValidation(t *testing.T) {
	type withSchema struct {
		WithSchemaID uuid.UUID `json:"with_schema_id"`
		Workouts     string    `json:"workouts"`
	}

	w := withSchema{}

	_, err := testService.client.RawPost("/with_schemas", &withSchema{Workouts: "foo"}, &w)
	if err != nil {
		t.Fatal(err)
	}

	w.Workouts = "bar"
	wRes := withSchema{}
	_, err = testService.client.RawPut("/with_schemas", &w, &wRes)
	if err != nil {
		t.Fatal(err)
	}
	if w.Workouts != wRes.Workouts {
		t.Fatal("unexpected result:", asJSON(wRes))
	}
}

func TestCollectionWithSchemaValidationPostInvalidSchema(t *testing.T) {
	type withSchema struct {
		WithSchemaID uuid.UUID `json:"with_schema_id"`
		Invalid      string    `json:"invalid"`
	}

	w := withSchema{}

	_, err := testService.client.RawPost("/with_schemas", &withSchema{Invalid: "foo"}, &w)
	if err == nil {
		t.Fatalf("Expecting validation failure")
	}
}

func TestCollectionWithSchemaValidationPutInvalidSchema(t *testing.T) {
	type withSchema struct {
		WithSchemaID uuid.UUID `json:"with_schema_id"`
		Workouts     string    `json:"workouts,omitempty"`
		Invalid      string    `json:"invalid,omitempty"`
	}

	w := withSchema{}

	_, err := testService.client.RawPost("/with_schemas", &withSchema{Workouts: "foo"}, &w)
	if err != nil {
		t.Fatal()
	}

	w.Workouts = ""
	w.Invalid = "bar"
	_, err = testService.client.RawPut("/with_schemas", &w, &withSchema{})
	if err == nil {
		t.Fatalf("Expecting validation failure")
	}
}

func TestFilters(t *testing.T) {

	b := B{}
	_, err := testService.client.RawPost("/bs", &b, &b)
	if err != nil {
		t.Fatal(err)
	}

	c := C{B: b}
	_, err = testService.client.RawPost("/bs/"+b.BID.String()+"/cs", &c, &c)
	if err != nil {
		t.Fatal(err)
	}

	var collectionResult []C
	// we now search for the searchable property and should only find our single item a
	_, err = testService.client.RawGet("/bs/all/cs?filter=b_id="+b.BID.String(), &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 1 {
		t.Fatal("unexpected number of items in collection, expected only 1:", asJSON(collectionResult))
	}
	if collectionResult[0].BID != b.BID {
		t.Fatal("wrong item in collection:", asJSON(collectionResult))
	}

	// we now search for the searchable property and should only find our single item a
	_, err = testService.client.RawGet("/bs/all/cs?filter=c_id="+c.CID.String(), &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 1 {
		t.Fatal("unexpected number of items in collection, expected only 1:", asJSON(collectionResult))
	}
	if collectionResult[0].BID != b.BID {
		t.Fatal("wrong item in collection:", asJSON(collectionResult))
	}

	_, err = testService.client.Collection("b").Clear()
	if err != nil {
		t.Fatal(err)
	}
}

// TestSearchEqual test searching in searchable_properties and in json properties
func TestSearchEqual(t *testing.T) {
	jsonConfig := `{
	"collections": [
	  {
		"resource": "a",
		"external_index": "external_id",
		"static_properties": ["static_prop"],
		"searchable_properties": ["searchable_prop"]
	  } 
	],
	"singletons": [],
	"blobs": [],
	"shortcuts": []
  }
`
	testService := CreateTestService(jsonConfig, t.Name())
	defer testService.Db.Close()

	numberOfElements := 16
	for i := 0; i < numberOfElements; i++ {
		_, err := testService.client.WithAdminAuthorization().RawPost("/as",
			A{
				ExternalID:     "external_id_" + strconv.Itoa(i),
				SearchableProp: "searchable_prop_" + strconv.Itoa(i%2),
				StaticProp:     "static_prop_" + strconv.Itoa(i%4),
				Foo:            "foo_" + strconv.Itoa(i%8),
			}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	var collectionResult []A

	// Search in external_index
	_, err := testService.client.RawGet("/as?filter=external_id=external_id_1", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 1 {
		t.Fatal("unexpected number of items in collection, expected 1, got", asJSON(collectionResult))
	}
	if collectionResult[0].ExternalID != "external_id_1" {
		t.Fatal("wrong item in collection:", collectionResult[0].ExternalID)
	}

	// Search in searchable_properties
	_, err = testService.client.RawGet("/as?filter=searchable_prop=searchable_prop_1", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 8 {
		t.Fatalf("unexpected number of items in collection, expected 8, got %v %s", len(collectionResult), asJSON(collectionResult))
	}
	if collectionResult[0].SearchableProp != "searchable_prop_1" {
		t.Fatal("wrong item in collection:", collectionResult[0].SearchableProp)
	}

	// Search in json document
	_, err = testService.client.RawGet("/as?filter=foo=foo_1", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 2 {
		t.Fatalf("unexpected number of items in collection, expected 2, got %v %s", len(collectionResult), asJSON(collectionResult))
	}
	if collectionResult[0].Foo != "foo_1" {
		t.Fatal("wrong item in collection:", collectionResult[0].Foo)
	}

	// Search in unknown json document
	_, err = testService.client.RawGet("/as?filter=foo2=foo_1", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 0 {
		t.Fatalf("unexpected number of items in collection, expected 0, got %v %s", len(collectionResult), asJSON(collectionResult))
	}

	// Search in json document and searchable properties
	_, err = testService.client.RawGet("/as?filter=foo=foo_0&filter=searchable_prop=searchable_prop_0", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 2 {
		t.Fatalf("unexpected number of items in collection, expected 1, got %v %s", len(collectionResult), asJSON(collectionResult))
	}
	if collectionResult[0].Foo != "foo_0" {
		t.Fatal("wrong item in collection:", collectionResult[0].Foo)
	}
	if collectionResult[0].SearchableProp != "searchable_prop_0" {
		t.Fatal("wrong item in collection:", collectionResult[0].SearchableProp)
	}
}

// TestSearchEqualAfterSchemaUpdate test searching in searchable_properties and in json properties
// after converting a static property into a searchable property in a schema update
func TestSearchEqualAfterSchemaUpdate(t *testing.T) {
	jsonConfigBefore := `{
	"collections": [
	  {
		"resource": "a",
		"external_index": "external_id",
		"static_properties": ["static_prop", "searchable_prop"]
	  }
	],
	"singletons": [],
	"blobs": [],
	"shortcuts": []
  }
`
	testServiceBefore := CreateTestService(jsonConfigBefore, t.Name())
	defer testServiceBefore.Db.Close()

	numberOfElements := 16
	for i := 0; i < numberOfElements; i++ {
		_, err := testServiceBefore.client.WithAdminAuthorization().RawPost("/as",
			A{
				ExternalID:     "external_id_" + strconv.Itoa(i),
				SearchableProp: "searchable_prop_" + strconv.Itoa(i%2),
				StaticProp:     "static_prop_" + strconv.Itoa(i%4),
				Foo:            "foo_" + strconv.Itoa(i%8),
			}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	var collectionResult []A
	// Search in searchable_properties. We use search not filter so we know that the search is done with an index
	status, err := testServiceBefore.client.RawGet("/as?search=searchable_prop=searchable_prop_1", &collectionResult)
	if err == nil || status != http.StatusBadRequest || len(collectionResult) > 0 {
		t.Fatal("expected this to fail, but it did not")
	}

	jsonConfig := `{
		"collections": [
		  {
			"resource": "a",
			"external_index": "external_id",
			"static_properties": ["static_prop"],
			"searchable_properties": ["searchable_prop"]
		  }
		],
		"singletons": [],
		"blobs": [],
		"shortcuts": []
	  }
	`

	testService := UpdateTestService(jsonConfig, t.Name())
	defer testService.Db.Close()

	// Search in external_index
	_, err = testService.client.RawGet("/as?filter=external_id=external_id_1", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 1 {
		t.Fatal("unexpected number of items in collection, expected 1, got", asJSON(collectionResult))
	}
	if collectionResult[0].ExternalID != "external_id_1" {
		t.Fatal("wrong item in collection:", collectionResult[0].ExternalID)
	}

	// Search in searchable_properties. We use search not filter so we know that the search is done with an index
	_, err = testService.client.RawGet("/as?search=searchable_prop=searchable_prop_1", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 8 {
		t.Fatalf("unexpected number of items in collection, expected 8, got %v %s", len(collectionResult), asJSON(collectionResult))
	}
	if collectionResult[0].SearchableProp != "searchable_prop_1" {
		t.Fatal("wrong item in collection:", collectionResult[0].SearchableProp)
	}

	// Search in json document
	_, err = testService.client.RawGet("/as?filter=foo=foo_1", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 2 {
		t.Fatalf("unexpected number of items in collection, expected 2, got %v %s", len(collectionResult), asJSON(collectionResult))
	}
	if collectionResult[0].Foo != "foo_1" {
		t.Fatal("wrong item in collection:", collectionResult[0].Foo)
	}

	// Search in unknown json document
	_, err = testService.client.RawGet("/as?filter=foo2=foo_1", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 0 {
		t.Fatalf("unexpected number of items in collection, expected 0, got %v %s", len(collectionResult), asJSON(collectionResult))
	}

	// Search in json document and searchable properties
	_, err = testService.client.RawGet("/as?filter=foo=foo_0&filter=searchable_prop=searchable_prop_0", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 2 {
		t.Fatalf("unexpected number of items in collection, expected 1, got %v %s", len(collectionResult), asJSON(collectionResult))
	}
	if collectionResult[0].Foo != "foo_0" {
		t.Fatal("wrong item in collection:", collectionResult[0].Foo)
	}
	if collectionResult[0].SearchableProp != "searchable_prop_0" {
		t.Fatal("wrong item in collection:", collectionResult[0].SearchableProp)
	}
}

// TestAddPropertyInSchemaUpdate test adding a complely new searchable property in a schema update
func TestAddPropertyInSchemaUpdate(t *testing.T) {
	jsonConfigBefore := `{
		"collections": [
		  {
			"resource": "a"
		  }
		],
		"singletons": [],
		"blobs": [],
		"shortcuts": []
	  }
	`
	testServiceBefore := CreateTestService(jsonConfigBefore, t.Name())
	defer testServiceBefore.Db.Close()

	jsonConfig := `{
		"collections": [
		  {
			"resource": "a",
			"searchable_properties": ["searchable_prop"]
		  }
		],
		"singletons": [],
		"blobs": [],
		"shortcuts": []
	  }
	`

	testService := UpdateTestService(jsonConfig, t.Name())
	defer testService.Db.Close()

	numberOfElements := 16
	for i := 0; i < numberOfElements; i++ {
		_, err := testService.client.WithAdminAuthorization().RawPost("/as",
			A{
				SearchableProp: "searchable_prop_" + strconv.Itoa(i%2),
			}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	var collectionResult []A

	// Search in searchable_properties
	_, err := testService.client.RawGet("/as?search=searchable_prop=searchable_prop_1", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 8 {
		t.Fatalf("unexpected number of items in collection, expected 8, got %v %s", len(collectionResult), asJSON(collectionResult))
	}
	if collectionResult[0].SearchableProp != "searchable_prop_1" {
		t.Fatal("wrong item in collection:", collectionResult[0].SearchableProp)
	}
}

func TestSearchPattern(t *testing.T) {
	jsonConfig := `{
	"collections": [
	  {
		"resource": "a",
		"external_index": "external_id",
		"static_properties": ["static_prop"],
		"searchable_properties": ["searchable_prop"]
	  } 
	],
	"singletons": [],
	"blobs": [],
	"shortcuts": []
  }
`
	testService := CreateTestService(jsonConfig, t.Name())
	defer testService.Db.Close()

	numberOfElements := 16
	for i := 0; i < numberOfElements; i++ {
		_, err := testService.client.WithAdminAuthorization().RawPost("/as",
			A{
				ExternalID:     "external_id_" + strconv.Itoa(i),
				SearchableProp: "searchable_prop_" + strconv.Itoa(i%2),
				Foo:            "foo_" + strconv.Itoa(i%8),
			}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	var collectionResult []A

	// Search in external_index
	_, err := testService.client.RawGet("/as?filter="+url.QueryEscape("external_id~%_id_1"), &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 1 {
		t.Fatalf("unexpected number of items in collection, expected 1, got %v %v", len(collectionResult), asJSON(collectionResult))
	}
	if collectionResult[0].ExternalID != "external_id_1" {
		t.Fatal("wrong item in collection:", collectionResult[0].ExternalID)
	}

	_, err = testService.client.RawGet("/as?filter="+url.QueryEscape("external_id~external%"), &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 16 {
		t.Fatal("unexpected number of items in collection, expected 16, got", asJSON(collectionResult))
	}

	// Search in json document
	_, err = testService.client.RawGet("/as?filter="+url.QueryEscape("foo~%o_1"), &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 2 {
		t.Fatalf("unexpected number of items in collection, expected 2, got %v %s", len(collectionResult), asJSON(collectionResult))
	}

	// Search in unknown json document
	_, err = testService.client.RawGet("/as?filter=foo2~foo_1", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 0 {
		t.Fatalf("unexpected number of items in collection, expected 0, got %v %s", len(collectionResult), asJSON(collectionResult))
	}

	// Search in json document and searchable properties
	_, err = testService.client.RawGet("/as?filter="+url.QueryEscape("foo~%o_0")+"&filter="+url.QueryEscape("searchable_prop~%_prop_0"), &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 2 {
		t.Fatalf("unexpected number of items in collection, expected 1, got %v %s", len(collectionResult), asJSON(collectionResult))
	}
	if collectionResult[0].Foo != "foo_0" {
		t.Fatal("wrong item in collection:", collectionResult[0].Foo)
	}
	if collectionResult[0].SearchableProp != "searchable_prop_0" {
		t.Fatal("wrong item in collection:", collectionResult[0].SearchableProp)
	}
}

func TestPatch(t *testing.T) {
	a := A{ExternalID: t.Name()}
	if _, err := testService.client.RawPost("/as", a, &a); err != nil {
		t.Fatal(err)
	}

	var result A
	status, err := testService.client.RawPatch("/as/"+a.AID.String(),
		map[string]string{"foo": "new_foo"}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK || result.Foo != "new_foo" {
		t.Fatalf("patch did not work")
	}

	// patching an unknown object should return 404 not found
	unknownID := uuid.New()
	status, err = testService.client.RawPatch("/as/"+unknownID.String(),
		map[string]string{"foo": "new_foo"}, &result)
	if err == nil || status != http.StatusNotFound {
		t.Fatalf("patch of unknown object did return %d", status)
	}

}

func TestCursorPaginationCollection(t *testing.T) {
	// Clear any existing data
	_, err := testService.client.RawDelete("/as")
	if err != nil {
		t.Fatal(err)
	}

	// Populate the DB with elements
	numberOfElements := 50
	timestamp := time.Now().UTC().Round(time.Millisecond)
	for i := 1; i <= numberOfElements; i++ {
		aNew := A{
			ExternalID: fmt.Sprintf("cursor_test_%d", i),
			Timestamp:  timestamp.Add(time.Duration(i) * time.Second),
		}

		if _, err := testService.client.RawPost("/as", &aNew, &A{}); err != nil {
			t.Fatal(err)
		}
	}

	// Test basic cursor pagination
	limit := 10
	var allReceived []A
	var nextToken string

	for page := 0; page < 6; page++ { // Get 6 pages to test beyond available data
		var path string
		if nextToken == "" {
			path = fmt.Sprintf("/as?limit=%d", limit)
		} else {
			path = fmt.Sprintf("/as?limit=%d&next_token=%s", limit, nextToken)
		}

		var as []A
		status, headers, err := testService.client.RawGetWithHeader(path, map[string]string{}, &as)
		if err != nil || status != http.StatusOK {
			t.Fatal("error: ", err, "status: ", status)
		}

		assert.Equal(t, strconv.Itoa(limit), headers.Get("Pagination-Limit"))

		allReceived = append(allReceived, as...)

		nextToken = headers.Get("Pagination-Next-Token")

		if len(as) < limit || nextToken == "" {
			break
		}
	}

	// Check that we got all elements
	if len(allReceived) != numberOfElements {
		t.Fatalf("Expected %d elements, got %d", numberOfElements, len(allReceived))
	}

	// Check that elements are ordered correctly (descending by timestamp)
	for i := 1; i < len(allReceived); i++ {
		if allReceived[i].Timestamp.After(allReceived[i-1].Timestamp) {
			t.Fatal("Results are not in descending timestamp order")
		}
	}
}

func TestCursorPaginationMutualExclusion(t *testing.T) {
	// Test that page and next_token are mutually exclusive
	cursor := backend.PaginationCursor{
		Timestamp: time.Now().UTC(),
		ID:        uuid.New(),
	}

	path := fmt.Sprintf("/as?page=2&next_token=%s", cursor.Encode())
	var as []A
	status, _, _ := testService.client.RawGetWithHeader(path, map[string]string{}, &as)

	if status != http.StatusBadRequest {
		t.Fatalf("Expected status 400 for mutually exclusive parameters, got %d", status)
	}
}

func TestCursorPaginationInvalidToken(t *testing.T) {
	// Test invalid cursor format
	path := "/as?next_token=invalid_token"
	var as []A
	status, _, _ := testService.client.RawGetWithHeader(path, map[string]string{}, &as)

	if status != http.StatusBadRequest {
		t.Fatalf("Expected status 400 for invalid cursor, got %d", status)
	}
}

func TestCursorPaginationCollectionWithOrdering(t *testing.T) {
	// Clear any existing data
	_, err := testService.client.RawDelete("/as")
	if err != nil {
		t.Fatal(err)
	}

	// Populate the DB with elements at specific times
	numberOfElements := 20
	baseTime := time.Now().UTC().Round(time.Millisecond)
	var createdElements []A

	for i := 1; i <= numberOfElements; i++ {
		aNew := A{
			ExternalID: fmt.Sprintf("order_test_%d", i),
			Timestamp:  baseTime.Add(time.Duration(i) * time.Second),
		}

		var result A
		if _, err := testService.client.RawPost("/as", &aNew, &result); err != nil {
			t.Fatal(err)
		}
		createdElements = append(createdElements, result)
	}

	// Test ascending order cursor pagination
	t.Run("ascending_order", func(t *testing.T) {
		limit := 7
		var allReceived []A
		var nextToken string

		for page := 0; page < 5; page++ {
			var path string
			if nextToken == "" {
				path = fmt.Sprintf("/as?limit=%d&order=asc", limit)
			} else {
				path = fmt.Sprintf("/as?limit=%d&order=asc&next_token=%s", limit, nextToken)
			}

			var as []A
			status, headers, err := testService.client.RawGetWithHeader(path, map[string]string{}, &as)
			if err != nil || status != http.StatusOK {
				t.Fatal("error: ", err, "status: ", status)
			}

			allReceived = append(allReceived, as...)
			nextToken = headers.Get("Pagination-Next-Token")

			if len(as) < limit || nextToken == "" {
				break
			}
		}

		// Check that we got all elements
		assert.Equal(t, numberOfElements, len(allReceived))

		// Check ascending order (timestamp should increase)
		for i := 1; i < len(allReceived); i++ {
			assert.True(t, allReceived[i].Timestamp.After(allReceived[i-1].Timestamp) || allReceived[i].Timestamp.Equal(allReceived[i-1].Timestamp),
				"Results are not in ascending timestamp order at index %d", i)
		}
		// Verify that all created elements are present in the result
		assert.Equal(t, len(createdElements), len(allReceived))
		for i := 0; i < len(createdElements); i++ {
			found := false
			for j := 0; j < len(allReceived); j++ {
				if createdElements[i].AID == allReceived[j].AID {
					found = true
					break
				}
			}
			assert.True(t, found, "Created element with ID %s not found in paginated results", createdElements[i].AID)
		}
	})

	// Test descending order cursor pagination
	t.Run("descending_order", func(t *testing.T) {
		limit := 6
		var allReceived []A
		var nextToken string

		for page := 0; page < 5; page++ {
			var path string
			if nextToken == "" {
				path = fmt.Sprintf("/as?limit=%d&order=desc", limit)
			} else {
				path = fmt.Sprintf("/as?limit=%d&order=desc&next_token=%s", limit, nextToken)
			}

			var as []A
			status, headers, err := testService.client.RawGetWithHeader(path, map[string]string{}, &as)
			if err != nil || status != http.StatusOK {
				t.Fatal("error: ", err, "status: ", status)
			}

			allReceived = append(allReceived, as...)
			nextToken = headers.Get("Pagination-Next-Token")

			if len(as) < limit || nextToken == "" {
				break
			}
		}

		// Check that we got all elements
		assert.Equal(t, numberOfElements, len(allReceived))

		// Check descending order (timestamp should decrease)
		for i := 1; i < len(allReceived); i++ {
			assert.True(t, allReceived[i].Timestamp.Before(allReceived[i-1].Timestamp) || allReceived[i].Timestamp.Equal(allReceived[i-1].Timestamp),
				"Results are not in descending timestamp order at index %d", i)
		}
		// Verify that all created elements are present in the result in reverse order
		assert.Equal(t, len(createdElements), len(allReceived))
		for i := 0; i < len(createdElements); i++ {
			// In descending order, the first element should be the last created element
			expectedIndex := len(createdElements) - 1 - i
			assert.Equal(t, createdElements[expectedIndex].AID, allReceived[i].AID,
				"Elements are not in reverse order at index %d", i)
		}
	})

	// Cleanup
	_, err = testService.client.RawDelete("/as")
	if err != nil {
		t.Fatal(err)
	}
}

func TestCursorPaginationCollectionEmptyCollection(t *testing.T) {
	// Clear any existing data
	_, err := testService.client.RawDelete("/as")
	if err != nil {
		t.Fatal(err)
	}

	// Test cursor pagination on empty collection
	var as []A
	status, headers, err := testService.client.RawGetWithHeader("/as?limit=10", map[string]string{}, &as)
	if err != nil || status != http.StatusOK {
		t.Fatal("error: ", err, "status: ", status)
	}

	assert.Equal(t, 0, len(as))
	assert.Equal(t, "10", headers.Get("Pagination-Limit"))
	assert.Empty(t, headers.Get("Pagination-Next-Token"))
}

func TestCursorPaginationCollectionWithTimeFiltering(t *testing.T) {
	// Clear any existing data
	_, err := testService.client.RawDelete("/as")
	if err != nil {
		t.Fatal(err)
	}

	beforeTime := time.Now().UTC().Add(-2 * time.Second) // Give more time buffer
	time.Sleep(10 * time.Millisecond)

	// Create some elements
	for i := 0; i < 8; i++ {
		aNew := A{
			ExternalID: fmt.Sprintf("time_filter_test_%d", i),
		}

		if _, err := testService.client.RawPost("/as", &aNew, &A{}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond) // Longer delay between elements
	}

	time.Sleep(10 * time.Millisecond)
	afterTime := time.Now().UTC().Add(2 * time.Second) // Give more time buffer

	// Test cursor pagination with 'from' filter
	var as []A
	fromPath := fmt.Sprintf("/as?from=%s&limit=5", beforeTime.Format(time.RFC3339))
	status, headers, err := testService.client.RawGetWithHeader(fromPath, map[string]string{}, &as)
	if err != nil || status != http.StatusOK {
		t.Fatal("error: ", err, "status: ", status)
	}

	assert.Equal(t, 5, len(as))
	assert.Equal(t, "5", headers.Get("Pagination-Limit"))
	// Should have next token since we limited to 5 but have 8 elements
	assert.NotEmpty(t, headers.Get("Pagination-Next-Token"))

	// Test cursor pagination with 'until' filter
	untilPath := fmt.Sprintf("/as?until=%s&limit=10", afterTime.Format(time.RFC3339))
	status, headers, err = testService.client.RawGetWithHeader(untilPath, map[string]string{}, &as)
	if err != nil || status != http.StatusOK {
		t.Fatal("error: ", err, "status: ", status)
	}

	assert.Equal(t, 8, len(as))
	assert.Equal(t, "10", headers.Get("Pagination-Limit"))
	// Should not have next token since we got all available elements
	assert.Empty(t, headers.Get("Pagination-Next-Token"))

	// Cleanup
	_, err = testService.client.RawDelete("/as")
	if err != nil {
		t.Fatal(err)
	}
}

func TestCursorPaginationWithDeletionAndReplacement(t *testing.T) {
	// Clear any existing data
	_, err := testService.client.RawDelete("/as")
	if err != nil {
		t.Fatal(err)
	}

	// Create 100 elements with known IDs
	numberOfElements := 100
	baseTime := time.Now().UTC().Round(time.Millisecond)
	var initialElements []A

	for i := 1; i <= numberOfElements; i++ {
		aNew := A{
			ExternalID: fmt.Sprintf("initial_%d", i),
			Timestamp:  baseTime.Add(time.Duration(i) * time.Second),
		}

		var result A
		if _, err := testService.client.RawPost("/as", &aNew, &result); err != nil {
			t.Fatal(err)
		}
		initialElements = append(initialElements, result)
	}

	// Test with ascending order
	t.Run("ascending_order", func(t *testing.T) {
		testCursorPaginationWithDeletionAndReplacementHelper(t, "asc", initialElements, "asc")

		// Clean up after ascending test
		_, err = testService.client.RawDelete("/as")
		if err != nil {
			t.Fatal(err)
		}

		// Recreate the initial elements for the descending test
		for i := 1; i <= numberOfElements; i++ {
			aNew := A{
				ExternalID: fmt.Sprintf("initial_%d", i),
				Timestamp:  baseTime.Add(time.Duration(i) * time.Second),
			}

			var result A
			if _, err := testService.client.RawPost("/as", &aNew, &result); err != nil {
				t.Fatal(err)
			}
		}
	})

	// Test with descending order
	t.Run("descending_order", func(t *testing.T) {
		testCursorPaginationWithDeletionAndReplacementHelper(t, "desc", initialElements, "desc")
	})

}

func testCursorPaginationWithDeletionAndReplacementHelper(t *testing.T, order string, initialElements []A, testPrefix string) {
	limit := 10
	var allFetchedElements []A
	var nextToken string
	fetchedInitialIDs := make(map[string]bool)
	replacementCounter := 1

	for {
		// Construct the path with appropriate parameters
		var path string
		if nextToken == "" {
			path = fmt.Sprintf("/as?limit=%d&order=%s", limit, order)
		} else {
			path = fmt.Sprintf("/as?limit=%d&order=%s&next_token=%s", limit, order, nextToken)
		}

		// Fetch the current page
		var currentPage []A
		status, headers, err := testService.client.RawGetWithHeader(path, map[string]string{}, &currentPage)
		if err != nil || status != http.StatusOK {
			t.Fatalf("Failed to fetch page: error=%v, status=%d", err, status)
		}

		// Add all elements from current page to our tracking
		allFetchedElements = append(allFetchedElements, currentPage...)

		// Track which initial elements we've seen
		for _, element := range currentPage {
			if strings.HasPrefix(element.ExternalID, "initial_") {
				fetchedInitialIDs[element.ExternalID] = true
			}
		}

		// If we have elements in the current page, delete the last one and replace it
		if len(currentPage) > 0 {
			lastElement := currentPage[len(currentPage)-1]

			// Delete the first element
			deleteStatus, err := testService.client.RawDelete("/as/" + lastElement.AID.String())
			if err != nil || (deleteStatus != http.StatusOK && deleteStatus != http.StatusNoContent) {
				t.Fatalf("Failed to delete element %s: error=%v, status=%d", lastElement.AID, err, deleteStatus)
			}

			// Create a replacement element with the same timestamp but different ID
			ts := lastElement.Timestamp
			if order == "asc" {
				ts = ts.Add(-time.Microsecond)
			} else {
				ts = ts.Add(time.Microsecond)
			}

			replacementElement := A{
				ExternalID: fmt.Sprintf("replacement_%s_%d", testPrefix, replacementCounter),
				Timestamp:  ts, // Same timestamp as deleted element
			}
			replacementCounter++

			var result A
			if _, err := testService.client.RawPost("/as", &replacementElement, &result); err != nil {
				t.Fatalf("Failed to create replacement element: %v", err)
			}
		}

		nextToken = headers.Get("Pagination-Next-Token")

		if nextToken == "" {
			break
		}
	}

	// Verification: Ensure all initial elements were fetched
	for _, initialElement := range initialElements {
		if !fetchedInitialIDs[initialElement.ExternalID] {
			t.Errorf("Initial element with ExternalID %s was not fetched during pagination", initialElement.ExternalID)
		}
	}

	// Verify we fetched at least as many elements as we initially created
	// (possibly more due to replacements)
	if len(allFetchedElements) < len(initialElements) {
		t.Errorf("Expected to fetch at least %d elements, but fetched %d", len(initialElements), len(allFetchedElements))
	}

	// Verify ordering is maintained throughout the pagination
	if order == "asc" {
		for i := 1; i < len(allFetchedElements); i++ {
			if allFetchedElements[i].Timestamp.Before(allFetchedElements[i-1].Timestamp) {
				t.Errorf("Ascending order violated at index %d: %v should be >= %v",
					i, allFetchedElements[i].Timestamp, allFetchedElements[i-1].Timestamp)
			}
		}
	} else {
		for i := 1; i < len(allFetchedElements); i++ {
			if allFetchedElements[i].Timestamp.After(allFetchedElements[i-1].Timestamp) {
				t.Errorf("Descending order violated at index %d: %v should be <= %v",
					i, allFetchedElements[i].Timestamp, allFetchedElements[i-1].Timestamp)
			}
		}
	}

	// Verify that we do not have any replacement elements in the fetched results
	for _, element := range allFetchedElements {
		if strings.HasPrefix(element.ExternalID, "replacement_") {
			t.Errorf("Found replacement element with ExternalID %s in fetched results, but replacement elements should not appear in pagination", element.ExternalID)
		}
	}

	t.Logf("Successfully fetched %d elements (expected at least %d) in %s order",
		len(allFetchedElements), len(initialElements), order)
	t.Logf("Fetched %d out of %d initial elements", len(fetchedInitialIDs), len(initialElements))
}
