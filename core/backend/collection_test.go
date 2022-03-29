// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend_test

import (
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"

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
		t.Fatal("ETag is not present in reponse's header from Get header")
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
		t.Fatal("ETag is not present in reponse's header from Get header")
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
	if _, err := testService.client.RawPost("/as", A{ExternalID: t.Name() + "1"}, &A{}); err != nil {
		t.Fatal(err)
	}

	_, h1, err := testService.client.RawGetWithHeader("/as", map[string]string{}, &[]A{})
	if err != nil {
		t.Fatal(err)
	}

	etag := h1.Get("ETag")
	if etag == "" {
		t.Fatal("ETag is not present in reponse's header from Get header")
	}

	if _, err := testService.client.RawPost("/as", A{ExternalID: t.Name() + "2"}, &A{}); err != nil {
		t.Fatal(err)
	}

	_, h2, err := testService.client.RawGetWithHeader("/as", map[string]string{}, &[]A{})
	if err != nil {
		t.Fatal(err)
	}

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
	// we now search for the searachable property and should only find our single item a
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

	// we now search for the searachable property and should only find our single item a
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

// TestStatistics verifies that the /kurbisio/statistics endpoint returns information about the backend
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
