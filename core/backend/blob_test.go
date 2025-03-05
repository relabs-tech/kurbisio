// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend_test

import (
	"errors"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEtagGetBlob(t *testing.T) {
	blobData, err := os.ReadFile("./testdata/dalarubettrich.png")
	if err != nil {
		t.Fatal(err)
	}
	header := map[string]string{
		"Content-Type":       "image/png",
		"Kurbisio-Meta-Data": `{"hello":"world"}`,
	}
	b := Blob{}
	if _, err = testService.client.RawPostBlob("/blobs", header, blobData, &b); err != nil {
		t.Fatal(err)
	}

	_, firstHeader, err := testService.client.RawGetBlobWithHeader(
		"/blobs/"+b.BlobID.String(), map[string]string{}, &[]byte{})
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
			header = map[string]string{}
			if etag != "" {
				header = map[string]string{"If-None-Match": tc.etag}
			}
			var receivedBuffer []byte
			status, h, _ := testService.client.RawGetBlobWithHeader(
				"/blobs/"+b.BlobID.String(), header, &receivedBuffer)

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
	_, err = testService.client.RawDelete("/blobs") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}
}

func TestEtagGetBlobCollection(t *testing.T) {
	blobData, err := os.ReadFile("./testdata/dalarubettrich.png")
	if err != nil {
		t.Fatal(err)
	}
	header := map[string]string{
		"Content-Type":       "image/png",
		"Kurbisio-Meta-Data": `{"hello":"world"}`,
	}
	if _, err = testService.client.RawPostBlob("/blobs", header, blobData, &Blob{}); err != nil {
		t.Fatal(err)
	}

	_, h1, err := testService.client.RawGetWithHeader("/blobs", map[string]string{}, &[]Blob{})
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
			header = map[string]string{}
			if etag != "" {
				header = map[string]string{"If-None-Match": tc.etag}
			}
			var receivedBuffer []byte
			status, h, _ := testService.client.RawGetWithHeader("/blobs", header, &[]Blob{})

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
	_, err = testService.client.RawDelete("/blobs") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}
}

// TestEtagBlobRegenerated checks that if the binary data of a mutable blob is modified through a
// PUT request, the ETag is modified
func TestEtagBlobRegenerated(t *testing.T) {
	blobData, err := os.ReadFile("./testdata/dalarubettrich.png")
	assert.Nil(t, err)
	header := map[string]string{
		"Content-Type":       "image/png",
		"Kurbisio-Meta-Data": `{"hello":"world"}`,
	}
	b := Blob{}
	if _, err = testService.client.RawPostBlob("/blobs", header, blobData, &b); err != nil {
		t.Fatal(err)
	}

	_, h1, err := testService.client.RawGetBlobWithHeader(
		"/blobs/"+b.BlobID.String(), map[string]string{}, &[]byte{})
	if err != nil {
		t.Fatal(err)
	}

	for i := range blobData {
		blobData[i] = 0
	}
	if _, err = testService.client.RawPutBlob("/blobs/"+b.BlobID.String(), header, blobData, &b); err != nil {
		t.Fatal(err)
	}
	_, h2, err := testService.client.RawGetBlobWithHeader(
		"/blobs/"+b.BlobID.String(), map[string]string{}, &[]byte{})
	if err != nil {
		t.Fatal(err)
	}

	if h1.Get("ETag") == h2.Get("ETag") {
		t.Fatal("ETag was not updated: ", h2.Get("ETag"))
	}
	_, err = testService.client.RawDelete("/blobs") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}

}

// TestEtagBlobCollectionRegenerated checks that if another element is added to a collection through
// a POST request, then ETag is modified
func TestEtagBlobCollectionRegenerated(t *testing.T) {
	blobData, err := os.ReadFile("./testdata/dalarubettrich.png")
	assert.Nil(t, err)
	header := map[string]string{
		"Content-Type":       "image/png",
		"Kurbisio-Meta-Data": `{"hello":"world"}`,
	}
	if _, err = testService.client.RawPostBlob("/blobs", header, blobData, &Blob{}); err != nil {
		t.Fatal(err)
	}

	_, h1, err := testService.client.RawGetWithHeader("/blobs", map[string]string{}, &[]Blob{})
	if err != nil {
		t.Fatal(err)
	}

	if h1.Get("ETag") == "" {
		t.Fatal("ETag is not present in reponse's header from Get header")
	}

	if _, err = testService.client.RawPostBlob("/blobs", header, blobData, &Blob{}); err != nil {
		t.Fatal(err)
	}

	_, h2, err := testService.client.RawGetWithHeader("/blobs", map[string]string{}, &[]Blob{})
	if err != nil {
		t.Fatal(err)
	}
	if h1.Get("ETag") == h2.Get("ETag") {
		t.Fatal("ETag was not updated: ", h2.Get("ETag"))
	}
	_, err = testService.client.RawDelete("/blobs") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}

}

func TestBlobExternalID(t *testing.T) {
	type B3 struct {
		Blob
		ExternalID string `json:"external_id"`
	}

	blobData, err := os.ReadFile("./testdata/dalarubettrich.png")
	assert.Nil(t, err)
	header := map[string]string{
		"Content-Type":       "image/png",
		"Kurbisio-Meta-Data": `{"hello":"world"}`,
		"External-Id":        "1",
	}
	_, err = testService.client.RawPostBlob("/blob3s", header, blobData, &B3{})
	assert.Nil(t, err)
	status, err := testService.client.RawPostBlob("/blob3s", header, blobData, &B3{})
	assert.Equal(t, http.StatusConflict, status, err)
}

func TestFiltersBlob(t *testing.T) {

	blobData := []byte{0, 1}
	header := map[string]string{
		"Content-Type":       "image/png",
		"Kurbisio-Meta-Data": `{"hello":"world"}`,
	}
	blob := Blob{}
	var err error
	if _, err = testService.client.RawPostBlob("/blobs", header, blobData, &blob); err != nil {
		t.Fatal(err)
	}

	if _, err = testService.client.RawPostBlob("/blobs", map[string]string{}, blobData, nil); err != nil {
		t.Fatal(err)
	}

	var collectionResult []Blob
	// we now search for the searchable property and should only find our single item a
	_, err = testService.client.RawGet("/blobs?filter=content_type=image/png", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 1 {
		t.Fatal("unexpected number of items in collection, expected only 1:", asJSON(collectionResult))
	}
	if collectionResult[0].BlobID != blob.BlobID {
		t.Fatal("wrong item in collection:", asJSON(collectionResult))
	}

	_, err = testService.client.RawDelete("/blobs") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}
}

func TestClearBlob(t *testing.T) {
	blobData := []byte{0, 1}
	header := map[string]string{
		"Content-Type":       "image/png",
		"Kurbisio-Meta-Data": `{"hello":"world"}`,
	}
	if _, err := testService.client.RawPostBlob("/blobs", header, blobData, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := testService.client.RawPostBlob("/blobs", header, blobData, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := testService.client.RawPostBlob("/blob2s", header, blobData, nil); err != nil {
		t.Fatal(err)
	}

	_, err := testService.client.RawDelete("/blobs") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}

	var collectionResult []Blob
	// All blobs should be deleted
	_, err = testService.client.RawGet("/blobs", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 0 {
		t.Fatalf("Expecting blobs to be cleared but there is still %d items", len(collectionResult))
	}

	// blob2 should stay untouched
	_, err = testService.client.RawGet("/blob2s", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 1 {
		t.Fatalf("Expecting blobs2 to be still there but there is %d items", len(collectionResult))
	}

	// Then we clean up anyway
	_, err = testService.client.RawDelete("/blob2s") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}

	// We now create blobs for two separate owners and clear only all the blob of one owner
	a1 := A{}
	a2 := A{}
	_, err = testService.client.RawPost("/as", &a1, &a1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = testService.client.RawPost("/as", &a2, &a2)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := testService.client.RawPostBlob("/as/"+a1.AID.String()+"/blobs", header, blobData, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := testService.client.RawPostBlob("/as/"+a2.AID.String()+"/blobs", header, blobData, nil); err != nil {
		t.Fatal(err)
	}

	// Then we clear a1's blobs
	_, err = testService.client.RawDelete("/as/" + a1.AID.String() + "/blobs") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}
	// All a1's blobs should be deleted
	_, err = testService.client.RawGet("/as/"+a1.AID.String()+"/blobs", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 0 {
		t.Fatalf("Expecting blobs to be cleared but there is still %d items", len(collectionResult))
	}
	// a2's blobs should NOT be deleted
	_, err = testService.client.RawGet("/as/"+a2.AID.String()+"/blobs", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 1 {
		t.Fatalf("Expecting a2's blobs to still be there but there are %d items", len(collectionResult))
	}
	_, err = testService.client.RawDelete("/blobs") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}

}

func TestDeleteBlob(t *testing.T) {
	blobData := []byte{0, 1}
	header := map[string]string{
		"Content-Type":       "image/png",
		"Kurbisio-Meta-Data": `{"hello":"world"}`,
	}
	blob1 := Blob{}
	if _, err := testService.client.RawPostBlob("/blobs", header, blobData, &blob1); err != nil {
		t.Fatal(err)
	}
	blob2 := Blob{}
	if _, err := testService.client.RawPostBlob("/blobs", header, blobData, &blob2); err != nil {
		t.Fatal(err)
	}

	_, err := testService.client.RawDelete("/blobs/" + blob2.BlobID.String()) // clear entire collection
	if err != nil {
		t.Fatal(err)
	}

	var collectionResult []Blob
	// we now search for the searachable property and should only find our single item a
	_, err = testService.client.RawGet("/blobs", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 1 {
		t.Fatalf("Expecting one blob but there is %d items", len(collectionResult))
	}
	if collectionResult[0].BlobID != blob1.BlobID {
		t.Fatalf("Expecting the remaining ID to be %s, but was %s", blob1.BlobID, collectionResult[0].BlobID)
	}

	_, err = testService.client.RawDelete("/blobs") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}

}

func TestBlobExes(t *testing.T) {
	blobData := []byte{0, 1}
	header := map[string]string{
		"Content-Type":       "image/png",
		"Kurbisio-Meta-Data": `{"hello":"world"}`,
	}
	// We now create blobs for two separate owners and clear only all the blob of one owner
	a1 := A{}
	a2 := A{}
	_, err := testService.client.RawPost("/as", &a1, &a1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = testService.client.RawPost("/as", &a2, &a2)
	if err != nil {
		t.Fatal(err)
	}

	b1 := BlobEx{}
	if _, err := testService.client.RawPostBlob("/as/"+a1.AID.String()+"/blobexes", header, blobData, &b1); err != nil {
		t.Fatal(err)
	}
	// check blob is stored
	status, _, err := testService.client.RawGetBlobWithHeader(
		"/as/"+a1.AID.String()+"/blobexes/"+b1.BlobExID.String(), map[string]string{}, &[]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatal(status)
	}
	// check kss file exists
	if _, err := os.Stat("kssdata/a_id/" + a1.AID.String() + "/blobex_id/" + b1.BlobExID.String() + "/file"); errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}

	b2 := BlobEx{}
	if _, err := testService.client.RawPostBlob("/as/"+a2.AID.String()+"/blobexes", header, blobData, &b2); err != nil {
		t.Fatal(err)
	}
	// check blob is stored
	status, _, err = testService.client.RawGetBlobWithHeader(
		"/as/"+a2.AID.String()+"/blobexes/"+b2.BlobExID.String(), map[string]string{}, &[]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatal(status)
	}
	// check kss file exists
	if _, err := os.Stat("kssdata/a_id/" + a2.AID.String() + "/blobex_id/" + b2.BlobExID.String() + "/file"); errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}

	var collectionResult []Blob

	// Then we clear a1's blobs
	_, err = testService.client.RawDelete("/as/" + a1.AID.String() + "/blobexes") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}
	// All a1's blobs should be deleted
	_, err = testService.client.RawGet("/as/"+a1.AID.String()+"/blobexes", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 0 {
		t.Fatalf("Expecting blobexes to be cleared but there is still %d items", len(collectionResult))
	}

	// a1's kss file should be deleted
	if _, err := os.Stat("kssdata/a_id/" + a1.AID.String() + "/blobex_id/" + b1.BlobExID.String() + "/file"); err == nil {
		t.Fatalf("Expecting kss file to be deleted, but still exists")
	}

	// a2's blobs should NOT be deleted
	_, err = testService.client.RawGet("/as/"+a2.AID.String()+"/blobexes", &collectionResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(collectionResult) != 1 {
		t.Fatalf("Expecting a2's blobs to still be there but there are %d items", len(collectionResult))
	}
	// a2's kss file should still exists
	if _, err := os.Stat("kssdata/a_id/" + a2.AID.String() + "/blobex_id/" + b2.BlobExID.String() + "/file"); errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}

	_, err = testService.client.RawDelete("/as/all/blobexes") // clear entire collection
	if err != nil {
		t.Fatal(err)
	}
	// a2's kss file should be deleted
	if _, err := os.Stat("kssdata/a_id/" + a2.AID.String() + "/blobex_id/" + b2.BlobExID.String() + "/file"); err == nil {
		t.Fatalf("Expecting kss file to be deleted, but still exists")
	}

}
