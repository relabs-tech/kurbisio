package backend

import (
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEtagGetBlob(t *testing.T) {
	blobData, err := ioutil.ReadFile("./testdata/dalarubettrich.png")
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
				if h.Get(f) != h.Get(f) {
					t.Fatalf("Expected same headers field for %s, got: '%s' instead of '%s'", f, h.Get(f), firstHeader.Get(f))
				}
			}
		})
	}
}

func TestEtagGetBlobCollection(t *testing.T) {
	blobData, err := ioutil.ReadFile("./testdata/dalarubettrich.png")
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
}

// TestEtagBlobRegenerated checks that if the binary data of a mutable blob is modified through a
// PUT request, the ETag is modified
func TestEtagBlobRegenerated(t *testing.T) {
	blobData, err := ioutil.ReadFile("./testdata/dalarubettrich.png")
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
}

// TestEtagBlobCollectionRegenerated checks that if another element is added to a collection through
// a POST request, then ETag is modified
func TestEtagBlobCollectionRegenerated(t *testing.T) {
	blobData, err := ioutil.ReadFile("./testdata/dalarubettrich.png")
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
}

func TestBlobExternalID(t *testing.T) {
	type B3 struct {
		Blob
		ExternalID string `json:"external_id"`
	}

	blobData, err := ioutil.ReadFile("./testdata/dalarubettrich.png")
	header := map[string]string{
		"Content-Type":       "image/png",
		"Kurbisio-Meta-Data": `{"hello":"world"}`,
		"External-Id":        "1",
	}
	status, err := testService.client.RawPostBlob("/blob3s", header, blobData, &B3{})
	status, err = testService.client.RawPostBlob("/blob3s", header, blobData, &B3{})
	assert.Equal(t, http.StatusUnprocessableEntity, status, err)
}
