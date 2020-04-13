package backend

import (
	"net/http"
	"testing"
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
			status, h, _ := testService.client.RawGetWithHeader("/as", header, &[]Blob{})

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

// TestEtagRegenerated checks that if the binary data of a mutable blob is modified through a
// PUT request, the ETag is modified
func TestEtagRegenerated(t *testing.T) {
	a := A{
		ExternalID: t.Name(),
		StaticProp: "a property",
	}
	if _, err := testService.client.RawPost("/as", a, &a); err != nil {
		t.Fatal(err)
	}

	_, h1, err := testService.client.RawGetWithHeader("/as", map[string]string{}, &[]A{})
	if err != nil {
		t.Fatal(err)
	}

	a.StaticProp = "a new property"
	if _, err = testService.client.RawPut("/as/"+a.AID.String(), a, &A{}); err != nil {
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
