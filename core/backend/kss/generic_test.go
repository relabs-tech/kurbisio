package kss_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/relabs-tech/kurbisio/core/backend/kss"
	"github.com/relabs-tech/kurbisio/core/client"
)

func test_PresignedURL_PostGet(t *testing.T, driver kss.Driver, cl client.Client) {
	// Test that to upload data can be done using signed URL

	key := "some_key"
	// Push some data
	pushURL, err := driver.GetPreSignedURL(kss.Put, key, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	called := make(chan bool)
	driver.WithCallBack(func(e kss.FileUpdatedEvent) error {
		if e.Key != key {
			return nil
		}
		if e.Type != "uploaded" {
			t.Fatalf("Expecting '%v' got '%v'", "uploaded", e.Type)
		}
		if e.Size != 3 {
			t.Fatalf("Expecting '%v' got '%v'", 3, e.Size)
		}
		if e.Etags == "" {
			t.Fatalf("Expecting '%v' got '%v'", "something", e.Etags)
		}
		called <- true
		return nil
	})

	_, err = cl.RawPut(pushURL, []byte("123"), nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-time.After(120 * time.Second):
		t.Fatal("Timeout waiting for event to be received")
	case <-called:
	}

	// Now try to read the data
	getURL, err := driver.GetPreSignedURL(kss.Get, key, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	var data []byte
	_, _, err = cl.RawGetBlobWithHeader(getURL, map[string]string{}, &data)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "123" {
		t.Fatalf("Expecting %v got '%v'", "123", string(data))
	}

	// Check that if we taint the URL, we are not authorized
	pushURL, err = driver.GetPreSignedURL(kss.Put, "some other key", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	tainted, err := url.Parse(pushURL)
	if err != nil {
		t.Fatal(err)
	}
	v := tainted.Query()
	v.Set("key", "another_key")
	tainted.RawQuery = v.Encode()
	status, _ := cl.RawPut(tainted.String(), []byte("123"), nil)

	if status != http.StatusForbidden {
		t.Fatalf("Expecting %v got '%v'", http.StatusForbidden, status)
	}

	// Check that if the URL is expired, we are not authorized
	pushURL, err = driver.GetPreSignedURL(kss.Put, key, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Millisecond)
	status, _ = cl.RawPut(pushURL, []byte("123"), nil)
	if status != http.StatusForbidden {
		t.Fatalf("Expecting %v got '%v'", http.StatusForbidden, status)
	}

	// Check that if we get a pre sign URL for Get, we cannot Post with it
	pushURL, err = driver.GetPreSignedURL(kss.Get, key, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	status, _ = cl.PostMultipart(pushURL, []byte("123"))
	if status != http.StatusForbidden {
		t.Fatalf("Expecting %v got '%v'", http.StatusForbidden, status)
	}

	err = driver.Delete("another_key")
	if err != nil {
		t.Fatal(err)
	}
	err = driver.Delete("some_key")
	if err != nil {
		t.Fatal(err)
	}

}

func test_Delete(t *testing.T, driver kss.Driver, cl client.Client) {
	// Test that a file can be deleted

	key := "some_key"
	// Push some data
	pushURL, err := driver.GetPreSignedURL(kss.Put, key, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	_, err = cl.PostMultipart(pushURL, []byte("123"))
	if err != nil {
		t.Fatal(err)
	}

	// Now try to read the data
	getURL, err := driver.GetPreSignedURL(kss.Get, key, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	var data []byte
	status, _, _ := cl.RawGetBlobWithHeader(getURL, map[string]string{}, &data)
	if status != http.StatusOK {
		t.Fatalf("Expecting %v got '%v'", http.StatusOK, status)
	}

	err = driver.Delete(key)
	if err != nil {
		t.Fatal(err)
	}
	status, _, _ = cl.RawGetBlobWithHeader(getURL, map[string]string{}, &data)
	if status != http.StatusNotFound {
		t.Fatalf("Expecting %v got '%v'", http.StatusNotFound, status)
	}
}
