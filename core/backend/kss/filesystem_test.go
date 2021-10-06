package kss_test

import (
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/relabs-tech/backends/core/backend/kss"
	"github.com/relabs-tech/backends/core/client"
)

func Test_PresignedURL_PostGet(t *testing.T) {
	// Test that to upload data can be done using signed URL
	router := mux.NewRouter()
	u, err := url.Parse("https://localhost")
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	f, err := kss.NewLocalFilesystem(router, kss.LocalConfiguration{dir, nil}, *u)
	if err != nil {
		t.Fatal(err)
	}
	key := "some key"
	// Push some data
	pushURL, err := f.GetPreSignedURL(http.MethodPost, key, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	urlWithoutHost := strings.TrimPrefix(pushURL, "https://localhost")
	cl := client.NewWithRouter(router)

	_, err = cl.PostMultipart(urlWithoutHost, []byte("123"))
	if err != nil {
		t.Fatal(err)
	}
	// Now try to read the data
	getURL, err := f.GetPreSignedURL(http.MethodGet, key, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	urlWithoutHost = strings.TrimPrefix(getURL, "https://localhost")
	var data []byte
	_, _, err = cl.RawGetBlobWithHeader(urlWithoutHost, map[string]string{}, &data)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "123" {
		t.Fatalf("Expecting %v got '%v'", "123", string(data))
	}

	// Check that if we taint the URL, we are not authorized
	pushURL, err = f.GetPreSignedURL(http.MethodPost, "some other key", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	tainted, err := url.Parse(pushURL)
	if err != nil {
		t.Fatal(err)
	}
	v := tainted.Query()
	v.Set("key", "another key")
	tainted.RawQuery = v.Encode()
	urlWithoutHost = strings.TrimPrefix(tainted.String(), "https://localhost")
	status, err := cl.RawPost(urlWithoutHost, []byte("123"), nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting %v got '%v'", http.StatusUnauthorized, status)
	}

	// Check that if the URL is expired, we are not authorized
	pushURL, err = f.GetPreSignedURL(http.MethodPost, key, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	status, err = cl.RawPost(urlWithoutHost, []byte("123"), nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting %v got '%v'", http.StatusUnauthorized, status)
	}

	// Check that if we get a pre sign URL for Get, we cannot Post with it
	pushURL, err = f.GetPreSignedURL(http.MethodGet, key, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	urlWithoutHost = strings.TrimPrefix(pushURL, "https://localhost")
	status, err = cl.RawPost(urlWithoutHost, []byte("123"), nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("Expecting %v got '%v'", http.StatusUnauthorized, status)
	}
}

func Test_Delete(t *testing.T) {
	// Test that a file can be deleted
	router := mux.NewRouter()
	u, err := url.Parse("https://localhost")
	if err != nil {
		t.Fatal(err)
	}

	dir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir) // clean up

	f, err := kss.NewLocalFilesystem(router, kss.LocalConfiguration{dir, nil}, *u)
	if err != nil {
		t.Fatal(err)
	}
	key := "some key"
	// Push some data
	pushURL, err := f.GetPreSignedURL(http.MethodPost, key, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	urlWithoutHost := strings.TrimPrefix(pushURL, "https://localhost")
	cl := client.NewWithRouter(router)

	_, err = cl.PostMultipart(urlWithoutHost, []byte("123"))
	if err != nil {
		t.Fatal(err)
	}

	// Now try to read the data
	getURL, err := f.GetPreSignedURL(http.MethodGet, key, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	urlWithoutHost = strings.TrimPrefix(getURL, "https://localhost")
	var data []byte
	_, _, err = cl.RawGetBlobWithHeader(urlWithoutHost, map[string]string{}, &data)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "123" {
		t.Fatalf("Expecting %v got '%v'", "123", string(data))
	}

	err = f.Delete(key)
	if err != nil {
		t.Fatal(err)
	}
	status, _, err := cl.RawGetBlobWithHeader(urlWithoutHost, map[string]string{}, &data)
	if status != http.StatusNotFound {
		t.Fatalf("Expecting %v got '%v'", http.StatusNotFound, status)
	}
}
