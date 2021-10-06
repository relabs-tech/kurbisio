package kss_test

import (
	"net/url"
	"os"
	"testing"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/relabs-tech/backends/core/backend/kss"
	"github.com/relabs-tech/backends/core/client"
)

func Test_Local_PresignedURL_PostGet(t *testing.T) {
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
	cl := client.NewWithRouter(router)

	test_PresignedURL_PostGet(t, f, cl)
}

func Test_Local_Delete(t *testing.T) {
	// Test that a file can be deleted
	router := mux.NewRouter()
	u, err := url.Parse("")
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
	cl := client.NewWithRouter(router)
	test_Delete(t, f, cl)

}
