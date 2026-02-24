// These tests require a running LocalStack instance.
// Start it with:
//
//	docker compose -f docker-compose.localstack.yml up -d
//
// Then run:
//
//	go test ./...

package kss_test

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/relabs-tech/kurbisio/core/backend/kss"
	"github.com/relabs-tech/kurbisio/core/client"
)

const (
	localStackEndpoint = "http://localhost:4566"
	localStackBucket   = "kss-test"
	localStackRegion   = "eu-central-1"
	localStackQueue    = "kss-test-queue"
)

// localStackConfig returns an S3Configuration pre-wired for LocalStack.
// AutoCreateBucket=true ensures the bucket and queue are created on first use.
func localStackConfig(keyPrefix string) kss.S3Configuration {
	hash := sha256.Sum256([]byte(keyPrefix))
	hashedPrefix := hex.EncodeToString(hash[:])
	queueName := hashedPrefix + localStackQueue
	return kss.S3Configuration{
		AccessID:             "test",
		AccessKey:            "test",
		AWSBucketName:        localStackBucket,
		AWSRegion:            localStackRegion,
		KeyPrefix:            keyPrefix,
		EndpointURL:          localStackEndpoint,
		UsePathStyle:         true,
		AutoCreateBucket:     true,
		SQSNotificationQueue: queueName,
	}
}

func Test_S3_PresignedURL_PutGet(t *testing.T) {
	s, err := kss.NewS3(localStackConfig(t.Name() + time.Now().Format("2006-01-0215.04.05.9.00") + "/"))
	if err != nil {
		t.Fatal(err)
	}
	cl := client.NewWithURL("")

	// NOTE: LocalStack (free tier) does not enforce presigned URL signatures, so the usual
	// URL-tampering and URL-expiry 403 checks from generic_test.go are skipped here.
	// This test covers the meaningful S3 + SQS behaviour: upload, callback delivery, download.

	key := "some_key"

	pushURL, err := s.GetPreSignedURL(kss.Put, key, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	called := make(chan bool, 1)
	s.WithCallBack(func(e kss.FileUpdatedEvent) error {
		if e.Key != key {
			return nil
		}
		if e.Type != "uploaded" {
			t.Errorf("Expecting 'uploaded' got '%v'", e.Type)
		}
		if e.Size != 3 {
			t.Errorf("Expecting size 3 got %v", e.Size)
		}
		if e.Etags == "" {
			t.Errorf("Expected a non-empty ETag")
		}
		called <- true
		return nil
	})

	_, err = cl.RawPut(pushURL, []byte("123"), nil)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-time.After(15 * time.Second):
		t.Fatal("Timeout waiting for SQS callback event")
	case <-called:
	}

	getURL, err := s.GetPreSignedURL(kss.Get, key, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var data []byte
	_, _, err = cl.RawGetBlobWithHeader(getURL, map[string]string{}, &data)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "123" {
		t.Fatalf("Expecting '123' got '%v'", string(data))
	}

	if err := s.Delete(key); err != nil {
		t.Fatal(err)
	}
	status, _, _ := cl.RawGetBlobWithHeader(getURL, map[string]string{}, &data)
	if status != http.StatusNotFound {
		t.Fatalf("After delete: expecting 404 got %v", status)
	}
}

func Test_S3_Delete(t *testing.T) {
	s, err := kss.NewS3(localStackConfig(t.Name() + time.Now().Format("2006-01-0215.04.05.9.00")))
	if err != nil {
		t.Fatal(err)
	}
	cl := client.NewWithURL("")
	test_Delete(t, s, cl)
}

func Test_S3_DeleteAllWithPrefix(t *testing.T) {
	s, err := kss.NewS3(localStackConfig(t.Name() + time.Now().Format("2006-01-0215.04.05.9.00") + "/"))
	if err != nil {
		t.Fatal(err)
	}
	cl := client.NewWithURL("")
	test_DeleteAllWithPrefix(t, s, cl)
}

func Test_S3_ListAllWithPrefix_DeleteAllWithPrefix(t *testing.T) {
	s, err := kss.NewS3(localStackConfig(t.Name() + time.Now().Format("2006-01-0215.04.05.9.00") + "/"))
	if err != nil {
		t.Fatal(err)
	}
	err = s.UploadData("key_to_not_delete", []byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < 3; n++ {
		err = s.UploadData("key/"+strconv.Itoa(n), []byte{1, 2, 3})
		if err != nil {
			t.Fatal(err)
		}
	}
	keys, err := s.ListAllWithPrefix("key/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 {
		t.Fatalf("Expecting %v, got %v", 3, len(keys))
	}

	keys, err = s.ListAllWithPrefix("")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 4 {
		t.Fatalf("Expecting %v, got %v", 4, len(keys))
	}

	err = s.DeleteAllWithPrefix("")
	if err != nil {
		t.Fatal(err)
	}

	keys, err = s.ListAllWithPrefix("")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("Expecting %v, got %v", 0, len(keys))
	}
}

func test_DeleteAllWithPrefix(t *testing.T, driver kss.Driver, cl client.Client) {
	var urls []string
	for _, key := range []string{"key/1", "key/2"} {
		pushURL, err := driver.GetPreSignedURL(kss.Put, key, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		_, err = cl.PostMultipart(pushURL, []byte("123"))
		if err != nil {
			t.Fatal(err)
		}

		getURL, err := driver.GetPreSignedURL(kss.Get, key, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		urls = append(urls, getURL)

		var data []byte
		_, _, err = cl.RawGetBlobWithHeader(getURL, map[string]string{}, &data)
		if err != nil {
			t.Fatal(err)
		}
	}

	err := driver.DeleteAllWithPrefix("key")
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range urls {
		var data []byte
		status, _, _ := cl.RawGetBlobWithHeader(u, map[string]string{}, &data)
		if status != http.StatusNotFound {
			t.Fatalf("Expecting %v got '%v'", http.StatusNotFound, status)
		}
	}
}
