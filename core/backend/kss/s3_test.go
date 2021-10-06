package kss_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/backends/core/backend/kss"
	"github.com/relabs-tech/backends/core/client"
)

func Test_S3Credential(t *testing.T) {
	if s3Credentials.AccessID == "" || s3Credentials.AccessKey == "" {
		t.Fatal("S3 tests require s3Credentials to be provided in environment variables")
	}
}

func TestMain(m *testing.M) {
	if err := envdecode.Decode(&s3Credentials); err != nil {
		panic(err)
	}
	m.Run()
}

var s3Credentials kss.S3Credentials

func Test_S3_PresignedURL_PostGet(t *testing.T) {
	// Test that to upload data can be done using signed URL

	s, err := kss.NewS3(kss.S3Configuration{
		AccessID:      s3Credentials.AccessID,
		AccessKey:     s3Credentials.AccessKey,
		AWSBucketName: "kss-test",
		AWSRegion:     "eu-central-1",
		KeyPrefix:     t.Name() + time.Now().Format(time.RFC3339) + "/",
	})
	if err != nil {
		t.Fatal(err)
	}
	cl := client.NewWithURL("")

	test_PresignedURL_PostGet(t, s, cl)
}

func Test_S3_Delete(t *testing.T) {

	s, err := kss.NewS3(kss.S3Configuration{
		AccessID:      s3Credentials.AccessID,
		AccessKey:     s3Credentials.AccessKey,
		AWSBucketName: "kss-test",
		AWSRegion:     "eu-central-1",
		KeyPrefix:     t.Name() + time.Now().Format(time.RFC3339) + "/",
	})
	if err != nil {
		t.Fatal(err)
	}
	cl := client.NewWithURL("")
	test_Delete(t, s, cl)
}

func Test_S3_DeleteAllWithPrefix(t *testing.T) {

	s, err := kss.NewS3(kss.S3Configuration{
		AccessID:      s3Credentials.AccessID,
		AccessKey:     s3Credentials.AccessKey,
		AWSBucketName: "kss-test",
		AWSRegion:     "eu-central-1",
		KeyPrefix:     t.Name() + time.Now().Format(time.RFC3339) + "/",
	})
	if err != nil {
		t.Fatal(err)
	}

	cl := client.NewWithURL("")
	test_DeleteAllWithPrefix(t, s, cl)
}

func Test_S3_ListAllWithPrefix_DeleteAllWithPrefix(t *testing.T) {
	s, err := kss.NewS3(kss.S3Configuration{
		AccessID:      s3Credentials.AccessID,
		AccessKey:     s3Credentials.AccessKey,
		AWSBucketName: "kss-test",
		AWSRegion:     "eu-central-1",
		KeyPrefix:     t.Name() + time.Now().Format(time.RFC3339) + "/",
	})
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
		t.Fatalf("Expecting %v, got %v", 1, len(keys))
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
