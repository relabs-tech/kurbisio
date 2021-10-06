//go:build long_s3_tests

package kss_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/relabs-tech/backends/core/backend/kss"
)

func Test_S3_LongListAllWithPrefix_DeleteAllWithPrefix(t *testing.T) {
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

	for n := 0; n < 1100; n++ {
		err = s.UploadData("key/"+strconv.Itoa(n), []byte{1, 2, 3})
		if err != nil {
			t.Fatal(err)
		}
	}
	keys, err := s.ListAllWithPrefix("key/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1100 {
		t.Fatalf("Expecting %v, got %v", 1100, len(keys))
	}

	err = s.DeleteAllWithPrefix("key/")
	if err != nil {
		t.Fatal(err)
	}

	keys, err = s.ListAllWithPrefix("key/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("Expecting %v, got %v", 0, len(keys))
	}
}
