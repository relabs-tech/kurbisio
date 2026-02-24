// These tests require running MinIO and ElasticMQ instances.
// Start them from the repository root with:
//
//	docker compose -f docker/docker-compose.local-aws.yml up -d
//
// Then run:
//
//	go test ./core/backend/kss -count 1

package kss_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/relabs-tech/kurbisio/core/backend/kss"
	"github.com/relabs-tech/kurbisio/core/client"
)

const (
	minioEndpoint     = "http://localhost:9000"
	elasticMQEndpoint = "http://localhost:9324"
	localBucket       = "kss-test"
	localRegion       = "eu-central-1"
)

// localS3Config creates buckets in MinIO. SQS is tested separately because MinIO
// cannot deliver bucket notifications directly to ElasticMQ.
func localS3Config(keyPrefix string) kss.S3Configuration {
	return kss.S3Configuration{
		AccessID:                "testuser",
		AccessKey:               "testpassword",
		AWSBucketName:           localBucket,
		AWSRegion:               localRegion,
		KeyPrefix:               keyPrefix,
		EndpointURL:             minioEndpoint,
		SQSEndpointURL:          elasticMQEndpoint,
		UsePathStyle:            true,
		AutoCreateBucket:        true,
		SkipBucketNotifications: true,
	}
}

func Test_S3_PresignedURL_PutGet(t *testing.T) {
	s, err := kss.NewS3(localS3Config(t.Name() + time.Now().Format("2006-01-0215.04.05.9.00") + "/"))
	if err != nil {
		t.Fatal(err)
	}
	cl := client.NewWithURL("")

	key := "some_key"

	pushURL, err := s.GetPreSignedURL(kss.Put, key, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	_, err = cl.RawPut(pushURL, []byte("123"), nil)
	if err != nil {
		t.Fatal(err)
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

	// MinIO validates signatures, including the object path and expiration.
	tampered, err := url.Parse(pushURL)
	if err != nil {
		t.Fatal(err)
	}
	tampered.Path += "-tampered"
	if status, _ := cl.RawPut(tampered.String(), []byte("123"), nil); status != http.StatusForbidden {
		t.Fatalf("Tampered URL: expecting 403 got %v", status)
	}
	expiredURL, err := s.GetPreSignedURL(kss.Put, key, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if status, _ := cl.RawPut(expiredURL, []byte("123"), nil); status != http.StatusForbidden {
		t.Fatalf("Expired URL: expecting 403 got %v", status)
	}
	if status, _ := cl.RawPut(getURL, []byte("123"), nil); status != http.StatusForbidden {
		t.Fatalf("Wrong HTTP method: expecting 403 got %v", status)
	}

	if err := s.Delete(key); err != nil {
		t.Fatal(err)
	}
	status, _, _ := cl.RawGetBlobWithHeader(getURL, map[string]string{}, &data)
	if status != http.StatusNotFound {
		t.Fatalf("After delete: expecting 404 got %v", status)
	}
}

// Test_S3_SQSNotification exercises queue creation and real ElasticMQ delivery.
// The event is published explicitly; this does not test an S3-to-SQS bridge.
func Test_S3_SQSNotification(t *testing.T) {
	cfg := localS3Config(t.Name() + "/")
	cfg.SQSNotificationQueue = "kss-test-" + uuid.NewString()
	s, err := kss.NewS3(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sqsClient := sqs.NewFromConfig(aws.Config{
		Region:      localRegion,
		Credentials: credentials.NewStaticCredentialsProvider("testuser", "testpassword", ""),
	}, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(elasticMQEndpoint)
	})
	queue, err := sqsClient.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{
		QueueName: aws.String(cfg.SQSNotificationQueue),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := sqsClient.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: queue.QueueUrl})
		if err != nil {
			t.Error(err)
		}
	})
	called := make(chan kss.FileUpdatedEvent, 1)
	s.WithCallBack(func(e kss.FileUpdatedEvent) error {
		called <- e
		return nil
	})
	// Presigning activates the queue listener, as it does for an external upload.
	if _, err := s.GetPreSignedURL(kss.Put, "some_key", time.Second); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(events.S3Event{Records: []events.S3EventRecord{{
		EventName: "ObjectCreated:Put",
		S3: events.S3Entity{
			Bucket: events.S3Bucket{Name: cfg.AWSBucketName},
			Object: events.S3Object{Key: cfg.KeyPrefix + "some_key", Size: 3, ETag: "test-etag"},
		},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqsClient.SendMessage(t.Context(), &sqs.SendMessageInput{
		QueueUrl: queue.QueueUrl, MessageBody: aws.String(string(body)),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-called:
		want := kss.FileUpdatedEvent{Type: "uploaded", Key: "some_key", Size: 3, Etags: "test-etag"}
		if e != want {
			t.Fatalf("Callback: got %+v, want %+v", e, want)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Timeout waiting for ElasticMQ callback event")
	}
}

func Test_S3_Delete(t *testing.T) {
	s, err := kss.NewS3(localS3Config(t.Name() + time.Now().Format("2006-01-0215.04.05.9.00")))
	if err != nil {
		t.Fatal(err)
	}
	cl := client.NewWithURL("")
	test_Delete(t, s, cl)
}

func Test_S3_DeleteAllWithPrefix(t *testing.T) {
	s, err := kss.NewS3(localS3Config(t.Name() + time.Now().Format("2006-01-0215.04.05.9.00") + "/"))
	if err != nil {
		t.Fatal(err)
	}
	cl := client.NewWithURL("")
	test_DeleteAllWithPrefix(t, s, cl)
}

func Test_S3_ListAllWithPrefix_DeleteAllWithPrefix(t *testing.T) {
	s, err := kss.NewS3(localS3Config(t.Name() + time.Now().Format("2006-01-0215.04.05.9.00") + "/"))
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
