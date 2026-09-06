package kss

import (
	"crypto/rsa"
	"time"
)

// kss package provide fonctionality to store large file outside of the standard Kurbisio database.
// There is currently to possible backends: a local file system and AWS S3

// Driver defines the interface for the KSS service
type Driver interface {
	GetPreSignedURL(method Method, key string, expireIn time.Duration) (URL string, err error)
	Delete(key string) error
	DeleteAllWithPrefix(key string) error
	WithCallBack(FileUpdatedCallBack)
	UploadData(key string, data []byte) error
	DownloadData(key string) ([]byte, error)
}

// FileUpdatedEvent contains information about a file event
type FileUpdatedEvent struct {
	Type  string
	Key   string
	Etags string
	Size  int64
}

// FileUpdatedCallBack are called when a new file has been uploaded
type FileUpdatedCallBack func(event FileUpdatedEvent) error

// DriverType represents the different type of KSS Drivers
type DriverType string

// DriverTypeLocal is the local filesystem implementation of the KSS service
const DriverTypeLocal DriverType = "Local"

// DriverTypeAWSS3 is the AWS S3 implementation of the KSS service
const DriverTypeAWSS3 DriverType = "AWSS3"

// None is used when there is no KSS implementation
const None DriverType = ""

// Method is the type of methodes supported for signed URLs
type Method string

// Get is the Method to Get an object
const Get Method = "GET"

// Put is the Method to Put an object
const Put Method = "PUT"

// Configuration contains the configuration for the KSS service
type Configuration struct {
	DriverType         DriverType
	LocalConfiguration *LocalConfiguration
	S3Configuration    *S3Configuration
}

// LocalConfiguration contains the configuration for the local filesystem KSS service
type LocalConfiguration struct {
	KeyPrefix  string
	PublicURL  string
	PrivateKey *rsa.PrivateKey
}

// S3Configuration contains the configuration for the S3 KSS service
type S3Configuration struct {
	// AccessID is the ID to use when accessing the S3 bucket
	AccessID string

	// AccessKey is the Key to use when accessing the S3 bucket
	AccessKey string

	// The name of the bucket to use for storing files
	AWSBucketName string

	// The AWS region in which the bucket is located
	AWSRegion string

	// The prefix that will be added to add keys
	KeyPrefix string

	// SQSNotificationQueue defines the queue to listen to get event notification
	SQSNotificationQueue string

	// EndpointURL overrides the S3 endpoint, e.g. "http://localhost:9000" for MinIO.
	// It also serves as the SQS endpoint unless SQSEndpointURL is set. Empty means use real AWS.
	EndpointURL string

	// SQSEndpointURL overrides the SQS endpoint, e.g. "http://localhost:9324" for ElasticMQ.
	SQSEndpointURL string

	// UsePathStyle enables path-style S3 addressing (required for MinIO).
	UsePathStyle bool

	// AutoCreateBucket creates the S3 bucket and SQS queue on startup if they do not
	// already exist. Useful for local development. Defaults to false to
	// avoid accidental resource creation against real AWS.
	AutoCreateBucket bool

	// SkipBucketNotifications skips automatic S3-to-SQS notification and queue policy
	// configuration when AutoCreateBucket is enabled. Use this with MinIO and ElasticMQ,
	// which require an external notification bridge or explicitly published SQS events.
	SkipBucketNotifications bool
}

// S3Credentials contains S3 Credentials
type S3Credentials struct {
	AccessID  string `env:"S3_ACCESS_ID" description:"the access ID to kss-test bucket"`
	AccessKey string `env:"S3_ACCESS_KEY" description:"the access ID to kss-test bucket"`
}
