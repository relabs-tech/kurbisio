package kss

import "time"

// kss package provide fonctionality to store large file outside of the standard Kurbisio database.
// There is currently to possible backends: a local file system and AWS S3

// Driver defines the interface for the KSS service
type Driver interface {
	GetPreSignedURL(method, key string, expiry time.Time) (URL string, err error)
	Delete(key string) error
	DeleteAllWithPrefix(key string) error
}

// DriverType represents the different type of KSS Drivers
type DriverType string

// DriverTypeLocal is the local filesystem implementation of the KSS service
const DriverTypeLocal DriverType = "Local"

// DriverTypeAWSS3 is the AWS S3 implementation of the KSS service
const DriverTypeAWSS3 DriverType = "AWSS3"

// None is used when there is no KSS implementation
const None DriverType = ""

// Configuration contains the configuration for the KSS service
type Configuration struct {
	DriverType         DriverType
	LocalConfiguration *LocalConfiguration
}

// LocalConfiguration contains the configuration for the local filesystem KSS service
type LocalConfiguration struct {
	BasePath string
}
