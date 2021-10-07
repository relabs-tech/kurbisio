package backend_test

import (
	"testing"

	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/backends/core/backend/kss"
)

var s3Credentials kss.S3Credentials

func TestCompanion_S3(t *testing.T) {
	if err := envdecode.Decode(&s3Credentials); err != nil {
		panic(err)
	}
	driversTypes := []kss.DriverType{kss.DriverTypeLocal}
	if s3Credentials.AccessID != "" || s3Credentials.AccessKey != "" {
		driversTypes = append(driversTypes, kss.DriverTypeAWSS3)
	}
	testCompanion(t, kss.DriverTypeAWSS3)
}

func TestCompanion_Delete_S3(t *testing.T) {
	if err := envdecode.Decode(&s3Credentials); err != nil {
		panic(err)
	}
	testCompanion_Delete(t, kss.DriverTypeAWSS3)
}

func TestCompanion_Notifications_S3(t *testing.T) {
	if err := envdecode.Decode(&s3Credentials); err != nil {
		panic(err)
	}
	testCompanion_Notifications(t, kss.DriverTypeAWSS3)
}
