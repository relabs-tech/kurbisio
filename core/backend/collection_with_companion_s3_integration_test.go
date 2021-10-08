//go:build integration

package backend_test

// These tests are time sensitive and sometimes failing because the SQS queue is slow
// to run these tests, execute" 'go test -tags=integration'

import (
	"testing"

	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/backends/core/backend/kss"
)

func TestCompanion_Notifications_S3(t *testing.T) {
	if err := envdecode.Decode(&s3Credentials); err != nil {
		panic(err)
	}
	testCompanion_Notifications(t, kss.DriverTypeAWSS3)
}
