//go:build integration

// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend_test

// These tests are time sensitive and sometimes failing because the SQS queue is slow
// to run these tests, execute" 'go test -tags=integration'

import (
	"testing"

	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/kurbisio/core/backend/kss"
)

func TestCompanion_Notifications_S3(t *testing.T) {
	if err := envdecode.Decode(&s3Credentials); err != nil {
		panic(err)
	}
	testCompanion_Notifications(t, kss.DriverTypeAWSS3)
}
