// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend_test

import (
	"os"

	"github.com/gorilla/mux"
	"github.com/joeshaw/envdecode"

	"github.com/relabs-tech/kurbisio/core/backend"
	"github.com/relabs-tech/kurbisio/core/backend/kss"
	"github.com/relabs-tech/kurbisio/core/client"
	"github.com/relabs-tech/kurbisio/core/csql"
)

// CreateTestService creates a new service that can be used for testing
// It is expected to close the Db from the returned object when the object is no longer used
func CreateTestService(config, schemaName string) *TestService {
	return createTestServiceInternal(config, schemaName, true) // clear schema
}

// UpdateTestService creates a new service that can be used for testing, reusing
// the data in the schema from the previous call.
// It is expected to close the Db from the returned object when the object is no longer used
func UpdateTestService(config, schemaName string) *TestService {
	return createTestServiceInternal(config, schemaName, false) // keep schema
}

func createTestServiceInternal(config, schemaName string, clearSchema bool) *TestService {

	s := TestService{}
	if err := envdecode.Decode(&s); err != nil {
		panic(err)
	}

	s.Db = csql.OpenWithSchema(s.Postgres, s.PostgresPassword, schemaName)
	if clearSchema {
		s.Db.ClearSchema()
	}

	s.Router = mux.NewRouter()

	dir, err := os.MkdirTemp("", "test")
	if err != nil {
		panic(err)
	}
	builder := backend.Builder{
		Config:               config,
		DB:                   s.Db,
		Router:               s.Router,
		AuthorizationEnabled: true,
		UpdateSchema:         true,
		KssConfiguration: kss.Configuration{
			DriverType: kss.DriverTypeLocal,
			LocalConfiguration: &kss.LocalConfiguration{
				KeyPrefix: dir,
			},
		},
	}
	s.backend = backend.New(&builder)
	s.client = client.NewWithRouter(s.Router).WithAdminAuthorization()
	s.clientNoAuth = client.NewWithRouter(s.Router)

	return &s
}
