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
func CreateTestService(config, schemaName string) *TestService {
	// ensure that we do not call garmin

	s := TestService{}
	if err := envdecode.Decode(&s); err != nil {
		panic(err)
	}

	s.Db = csql.OpenWithSchema(s.Postgres, s.PostgresPassword, schemaName)
	s.Db.ClearSchema()

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
