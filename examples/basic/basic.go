// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package main

import (
	"log"
	"net/http"

	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/kurbisio/core/backend"
	"github.com/relabs-tech/kurbisio/core/csql"

	"github.com/gorilla/mux"
)

var configurationJSON string = `  
{
	"collections": [
	  {
		"resource": "user",
		"external_index": "identity"
	  },
	  {
		"resource": "device",
		"external_index": "thing"
	  }
	],
	"singletons": [
	  {
		"resource": "user/profile"
	  }
	],
	"relations": [
	  {
		"resource": "device_ownership",
		"left": "user",
		"right": "device"
	  }
	]
}
`

// Service holds the configuration for this service
type Service struct {
	csql.DBConfig
}

func main() {
	service := &Service{}
	if err := envdecode.Decode(service); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(service.PostgresConfigString(), "basic")
	defer db.Close()

	router := mux.NewRouter()
	backend.New(&backend.Builder{
		Config: configurationJSON,
		DB:     db,
		Router: router,
	})

	log.Println("listen on port :3000")
	http.ListenAndServe(":3000", router)
}
