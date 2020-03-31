package main

import (
	"log"
	"net/http"

	"github.com/relabs-tech/backends/core/sql"

	"github.com/joeshaw/envdecode"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"

	"github.com/relabs-tech/backends/core/backend"
	"github.com/relabs-tech/backends/iot/credentials"
	"github.com/relabs-tech/backends/iot/mqtt"
	"github.com/relabs-tech/backends/iot/twin"
)

var configurationJSON string = `{
	"collections": [
	  {
		"resource": "device",
		"external_index": "thing",
		"static_properties": ["provisioning_status"]
	  },
	  {
		"resource": "device/data"
	  },
	  {
		"resource": "fleet"
	  },
	  {
		"resource": "fleet/user"
	  },
	  {
		"resource": "fleet/location"
	  }
	],
	"singletons": [
	  {
		"resource": "fleet/user/profile"
	  }
	],
	"relations": [
	  {
		"origin": "device",
		"resource": "fleet/device"
	  },
	  {
		"origin": "fleet/location",
		"resource": "fleet/user/location"
	  },
	  {
		"origin": "fleet/device",
		"resource": "fleet/user/device"
	  }
	]
  }
`

// Service holds the configuration for this service
//
// use POSTGRES="host=localhost port=5432 user=postgres password=docker dbname=postgres sslmode=disable"
type Service struct {
	Postgres string `env:"POSTGRES,required" description:"the connection string for the Postgres DB"`
}

func main() {
	service := &Service{}
	if err := envdecode.Decode(service); err != nil {
		panic(err)
	}

	db := sql.MustOpenWithSchema(service.Postgres, "fleet")
	defer db.Close()

	router := mux.NewRouter()

	backend.MustNew(&backend.Builder{
		Config: configurationJSON,
		DB:     db,
		Router: router,
	})

	iotBroker := mqtt.MustNewBroker(&mqtt.Builder{
		DB:         db,
		CertFile:   "server.crt",
		KeyFile:    "server.key",
		CACertFile: "ca.crt",
	})

	twin.MustNewAPI(&twin.Builder{
		DB:        db,
		Publisher: iotBroker,
		Router:    router,
	})

	credentials.MustNewAPI(&credentials.Builder{
		DB:         db,
		Router:     router,
		CACertFile: "ca.crt",
		CAKeyFile:  "ca.key",
	})

	log.Println("listen on port :3000")
	go http.ListenAndServe(":3000", router)

	iotBroker.Run()
}
