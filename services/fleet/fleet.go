package main

import (
	"log"
	"net/http"

	"github.com/relabs-tech/backends/core/csql"

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
		"searchable_properties": ["provisioning_status"]
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
		"left": "device",
		"right": "fleet"
	  },
	  {
		"left": "fleet/location",
		"right": "fleet/user"
	  },
	  {
		"left": "fleet/device",
		"right": "fleet/user"
	  }
	]
  }
`

// Service holds the configuration for this service
//
// use POSTGRES="host=localhost port=5432 user=postgres dbname=postgres sslmode=disable"
// and POSTRGRES_PASSWORD="docker"
type Service struct {
	Postgres         string `env:"POSTGRES,required" description:"the connection string for the Postgres DB without password"`
	PostgresPassword string `env:"POSTGRES_PASSWORD,optional" description:"password to the Postgres DB"`
	LogLevel         string `env:"LOG_LEVEL,optional,default=info" description:"The level used for logger, can be debug, warning, info, error"`
}

func main() {
	service := &Service{}
	if err := envdecode.Decode(service); err != nil {
		panic(err)
	}

	db := csql.OpenWithSchema(service.Postgres, service.PostgresPassword, "fleet")
	defer db.Close()

	router := mux.NewRouter()

	backend.New(&backend.Builder{
		Config:   configurationJSON,
		DB:       db,
		Router:   router,
		LogLevel: service.LogLevel,
	})

	iotBroker := mqtt.NewBroker(&mqtt.Builder{
		DB:         db,
		CertFile:   "server.crt",
		KeyFile:    "server.key",
		CACertFile: "ca.crt",
	})

	twin.NewAPI(&twin.Builder{
		DB:        db,
		Publisher: iotBroker,
		Router:    router,
	})

	credentials.NewAPI(&credentials.Builder{
		DB:               db,
		Router:           router,
		CACertFile:       "ca.crt",
		CAKeyFile:        "ca.key",
		KurbisioThingKey: "fleet-thing-secret",
	})

	log.Println("listen on port :3000")
	go http.ListenAndServe(":3000", router)

	iotBroker.Run()
}
