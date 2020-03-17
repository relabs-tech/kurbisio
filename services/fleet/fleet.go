package main

import (
	"database/sql"
	"log"
	"net/http"

	"github.com/joeshaw/envdecode"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"

	"github.com/relabs-tech/backends/baas"
	"github.com/relabs-tech/backends/iot/api"
	"github.com/relabs-tech/backends/iot/broker"
)

var configurationJSON string = `{
	"resources": [
	  {
		"resource": "device",
		"external_indices": ["equipment_id"],
		"extra_properties": ["authorization_status"]
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
		"resource": "fleet/user/profile"
	  },
	  {
		"resource": "fleet/location"
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

	db, err := sql.Open("postgres", service.Postgres)
	if err != nil {
		panic(err)
	}

	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	schema := "fleet_example"
	router := mux.NewRouter()
	baas.MustNewBackend(configurationJSON).WithSchema(schema).Create(db, router)
	iotBroker := broker.MustNewBroker(db, schema, "server.crt", "server.key")
	api.MustNewService().WithSchema(schema).WithMessagePublisher(iotBroker).Create(db, router)

	log.Println("listen on port :3000")
	go http.ListenAndServe(":3000", router)

	iotBroker.Run()
}
