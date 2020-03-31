package main

import (
	"log"
	"net/http"

	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/backends/core/backend"
	"github.com/relabs-tech/backends/core/sql"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
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
		"resource": "user/profile",
	  }
	],
	"relations": [
	  {
		"origin": "device",
		"resource": "user/device"
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

	db := sql.MustOpenWithSchema(service.Postgres, "basic")
	defer db.Close()

	router := mux.NewRouter()
	backend.MustNew(&backend.Builder{
		Config: configurationJSON,
		DB:     db,
		Router: router,
	})

	log.Println("listen on port :3000")
	http.ListenAndServe(":3000", router)
}
