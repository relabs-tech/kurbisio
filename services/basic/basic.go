package main

import (
	"database/sql"
	"log"
	"net/http"

	"github.com/joeshaw/envdecode"
	"github.com/relabs-tech/backends/core/backend"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

var configurationJSON string = `  
{
	"resources": [
	  {
		"resource": "user",
		"external_unique_indices": ["email"]
	  },
	  {
		"resource": "user/profile",
		"single": true
	  },
	  {
		"resource": "device",
		"external_indices": ["equipment_id"]
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

	db, err := sql.Open("postgres", service.Postgres)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	schema := "basic"
	router := mux.NewRouter()
	backend.MustNew(&backend.Builder{
		Config: configurationJSON,
		Schema: schema,
		DB:     db,
		Router: router,
	})

	log.Println("listen on port :3000")
	http.ListenAndServe(":3000", router)
}
