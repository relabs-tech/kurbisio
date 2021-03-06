package twin

import (
	"database/sql"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/goccy/go-json"

	"github.com/relabs-tech/kurbisio/core/logger"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/csql"
	"github.com/relabs-tech/kurbisio/iot"
)

// API is the IoT appliance RESTful interface for the device twin.
type API struct {
	db                   *csql.DB
	publisher            iot.MessagePublisher
	authorizationEnabled bool
}

// Builder is a builder helper for the IoT API
type Builder struct {
	// DB is a postgres database. This is mandatory.
	DB *csql.DB
	// Router is a mux router. This is mandatory.
	Router *mux.Router
	// Publisher is an iot.MessagePublisher
	Publisher iot.MessagePublisher
	// If AuthorizationEnabled is true, the twin rest api requires admin authorization
	AuthorizationEnabled bool
}

// NewAPI realizes the actual API. It creates the sql relations for the device twin
// (if they do not exist) and adds actual routes to router
func NewAPI(b *Builder) *API {

	if b.DB == nil {
		panic("DB is missing")
	}

	if b.Router == nil {
		panic("Router is missing")
	}

	CreateTwinTableIfNotExists(b.DB)

	s := &API{
		db:                   b.DB,
		publisher:            b.Publisher,
		authorizationEnabled: b.AuthorizationEnabled,
	}
	s.handleRoutes(b.Router)

	return s
}

type twin struct {
	Key         string          `json:"key"`
	Request     json.RawMessage `json:"request"`
	Report      json.RawMessage `json:"report"`
	RequestedAt time.Time       `json:"requested_at"`
	ReportedAt  time.Time       `json:"reported_at"`
}

// HandleRoutes adds handlers for routes for the twin service
func (s *API) handleRoutes(router *mux.Router) {
	log.Println("twin: handle route /devices/{device_id}/twin GET")
	log.Println("twin: handle route /devices/{device_id}/twin/{key} GET")
	log.Println("twin: handle route /devices/{device_id}/twin/{key}/request GET,PUT")
	log.Println("twin: handle route /devices/{device_id}/twin/{key}/report GET,PUT")

	router.HandleFunc("/devices/{device_id}/twin", func(w http.ResponseWriter, r *http.Request) {
		if s.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			// TODO: Potentially give access to admin viewer role
			if !auth.HasRole("admin") {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		params := mux.Vars(r)
		deviceID, err := uuid.Parse(params["device_id"])
		if err != nil {
			http.Error(w, "invalid device id", http.StatusBadRequest)
			return
		}

		rows, err := s.db.Query(
			`SELECT key,request,report,requested_at,reported_at FROM `+s.db.Schema+`."_twin_" WHERE device_id=$1;`,
			deviceID)
		if err == sql.ErrNoRows {
			http.Error(w, "no such twin", http.StatusNotFound)
			return
		}
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 2640")
			http.Error(w, "Error 2640", http.StatusInternalServerError)
			return
		}
		response := []twin{}
		defer rows.Close()
		for rows.Next() {
			t := twin{}
			err := rows.Scan(&t.Key, &t.Request, &t.Report, &t.RequestedAt, &t.ReportedAt)
			if err != nil {
				log.Println("error when scanning: ", err.Error())
			}
			response = append(response, t)
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		jsonData, _ := json.Marshal(response)
		w.Write(jsonData)
	}).Methods(http.MethodOptions, http.MethodGet)

	router.HandleFunc("/devices/{device_id}/twin/{key}", func(w http.ResponseWriter, r *http.Request) {
		if s.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			// TODO: Potentially give access to admin viewer role
			if !auth.HasRole("admin") {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		params := mux.Vars(r)
		deviceID, err := uuid.Parse(params["device_id"])
		if err != nil {
			http.Error(w, "invalid device id", http.StatusBadRequest)
			return
		}
		key := params["key"]
		t := twin{}
		err = s.db.QueryRow(
			`SELECT key,request,report,requested_at,reported_at FROM `+s.db.Schema+`."_twin_" WHERE device_id=$1 AND key=$2;`,
			deviceID, key).Scan(&t.Key, &t.Request, &t.Report, &t.RequestedAt, &t.ReportedAt)
		if err == sql.ErrNoRows {
			http.Error(w, "no such twin", http.StatusNotFound)
			return
		}
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 2641")
			http.Error(w, "Error 2641", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		jsonData, _ := json.Marshal(t)
		w.Write(jsonData)
	}).Methods(http.MethodOptions, http.MethodGet)

	router.HandleFunc("/devices/{device_id}/twin/{key}/request", func(w http.ResponseWriter, r *http.Request) {
		if s.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			// TODO: Potentially give access to admin viewer role
			if !auth.HasRole("admin") {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		params := mux.Vars(r)
		deviceID, err := uuid.Parse(params["device_id"])
		if err != nil {
			http.Error(w, "invalid device id", http.StatusBadRequest)
			return
		}
		key := params["key"]
		t := twin{}
		err = s.db.QueryRow(
			`SELECT request FROM `+s.db.Schema+`."_twin_" WHERE device_id=$1 AND key=$2;`,
			deviceID, key).Scan(&t.Request)
		if err == sql.ErrNoRows {
			http.Error(w, "no such twin", http.StatusNotFound)
			return
		}
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 2642")
			http.Error(w, "Error 2642", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		jsonData, _ := json.Marshal(t.Request)
		w.Write(jsonData)
	}).Methods(http.MethodOptions, http.MethodGet)

	router.HandleFunc("/devices/{device_id}/twin/{key}/report", func(w http.ResponseWriter, r *http.Request) {
		if s.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			// TODO: Potentially give access to admin viewer role
			if !auth.HasRole("admin") {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		params := mux.Vars(r)
		deviceID, err := uuid.Parse(params["device_id"])
		if err != nil {
			http.Error(w, "invalid device id", http.StatusBadRequest)
			return
		}
		key := params["key"]
		t := twin{}
		err = s.db.QueryRow(
			`SELECT report FROM `+s.db.Schema+`."_twin_" WHERE device_id=$1 AND key=$2;`,
			deviceID, key).Scan(&t.Report)
		if err == sql.ErrNoRows {
			http.Error(w, "no such twin", http.StatusNotFound)
			return
		}
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 2643")
			http.Error(w, "Error 2643", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		jsonData, _ := json.Marshal(t.Report)
		w.Write(jsonData)
	}).Methods(http.MethodOptions, http.MethodGet)

	router.HandleFunc("/devices/{device_id}/twin/{key}/request", func(w http.ResponseWriter, r *http.Request) {
		if s.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			// TODO: Potentially give access to admin viewer role
			if !auth.HasRole("admin") {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		params := mux.Vars(r)
		deviceID, err := uuid.Parse(params["device_id"])
		if err != nil {
			http.Error(w, "invalid device id", http.StatusBadRequest)
			return
		}
		key := params["key"]
		body, _ := io.ReadAll(r.Body)

		if !json.Valid(body) {
			http.Error(w, "invalid json data", http.StatusBadRequest)
			return
		}

		now := time.Now().UTC()
		never := time.Time{}
		res, err := s.db.Exec(
			`INSERT INTO `+s.db.Schema+`."_twin_"(device_id,key,request,report,requested_at,reported_at)
VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT (device_id, key) DO UPDATE SET request=$3,requested_at=$5;`,
			deviceID, key, string(body), "{}", now, never)

		if err != nil {
			http.Error(w, "no such device", http.StatusBadRequest)
			return
		}
		count, err := res.RowsAffected()
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 2644")
			http.Error(w, "Error 2644", http.StatusInternalServerError)
			return
		}

		if s.publisher != nil {
			s.publisher.PublishMessageQ1("kurbisio/"+deviceID.String()+"/twin/requests/"+key, body)
		}

		if count > 0 {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}

	}).Methods(http.MethodOptions, http.MethodPut)

	router.HandleFunc("/devices/{device_id}/twin/{key}/report", func(w http.ResponseWriter, r *http.Request) {
		if s.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			// TODO: Potentially give access to admin viewer role
			if !auth.HasRole("admin") {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		params := mux.Vars(r)
		deviceID, err := uuid.Parse(params["device_id"])
		if err != nil {
			http.Error(w, "invalid device id", http.StatusBadRequest)
			return
		}
		key := params["key"]
		body, _ := io.ReadAll(r.Body)
		if !json.Valid(body) {
			http.Error(w, "invalid json data", http.StatusBadRequest)
			return
		}

		now := time.Now().UTC()
		never := time.Time{}
		res, err := s.db.Exec(
			`INSERT INTO `+s.db.Schema+`."_twin_"(device_id,key,request,report,requested_at,reported_at)
VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT (device_id, key) DO UPDATE SET report=$4,reported_at=$6;`,
			deviceID, key, "{}", string(body), never, now)

		if err != nil {
			http.Error(w, "no such device", http.StatusBadRequest)
			return
		}
		count, err := res.RowsAffected()
		if err != nil {
			logger.Default().WithError(err).Errorf("Error 2645")
			http.Error(w, "Error 2645", http.StatusInternalServerError)
			return
		}

		if count > 0 {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}

	}).Methods(http.MethodOptions, http.MethodPut)

}

// CreateTwinTableIfNotExists creates the SQL table for the
// device twin.
//
// The function requires that the database manages a resource "device".
// The twin table is a system table and named "_twin_".
func CreateTwinTableIfNotExists(db *csql.DB) {
	// poor man's database migrations
	_, err := db.Exec(`CREATE table IF NOT EXISTS ` + db.Schema + `."_twin_"
(device_id uuid references ` + db.Schema + `.device(device_id) ON DELETE CASCADE,
key varchar NOT NULL,
request json NOT NULL,
report json NOT NULL,
requested_at timestamp NOT NULL,
reported_at timestamp NOT NULL,
PRIMARY KEY(device_id, key)
);`)

	if err != nil {
		panic(err)
	}

}
