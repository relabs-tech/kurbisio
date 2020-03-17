package api

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq" // for the postgres database
	"github.com/relabs-tech/backends/iot"
)

// Service is a REST interface for the digital iot API
type Service struct {
	schema    string
	db        *sql.DB
	publisher iot.MessagePublisher
}

// MustNewService returns a new API service
func MustNewService() *Service {
	s := &Service{
		schema: "public",
	}
	return s
}

// WithSchema sets a database schema name for the generated sql relations. The default
// schema is "public".
func (s *Service) WithSchema(schema string) *Service {
	s.schema = schema
	return s
}

// WithMessagePublisher adds an IoT Message Publisher for twin requests
func (s *Service) WithMessagePublisher(publisher iot.MessagePublisher) *Service {
	s.publisher = publisher
	return s
}

// Create creates the sql relations (if they do not exist) and adds routes to the passed router
func (s *Service) Create(db *sql.DB, router *mux.Router) *Service {

	// poor man's database migrations
	_, err := s.db.Query(
		`CREATE extension IF NOT EXISTS "uuid-ossp";
CREATE table IF NOT EXISTS ` + s.schema + `.twin 
(device_id uuid references ` + s.schema + `.device(device_id) ON DELETE CASCADE, 
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

	s.handleRoutes(router)

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
func (s *Service) handleRoutes(router *mux.Router) {
	log.Println("twin: handle route /devices/{device_id}/twin GET")
	log.Println("twin: handle route /devices/{device_id}/twin/{key} GET")
	log.Println("twin: handle route /devices/{device_id}/twin/{key}/request GET,PUT")
	log.Println("twin: handle route /devices/{device_id}/twin/{key}/report GET,PUT")

	router.HandleFunc("/devices/{device_id}/twin", func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		deviceID, err := uuid.Parse(params["device_id"])

		rows, err := s.db.Query(
			`SELECT key,request,report,requested_at,reported_at FROM `+s.schema+`.twin WHERE device_id=$1;`,
			deviceID)
		if err == sql.ErrNoRows {
			http.Error(w, "no such twin", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
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
		w.Header().Set("Content-Type", "application/json")
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(response)
	}).Methods(http.MethodGet)

	router.HandleFunc("/devices/{device_id}/twin/{key}", func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		deviceID, err := uuid.Parse(params["device_id"])
		key := params["key"]
		t := twin{}
		err = s.db.QueryRow(
			`SELECT key,request,report,requested_at,reported_at FROM `+s.schema+`.twin WHERE device_id=$1 AND key=$2;`,
			deviceID, key).Scan(&t.Key, &t.Request, &t.Report, &t.RequestedAt, &t.ReportedAt)
		if err == sql.ErrNoRows {
			http.Error(w, "no such twin", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(t)
	}).Methods(http.MethodGet)

	router.HandleFunc("/devices/{device_id}/twin/{key}/request", func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		deviceID, err := uuid.Parse(params["device_id"])
		key := params["key"]
		t := twin{}
		err = s.db.QueryRow(
			`SELECT request FROM `+s.schema+`.twin WHERE device_id=$1 AND key=$2;`,
			deviceID, key).Scan(&t.Request)
		if err == sql.ErrNoRows {
			http.Error(w, "no such twin", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(t.Request)
	}).Methods(http.MethodGet)

	router.HandleFunc("/devices/{device_id}/twin/{key}/report", func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		deviceID, err := uuid.Parse(params["device_id"])
		key := params["key"]
		t := twin{}
		err = s.db.QueryRow(
			`SELECT report FROM `+s.schema+`.twin WHERE device_id=$1 AND key=$2;`,
			deviceID, key).Scan(&t.Report)
		if err == sql.ErrNoRows {
			http.Error(w, "no such twin", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(t.Report)
	}).Methods(http.MethodGet)

	router.HandleFunc("/devices/{device_id}/twin/{key}/request", func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		deviceID, err := uuid.Parse(params["device_id"])
		key := params["key"]
		body, _ := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid device id", http.StatusBadRequest)
			return
		}

		if !json.Valid(body) {
			http.Error(w, "invalid json data", http.StatusBadRequest)
			return
		}

		now := time.Now().UTC()
		never := time.Time{}
		res, err := s.db.Exec(
			`INSERT INTO `+s.schema+`.twin(device_id,key,request,report,requested_at,reported_at)
VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT (device_id, key) DO UPDATE SET request=$3,requested_at=$5;`,
			deviceID, key, string(body), "{}", now, never)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		count, err := res.RowsAffected()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if s.publisher != nil {
			s.publisher.PublishMessageQ1("kurbisio/twin/"+deviceID.String()+"/requests/"+key, body)
		}

		if count > 0 {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}

	}).Methods(http.MethodPut)

	router.HandleFunc("/devices/{device_id}/twin/{key}/report", func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		deviceID, err := uuid.Parse(params["device_id"])
		key := params["key"]
		body, _ := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid device id", http.StatusBadRequest)
			return
		}

		if !json.Valid(body) {
			http.Error(w, "invalid json data", http.StatusBadRequest)
			return
		}

		now := time.Now().UTC()
		never := time.Time{}
		res, err := s.db.Exec(
			`INSERT INTO `+s.schema+`.twin(device_id,key,request,report,requested_at,reported_at)
VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT (device_id, key) DO UPDATE SET report=$4,reported_at=$6;`,
			deviceID, key, "{}", string(body), never, now)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		count, err := res.RowsAffected()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if count > 0 {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}

	}).Methods(http.MethodPut)

	caCertData, err := ioutil.ReadFile("ca.crt")
	if err != nil {
		panic(err)
	}
	caKeyData, err := ioutil.ReadFile("ca.key")
	if err != nil {
		panic(err)
	}
	caCertDataPEM, _ := pem.Decode(caCertData)
	caCert, err := x509.ParseCertificate(caCertDataPEM.Bytes)
	if err != nil {
		panic(err)
	}
	caKeyDataPEM, _ := pem.Decode(caKeyData)
	caPrivKey, err := x509.ParsePKCS8PrivateKey(caKeyDataPEM.Bytes)
	if err != nil {
		panic(err)
	}

	log.Println("device authorization: handle route /authorizations/{equipment_id} GET")

	router.HandleFunc("/authorizations/{equipment_id}",
		func(w http.ResponseWriter, r *http.Request) {
			params := mux.Vars(r)
			equipmentID := params["equipment_id"]
			log.Println("authorization request of", equipmentID)

			var deviceID uuid.UUID
			var authorizationStatus string
			err := s.db.QueryRow(
				`SELECT device_id, authorization_status FROM `+s.schema+`.device 
WHERE equipment_id=$1 AND authorization_status IN ('waiting', 'authorized') ORDER BY authorization_status;`,
				equipmentID).Scan(&deviceID, &authorizationStatus)

			if err == sql.ErrNoRows {
				http.Error(w, "device not registered", http.StatusBadRequest)
				return
			}

			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if authorizationStatus == "authorized" {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(
					struct {
						DeviceID uuid.UUID `json:"device_id"`
					}{
						DeviceID: deviceID,
					})
				return
			}

			// authorization status is waiting, we generate a new certificate and set the status to authorized

			cert := &x509.Certificate{
				SerialNumber: big.NewInt(1658),
				Subject: pkix.Name{
					CommonName: deviceID.String(),
				},
				NotBefore:    time.Now(),
				NotAfter:     time.Now().AddDate(1, 0, 0), // one year later. TODO, why not quicker? Or longer?
				SubjectKeyId: []byte{1, 2, 3, 4, 6},
				ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
				KeyUsage:     x509.KeyUsageDigitalSignature,
			}

			// this is the part that takes time
			certPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			certBytes, err := x509.CreateCertificate(rand.Reader, cert, caCert, &certPrivKey.PublicKey, caPrivKey)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			certPEM := new(bytes.Buffer)
			pem.Encode(certPEM, &pem.Block{
				Type:  "CERTIFICATE",
				Bytes: certBytes,
			})

			certPrivKeyPEM := new(bytes.Buffer)
			pem.Encode(certPrivKeyPEM, &pem.Block{
				Type:  "RSA PRIVATE KEY",
				Bytes: x509.MarshalPKCS1PrivateKey(certPrivKey),
			})

			query := fmt.Sprintf("UPDATE %s.device SET authorization_status='authorized' WHERE device_id=$1", s.schema)
			res, err := s.db.Exec(query, deviceID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			count, err := res.RowsAffected()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if count != 1 {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(
				struct {
					DeviceID    uuid.UUID `json:"device_id"`
					Certificate string    `json:"cert"`
					Key         string    `json:"key"`
				}{
					DeviceID:    deviceID,
					Certificate: certPEM.String(),
					Key:         certPrivKeyPEM.String(),
				})

		}).Methods(http.MethodGet)
}
