package authorization

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
)

// API is the IoT appliance RESTful interface for device authorization
type API struct {
	schema string
	db     *sql.DB
}

// Builder is a builder helper for the API
type Builder struct {
	// Schema is optional. When set, the backend uses the data schema name for
	// generated sql relations. The default schema is "public"
	Schema string
	// DB is a postgres database. This is mandatory.
	DB *sql.DB
	// Router is a mux router. This is mandatory.
	Router *mux.Router
	// CACertFile is the file path to the X509 certificate of the certificate authority.
	// This is mandatory
	CACertFile string
	// CAKeyFile is the file path to the X509 private key of the certificate authority.
	// This is mandatory
	CAKeyFile string
}

// MustNewAPI realizes the actual API. It creates the sql relations for the device twin
// (if they do not exist) and adds actual routes to router
func MustNewAPI(b *Builder) *API {

	schema := b.Schema
	if len(schema) == 0 {
		schema = "public"
	}

	if b.DB == nil {
		panic("DB is missing")
	}

	if b.Router == nil {
		panic("Router is missing")
	}

	s := &API{
		schema: b.Schema,
		db:     b.DB,
	}

	if len(b.CACertFile) == 0 {
		panic("ca-cert file misssing")
	}

	if len(b.CAKeyFile) == 0 {
		panic("ca-key file misssing")
	}

	s.handleRoutes(b.CACertFile, b.CAKeyFile, b.Router)

	return s
}

func (s *API) handleRoutes(caCertFile, caKeyFile string, router *mux.Router) {
	log.Println("device authorization: handle route /authorizations/{equipment_id} GET")

	caCertData, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		panic(err)
	}
	caKeyData, err := ioutil.ReadFile(caKeyFile)
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
				http.Error(w, "device not registered or not waiting for authorization", http.StatusBadRequest)
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
