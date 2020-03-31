package credentials

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
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
	"github.com/relabs-tech/backends/core/access"
	"github.com/relabs-tech/backends/core/sql"
)

// API is the IoT appliance RESTful interface for providing device credentials to things
type API struct {
	schema           string
	db               *sql.DB
	kurbisioThingKey string
}

// Builder is a builder helper for the API
type Builder struct {
	// Schema is optional. When set, the API uses the data schema name for
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
	// KurbisioThingKey is a key used as shared secret for thing authentication.
	// The default is "secret"
	KurbisioThingKey string
}

// NewAPI realizes the credentials service. It creates the sql relations for the device twin
// (if they do not exist) and adds the /credentials route to the router.
// It also installs thing authorization middleware on the router.
func NewAPI(b *Builder) *API {

	schema := b.Schema
	if len(schema) == 0 {
		schema = "public"
	}

	kurbisioThingKey := b.KurbisioThingKey
	if len(kurbisioThingKey) == 0 {
		kurbisioThingKey = "secret"
	}

	if b.DB == nil {
		panic("DB is missing")
	}

	if b.Router == nil {
		panic("Router is missing")
	}

	s := &API{
		schema:           schema,
		db:               b.DB,
		kurbisioThingKey: kurbisioThingKey,
	}

	if len(b.CACertFile) == 0 {
		panic("ca-cert file misssing")
	}

	if len(b.CAKeyFile) == 0 {
		panic("ca-key file misssing")
	}

	s.handleRoutes(b.CACertFile, b.CAKeyFile, b.Router)
	s.addMiddleware(b.Router)

	return s
}

func (a *API) addMiddleware(router *mux.Router) {
	authCache := access.NewAuthorizationCache()
	authQuery := fmt.Sprintf("SELECT device_id FROM %s.device WHERE token=$1;", a.schema)

	router.Use(
		func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				auth := access.AuthorizationFromContext(r.Context())
				if auth != nil { // already authorized?
					h.ServeHTTP(w, r)
					return
				}

				key := r.Header.Get("Kurbisio-Thing-Key")
				thing := r.Header.Get("Kurbisio-Thing-Identifier")
				if key == a.kurbisioThingKey && len(thing) > 0 {
					auth := access.Authorization{
						Properties: map[string]string{"thing": thing},
						Roles:      []string{"thing"},
					}
					ctx := auth.ContextWithAuthorization(r.Context())
					r = r.WithContext(ctx)
				}

				token := r.Header.Get("Kurbisio-Device-Token")
				if len(token) > 0 {
					auth = authCache.Read(token)
					if auth == nil {
						var deviceID uuid.UUID
						err := a.db.QueryRow(authQuery, token).Scan(&deviceID)

						if err == sql.ErrNoRows {
							http.Error(w, "invalid device token", http.StatusUnauthorized)
							return
						}

						if err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}
						auth = &access.Authorization{
							Roles:     []string{"device"},
							Resources: map[string]uuid.UUID{"device_id": deviceID},
						}
					}

					ctx := auth.ContextWithAuthorization(r.Context())
					r = r.WithContext(ctx)
				}

				h.ServeHTTP(w, r)
			})
		})
}

func (a *API) handleRoutes(caCertFile, caKeyFile string, router *mux.Router) {
	log.Println("device credentials: handle route /credentials GET")

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

	router.HandleFunc("/credentials",
		func(w http.ResponseWriter, r *http.Request) {
			auth := access.AuthorizationFromContext(r.Context())
			if auth == nil || !auth.HasRole("thing") {
				http.Error(w, "thing not authorized", http.StatusUnauthorized)
				return
			}
			thing, _ := auth.Property("thing")
			log.Println("credential request from", thing)

			var deviceID, token uuid.UUID
			var provisioningStatus string
			err := a.db.QueryRow(
				`SELECT device_id, provisioning_status, token FROM `+
					a.schema+`.device WHERE thing=$1 AND provisioning_status IN ('waiting', 'provisioned');`,
				thing).Scan(&deviceID, &provisioningStatus, &token)

			if err == sql.ErrNoRows {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if provisioningStatus == "provisioned" {
				// all good, but credentials can only be downloaded once
				w.WriteHeader(http.StatusNoContent)
				return
			}

			// provisioning status is 'waiting'. Hence we generate a new certificate and set the status to 'provisioned'
			cert := &x509.Certificate{
				SerialNumber: big.NewInt(1658),
				Subject: pkix.Name{
					CommonName: deviceID.String(),
				},
				NotBefore:    time.Now(),
				NotAfter:     time.Now().AddDate(99, 0, 0), // ninety-nine years later
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

			query := fmt.Sprintf("UPDATE %s.device SET provisioning_status='provisioned' WHERE device_id=$1", a.schema)
			res, err := a.db.Exec(query, deviceID)
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
					Token       uuid.UUID `json:"token"`
				}{
					DeviceID:    deviceID,
					Certificate: certPEM.String(),
					Key:         certPrivKeyPEM.String(),
					Token:       token,
				})

		}).Methods(http.MethodGet)
}
