package access

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core/csql"
	"github.com/relabs-tech/backends/core/logger"
	"github.com/relabs-tech/backends/core/registry"
)

// JwtMiddlewareBuilder is a helper builder for JwtMiddelware
type JwtMiddlewareBuilder struct {
	// PublicKeyDownloadURL is the download url for public keys. In case of google, this would be
	//  "https://www.googleapis.com/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com"
	PublicKeyDownloadURL string
	// Issuer is the accepted issuer for the token
	Issuer string
	// DB is the postgres database. Must have a collection resource "account" with an external index
	// "identity".
	DB *csql.DB
}

// NewJwtMiddelware returns a middleware handler to validate
// JWT bearer token.
//
// Java-Web-Token (JWT) are accepted as "Authorization: Bearer"
// header or as "Kurbisio-JWT"-cookie.
//
// This middleware requires that there is a resource "account" in the
// database, with an external index "identity", which stores
// the authorization for each identity as properties. An account identity
// is a combination of the token issuer with the user's email,
// separated by the pipe symbol '|'. Example:
//   "https://securetoken.google.com/loyalty2u-ea4fd|test@example.com"
//
// This is a final handler with regards to the bearer token. It will return
// http.StatusUnauthorized when a token is available but insufficent to
// authorize the request.
func NewJwtMiddelware(jmb *JwtMiddlewareBuilder) mux.MiddlewareFunc {

	jwtRegistry := registry.New(jmb.DB).Accessor("_jwt_")
	var wellKnownCertificates map[string]string
	timestamp, err := jwtRegistry.Read(jmb.PublicKeyDownloadURL, &wellKnownCertificates)
	if err != nil {
		panic(err)
	}
	if time.Now().Sub(timestamp) > 6*time.Hour {
		// time to check for new keys
		res, err := http.Get(jmb.PublicKeyDownloadURL)
		if err != nil {
			return func(h http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeHTTP(w, r) })
			}
		}

		defer res.Body.Close()
		decoder := json.NewDecoder(res.Body)
		err = decoder.Decode(&wellKnownCertificates)
		if err != nil {
			panic(err)
		}
		jwtRegistry.Write(jmb.PublicKeyDownloadURL, wellKnownCertificates)
	}
	wellKnownKeys := map[string]interface{}{}
	for kid, cert := range wellKnownCertificates {
		key, err := jwt.ParseRSAPublicKeyFromPEM([]byte(cert))
		if err != nil {
			log.Println("certificate error", err)
		} else {
			wellKnownKeys[kid] = key
		}
	}

	rlog := logger.Default()

	jwksLookup := func(token *jwt.Token) (interface{}, error) {
		kid := token.Header["kid"].(string)
		if rlog == nil {
			rlog = logger.Default()
		}
		key, ok := wellKnownKeys[kid]
		if ok {
			rlog.Debugln("jwksLookup: got key for kid", kid)
			return key, nil
		}
		rlog.Warningf("have %d well known keys, but not this one", len(wellKnownKeys))
		return nil, errors.New("cannot verify token")
	}

	authQuery := fmt.Sprintf("SELECT account_id, properties FROM %s.account WHERE identity=$1;", jmb.DB.Schema)
	authCache := NewAuthorizationCache()

	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := AuthorizationFromContext(r.Context())
			identity := IdentityFromContext(r.Context())

			if auth != nil || len(identity) > 0 { // already authorized or at least authenticated?
				h.ServeHTTP(w, r)
				return
			}

			rlog = logger.FromContext(r.Context())

			tokenString := ""
			bearer := r.Header.Get("Authorization")
			if len(bearer) > 0 && bearer != "null" {
				if len(bearer) >= 8 && strings.ToLower(bearer[:7]) == "bearer " {
					tokenString = bearer[7:]
				} else {
					tokenString = bearer
				}
			} else if cookie, _ := r.Cookie("Kurbisio-JWT"); cookie != nil {
				tokenString = cookie.Value
			}
			if len(tokenString) == 0 {
				fmt.Printf("--------- Bearer: '%s'\n", bearer)
				h.ServeHTTP(w, r) // no token no auth, moving on
				return
			}

			claims := struct {
				EMail string `json:"email"`
				jwt.StandardClaims
			}{}
			token, err := jwt.ParseWithClaims(tokenString, &claims, jwksLookup)

			if err != nil || !token.Valid || claims.Issuer != jmb.Issuer {
				http.Error(w, "invalid token", http.StatusUnauthorized)
				return
			}

			// identity is a combination of issuer and email
			identity = claims.Issuer + "|" + claims.EMail

			// now that we have authenticated the requester, we store their identity in the context
			ctx := ContextWithIdentity(r.Context(), identity)
			ctx, rlog = logger.ContextWithLoggerIdentity(ctx, identity)

			// look up authorization for the token. We do this by tokenString, and not
			// by identity, so the frontend can enforce a new database lookup with a new token.
			auth = authCache.Read(tokenString)
			if auth == nil {

				var authID uuid.UUID
				var properties json.RawMessage
				err = jmb.DB.QueryRow(authQuery, identity).Scan(&authID, &properties)

				if err != nil && err != sql.ErrNoRows {
					rlog.WithError(err).Errorf("Error 4723: cannot execute authorization query `%s`", authQuery)
					http.Error(w, "Error 4723", http.StatusInternalServerError)
					return
				}
				if err == nil {
					auth = &Authorization{}
					json.Unmarshal(properties, auth)
					authCache.Write(tokenString, auth)
				}
			}

			ctx = ContextWithAuthorization(ctx, auth)
			r = r.WithContext(ctx)
			h.ServeHTTP(w, r)
		})
	}

}

func asJSON(object interface{}) string {
	j, _ := json.Marshal(object)
	return string(j)
}
