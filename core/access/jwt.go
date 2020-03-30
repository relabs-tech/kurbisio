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
	"github.com/relabs-tech/backends/core/registry"
)

// JwtMiddlewareBuilder is a helper builder for JwtMiddelware
type JwtMiddlewareBuilder struct {
	PublicKeyDownloadURL string
	DB                   *sql.DB
	Schema               string
}

// MustNewJwtMiddelware returns a middleware handler to validate
// JWT bearer token.
//
// Java-Web-Token (JWT) are accepted as "Authorization: Bearer"
// header or as "Kurbisio-JWT"-cookie.
//
// This is a final handler. It will return http.StatusUnauthorized
// errors if the caller cannot be authorized
func MustNewJwtMiddelware(jmb *JwtMiddlewareBuilder) mux.MiddlewareFunc {

	jwtRegistry := registry.MustNew(jmb.DB, jmb.Schema).Accessor("_jwt_")
	var wellKnownCertificates map[string]string
	createdAt, err := jwtRegistry.Read(jmb.PublicKeyDownloadURL, &wellKnownCertificates)
	if err != nil {
		panic(err)
	}
	if time.Now().Sub(createdAt) > 6*time.Hour {
		// time to check for new keys
		res, err := http.Get(jmb.PublicKeyDownloadURL)
		if err != nil {
			panic(err)
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

	jwksLookup := func(token *jwt.Token) (interface{}, error) {
		kid := token.Header["kid"].(string)
		log.Println("kid:" + kid + ":")
		key, ok := wellKnownKeys[kid]
		if ok {
			log.Println("jwksLookup: got key for kid", kid)
			return key, nil
		}
		log.Println(wellKnownKeys)
		return nil, errors.New("cannot verify token")
	}

	authQuery := fmt.Sprintf("SELECT authorization_id, properties FROM %s.authorization WHERE email=$1 AND issuer=$2;", jmb.Schema)

	authCache := NewAuthorizationCache()

	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := AuthorizationFromContext(r.Context())
			if auth != nil { // already authorized?
				h.ServeHTTP(w, r)
				return
			}

			tokenString := ""
			bearer := r.Header.Get("Authorization")
			if len(bearer) > 0 {
				if len(bearer) >= 8 && strings.ToLower(bearer[:7]) == "bearer " {
					tokenString = bearer[7:]
				}
			} else if cookie, _ := r.Cookie("Kurbisio-JWT"); cookie != nil {
				tokenString = cookie.Value
			}
			if len(tokenString) == 0 {
				http.Error(w, "bearer token missing", http.StatusUnauthorized)
				return
			}

			claims := struct {
				EMail string `json:"email"`
				jwt.StandardClaims
			}{}
			token, err := jwt.ParseWithClaims(tokenString, &claims, jwksLookup)

			// TODO remove this hack, it's only for avoiding that token expire
			if err == nil || !strings.HasPrefix(err.Error(), "token is expired by") {

				if err != nil {
					http.Error(w, err.Error(), http.StatusUnauthorized)
					return
				}
				if !token.Valid {
					log.Println("token not valid")
					http.Error(w, "invalid bearer token", http.StatusUnauthorized)
					return
				}
			}

			// if err != nil {
			// 	http.Error(w, err.Error(), http.StatusUnauthorized)
			// 	return
			// }
			// if !token.Valid {
			// 	http.Error(w, "invalid bearer token", http.StatusUnauthorized)
			// 	return
			// }

			// look up authorization for the token. We do this by tokenString, and not
			// by email, so the frontend can enforce a new database lookup with a new token.
			auth = authCache.Read(tokenString)
			if auth == nil {

				var authID uuid.UUID
				var properties json.RawMessage
				err = jmb.DB.QueryRow(authQuery, claims.EMail, claims.Issuer).Scan(&authID, &properties)

				if err == sql.ErrNoRows {
					http.Error(w, "no authorization for "+claims.EMail+" from "+claims.Issuer, http.StatusUnauthorized)
					return
				}

				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				auth = &Authorization{}
				json.Unmarshal(properties, auth)
				authCache.Write(tokenString, auth)
			}

			ctx := auth.ContextWithAuthorization(r.Context())
			r = r.WithContext(ctx)
			h.ServeHTTP(w, r)
		})
	}

}

func asJSON(object interface{}) string {
	j, _ := json.MarshalIndent(object, "", "  ")
	return string(j)
}
