package access

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core/csql"
)

// BackdoorMiddlewareBuilder is a helper builder for JwtMiddelware
type BackdoorMiddlewareBuilder struct {
	// Backdoors is a mapping from a bearer token to an actual authorization
	Backdoors map[string]Authorization
	// VIPs is a mapping from a bearer token to an identity
	VIPs map[string]string
	// DB is the postgres database. Must have a collection resource "account" with an external index
	// "identity". The database is only used for VIP tickets.
	DB *csql.DB
}

// NewBackdoorMiddelware returns a middleware handler for a backdoor
//
// The key for the backdoors map is the bearer token passed with the request.
//
// Example: if you specify the backdoor
//   "please": Authorization{Roles:[]string{"admin"}}
// then any request with an authorization bearer token consisting of the single
// magic word "please" will be authorized with the admin role.
//
// With curl, use -H 'Authorization: Bearer please' or pass a cookie with
// -b 'Kurbisio-JWT=please'
//
// The backdoor also accepts special tickets for VIPs. It then looks up the
// final authorization from the account collection in the backend.
func NewBackdoorMiddelware(bmb *BackdoorMiddlewareBuilder) mux.MiddlewareFunc {

	if len(bmb.VIPs) == 0 && bmb.DB == nil {
		panic("backdoor middleware requires DB for VIP tickets")
	}

	authQuery := fmt.Sprintf("SELECT account_id, properties FROM %s.account WHERE identity=$1;", bmb.DB.Schema)

	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := AuthorizationFromContext(r.Context())
			identity := IdentityFromContext(r.Context())
			if auth != nil || len(identity) > 0 { // already authorized or at least authenticated?
				h.ServeHTTP(w, r)
				return
			}
			tokenString := ""
			bearer := r.Header.Get("Authorization")
			if len(bearer) > 0 {
				if len(bearer) >= 8 && strings.ToLower(bearer[:7]) == "bearer " {
					tokenString = bearer[7:]
				} else {
					tokenString = bearer
				}
			} else if cookie, _ := r.Cookie("Kurbisio-JWT"); cookie != nil {
				tokenString = cookie.Value
			}
			if len(tokenString) == 0 {
				h.ServeHTTP(w, r)
				return
			}

			// check backdoors
			if bmb.Backdoors != nil {
				if tryAuth, ok := bmb.Backdoors[tokenString]; ok {
					auth = &tryAuth
				}
			}

			// check vip tickets
			if auth == nil && bmb.VIPs != nil {
				if vip, ok := bmb.VIPs[tokenString]; ok {

					// at least we have an identity
					identity = vip
					// maybe we also have an authorization
					var authID uuid.UUID
					var properties json.RawMessage
					err := bmb.DB.QueryRow(authQuery, vip).Scan(&authID, &properties)

					if err == nil {
						auth = &Authorization{}
						json.Unmarshal(properties, auth)
					}
				}
			}

			ctx := r.Context()
			if len(identity) > 0 {
				ctx = ContextWithIdentity(ctx, identity)
			}
			if auth != nil {
				ctx = ContextWithAuthorization(ctx, auth)
			}
			h.ServeHTTP(w, r.WithContext(ctx))
		})
	}

}
