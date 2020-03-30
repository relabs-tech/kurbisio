package access

import (
	"net/http"

	"github.com/gorilla/mux"
)

// NewAdminBackdoorMiddelware returns a middleware handler for an admin backdoor
//
// The admin backdoor authorizes request with the role "admin" based on a
// authorization bearer token consisting of the single magic word "please".
// With curl, use -H 'Authorization: Bearer please' or pass a cookie with
// -b 'Kurbisio-JWT=please'
func NewAdminBackdoorMiddelware() mux.MiddlewareFunc {

	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := AuthorizationFromContext(r.Context())
			if auth != nil { // already authorized?
				h.ServeHTTP(w, r)
				return
			}

			token := r.Header.Get("Authorization")
			cookie, _ := r.Cookie("Kurbisio-JWT")
			if token == "Bearer please" || (cookie != nil && cookie.Value == "please") {
				auth := Authorization{
					Roles: []string{"admin"},
				}
				ctx := auth.ContextWithAuthorization(r.Context())
				r = r.WithContext(ctx)
			}

			h.ServeHTTP(w, r)
		})
	}

}
