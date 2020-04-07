/*Package access provides utilities for access control
 */
package access

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core"
)

// contextKey is the type for context keys. Go linter does not like plain strings
type contextKey string

// the predefined context key
const (
	contextKeyAuthorization contextKey = "_authorization_"
)

/*Authorization is a context object which stores authorization information
for user, things, or machines.

An authorization carries a list or roles and identifiers of resources from the
backend configuration. It can also carry additional properties.

Authorizations are added to a request context with

  ctx = auth.ContextWithAuthorization(ctx)

and retrieved with

  auth := AuthorizationFromContext(ctx)

Authorization objects are added to the context by by different middleware
implementations, depending on authorization tokens in the HTTP request.
Kurbisio supports JWT bearer token, Kurbisio-Device-Token,
Kurbisio-Machine-Token and a pair of Kurbisio-Thing-Key/Kurbisio-Thing-Identifier.

For the benefit of simple frontend development, it also supports a Kurbisio-JWT cookie.

*/
type Authorization struct {
	Roles     []string          `json:"roles"`
	Selectors map[string]string `json:"selectors,omitempty"`
}

// HasRole returns true if the authorization contains the requested role;
// otherwise it returns false.
func (a *Authorization) HasRole(role string) bool {
	if a == nil || a.Roles == nil {
		return false
	}
	for _, hasRole := range a.Roles {
		if role == hasRole {
			return true
		}
	}
	return false
}

// HasRoles returns true if the authorization contains any roles;
// otherwise it returns false
func (a *Authorization) HasRoles() bool {
	if a == nil {
		return false
	}
	return len(a.Roles) > 0
}

// Selector returns the value for the requested key; if the
// selector does not exist, it returns an empty string and false.
func (a *Authorization) Selector(key string) (string, bool) {
	if a == nil {
		return "", false
	}
	value, ok := a.Selectors[key]
	return value, ok
}

// Permit models an authorization permit for a resource. It gives the role
// permission to execute any of the listed operations, provided that
// it can satisfy the requested selectors.
type Permit struct {
	Role       string           `json:"role"`
	Operations []core.Operation `json:"operations"`
	Selectors  []string         `json:"selectors"`
}

// IsAuthorized returns true if the authorization is authorized for the requested
// resource and operation according to the passed permits.
//
// The permits are a list of Permit objects, each containing a role, a list of operations and a
// list of required selectors.
//
// The "admin" role has a universal permit for all operations. If a permit if given to "everybody",
// then this permit applies to all roles but "public"
func (a *Authorization) IsAuthorized(resources []string, operation core.Operation, params map[string]string, permits []Permit) bool {

	if a.HasRole("admin") {
		return true // admin is always authorized
	}

	for _, permit := range permits {

		// check if permit is applicable
		if !(a.HasRole(permit.Role) || (a.HasRoles() && permit.Role == "everybody") || permit.Role == "public") {
			continue
		}
		// check if the permit contains the necessary permission for the requested operation
		found := false
		for i := 0; i < len(permit.Operations) && !found; i++ {
			found = permit.Operations[i] == operation
		}
		if !found {
			continue
		}
		// check that we have all requested selectors
		fail := false
		for i := 0; i < len(permit.Selectors) && !fail; i++ {
			id := permit.Selectors[i] + "_id"
			selector, ok := a.Selector(id)
			fail = !ok || selector != params[id]
		}
		if !fail {
			return true
		}
	}
	return false
}

// ContextWithAuthorization returns a new context with this authorization added to it
func (a *Authorization) ContextWithAuthorization(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKeyAuthorization, a)

}

// AuthorizationFromContext retrieves an authorization from the context
func AuthorizationFromContext(ctx context.Context) *Authorization {
	a, ok := ctx.Value(contextKeyAuthorization).(*Authorization)
	if ok {
		return a
	}
	return nil
}

// AuthorizationCache is an in-memory cache for authorizations. It is used by
// jwt middleware to cache authorization objects for bearer tokens.
// The purpose of the cache is to reduce the number of database queries, without
// the cache the middleware would have to lookup the authorization for every single
// request.
type AuthorizationCache struct {
	mutex sync.RWMutex
	cache map[string]*Authorization
}

// NewAuthorizationCache creates a new authorization cache
func NewAuthorizationCache() *AuthorizationCache {
	return &AuthorizationCache{cache: make(map[string]*Authorization)}
}

// Read returns an authorization from in-process cache.
// Token should be the temporary token the authorization was derived from, not any of the ids.
// This function is go-route safe
func (a *AuthorizationCache) Read(token string) *Authorization {
	a.mutex.RLock()
	auth, ok := a.cache[token]
	a.mutex.RUnlock()
	if ok {
		return auth
	}
	return nil
}

// Write stores an authorization in the in-memory cache.
// Token should be the temporary token it was derived from, not any of the ids.
// This function is go-route safe
func (a *AuthorizationCache) Write(token string, auth *Authorization) {
	a.mutex.Lock()
	a.cache[token] = auth
	a.mutex.Unlock()
}

// HandleAuthorizationRoute adds a route /authorization GET to the router
//
// The route returns the current authorization for the authenticated
// requester.
func HandleAuthorizationRoute(router *mux.Router) {
	log.Println("authorization")
	log.Println("  handle route: /authorization GET")
	router.HandleFunc("/authorization", func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		auth := AuthorizationFromContext(r.Context())
		if auth == nil {
			w.WriteHeader(http.StatusNoContent)
		} else {
			jsonData, _ := json.MarshalIndent(auth, "", " ")
			w.Header().Set("Content-Type", "application/json")
			w.Write(jsonData)
		}
	}).Methods(http.MethodOptions, http.MethodGet)

}
