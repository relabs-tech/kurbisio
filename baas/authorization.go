package baas

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

// contextKey is the type for context keys. Go linter does not like plain strings
type contextKey string

// the predefined context key
const (
	contextKeyAuthorization contextKey = "baas_authorization_context_key"
)

/*Authorization is a context object which stores authorization information
for the user who is currently logged in.

An authorization carries a list or roles and various identifiers corresponding to
resources in the backend configuration.

Authorizations are added to a request context with

  ctx = auth.NewContextWithAuthorization(ctx)

and retrieved with

  auth := baas.AuthorizationFromContext(ctx)

The backend uses the authorization object for role based access control.
It is added to the context by a middleware based on the passed authroization
bearer token.
*/
type Authorization struct {
	Roles     []string             `json:"roles"`
	Resources map[string]uuid.UUID `json:"resources"`
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

// Identifier returns the identifier for the requested resource; if the
// identifier does not exist, it returns an empty uuid and false.
func (a *Authorization) Identifier(resource string) (uuid.UUID, bool) {
	if a == nil || a.Resources == nil {
		return uuid.UUID{}, false
	}
	value, ok := a.Resources[resource]
	return value, ok
}

// NewContextWithAuthorization returns a new context with this authorization added to it
func (a *Authorization) NewContextWithAuthorization(ctx context.Context) context.Context {
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

var cacheMutex sync.RWMutex
var cache map[string]*Authorization = make(map[string]*Authorization)

// AuthorizationFromCache returns an authorization from cache.
// This function is go-route safe
func AuthorizationFromCache(key string) *Authorization {
	cacheMutex.RLock()
	auth, ok := cache[key]
	cacheMutex.RUnlock()
	if ok {
		return auth
	}
	return nil
}

// AuthorizationToCache store an authorization in the cache.
// This function is go-route safe
func AuthorizationToCache(key string, auth *Authorization) {
	cacheMutex.Lock()
	cache[key] = auth
	cacheMutex.Unlock()
}
