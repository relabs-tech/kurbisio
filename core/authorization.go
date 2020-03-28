package core

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

// contextKey is the type for context keys. Go linter does not like plain strings
type contextKey string

// the predefined context key
const (
	contextKeyAuthorization contextKey = "_authorization_"
)

/*Authorization is a context object which stores authorization information
for the user who is currently logged in.

An authorization carries a list or roles and various identifiers corresponding to
resources in the backend configuration.

Authorizations are added to a request context with

  ctx = auth.ContextWithAuthorization(ctx)

and retrieved with

  auth := AuthorizationFromContext(ctx)

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
