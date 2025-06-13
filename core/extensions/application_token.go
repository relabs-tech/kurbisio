package extensions

import (
	"context"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/backend"
	"github.com/relabs-tech/kurbisio/core/logger"
)

const (
	ApplicationTokenHeader   = "X-Application-Token"
	applicationTokenDisabled = "application_token_disabled"
)

// ApplicationToken is a Kurbisio extension that limits roles based on an application token.
//
// Features:
// - Application tokens and their allowed roles are configured directly in the extension.
// - Middleware checks the "X-Application-Token" header.
// - If a valid token is found, the request's authorization roles are filtered based on "allowed_roles".
// - If RequireToken is true, requests without the token header will be rejected.
// - If one of the roles is *, all roles are allowed.
type ApplicationToken struct {
	// ConfiguredTokens maps an application token string to a list of allowed roles.
	AllowedRoles map[string][]string
	// RequireToken, if true, will cause requests without the X-Application-Token header to be rejected with Unauthorized.
	RequireToken bool
}

// NewApplicationToken creates a new ApplicationToken extension with the given token configuration.
func NewApplicationToken(allowedRoles map[string][]string, requireToken bool) *ApplicationToken {
	return &ApplicationToken{
		AllowedRoles: allowedRoles,
		RequireToken: requireToken,
	}
}

// DisableApplicationToken disables the application token extension for the given context.
func DisableApplicationToken(ctx context.Context) context.Context {
	return context.WithValue(ctx, applicationTokenDisabled, true)
}

// IsApplicationTokenDisabled checks if the application token extension is disabled for the given context.
func IsApplicationTokenDisabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	if v := ctx.Value(applicationTokenDisabled); v != nil {
		return v.(bool)
	}
	return false
}

// GetName returns the name of the extension.
func (a *ApplicationToken) GetName() string {
	return "ApplicationToken"
}

// UpdateConfig updates the Kurbisio configuration.
// This method is a no-op for the ApplicationToken extension.
func (a *ApplicationToken) UpdateConfig(config backend.Configuration) (backend.Configuration, error) {
	return config, nil
}

// UpdateMux adds middleware to the router to handle application token authentication.
func (a *ApplicationToken) UpdateMux(router *mux.Router) error {
	if (len(a.AllowedRoles) == 0) && !a.RequireToken {
		// If no tokens are configured and token is not required, the middleware is pointless or has no effect.
		rlog := logger.FromContext(context.Background())
		rlog.Warn("ApplicationToken extension loaded but no tokens configured.")
		return nil
	}

	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if IsApplicationTokenDisabled(r.Context()) {
				next.ServeHTTP(w, r)
				return
			}

			appToken := r.Header.Get(ApplicationTokenHeader)
			rlog := logger.FromContext(r.Context())

			if appToken == "" {
				if a.RequireToken {
					rlog.Warnf("Request to %s rejected: X-Application-Token header is missing and RequireToken is true.", r.URL.Path)
					http.Error(w, "Unauthorized: Application token is required.", http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r)
				return
			}
			allRolesAllowed := false
			for _, allowedRole := range a.AllowedRoles[appToken] {
				if allowedRole == "*" {
					allRolesAllowed = true
					break
				}
			}
			if allRolesAllowed {
				next.ServeHTTP(w, r)
				return
			}

			// Look up the token in the configured map
			allowedRoles, tokenFound := a.AllowedRoles[appToken]

			if !tokenFound {
				rlog.Warnf("Application token not found in configuration: %s", appToken)
				if a.RequireToken {
					http.Error(w, "Unauthorized: Invalid application token.", http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			currentAuth := access.AuthorizationFromContext(r.Context())

			if currentAuth != nil && len(currentAuth.Roles) > 0 {
				// Filter existing roles based on what the application token allows.
				allowedRolesMap := make(map[string]bool)
				for _, role := range allowedRoles {
					allowedRolesMap[role] = true
				}

				filteredRoles := []string{}
				for _, existingRole := range currentAuth.Roles {
					if allowedRolesMap[existingRole] {
						filteredRoles = append(filteredRoles, existingRole)
					}
				}

				if len(filteredRoles) == 0 && len(currentAuth.Roles) > 0 {
					rlog.Warnf("Application token %s restricted all existing roles. User effectively has no roles.", appToken)
				}

				newAuth := &access.Authorization{
					Roles:     filteredRoles,
					Selectors: currentAuth.Selectors,
				}
				ctx := access.ContextWithAuthorization(r.Context(), newAuth)
				r = r.WithContext(ctx)
				rlog.Infof("Filtered authorization roles to %v for application token", filteredRoles)
			}
			next.ServeHTTP(w, r)
		})
	})
	return nil
}
