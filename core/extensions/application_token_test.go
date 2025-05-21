package extensions_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/backend"
	"github.com/relabs-tech/kurbisio/core/extensions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplicationToken_UpdateConfig(t *testing.T) {
	ext := extensions.NewApplicationToken(nil, false)
	// UpdateConfig is a no-op, so just call it for coverage
	_, err := ext.UpdateConfig(backend.Configuration{}) // Assuming backend.Configuration is the correct type
	assert.NoError(t, err)
}

func TestApplicationToken_Middleware(t *testing.T) {
	configuredTokens := map[string][]string{
		"valid-token-1":             {"role-a", "role-b"},
		"valid-token-2":             {"role-c"},
		"token-for-no-initial-auth": {"role-x", "role-y"},
	}

	testCases := []struct {
		name               string
		requireToken       bool
		configuredTokens   map[string][]string
		requestTokenHeader string
		initialAuth        *access.Authorization
		expectedStatusCode int
		expectedRoles      []string
	}{
		{
			name:               "RequireToken:true, HeaderMissing",
			requireToken:       true,
			configuredTokens:   configuredTokens,
			expectedStatusCode: http.StatusUnauthorized,
		},
		{
			name:               "RequireToken:false, HeaderMissing",
			requireToken:       false,
			configuredTokens:   configuredTokens,
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "RequireToken:false, HeaderMissing, WithInitialAuth",
			requireToken:       false,
			configuredTokens:   configuredTokens,
			initialAuth:        &access.Authorization{Roles: []string{"initial-role"}},
			expectedStatusCode: http.StatusOK,
			expectedRoles:      []string{"initial-role"}, // Should remain unchanged
		},
		{
			name:               "RequireToken:true, HeaderPresent, TokenInvalid",
			requireToken:       true,
			configuredTokens:   configuredTokens,
			requestTokenHeader: "invalid-token",
			expectedStatusCode: http.StatusUnauthorized,
		},
		{
			name:               "RequireToken:true, HeaderPresent, TokenInvalid, WithInitialAuth",
			requireToken:       true,
			configuredTokens:   configuredTokens,
			requestTokenHeader: "invalid-token",
			initialAuth:        &access.Authorization{Roles: []string{"initial-role"}},
			expectedStatusCode: http.StatusUnauthorized,
			expectedRoles:      []string{"initial-role"},
		},
		{
			name:               "RequireToken:false, HeaderPresent, TokenValid, NoInitialAuth",
			requireToken:       false,
			configuredTokens:   configuredTokens,
			requestTokenHeader: "token-for-no-initial-auth",
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "RequireToken:false, HeaderPresent, TokenValid, WithInitialAuth, RolesFiltered",
			requireToken:       false,
			configuredTokens:   configuredTokens,
			requestTokenHeader: "valid-token-1",
			initialAuth:        &access.Authorization{Roles: []string{"role-a", "role-c", "initial-role"}, Selectors: map[string]string{"key": "value"}},
			expectedStatusCode: http.StatusOK,
			expectedRoles:      []string{"role-a"}, // Only role-a is in valid-token-1 and initialAuth
		},
		{
			name:               "RequireToken:false, HeaderPresent, TokenValid, WithInitialAuth, NoOverlappingRoles",
			requireToken:       false,
			configuredTokens:   configuredTokens,
			requestTokenHeader: "valid-token-2", // Allows role-c
			initialAuth:        &access.Authorization{Roles: []string{"role-a", "initial-role"}},
			expectedStatusCode: http.StatusOK,
			expectedRoles:      []string{}, // No overlap
		},
		{
			name:               "RequireToken:false, HeaderPresent, TokenValid, AllRolesAllowed",
			requireToken:       false,
			configuredTokens:   map[string][]string{"all-roles-token": {"*"}},
			requestTokenHeader: "all-roles-token",
			initialAuth:        &access.Authorization{Roles: []string{"role-x", "role-y", "admin"}, Selectors: map[string]string{"tenant": "t1"}},
			expectedStatusCode: http.StatusOK,
			expectedRoles:      []string{"role-x", "role-y", "admin"}, // All initial roles should be preserved
		},
		{
			name:               "Initialization: NoTokensConfigured, RequireToken:false",
			requireToken:       false,
			configuredTokens:   map[string][]string{}, // Empty map
			expectedStatusCode: http.StatusOK,         // Middleware shouldn't run or alter request
		},
		{
			name:               "Initialization: NilTokensConfigured, RequireToken:false",
			requireToken:       false,
			configuredTokens:   nil, // Nil map
			expectedStatusCode: http.StatusOK,
		},
		{
			name:               "Initialization: NoTokensConfigured, RequireToken:true, HeaderMissing",
			requireToken:       true,
			configuredTokens:   map[string][]string{},
			expectedStatusCode: http.StatusUnauthorized,
		},
		{
			name:               "Initialization: NoTokensConfigured, RequireToken:true, HeaderPresentButInvalid",
			requireToken:       true,
			configuredTokens:   map[string][]string{},
			requestTokenHeader: "some-token",
			expectedStatusCode: http.StatusUnauthorized,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ext := extensions.NewApplicationToken(tc.configuredTokens, tc.requireToken)
			router := mux.NewRouter()
			err := ext.UpdateMux(router)
			require.NoError(t, err) // Assuming UpdateMux doesn't error out in these valid test cases

			// Dummy handler to check the context
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				auth := access.AuthorizationFromContext(r.Context())
				if auth != nil {
					assert.ElementsMatch(t, tc.expectedRoles, auth.Roles, "Roles in context do not match expected roles")
					if tc.initialAuth != nil && len(tc.expectedRoles) > 0 { // Check if selectors are preserved when roles are filtered
						assert.Equal(t, tc.initialAuth.Selectors, auth.Selectors, "Selectors should be preserved")
					}
				}

				w.WriteHeader(http.StatusOK) // Actual handler success
			})

			// If middleware is not expected to run (e.g. no tokens and not required),
			// we wrap the handler directly without the router's Use chain for that specific sub-test logic.
			// However, UpdateMux already handles not adding the middleware. So we always use the router.
			finalHandler := router
			router.Handle("/test", handler) // Apply the handler to a route

			req := httptest.NewRequest("GET", "/test", nil)
			if tc.requestTokenHeader != "" {
				req.Header.Set(extensions.ApplicationTokenHeader, tc.requestTokenHeader)
			}
			if tc.initialAuth != nil {
				ctx := access.ContextWithAuthorization(req.Context(), tc.initialAuth)
				req = req.WithContext(ctx)
			}

			rr := httptest.NewRecorder()
			finalHandler.ServeHTTP(rr, req)

			assert.Equal(t, tc.expectedStatusCode, rr.Code, "HTTP status code does not match")
		})
	}
}
