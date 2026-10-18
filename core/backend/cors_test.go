// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCORS_AllowedOrigins verifies that origins in the allowlist are accepted
func TestCORS_AllowedOrigins(t *testing.T) {
	allowedOrigins := []string{
		"https://example.com",
		"https://app.example.com",
	}

	corsMiddleware := CORS(allowedOrigins...)

	// Create a simple handler that the middleware will wrap
	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"test-etag-123"`)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))

	tests := []struct {
		name          string
		origin        string
		expectAllowed bool
	}{
		{
			name:          "allowed origin https://example.com",
			origin:        "https://example.com",
			expectAllowed: true,
		},
		{
			name:          "allowed origin https://app.example.com",
			origin:        "https://app.example.com",
			expectAllowed: true,
		},
		{
			name:          "disallowed origin https://evil.com",
			origin:        "https://evil.com",
			expectAllowed: false,
		},
		{
			name:          "disallowed origin https://phishing.com",
			origin:        "https://phishing.com",
			expectAllowed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			req.Header.Set("Origin", tt.origin)

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if tt.expectAllowed {
				assert.Equal(t, tt.origin, rec.Header().Get("Access-Control-Allow-Origin"),
					"Expected origin %s to be allowed", tt.origin)
			} else {
				// When origin is not allowed, the Access-Control-Allow-Origin header should not be set to the origin
				assert.NotEqual(t, tt.origin, rec.Header().Get("Access-Control-Allow-Origin"),
					"Expected origin %s to be rejected", tt.origin)
			}
		})
	}
}

// TestCORS_LocalhostAlwaysAllowed verifies that localhost origins are always allowed
func TestCORS_LocalhostAlwaysAllowed(t *testing.T) {
	// Create CORS middleware with specific allowed origins that don't include localhost
	allowedOrigins := []string{"https://example.com"}
	corsMiddleware := CORS(allowedOrigins...)

	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))

	localhostOrigins := []string{
		"http://localhost:3000",
		"http://localhost:8080",
		"http://localhost",
		"http://127.0.0.1:3000",
		"http://127.0.0.1:8080",
		"http://127.0.0.1",
	}

	for _, origin := range localhostOrigins {
		t.Run(origin, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			req.Header.Set("Origin", origin)

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, origin, rec.Header().Get("Access-Control-Allow-Origin"),
				"Expected localhost origin %s to be always allowed", origin)
		})
	}
}

// TestCORS_NoAllowlistAllowsAll verifies that when no allowlist is provided, all origins are allowed
func TestCORS_NoAllowlistAllowsAll(t *testing.T) {
	// Create CORS middleware without specific allowed origins
	corsMiddleware := CORS()

	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))

	testOrigins := []string{
		"https://example.com",
		"https://some-random-site.com",
		"https://anything.goes.com",
		"http://localhost:3000",
	}

	for _, origin := range testOrigins {
		t.Run(origin, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			req.Header.Set("Origin", origin)

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, origin, rec.Header().Get("Access-Control-Allow-Origin"),
				"Expected origin %s to be allowed when no allowlist is specified", origin)
		})
	}
}

// TestCORS_ExposedHeaders verifies that critical headers like ETag are exposed for cross-origin requests
func TestCORS_ExposedHeaders(t *testing.T) {
	corsMiddleware := CORS("https://example.com")

	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set various headers that should be exposed
		w.Header().Set("ETag", `"test-etag-value"`)
		w.Header().Set("If-None-Match", `"some-value"`)
		w.Header().Set("Kurbisio-Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Origin", "https://example.com")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Check that Access-Control-Expose-Headers includes the critical headers
	exposedHeaders := rec.Header().Get("Access-Control-Expose-Headers")

	assert.NotEmpty(t, exposedHeaders, "Access-Control-Expose-Headers should be set")
	// Note: gorilla/handlers may normalize header names (e.g., "Etag" instead of "ETag")
	assert.True(t,
		strings.Contains(exposedHeaders, "ETag") || strings.Contains(exposedHeaders, "Etag"),
		"ETag should be in exposed headers")
	assert.Contains(t, exposedHeaders, "If-None-Match", "If-None-Match should be in exposed headers")
	assert.Contains(t, exposedHeaders, "Kurbisio-Content-Encoding", "Kurbisio-Content-Encoding should be in exposed headers")

	// Verify the actual header values are set
	assert.Equal(t, `"test-etag-value"`, rec.Header().Get("ETag"))
	assert.Equal(t, `"some-value"`, rec.Header().Get("If-None-Match"))
	assert.Equal(t, "gzip", rec.Header().Get("Kurbisio-Content-Encoding"))
}

// TestCORS_PreflightRequest verifies that preflight OPTIONS requests are handled correctly
func TestCORS_PreflightRequest(t *testing.T) {
	corsMiddleware := CORS("https://example.com")

	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))

	req := httptest.NewRequest("OPTIONS", "/test", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "Content-Type, Authorization")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Verify CORS preflight response - origin should be allowed
	assert.Equal(t, "https://example.com", rec.Header().Get("Access-Control-Allow-Origin"))

	allowedHeaders := rec.Header().Get("Access-Control-Allow-Headers")
	assert.Contains(t, allowedHeaders, "Content-Type", "Content-Type should be in allowed headers")
	assert.Contains(t, allowedHeaders, "Authorization", "Authorization should be in allowed headers")

	// Max-Age should be set if this is processed as a preflight
	maxAge := rec.Header().Get("Access-Control-Max-Age")
	assert.Equal(t, "600", maxAge, "Access-Control-Max-Age should be 600")
}

// TestCORS_DisallowedOriginHeaders verifies that CORS headers are not set for disallowed origins
func TestCORS_DisallowedOriginHeaders(t *testing.T) {
	corsMiddleware := CORS("https://example.com")

	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"test-etag"`)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Origin", "https://malicious.com")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// The request should still be processed (status 200)
	assert.Equal(t, http.StatusOK, rec.Code)

	// But CORS headers should not allow the malicious origin
	assert.NotEqual(t, "https://malicious.com", rec.Header().Get("Access-Control-Allow-Origin"),
		"Malicious origin should not be allowed")
}

func TestCORS_RejectedPreflightWritesForbidden(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut} {
		t.Run(method, func(t *testing.T) {
			called := false
			handler := CORS("https://example.com")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			}))
			req := httptest.NewRequest(http.MethodOptions, "/test", nil)
			req.Header.Set("Origin", "https://unapproved.example")
			req.Header.Set("Access-Control-Request-Method", method)
			req.Header.Set("Access-Control-Request-Headers", "Authorization, Content-Type")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Empty(t, rec.Body.String())
			assert.False(t, called, "rejected preflights must not invoke the downstream handler")
			for _, name := range []string{"Access-Control-Allow-Origin", "Access-Control-Allow-Methods", "Access-Control-Allow-Headers", "Access-Control-Allow-Credentials"} {
				assert.Empty(t, rec.Header().Get(name), "rejected preflight grants access through %s", name)
			}
		})
	}
}

func TestCORS_PreflightOriginRules(t *testing.T) {
	for _, test := range []struct {
		name    string
		origins []string
		origin  string
	}{
		{"approved", []string{"https://example.com"}, "https://example.com"},
		{"localhost", []string{"https://example.com"}, "http://localhost:3005"},
		{"loopback", []string{"https://example.com"}, "http://127.0.0.1:3005"},
		{"no allowlist", nil, "https://other.example"},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := false
			handler := CORS(test.origins...)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
			}))
			req := httptest.NewRequest(http.MethodOptions, "/test", nil)
			req.Header.Set("Origin", test.origin)
			req.Header.Set("Access-Control-Request-Method", http.MethodPut)
			req.Header.Set("Access-Control-Request-Headers", "Authorization, Content-Type")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, test.origin, rec.Header().Get("Access-Control-Allow-Origin"))
			assert.Equal(t, http.MethodPut, rec.Header().Get("Access-Control-Allow-Methods"))
			assert.Contains(t, rec.Header().Get("Access-Control-Allow-Headers"), "Authorization")
			assert.Equal(t, "600", rec.Header().Get("Access-Control-Max-Age"))
			assert.False(t, called)
		})
	}
}

func TestCORS_NoOriginUnchanged(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		t.Run(method, func(t *testing.T) {
			called := false
			handler := CORS("https://example.com")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusAccepted)
			}))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(method, "/test", nil))
			if method == http.MethodGet {
				assert.True(t, called)
				assert.Equal(t, http.StatusAccepted, rec.Code)
			} else {
				assert.False(t, called)
				assert.Equal(t, http.StatusOK, rec.Code)
			}
			assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
		})
	}
}
