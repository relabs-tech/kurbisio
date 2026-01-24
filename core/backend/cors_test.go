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

	corsMiddleware := CORS(WithOrigins(allowedOrigins...))

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
	corsMiddleware := CORS(WithLocalhost(), WithOrigins(allowedOrigins...))

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

// TestCORS_LocalhostBypassRejected verifies that origins that merely start with "localhost" or
// "127.0.0.1" in their hostname are rejected when WithLocalhost() is active.
func TestCORS_LocalhostBypassRejected(t *testing.T) {
	corsMiddleware := CORS(WithLocalhost(), WithOrigins("https://example.com"))

	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	bypassAttempts := []string{
		"http://localhost.evil.com",
		"http://localhost-malicious.com",
		"http://localhost.attacker.io",
		"http://127.0.0.1.evil.com",
		"http://127.0.0.1-bad.com",
		"https://localhost", // https, not http
		"https://127.0.0.1", // https, not http
	}

	for _, origin := range bypassAttempts {
		t.Run(origin, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			req.Header.Set("Origin", origin)

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			assert.NotEqual(t, origin, rec.Header().Get("Access-Control-Allow-Origin"),
				"Origin %s should not bypass the localhost allowlist", origin)
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
	corsMiddleware := CORS(WithOrigins("https://example.com"))

	handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set various headers that should be exposed
		w.Header().Set("ETag", `"test-etag-value"`)
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
	assert.Contains(t, exposedHeaders, "Kurbisio-Content-Encoding", "Kurbisio-Content-Encoding should be in exposed headers")

	// Verify the actual header values are set
	assert.Equal(t, `"test-etag-value"`, rec.Header().Get("ETag"))
	assert.Equal(t, "gzip", rec.Header().Get("Kurbisio-Content-Encoding"))
}

// TestCORS_PreflightRequest verifies that preflight OPTIONS requests are handled correctly
func TestCORS_PreflightRequest(t *testing.T) {
	corsMiddleware := CORS(WithOrigins("https://example.com"))

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
	corsMiddleware := CORS(WithOrigins("https://example.com"))

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
