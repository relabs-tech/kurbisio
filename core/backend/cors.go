package backend

import (
	"net/http"
	"net/url"

	"github.com/gorilla/handlers"
)

// CORSOption configures the CORS middleware.
type CORSOption func(*corsConfig)

type corsConfig struct {
	allowLocalhost bool
	allowedOrigins []string
}

// WithOrigins adds the given origins to the CORS allowlist.
func WithOrigins(origins ...string) CORSOption {
	return func(c *corsConfig) {
		c.allowedOrigins = append(c.allowedOrigins, origins...)
	}
}

// WithLocalhost allows http://localhost and http://127.0.0.1 origins.
func WithLocalhost() CORSOption {
	return func(c *corsConfig) {
		c.allowLocalhost = true
	}
}

// isLocalhostOrigin returns true only when the origin has scheme "http" and a hostname
// of exactly "localhost" or "127.0.0.1" (with any port or none). This prevents
// bypasses via origins like "http://localhost.evil.com" or "http://localhost-bad".
func isLocalhostOrigin(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Scheme != "http" {
		return false
	}
	host := u.Hostname() // strips port
	return host == "localhost" || host == "127.0.0.1"
}

// CORS returns a CORS middleware handler with predefined settings suitable for Kurbisio backend services.
// If no WithOrigins option is provided, all origins are allowed.
// Use WithLocalhost() to additionally allow http://localhost and http://127.0.0.1 origins.
func CORS(opts ...CORSOption) func(http.Handler) http.Handler {
	cfg := &corsConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	allowedOriginsMap := map[string]bool{}
	for _, origin := range cfg.allowedOrigins {
		allowedOriginsMap[origin] = true
	}
	return handlers.CORS(
		handlers.ExposedHeaders([]string{
			"Access-Control-Allow-Origin",
			"Access-Control-Allow-Methods",
			"Access-Control-Allow-Headers",
			"Access-Control-Expose-Headers",
			"Access-Control-Max-Age",
			"ETag",
			"If-None-Match",
			"Kurbisio-Content-Encoding",
		}),
		handlers.AllowedOriginValidator(func(o string) bool {
			if len(allowedOriginsMap) == 0 {
				return true // Allow all origins if no specific allowed origins are provided
			}
			_, ok := allowedOriginsMap[o]
			return ok || (cfg.allowLocalhost && isLocalhostOrigin(o))
		}),
		handlers.AllowedMethods([]string{
			"DELETE",
			"GET",
			"OPTIONS",
			"PATCH",
			"POST",
			"PUT",
		}),
		handlers.AllowedHeaders([]string{
			"Accept-Encoding",
			"Authorization",
			"Content-Length",
			"Content-Type",
			"ETag",
			"If-None-Match",
			"Kurbisio-Content-Encoding",
			"X-Application-Token",
			"X-CSRF-Token",
		}),
		handlers.MaxAge(600),
	)
}
