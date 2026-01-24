package backend

import (
	"net/http"
	"strings"

	"github.com/gorilla/handlers"
)

// CORS returns a CORS middleware handler with predefined settings suitable for Kurbisio backend services
// with the allowed origin set to allowedOrigins. If no allowed origins are provided, it defaults to allowing all origins
func CORS(allowedOrigins ...string) func(http.Handler) http.Handler {
	allowedOriginsMap := map[string]bool{}
	for _, origin := range allowedOrigins {
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
			return ok || strings.HasPrefix(o, "http://localhost") || strings.HasPrefix(o, "http://127.0.0.1")
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
