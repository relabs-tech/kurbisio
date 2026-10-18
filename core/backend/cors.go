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
	isOriginAllowed := func(origin string) bool {
		if len(allowedOriginsMap) == 0 {
			return true // Allow all origins if no specific allowed origins are provided.
		}
		_, ok := allowedOriginsMap[origin]
		return ok || strings.HasPrefix(origin, "http://localhost") || strings.HasPrefix(origin, "http://127.0.0.1")
	}
	cors := handlers.CORS(
		handlers.ExposedHeaders([]string{
			"Access-Control-Allow-Headers",
			"Access-Control-Allow-Methods",
			"Access-Control-Allow-Origin",
			"Access-Control-Expose-Headers",
			"Access-Control-Max-Age",
			"ETag",
			"If-None-Match",
			"Kurbisio-Content-Encoding",
			"Pagination-Limit",
			"Pagination-Next-Token",
			"X-RateLimit-Limit",
			"X-RateLimit-Remaining",
			"X-RateLimit-Reset",
		}),
		handlers.AllowedOriginValidator(isOriginAllowed),
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
	return func(next http.Handler) http.Handler {
		handler := cors(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if r.Method == http.MethodOptions && origin != "" && !isOriginAllowed(origin) {
				// Gorilla returns without writing for rejected preflights. Native HTTP
				// supplies an implicit 200, but Lambda's proxy writer requires a status.
				w.WriteHeader(http.StatusForbidden)
				return
			}
			handler.ServeHTTP(w, r)
		})
	}
}
