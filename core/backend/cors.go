package backend

import (
	"net/http"

	"github.com/relabs-tech/backends/core/logger"
)

func (b *Backend) handleCORS() {

	corseMiddleware := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
			w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, If-None-Match")
			w.Header().Set("Access-Control-Expose-Headers", "*")

			if r.Method == http.MethodOptions {
				logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method, " (handled by CORS middleware)")
				w.WriteHeader(http.StatusOK)
				return
			}
			h.ServeHTTP(w, r)
		})
	}
	b.router.Use(corseMiddleware)
}
