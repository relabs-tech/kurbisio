package backend

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func (b *Backend) handleCORS(router *mux.Router) {

	corseMiddleware := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
			w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
			if r.Method == http.MethodOptions {
				log.Println("called route for", r.URL, r.Method, " (handled by CORS middleware)")
				w.WriteHeader(http.StatusOK)
				return
			}
			h.ServeHTTP(w, r)
		})
	}
	router.Use(corseMiddleware)
}
