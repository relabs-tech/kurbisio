package backend

import (
	"net/http"

	"github.com/gorilla/mux"
)

func (b *Backend) handleCORS(router *mux.Router) {
	router.HandleFunc("/{rest:.+}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}).Methods(http.MethodOptions)

	corseMiddleware := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
			w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
			if r.Method == "OPTIONS" {
				h.ServeHTTP(w, r)
				return
			}
			h.ServeHTTP(w, r)
		})
	}
	router.Use(corseMiddleware)
}