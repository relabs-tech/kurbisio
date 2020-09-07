package backend

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core/access"
	"github.com/relabs-tech/backends/core/logger"
)

var (
	// Version is the version of the curent build
	Version = "unset"
)

func (b *Backend) handleVersion(router *mux.Router) {
	logger.Default().Debugln("version")
	logger.Default().Debugln("  handle version route: /version GET")
	router.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		b.versionWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodGet)
}

func (b *Backend) versionWithAuth(w http.ResponseWriter, r *http.Request) {
	if b.authorizationEnabled {
		auth := access.AuthorizationFromContext(r.Context())
		if !auth.HasRole("admin") {
			http.Error(w, "not authorized", http.StatusUnauthorized)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	data, _ := json.Marshal(map[string]string{"version": Version})
	w.Write(data)
}
