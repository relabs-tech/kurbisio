// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend

import (
	"net/http"

	"github.com/goccy/go-json"

	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/logger"
)

var (
	// Version is the version of the current build
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
		if !auth.HasRoles() { // version is authorized to everybody
			http.Error(w, "not authorized", http.StatusUnauthorized)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	data, _ := json.Marshal(map[string]string{"version": Version})
	w.Write(data)
}
