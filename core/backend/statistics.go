package backend

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core/access"
	"github.com/relabs-tech/backends/core/logger"
)

// resourceStatistics represents information about a resource
type resourceStatistics struct {
	Resource     string  `json:"resource"`
	Count        int64   `json:"count"`
	SizeMB       float64 `json:"size_mb"`
	AverageSizeB float64 `json:"average_size_b"`
}

// statistics represents information about the backend resources
type statisticsDetails struct {
	Resources []resourceStatistics `json:"resources"`
}

func (b *Backend) handleStatistics(router *mux.Router) {
	logger.Default().Debugln("statistics")
	logger.Default().Debugln("  handle statistics route: /kuribisio/statistics GET")
	router.HandleFunc("/kurbisio/statistics", func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		b.statisticsWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodGet)
}

func (b *Backend) statisticsWithAuth(w http.ResponseWriter, r *http.Request) {
	if b.authorizationEnabled {
		auth := access.AuthorizationFromContext(r.Context())
		if !auth.HasRole("admin") {
			http.Error(w, "not authorized", http.StatusUnauthorized)
			return
		}
	}
	s := statisticsDetails{}
	var tables sort.StringSlice
	for _, r := range b.config.Collections {
		tables = append(tables, r.Resource)
	}
	for _, r := range b.config.Blobs {
		tables = append(tables, r.Resource)
	}
	for _, r := range b.config.Singletons {
		tables = append(tables, r.Resource)
	}
	// Sort the resources so that ETag is unchanged regardless of the order of resources
	tables.Sort()
	for _, resource := range tables {
		row := b.db.QueryRow(fmt.Sprintf(`SELECT pg_total_relation_size('%s."%s"'), count(*) FROM %s."%s" `, b.db.Schema, resource, b.db.Schema, resource))
		var size, count int64
		if err := row.Scan(&size, &count); err != nil {
			logger.FromContext(nil).WithError(err).Errorln("Error 4028: Scan")
			http.Error(w, "Error 4028: ", http.StatusInternalServerError)
			return
		}
		var averageSize float64 = 0
		if count != 0 {
			averageSize = float64(size / count)
		}

		s.Resources = append(s.Resources, resourceStatistics{
			Resource:     resource,
			Count:        count,
			SizeMB:       float64(size) / 1024. / 1024.,
			AverageSizeB: averageSize,
		})
	}

	jsonData, _ := json.Marshal(s)
	etag := bytesToEtag(jsonData)
	w.Header().Set("Etag", etag)
	if ifNoneMatchFound(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(jsonData)
}
