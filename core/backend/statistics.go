// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/goccy/go-json"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/logger"
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
	Collections []resourceStatistics `json:"collections"`
	Singletons  []resourceStatistics `json:"singletons"`
	Relations   []resourceStatistics `json:"relations"`
	Blobs       []resourceStatistics `json:"blobs"`
}

func (b *Backend) handleStatistics(router *mux.Router) {
	logger.Default().Debugln("statistics")
	logger.Default().Debugln("  handle statistics route: /kuribisio/statistics GET")
	router.Handle("/kurbisio/statistics", handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		b.statisticsWithAuth(w, r)
	}))).Methods(http.MethodOptions, http.MethodGet)
}

func (b *Backend) statisticsWithAuth(w http.ResponseWriter, r *http.Request) {
	if b.authorizationEnabled {
		auth := access.AuthorizationFromContext(r.Context())
		if !auth.HasRole("admin") && !auth.HasRole("admin viewer") {
			http.Error(w, "not authorized", http.StatusUnauthorized)
			return
		}
	}
	s := statisticsDetails{}
	var collections, singletons, relations, blobs sort.StringSlice
	for _, r := range b.config.Collections {
		collections = append(collections, r.Resource)
	}
	for _, r := range b.config.Singletons {
		singletons = append(singletons, r.Resource)
	}
	for _, r := range b.config.Relations {
		relations = append(relations, r.Left+":"+r.Right)
	}
	for _, r := range b.config.Blobs {
		blobs = append(blobs, r.Resource)
	}
	// Sort the resources so that ETag is unchanged regardless of the order of resources
	collections.Sort()
	singletons.Sort()
	relations.Sort()
	blobs.Sort()

	queryStatisticsFromDB := func(stats *[]resourceStatistics, resources sort.StringSlice) {
		*stats = []resourceStatistics{} // do not return null in json, but empty array
		for _, resource := range resources {
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

			*stats = append(*stats, resourceStatistics{
				Resource:     resource,
				Count:        count,
				SizeMB:       float64(size) / 1024. / 1024.,
				AverageSizeB: averageSize,
			})
		}
	}
	queryStatisticsFromDB(&s.Collections, collections)
	queryStatisticsFromDB(&s.Singletons, singletons)
	queryStatisticsFromDB(&s.Relations, relations)
	queryStatisticsFromDB(&s.Blobs, blobs)

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
