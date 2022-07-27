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
	"strings"

	"github.com/goccy/go-json"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/logger"
)

// ResourceStatistics represents information about a resource
type ResourceStatistics struct {
	Resource     string  `json:"resource"`
	Count        int64   `json:"count"`
	SizeMB       float64 `json:"size_mb"`
	AverageSizeB float64 `json:"average_size_b"`
}

// StatisticsDetails represents information about the backend resources
type StatisticsDetails struct {
	Collections []ResourceStatistics `json:"collections"`
	Singletons  []ResourceStatistics `json:"singletons"`
	Relations   []ResourceStatistics `json:"relations"`
	Blobs       []ResourceStatistics `json:"blobs"`
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

	s := StatisticsDetails{}
	var collections, singletons, relations, blobs sort.StringSlice
	for _, r := range b.config.Collections {
		collections = append(collections, r.Resource)
	}
	for _, r := range b.config.Singletons {
		singletons = append(singletons, r.Resource)
	}
	for _, r := range b.config.Relations {
		resource := r.Left + ":" + r.Right
		if r.Resource != "" {
			resource = r.Resource + ":" + resource
		}
		relations = append(relations, resource)
	}
	for _, r := range b.config.Blobs {
		blobs = append(blobs, r.Resource)
	}
	// Sort the resources so that ETag is unchanged regardless of the order of resources
	collections.Sort()
	singletons.Sort()
	relations.Sort()
	blobs.Sort()

	var allResources []string
	allResources = append(allResources, collections...)
	allResources = append(allResources, singletons...)
	allResources = append(allResources, relations...)
	allResources = append(allResources, blobs...)

	var err error
	urlQuery := r.URL.Query()
	filter := map[string]bool{}
	for key, array := range urlQuery {
		if key != "resource" && len(array) > 1 {
			http.Error(w, "illegal parameter array '"+key+"'", http.StatusBadRequest)
			return
		}
		switch key {
		case "resource":
			for _, values := range array {
				for _, value := range strings.Split(values, ",") {
					found := false
					for _, r := range allResources {
						if value == r {
							found = true
							filter[value] = true
							break
						}
					}
					if !found {
						err = fmt.Errorf("unknown resource %s", value)
					}
				}
			}
		default:
			err = fmt.Errorf("unknown")
		}

		if err != nil {
			http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	queryStatisticsFromDB := func(stats *[]ResourceStatistics, resources sort.StringSlice) {
		*stats = []ResourceStatistics{} // do not return null in json, but empty array
		for _, resource := range resources {
			if len(filter) > 0 && filter[resource] == false {
				continue
			}
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

			*stats = append(*stats, ResourceStatistics{
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
