package backend

import (
	"fmt"
	"log"
	"strings"

	"net/http"

	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core"
	"github.com/relabs-tech/backends/core/access"
)

func (b *Backend) createRelationResource(router *mux.Router, rc relationConfiguration) {
	schema := b.db.Schema
	resource := rc.Resource
	resources := strings.Split(resource, "/")
	this := resources[len(resources)-1]
	dependencies := resources[:len(resources)-1]

	origin := rc.Origin
	origins := strings.Split(origin, "/")

	columns := map[string]string{}
	resourceColumns := []string{}
	originColumns := []string{}
	createColumns := []string{}

	log.Println("create relation:", resource)
	createQuery := fmt.Sprintf("CREATE table IF NOT EXISTS %s.\"%s\"", schema, resource)

	for _, r := range resources {
		resourceColumns = append(resourceColumns, r+"_id")
		columns[r] = r
	}
	for _, o := range origins {
		originColumns = append(originColumns, o+"_id")
		columns[o] = o
	}

	for c := range columns {
		createColumn := c + "_id uuid NOT NULL"
		createColumns = append(createColumns, createColumn)
	}

	if len(dependencies) > 0 {
		foreignColumns := strings.Join(resourceColumns[:len(resourceColumns)-1], ",")
		createColumn := "FOREIGN KEY (" + foreignColumns + ") " +
			"REFERENCES " + schema + ".\"" + strings.Join(dependencies, "/") + "\" " +
			"(" + foreignColumns + ") ON DELETE CASCADE"
		createColumns = append(createColumns, createColumn)
	}

	foreignColumns := strings.Join(originColumns, ",")
	createColumn := "FOREIGN KEY (" + foreignColumns + ") " +
		"REFERENCES " + schema + ".\"" + origin + "\" " +
		"(" + foreignColumns + ") ON DELETE CASCADE"
	createColumns = append(createColumns, createColumn)

	if len(columns) > 1 {
		createColumn := "UNIQUE (" + strings.Join(resourceColumns, ",") + ")"
		createColumns = append(createColumns, createColumn)
	}

	createQuery += "(" + strings.Join(createColumns, ", ") + ");"

	_, err := b.db.Query(createQuery)
	if err != nil {
		panic(err)
	}

	collection := b.collectionHelper[this]

	sqlInjectRelation := fmt.Sprintf("AND %s_id IN (SELECT %s_id FROM %s.\"%s\" WHERE %%s) ", this, this, schema, resource)
	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" (%s) VALUES(%s);", schema, resource, strings.Join(resourceColumns, ","), parameterString(len(resourceColumns)))
	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" WHERE %s;", schema, resource, compareString(resourceColumns))

	allRoute := ""
	oneRoute := ""
	for _, r := range resources {
		allRoute = oneRoute + "/" + plural(r)
		oneRoute = oneRoute + "/" + plural(r) + "/{" + r + "_id}"
	}

	log.Println("  handle routes:", allRoute, "GET,POST,PUT")
	log.Println("  handle routes:", oneRoute, "GET,DELETE")

	// READ ALL
	router.HandleFunc(allRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationRead, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		queryParameters := make([]interface{}, len(resourceColumns)-1)
		for i := 0; i < len(resourceColumns)-1; i++ { // skip ID
			queryParameters[i] = params[resourceColumns[i]]
		}
		injectRelation := &relationInjection{
			subquery:        sqlInjectRelation,
			columns:         resourceColumns[:len(resourceColumns)-1],
			queryParameters: queryParameters,
		}

		collection.getAll(w, r, injectRelation)
	}).Methods(http.MethodGet)

	// READ
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationRead, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		collection.getOne(w, r)
	}).Methods(http.MethodGet)

	// CREATE
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationCreate, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		queryParameters := make([]interface{}, len(resourceColumns))
		for i := 0; i < len(resourceColumns); i++ {
			queryParameters[i] = params[resourceColumns[i]]
		}
		res, err := b.db.Exec(insertQuery, queryParameters...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		count, err := res.RowsAffected()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if count > 0 {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	}).Methods(http.MethodPut)

	// DELETE
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationDelete, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		queryParameters := make([]interface{}, len(resourceColumns))
		for i := 0; i < len(resourceColumns); i++ {
			queryParameters[i] = params[resourceColumns[i]]
		}
		res, err := b.db.Exec(deleteQuery, queryParameters...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		count, err := res.RowsAffected()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if count > 0 {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}).Methods(http.MethodDelete)

}
