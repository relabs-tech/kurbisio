// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/lib/pq"
	"github.com/relabs-tech/kurbisio/core"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/logger"
)

func (b *Backend) createRelationResource(router *mux.Router, rc relationConfiguration) {
	schema := b.db.Schema
	leftResources := strings.Split(rc.Left, "/")
	left := leftResources[len(leftResources)-1]

	rightResources := strings.Split(rc.Right, "/")
	right := rightResources[len(rightResources)-1]

	// do the relation
	leftResources = append(leftResources, right)
	rightResources = append(rightResources, left)

	columns := []string{}
	validateColumns := []string{}
	createColumns := []string{"serial SERIAL"}

	resource := rc.Left + ":" + rc.Right
	pathPrefix := ""
	resourcePrefix := ""

	if rc.Resource != "" {
		resource = rc.Resource + ":" + resource
		pathPrefix = "/" + rc.Resource
		resourcePrefix = rc.Resource + "/"
	}

	rlog := logger.Default()
	rlog.Debugln("create relation:", resource)
	if rc.Description != "" {
		rlog.Debugln("  description:", rc.Description)
	}
	createQuery := fmt.Sprintf("CREATE table IF NOT EXISTS %s.\"%s\"", schema, resource)

	leftColumns := []string{}
	for _, r := range leftResources {
		id := r + "_id"
		leftColumns = append(leftColumns, id)
		columns = append(columns, id)
	}
	sort.Strings(columns)

	rightColumns := []string{}
	for _, r := range rightResources {
		id := r + "_id"
		rightColumns = append(rightColumns, id)
		validateColumns = append(validateColumns, id)
	}
	sort.Strings(validateColumns)

	// now columns and validateColumns should contain exactly the same identifiers
	if !reflect.DeepEqual(columns, validateColumns) {
		panic(fmt.Sprintf(`"%s" and "%s" do not share a compatible base, symmetric relation not possible`, left, right))
	}

	for _, c := range columns {
		createColumn := fmt.Sprintf("%s uuid NOT NULL", c)
		createColumns = append(createColumns, createColumn)
	}

	// left reference
	leftResource := rc.Left
	if relationResource, ok := b.relations[leftResource]; ok {
		// left resource is a relation, use relation table name
		leftResource = relationResource
	}
	{
		foreignColumns := strings.Join(leftColumns[:len(leftColumns)-1], ",")
		createColumn := "FOREIGN KEY (" + foreignColumns + ") " +
			"REFERENCES " + schema + ".\"" + leftResource + "\" " +
			"(" + foreignColumns + ") ON DELETE CASCADE"
		createColumns = append(createColumns, createColumn)
	}

	// right reference
	rightResource := rc.Right
	if relationResource, ok := b.relations[rightResource]; ok {
		// right resource is a relation, use relation table name
		rightResource = relationResource
	}
	{
		foreignColumns := strings.Join(rightColumns[:len(rightColumns)-1], ",")
		createColumn := "FOREIGN KEY (" + foreignColumns + ") " +
			"REFERENCES " + schema + ".\"" + rightResource + "\" " +
			"(" + foreignColumns + ") ON DELETE CASCADE"
		createColumns = append(createColumns, createColumn)
	}

	// relation is unique
	createColumn := "UNIQUE (" + strings.Join(columns, ",") + ")"
	createColumns = append(createColumns, createColumn)

	createQuery += "(" + strings.Join(createColumns, ", ") + ");"
	createQuery += fmt.Sprintf("ALTER TABLE %s.\"%s\" ADD COLUMN IF NOT EXISTS timestamp timestamp NOT NULL DEFAULT now();", schema, resource)

	if b.updateSchema {
		_, err := b.db.Exec(createQuery)
		if err != nil {
			panic(err)
		}
	}

	leftCollection, ok := b.collectionFunctions[rc.Left]
	if !ok {
		panic(fmt.Sprintf("missing left resource `%s`", rc.Left))
	}
	rightCollection, ok := b.collectionFunctions[rc.Right]
	if !ok {
		panic(fmt.Sprintf("missing right resource `%s`", rc.Right))
	}

	// register this relation, so that other relations can relate to it
	virtualLeftResource := resourcePrefix + rc.Left + "/" + right
	b.relations[virtualLeftResource] = resource
	virtualLeftCollection := collectionFunctions{
		permits: rc.LeftPermits,
		list:    rightCollection.list,
		read:    rightCollection.read,
	}

	b.collectionFunctions[virtualLeftResource] = &virtualLeftCollection

	virtualRightResource := resourcePrefix + rc.Right + "/" + left
	b.relations[virtualRightResource] = resource
	virtualRightCollection := collectionFunctions{
		permits: rc.RightPermits,
		list:    leftCollection.list,
		read:    leftCollection.read,
	}

	b.collectionFunctions[virtualRightResource] = &virtualRightCollection

	// The limit ensures reasonable fast database queries with the nested relational query. If we ever come
	// into a situation where relations are much larger than that, we would need to work out something
	// different: extend the relation table with all columns necessary to do pagination (timestamp,
	// searchable properties, external indices) and keep those in sync with the original table.
	sqlPagination := " ORDER BY serial LIMIT 1000"

	leftQuery := fmt.Sprintf("SELECT %s_id, timestamp FROM %s.\"%s\" WHERE ", right, schema, resource) +
		compareIDsString(leftColumns[:len(leftColumns)-1]) + sqlPagination + ";"
	rightQuery := fmt.Sprintf("SELECT %s_id, timestamp FROM %s.\"%s\" WHERE ", left, schema, resource) +
		compareIDsString(rightColumns[:len(rightColumns)-1]) + sqlPagination + ";"

	leftSQLInjectRelation := fmt.Sprintf(" AND %s_id IN (SELECT %s_id FROM %s.\"%s\" WHERE %%s %s) ", right, right, schema, resource, sqlPagination)
	rightSQLInjectRelation := fmt.Sprintf(" AND %s_id IN (SELECT %s_id FROM %s.\"%s\" WHERE %%s %s) ", left, left, schema, resource, sqlPagination)
	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" (%s) VALUES(%s);", schema, resource, strings.Join(columns, ","), parameterString(len(columns)))
	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" WHERE %s;", schema, resource, compareIDsString(columns))

	leftListRoute := pathPrefix
	leftItemRoute := pathPrefix
	for _, r := range leftResources {
		leftListRoute = leftItemRoute + "/" + core.Plural(r)
		leftItemRoute = leftItemRoute + "/" + core.Plural(r) + "/{" + r + "_id}"
	}

	rightListRoute := pathPrefix
	rightItemRoute := pathPrefix
	for _, r := range rightResources {
		rightListRoute = rightItemRoute + "/" + core.Plural(r)
		rightItemRoute = rightItemRoute + "/" + core.Plural(r) + "/{" + r + "_id}"
	}

	rlog.Debugln("  handle routes:", leftListRoute, "GET")
	rlog.Debugln("  handle routes:", leftItemRoute, "GET,PUT,DELETE")
	rlog.Debugln("  handle routes:", rightListRoute, "GET")
	rlog.Debugln("  handle routes:", rightItemRoute, "GET,PUT,DELETE")

	// LIST LEFT
	router.HandleFunc(leftListRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationList, params, rc.LeftPermits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		var idonly, withtimestamp bool
		var err error
		urlQuery := r.URL.Query()
		for key, array := range urlQuery {
			switch key {
			case "idonly":
				idonly, err = strconv.ParseBool(array[0])
				if err != nil {
					http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
					return
				}
			case "withtimestamp":
				withtimestamp, err = strconv.ParseBool(array[0])
				if err != nil {
					http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
					return
				}
			default:
			}
		}

		queryParameters := make([]interface{}, len(leftColumns)-1)
		for i := 0; i < len(leftColumns)-1; i++ { // skip ID
			queryParameters[i] = params[leftColumns[i]]
		}

		if idonly {
			response := []uuid.UUID{}
			responseWithTimestamp := []map[string]interface{}{}
			idName := fmt.Sprintf("%s_id", left)

			rows, err := b.db.Query(leftQuery, queryParameters...)
			if err != sql.ErrNoRows {
				if err != nil {
					rlog.WithError(err).Errorln("Error 4123: cannot query database")
					http.Error(w, "Error 4123: ", http.StatusInternalServerError)
					return
				}
				defer rows.Close()
				for rows.Next() {
					id := uuid.UUID{}
					timestamp := time.Time{}
					err := rows.Scan(&id, &timestamp)
					if err != nil {
						rlog.WithError(err).Errorln("Error 4124: Next")
						http.Error(w, "Error 4124: ", http.StatusInternalServerError)
						return
					}
					response = append(response, id)
					responseWithTimestamp = append(responseWithTimestamp, map[string]interface{}{
						"timestamp": timestamp,
						idName:      id,
					})
				}
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")

			if withtimestamp {
				jsonData, _ := json.Marshal(responseWithTimestamp)
				w.Write(jsonData)
				return
			}

			jsonData, _ := json.Marshal(response)
			w.Write(jsonData)
			return
		}

		injectRelation := &relationInjection{
			subquery:        leftSQLInjectRelation,
			columns:         leftColumns[:len(leftColumns)-1], // skip ID
			queryParameters: queryParameters,
		}

		rightCollection.list(w, r, injectRelation)
	}).Methods(http.MethodOptions, http.MethodGet)

	// LIST RIGHT
	router.HandleFunc(rightListRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationList, params, rc.RightPermits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		var idonly, withtimestamp bool
		var err error
		urlQuery := r.URL.Query()
		for key, array := range urlQuery {
			switch key {
			case "idonly":
				idonly, err = strconv.ParseBool(array[0])
				if err != nil {
					http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
					return
				}
			case "withtimestamp":
				withtimestamp, err = strconv.ParseBool(array[0])
				if err != nil {
					http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
					return
				}
			default:
			}
		}

		queryParameters := make([]interface{}, len(rightColumns)-1)
		for i := 0; i < len(rightColumns)-1; i++ { // skip ID
			queryParameters[i] = params[rightColumns[i]]
		}

		if idonly {
			response := []uuid.UUID{}
			responseWithTimestamp := []map[string]interface{}{}
			idName := fmt.Sprintf("%s_id", left)

			rows, err := b.db.Query(rightQuery, queryParameters...)
			if err != sql.ErrNoRows {
				if err != nil {
					rlog.WithError(err).Errorln("Error 4125: Query")
					http.Error(w, "Error 4125: ", http.StatusInternalServerError)
					return
				}
				defer rows.Close()
				for rows.Next() {
					id := uuid.UUID{}
					timestamp := time.Time{}
					err := rows.Scan(&id, &timestamp)
					if err != nil {
						rlog.WithError(err).Errorln("Error 4126: Scan")
						http.Error(w, "Error 4126: ", http.StatusInternalServerError)
						return
					}
					response = append(response, id)
					responseWithTimestamp = append(responseWithTimestamp, map[string]interface{}{
						"timestamp": timestamp,
						idName:      id,
					})
				}
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")

			if withtimestamp {
				jsonData, _ := json.Marshal(responseWithTimestamp)
				w.Write(jsonData)
				return
			}

			jsonData, _ := json.Marshal(response)
			w.Write(jsonData)
			return
		}

		injectRelation := &relationInjection{
			subquery:        rightSQLInjectRelation,
			columns:         rightColumns[:len(rightColumns)-1], // skip ID
			queryParameters: queryParameters,
		}

		leftCollection.list(w, r, injectRelation)
	}).Methods(http.MethodOptions, http.MethodGet)

	// READ LEFT
	router.HandleFunc(leftItemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationRead, params, rc.LeftPermits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		queryParameters := make([]interface{}, len(leftColumns))
		for i := 0; i < len(leftColumns); i++ {
			queryParameters[i] = params[leftColumns[i]]
		}
		injectRelation := &relationInjection{
			subquery:        leftSQLInjectRelation,
			columns:         leftColumns,
			queryParameters: queryParameters,
		}

		rightCollection.read(w, r, injectRelation)
	}).Methods(http.MethodOptions, http.MethodGet)

	// READ RIGHT
	router.HandleFunc(rightItemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationRead, params, rc.RightPermits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		queryParameters := make([]interface{}, len(rightColumns))
		for i := 0; i < len(rightColumns); i++ {
			queryParameters[i] = params[rightColumns[i]]
		}
		injectRelation := &relationInjection{
			subquery:        rightSQLInjectRelation,
			columns:         rightColumns,
			queryParameters: queryParameters,
		}

		leftCollection.read(w, r, injectRelation)
	}).Methods(http.MethodOptions, http.MethodGet)

	create := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		queryParameters := make([]interface{}, len(columns))
		for i := 0; i < len(columns); i++ {
			queryParameters[i] = params[columns[i]]
		}
		res, err := b.db.Exec(insertQuery, queryParameters...)
		if err != nil {
			var code pq.ErrorCode
			if err, ok := err.(*pq.Error); ok {
				code = err.Code
			}
			switch code {
			case "23505":
				// put is omnipotent, so no error if the relation already exists
				w.WriteHeader(http.StatusNoContent)
			case "23503":
				http.Error(w, "resource does not exist", http.StatusBadRequest)
			default:
				rlog.WithError(err).Errorln("Error 4127: Exec")
				http.Error(w, "Error 4127: ", http.StatusInternalServerError)
			}
			return
		}
		count, err := res.RowsAffected()

		if err != nil {
			rlog.WithError(err).Errorln("Error 4128: RowsAffected")
			http.Error(w, "Error 4128: ", http.StatusInternalServerError)
			return
		}

		if count > 0 {
			w.WriteHeader(http.StatusCreated)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	}

	// CREATE LEFT
	router.HandleFunc(leftItemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationCreate, params, rc.LeftPermits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
			if !auth.IsAuthorized(core.OperationRead, params, rightCollection.permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		create(w, r)
	}).Methods(http.MethodOptions, http.MethodPut)

	// CREATE RIGHT
	router.HandleFunc(rightItemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationCreate, params, rc.RightPermits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
			if !auth.IsAuthorized(core.OperationRead, params, leftCollection.permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		create(w, r)
	}).Methods(http.MethodOptions, http.MethodPut)

	delete := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		queryParameters := make([]interface{}, len(columns))
		for i := 0; i < len(columns); i++ {
			queryParameters[i] = params[columns[i]]
		}
		res, err := b.db.Exec(deleteQuery, queryParameters...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		count, err := res.RowsAffected()
		if err != nil {
			rlog.WithError(err).Errorln("Error 4129: RowsAffected")
			http.Error(w, "Error 4129: ", http.StatusInternalServerError)
			return
		}

		if count > 0 {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}

	// DELETE LEFT
	router.HandleFunc(leftItemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationDelete, params, rc.LeftPermits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		delete(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

	// DELETE RIGHT
	router.HandleFunc(rightItemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationDelete, params, rc.RightPermits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		delete(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

}
