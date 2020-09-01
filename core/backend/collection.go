package backend

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"

	"net/http"
	"net/http/httptest"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core"
	"github.com/relabs-tech/backends/core/access"
	"github.com/relabs-tech/backends/core/csql"
	"github.com/relabs-tech/backends/core/logger"
)

func (b *Backend) createCollectionResource(router *mux.Router, rc collectionConfiguration, singleton bool) {
	schema := b.db.Schema
	resource := rc.Resource

	nillog := logger.FromContext(nil)
	if singleton {
		nillog.Infoln("create singleton collection:", resource)
	} else {
		nillog.Infoln("create collection:", resource)
	}
	if rc.Description != "" {
		nillog.Infoln("  description:", rc.Description)
	}

	if rc.SchemaID != "" {
		if !b.jsonValidator.HasSchema(rc.SchemaID) {
			nillog.Errorf("ERROR: invalid configuration for resource %s, schemaID %s is unknown. Validation is deactivated for this resource",
				rc.Resource, rc.SchemaID)
		}
	}

	resources := strings.Split(rc.Resource, "/")
	this := resources[len(resources)-1]
	primary := this
	owner := ""
	if singleton {
		if len(resource) < 2 {
			panic(fmt.Errorf("singleton resource %s lacks owner", this))
		}
		owner = resources[len(resources)-2]
		primary = owner
	}
	dependencies := resources[:len(resources)-1]

	createQuery := fmt.Sprintf("CREATE table IF NOT EXISTS %s.\"%s\"", schema, resource)
	var createColumns []string
	var columns []string
	if !singleton {
		columns = append(columns, this+"_id")
		createColumns = append(createColumns, this+"_id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY")
	}

	createColumns = append(createColumns, "created_at timestamp NOT NULL DEFAULT now()")
	createColumns = append(createColumns, "revision INTEGER NOT NULL DEFAULT 1")

	var foreignColumns []string
	for i := len(dependencies) - 1; i >= 0; i-- {
		that := dependencies[i]
		createColumn := fmt.Sprintf("%s_id uuid NOT NULL", that)
		createColumns = append(createColumns, createColumn)
		columns = append(columns, that+"_id")
		foreignColumns = append(foreignColumns, that+"_id")
	}

	if len(dependencies) > 0 {
		foreign := strings.Join(foreignColumns, ",")
		createColumn := "FOREIGN KEY (" + foreign + ") " +
			"REFERENCES " + schema + ".\"" + strings.Join(dependencies, "/") + "\" " +
			"(" + foreign + ") ON DELETE CASCADE"
		createColumns = append(createColumns, createColumn)
	}

	// enforce a unique constraint on all our identifying indices. This enables child
	// resources to have a composite foreign key on us
	if len(columns) > 1 {
		createColumn := "UNIQUE (" + strings.Join(columns, ",") + ")"
		createColumns = append(createColumns, createColumn)
	}

	createColumns = append(createColumns, "properties json NOT NULL DEFAULT '{}'::jsonb")
	// query to create all indices after the table creation
	createIndicesQuery := fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s\"(created_at);",
		"sort_index_"+primary+"_created_at",
		schema, resource)
	propertiesIndex := len(columns) // where properties start
	columns = append(columns, "properties")

	staticPropertiesIndex := len(columns) // where static properties start
	// static properties are varchars
	for _, property := range rc.StaticProperties {
		createColumn := fmt.Sprintf("\"%s\" varchar NOT NULL DEFAULT ''", property)
		createColumns = append(createColumns, createColumn)
		columns = append(columns, property)
	}

	searchablePropertiesIndex := len(columns) // where searchable properties start
	// static searchable properties are varchars with a non-unique index
	for _, property := range rc.SearchableProperties {
		createColumn := fmt.Sprintf("\"%s\" varchar NOT NULL DEFAULT ''", property)
		createIndicesQuery = createIndicesQuery + fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s\"(%s);",
			"searchable_property_"+this+"_"+property,
			schema, resource, property)
		createColumns = append(createColumns, createColumn)
		columns = append(columns, property)

	}

	propertiesEndIndex := len(columns) // where properties end

	// an external index is a manadory and unique varchar property.
	if len(rc.ExternalIndex) > 0 {
		name := rc.ExternalIndex
		createColumn := fmt.Sprintf("\"%s\" varchar NOT NULL", name)
		createIndicesQuery = createIndicesQuery + fmt.Sprintf("CREATE UNIQUE index IF NOT EXISTS %s ON %s.\"%s\"(%s);",
			"external_index_"+this+"_"+name,
			schema, resource, name)
		createColumns = append(createColumns, createColumn)
		columns = append(columns, name)
	}

	// the "device" collection gets an additional UUID column for the web token
	if this == "device" {
		createColumn := "token uuid NOT NULL DEFAULT uuid_generate_v4()"
		createColumns = append(createColumns, createColumn)
	}

	createQuery += "(" + strings.Join(createColumns, ", ") + ");" + createIndicesQuery

	var err error
	if b.updateSchema {
		_, err = b.db.Query(createQuery)
		if err != nil {
			nillog.Errorf("Error while updating schema when running: %s", createQuery)
			panic(err)
		}
	}

	singletonRoute := ""
	listRoute := ""
	itemRoute := ""
	for _, r := range resources {
		singletonRoute = itemRoute + "/" + r
		listRoute = itemRoute + "/" + core.Plural(r)
		itemRoute = itemRoute + "/" + core.Plural(r) + "/{" + r + "_id}"
	}

	if singleton {
		nillog.Infoln("  handle singleton routes:", singletonRoute, "GET,PUT,PATCH,DELETE")
	}
	nillog.Infoln("  handle collection routes:", listRoute, "GET,POST,PUT,PATCH,DELETE")
	nillog.Infoln("  handle collection routes:", itemRoute, "GET,PUT,PATCH,DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", created_at, revision FROM %s.\"%s\" ", schema, resource)
	sqlWhereOne := "WHERE " + compareIDsString(columns[:propertiesIndex])

	readQueryWithTotal := "SELECT " + strings.Join(columns, ", ") +
		fmt.Sprintf(", created_at, revision, count(*) OVER() AS full_count FROM %s.\"%s\" ", schema, resource)
	sqlWhereAll := "WHERE "
	if propertiesIndex > 1 {
		sqlWhereAll += compareIDsString(columns[1:propertiesIndex]) + " AND "
	}
	sqlWhereAll += fmt.Sprintf("($%d OR created_at<=$%d) AND ($%d OR created_at>=$%d) ",
		propertiesIndex, propertiesIndex+1, propertiesIndex+2, propertiesIndex+3)
	sqlPaginationDesc := fmt.Sprintf("ORDER BY created_at DESC,%s DESC LIMIT $%d OFFSET $%d;",
		columns[0], propertiesIndex+4, propertiesIndex+5)

	sqlPaginationAsc := fmt.Sprintf("ORDER BY created_at ASC,%s ASC LIMIT $%d OFFSET $%d;",
		columns[0], propertiesIndex+4, propertiesIndex+5)

	clearQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)
	sqlReturnPrimaryID := " RETURNING " + primary + "_id;"

	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource) + "(" + strings.Join(columns, ", ") + ", created_at)"
	insertQuery += "VALUES(" + parameterString(len(columns)+1) + ")"
	insertQuery += " RETURNING " + primary + "_id;"

	updateQuery := fmt.Sprintf("UPDATE %s.\"%s\" SET ", schema, resource)
	sets := make([]string, len(columns)-propertiesIndex)
	for i := propertiesIndex; i < len(columns); i++ {
		sets[i-propertiesIndex] = columns[i] + " = $" + strconv.Itoa(i+1)
	}
	updateQuery += strings.Join(sets, ", ") + ", created_at = $" + strconv.Itoa(len(columns)+1)
	updateQuery += ", revision = revision + 1 " + sqlWhereOne + " RETURNING " + primary + "_id;"

	updatePropertyQuery := fmt.Sprintf("UPDATE %s.\"%s\" SET ", schema, resource)
	updatePropertyQuery += " %s = $" + strconv.Itoa(propertiesIndex+1)
	updatePropertyQuery += ", revision = revision + 1 " + sqlWhereOne + " RETURNING " + primary + "_id;"

	createScanValuesAndObject := func(createdAt *time.Time, revision *int, extra ...interface{}) ([]interface{}, map[string]interface{}) {
		values := make([]interface{}, len(columns)+2, len(columns)+2+len(extra))
		object := map[string]interface{}{}
		var i int
		for ; i < propertiesIndex; i++ {
			values[i] = &uuid.UUID{}
			object[columns[i]] = values[i]
		}
		values[i] = &json.RawMessage{}
		object[columns[i]] = values[i]
		i++

		for ; i < len(columns); i++ {
			str := ""
			values[i] = &str
			object[columns[i]] = values[i]

		}

		values[i] = createdAt
		object["created_at"] = createdAt
		i++
		values[i] = revision
		object["revision"] = revision
		values = append(values, extra...)
		return values, object
	}

	mergeProperties := func(object map[string]interface{}) {
		rawJSON := object["properties"].(*json.RawMessage)
		delete(object, "properties")
		var properties map[string]interface{}
		err := json.Unmarshal([]byte(*rawJSON), &properties)
		if err != nil {
			return
		}
		for key, value := range properties {
			if _, ok := object[key]; !ok { // dynamic properties must not overwrite static properties
				object[key] = value
			}
		}
	}

	list := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
		var (
			queryParameters []interface{}
			sqlQuery        string
			limit           int = 100
			page            int = 1
			until           time.Time
			from            time.Time
			externalColumn  string
			externalValue   string
			ascendingOrder  bool
		)
		urlQuery := r.URL.Query()
		parameters := map[string]string{}
		for key, array := range urlQuery {
			var err error
			if len(array) > 1 {
				http.Error(w, "illegal parameter array '"+key+"'", http.StatusBadRequest)
				return
			}
			value := array[0]
			switch key {
			case "limit":
				limit, err = strconv.Atoi(value)
				if err == nil && (limit < 1 || limit > 100) {
					err = fmt.Errorf("out of range")
				}
			case "page":
				page, err = strconv.Atoi(value)
				if err == nil && page < 1 {
					err = fmt.Errorf("out of range")
				}
			case "until":
				until, err = time.Parse(time.RFC3339, value)

			case "from":
				from, err = time.Parse(time.RFC3339, value)

			case "filter":
				i := strings.IndexRune(value, '=')
				if i < 0 {
					err = fmt.Errorf("cannot parse filter, must be of type property=value")
					break
				}
				filterKey := value[:i]
				filterValue := value[i+1:]

				found := false
				for i := searchablePropertiesIndex; i < len(columns) && !found; i++ {
					if filterKey == columns[i] {
						externalValue = filterValue
						externalColumn = columns[i]
						found = true
					}
				}
				if !found {
					err = fmt.Errorf("unknown filter property '%s'", filterKey)
				}
			case "order":
				if value != "asc" && value != "desc" {
					err = fmt.Errorf("order must be asc or desc")
					break
				}
				ascendingOrder = (value == "asc")

			default:
				err = fmt.Errorf("unknown")
			}

			parameters[key] = value
			if err != nil {
				http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
				return
			}
		}
		params := mux.Vars(r)
		if externalValue == "" { // get entire collection
			sqlQuery = readQueryWithTotal + sqlWhereAll
			queryParameters = make([]interface{}, propertiesIndex-1+6)
			for i := 1; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-1] = params[columns[i]]
			}
		} else {
			sqlQuery = readQueryWithTotal + sqlWhereAll + fmt.Sprintf("AND (%s=$%d) ", externalColumn, propertiesIndex+6)
			queryParameters = make([]interface{}, propertiesIndex-1+6+1)
			for i := 1; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-1] = params[columns[i]]
			}
			queryParameters[propertiesIndex-1+6] = externalValue
		}

		// add before and after and pagination
		queryParameters[propertiesIndex-1+0] = until.IsZero()
		queryParameters[propertiesIndex-1+1] = until.UTC()
		queryParameters[propertiesIndex-1+2] = from.IsZero()
		queryParameters[propertiesIndex-1+3] = from.UTC()
		queryParameters[propertiesIndex-1+4] = limit
		queryParameters[propertiesIndex-1+5] = (page - 1) * limit

		if relation != nil {
			// inject subquery for relation
			sqlQuery += fmt.Sprintf(relation.subquery,
				compareIDsStringWithOffset(len(queryParameters), relation.columns))
			queryParameters = append(queryParameters, relation.queryParameters...)
		}

		if ascendingOrder {
			sqlQuery += sqlPaginationAsc

		} else {
			sqlQuery += sqlPaginationDesc
		}

		rows, err := b.db.Query(sqlQuery, queryParameters...)
		if err != nil {
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		response := []interface{}{}
		defer rows.Close()
		var totalCount int
		for rows.Next() {
			var createdAt time.Time
			values, object := createScanValuesAndObject(&createdAt, new(int), &totalCount)
			err := rows.Scan(values...)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			mergeProperties(object)
			// if we did not have from, take it from the first object
			if from.IsZero() {
				from = createdAt
			}
			response = append(response, object)
		}

		// do request interceptors
		jsonData, _ := json.Marshal(response)
		data, err := b.intercept(r.Context(), resource, core.OperationList, uuid.UUID{}, parameters, jsonData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if data != nil {
			jsonData = data
		}

		etag := bytesPlusTotalCountToEtag(jsonData, totalCount)
		w.Header().Set("Etag", etag)
		if ifNoneMatchFound(r.Header.Get("If-None-Match"), etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Pagination-Limit", strconv.Itoa(limit))
		w.Header().Set("Pagination-Total-Count", strconv.Itoa(totalCount))
		w.Header().Set("Pagination-Page-Count", strconv.Itoa(((totalCount-1)/limit)+1))
		w.Header().Set("Pagination-Current-Page", strconv.Itoa(page))
		if !from.IsZero() {
			w.Header().Set("Pagination-Until", from.Format(time.RFC3339))
		}
		w.Write(jsonData)

	}

	listWithAuth := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationList, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		list(w, r, nil)
	}

	read := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
		params := mux.Vars(r)

		resourceID := params[this+"_id"]
		if resourceID == "all" {
			http.Error(w, "all is not a valid "+this, http.StatusBadRequest)
			return
		}
		if singleton {
			if params[owner+"_id"] == "all" {
				if resourceID == "" {
					http.Error(w, "all is not a valid "+owner+"_id for requesting a single "+this+". Did you meant to say "+core.Plural(this)+"?", http.StatusBadRequest)
					return
				}
				params[owner+"_id"] = resourceID
			} else if resourceID != "" && resourceID != params[owner+"_id"] {
				http.Error(w, "identifier mismatch for "+this, http.StatusBadRequest)
				return
			}
		}

		queryParameters := make([]interface{}, propertiesIndex)
		for i := 0; i < propertiesIndex; i++ {
			queryParameters[i] = params[columns[i]]
		}

		subQuery := ""
		if relation != nil {
			// inject subquery for relation
			subQuery = fmt.Sprintf(relation.subquery,
				compareIDsStringWithOffset(len(queryParameters), relation.columns))
			queryParameters = append(queryParameters, relation.queryParameters...)
		}

		values, response := createScanValuesAndObject(&time.Time{}, new(int))
		err = b.db.QueryRow(readQuery+sqlWhereOne+subQuery+";", queryParameters...).Scan(values...)
		if err == csql.ErrNoRows {
			if singleton {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			status := http.StatusInternalServerError

			// Invalid UUIDs are reported as "invalid_text_representation" which is Code 22P02
			if err, ok := err.(*pq.Error); ok && err.Code == "22P02" {
				status = http.StatusBadRequest
			}
			http.Error(w, err.Error(), status)
			return
		}
		mergeProperties(response)

		// do request interceptors
		jsonData, _ := json.Marshal(response)
		data, err := b.intercept(r.Context(), resource, core.OperationRead, *values[0].(*uuid.UUID), nil, jsonData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if data != nil {
			jsonData = data
		}

		// add children if requested
		urlQuery := r.URL.Query()
		for key, array := range urlQuery {
			switch key {
			case "children":
				if data != nil { // data was changed in interceptor
					err = json.Unmarshal(jsonData, &response)
					if err != nil {
						http.Error(w, "interceptor: "+err.Error(), http.StatusInternalServerError)
						return
					}
				}

				status, err := b.addChildrenToGetResponse(array, r, response)
				if err != nil {
					http.Error(w, err.Error(), status)
					return
				}
				jsonData, _ = json.Marshal(response)
			default:
				http.Error(w, "parameter '"+key+"': unknown query parameter", http.StatusBadRequest)
			}
		}

		etag := bytesToEtag(jsonData)
		w.Header().Set("Etag", etag)
		if ifNoneMatchFound(r.Header.Get("If-None-Match"), etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	}

	readWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationRead, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		read(w, r, nil)
	}

	updatePropertyWithAuth := func(w http.ResponseWriter, r *http.Request, property string) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationUpdate, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		resourceID := params[this+"_id"]
		if resourceID == "all" {
			http.Error(w, "all is not a valid "+this, http.StatusBadRequest)
			return
		}
		if singleton {
			if params[owner+"_id"] == "all" {
				if resourceID == "" {
					http.Error(w, "all is not a valid "+owner+"_id for updating properties of a single "+this+". Did you meant to say "+core.Plural(this)+"?", http.StatusBadRequest)
					return
				}
				params[owner+"_id"] = resourceID
			} else if resourceID != "" && resourceID != params[owner+"_id"] {
				http.Error(w, "identifier mismatch for "+this, http.StatusBadRequest)
				return
			}
		}

		found := false
		for i := staticPropertiesIndex; i < len(columns) && !found; i++ {
			if property == columns[i] {
				found = true
			}
		}
		if !found {
			http.Error(w, "unknown static property", http.StatusBadRequest)
			return
		}

		value := params[property]
		query := fmt.Sprintf(updatePropertyQuery, property)

		queryParameters := make([]interface{}, propertiesIndex+1)
		i := 0
		for ; i < propertiesIndex; i++ {
			queryParameters[i] = params[columns[i]]
		}
		queryParameters[i] = value

		var primaryID uuid.UUID
		err = b.db.QueryRow(query, queryParameters...).Scan(&primaryID)
		if err == csql.ErrNoRows {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}

	deleteWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationDelete, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		resourceID := params[this+"_id"]
		if resourceID == "all" {
			http.Error(w, "all is not a valid "+this, http.StatusBadRequest)
			return
		}
		if singleton {
			if params[owner+"_id"] == "all" {
				if resourceID == "" {
					http.Error(w, "all is not a valid "+owner+"_id for deleting a single "+this+". Did you meant to say "+core.Plural(this)+"?", http.StatusBadRequest)
					return
				}
				params[owner+"_id"] = resourceID
			} else if resourceID != "" && resourceID != params[owner+"_id"] {
				http.Error(w, "identifier mismatch for "+this, http.StatusBadRequest)
				return
			}
		}

		primaryID, err := uuid.Parse(params[columns[0]])
		if err != nil {
			http.Error(w, "broken primary identifier", http.StatusBadRequest)
			return
		}

		_, err = b.intercept(r.Context(), resource, core.OperationDelete, primaryID, nil, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		queryParameters := make([]interface{}, propertiesIndex)
		for i := 0; i < propertiesIndex; i++ {
			queryParameters[i] = params[columns[i]]
		}

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		err = b.db.QueryRow(deleteQuery+sqlWhereOne+sqlReturnPrimaryID, queryParameters...).Scan(&primaryID)
		if err == csql.ErrNoRows {
			tx.Rollback()
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		notification := make(map[string]interface{})
		for i := 0; i < propertiesIndex; i++ {
			notification[columns[i]] = params[columns[i]]
		}
		jsonData, _ := json.Marshal(notification)
		err = b.commitWithNotification(r.Context(), tx, resource, core.OperationDelete, primaryID, jsonData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)

	}

	clearWithAuth := func(w http.ResponseWriter, r *http.Request) {

		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationClear, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		var (
			queryParameters []interface{}
			sqlQuery        string
			until           time.Time
			from            time.Time
			externalColumn  string
			externalValue   string
		)
		parameters := map[string]string{}
		urlQuery := r.URL.Query()
		for key, array := range urlQuery {
			var err error
			if len(array) > 1 {
				http.Error(w, "illegal parameter array '"+key+"'", http.StatusBadRequest)
				return
			}
			value := array[0]
			switch key {
			case "until":
				until, err = time.Parse(time.RFC3339, value)
			case "from":
				from, err = time.Parse(time.RFC3339, value)
			case "filter":
				i := strings.IndexRune(value, '=')
				if i < 0 {
					err = fmt.Errorf("cannot parse filter, must be of type property=value")
					break
				}
				filterKey := value[:i]
				filterValue := value[i+1:]

				found := false
				for i := searchablePropertiesIndex; i < len(columns) && !found; i++ {
					if filterKey == columns[i] {
						externalValue = filterValue
						externalColumn = columns[i]
						found = true
					}
				}
				if !found {
					err = fmt.Errorf("unknown filter property '%s'", filterKey)
				}

			default:
				err = fmt.Errorf("unknown")
			}

			if err != nil {
				http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
				return
			}
			parameters[key] = value
		}

		_, err = b.intercept(r.Context(), resource, core.OperationClear, uuid.UUID{}, parameters, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if externalValue == "" { // delete entire collection
			sqlQuery = clearQuery + sqlWhereAll + ";"
			queryParameters = make([]interface{}, propertiesIndex-1+4)
			for i := 1; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-1] = params[columns[i]]
			}
		} else {
			sqlQuery = clearQuery + sqlWhereAll + fmt.Sprintf("AND (%s=$%d);", externalColumn, propertiesIndex+4)
			queryParameters = make([]interface{}, propertiesIndex-1+4+1)
			for i := 1; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-1] = params[columns[i]]
			}
			queryParameters[propertiesIndex-1+4] = externalValue
		}

		// add before and after and pagination
		queryParameters[propertiesIndex-1+0] = until.IsZero()
		queryParameters[propertiesIndex-1+1] = until.UTC()
		queryParameters[propertiesIndex-1+2] = from.IsZero()
		queryParameters[propertiesIndex-1+3] = from.UTC()

		_, err = tx.Exec(sqlQuery, queryParameters...)
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		notificationJSON, _ := json.Marshal(parameters)
		err = b.commitWithNotification(r.Context(), tx, resource, core.OperationClear, uuid.UUID{}, notificationJSON)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}

	create := func(w http.ResponseWriter, r *http.Request, bodyJSON map[string]interface{}) {

		rlog := logger.FromContext(r.Context())

		params := mux.Vars(r)
		if bodyJSON == nil {
			err := json.NewDecoder(r.Body).Decode(&bodyJSON)
			if err != nil {
				http.Error(w, "invalid json data: "+err.Error(), http.StatusBadRequest)
				return
			}
		}

		// build insert query and validate that we have all parameters
		values := make([]interface{}, len(columns)+1)
		var i int

		if !singleton {
			// the primary resource identifier, use as specified or create a new one. Singletons
			// do not have this, they are fully specified by their owner ID
			primaryID, ok := bodyJSON[columns[0]]
			if !ok || primaryID == "00000000-0000-0000-0000-000000000000" {
				primaryID = uuid.New()
				// update the bodyJSON so we can validate
				bodyJSON[columns[0]] = primaryID
			}
			values[0] = primaryID
			i++
		}

		for ; i < propertiesIndex; i++ { // the core identifiers
			k := columns[i]
			value, ok := bodyJSON[k]

			// zero uuid counts as no uuid for creation
			ok = ok && value != "00000000-0000-0000-0000-000000000000"

			param, _ := params[k]
			// identifiers in the url parameters must match the ones in the json document
			if ok && param != "all" && param != value.(string) {
				http.Error(w, "illegal "+k, http.StatusBadRequest)
				return
			}
			// if we have no identifier in the url parameters, but in the json document, use
			// the ones from the json document
			if param == "all" {
				if ok && value != "00000000-0000-0000-0000-000000000000" {
					values[i] = value
				} else {
					http.Error(w, "missing "+columns[i], http.StatusBadRequest)
					return
				}
			} else {
				// we use the url parameters, update the bodyJSON so we can validate
				bodyJSON[k] = param
				values[i] = param
			}
		}

		jsonData, _ := json.Marshal(bodyJSON)

		if rc.SchemaID != "" {
			if !b.jsonValidator.HasSchema(rc.SchemaID) {
				rlog.Errorf("ERROR: invalid configuration for resource %s, schemaID %s is unknown. Validation is deactivated for this resource", rc.Resource, rc.SchemaID)
			} else if err := b.jsonValidator.ValidateString(string(jsonData), rc.SchemaID); err != nil {
				rlog.Errorf("properties '%v' field does not follow schemaID %s, %v",
					string(jsonData), rc.SchemaID, err)
				http.Error(w, fmt.Sprintf("document '%v' field does not follow schemaID %s, %v",
					string(jsonData), rc.SchemaID, err), http.StatusBadRequest)
				return
			}
		}

		// primaryID can be string or uuid.UUID
		primaryUUID, ok := bodyJSON[columns[0]].(uuid.UUID)
		if !ok {
			primaryString, ok := bodyJSON[columns[0]].(string)
			if ok {
				primaryUUID, err = uuid.Parse(primaryString)
				if err != nil {
					http.Error(w, "broken primary identifier", http.StatusBadRequest)
					return
				}
			}
		}
		data, err := b.intercept(r.Context(), resource, core.OperationCreate, primaryUUID, nil, jsonData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if data != nil {
			json.Unmarshal(data, &bodyJSON)
			if err != nil {
				http.Error(w, "interceptor: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}

		// extract the dynamic properties
		extract := map[string]interface{}{}
	property_loop:
		for key, value := range bodyJSON {
			for i := 0; i < propertiesIndex; i++ {
				if key == columns[i] {
					continue property_loop
				}
			}
			for i := propertiesIndex + 1; i < propertiesEndIndex; i++ {
				if key == columns[i] {
					continue property_loop
				}
			}
			if key == "created_at" || key == "revision" {
				continue
			}
			extract[key] = value
		}

		propertiesJSON, _ := json.Marshal(extract)
		values[i] = propertiesJSON
		i++

		// static properties, non mandatory
		for ; i < propertiesEndIndex; i++ {
			value, ok := bodyJSON[columns[i]]
			if !ok {
				value = ""
			}
			values[i] = value
		}

		// external (unique) indices, mandatory
		for ; i < len(columns); i++ {
			value, ok := bodyJSON[columns[i]]
			if !ok {
				http.Error(w, "missing external index "+columns[i], http.StatusBadRequest)
				return
			}
			values[i] = value
		}

		// next value is created_at
		createdAt := time.Now().UTC()
		if value, ok := bodyJSON["created_at"]; ok {
			timestamp, _ := value.(string)
			t, err := time.Parse(time.RFC3339, timestamp)
			if err != nil {
				http.Error(w, "illegal created_at: "+err.Error(), http.StatusBadRequest)
				return
			}
			if !t.IsZero() {
				createdAt = t.UTC()
			}
		}
		values[i] = &createdAt
		i++

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var id uuid.UUID
		err = tx.QueryRow(insertQuery, values...).Scan(&id)
		if err == csql.ErrNoRows {
			tx.Rollback()
			http.Error(w, "singleton "+this+" already exists", http.StatusUnprocessableEntity)
			return
		} else if err != nil {
			status := http.StatusInternalServerError
			// Non unique external keys are reported as code Code 23505
			if err, ok := err.(*pq.Error); ok && err.Code == "23505" {
				status = http.StatusUnprocessableEntity
			}
			tx.Rollback()
			http.Error(w, err.Error(), status)
			return
		}

		// re-read data and return as json
		values, response := createScanValuesAndObject(&time.Time{}, new(int))
		err = tx.QueryRow(readQuery+"WHERE "+primary+"_id = $1;", id).Scan(values...)
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		mergeProperties(response)

		jsonData, _ = json.Marshal(response)
		err = b.commitWithNotification(r.Context(), tx, resource, core.OperationCreate, id, jsonData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write(jsonData)

	}

	createWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationCreate, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		create(w, r, nil)
	}

	updateWithAuth := func(w http.ResponseWriter, r *http.Request) {

		rlog := logger.FromContext(r.Context())

		params := mux.Vars(r)
		var bodyJSON map[string]interface{}
		err = json.NewDecoder(r.Body).Decode(&bodyJSON)
		if err != nil {
			http.Error(w, "invalid json data: "+err.Error(), http.StatusBadRequest)
			return
		}

		// primary id can come from parameter (fully qualified put) or from body json (collection put).
		primaryID := params[columns[0]]
		if len(primaryID) == 0 || primaryID == "all" {
			var ok bool
			primaryID, ok = bodyJSON[columns[0]].(string)
			if !ok {
				http.Error(w, "missing "+columns[0], http.StatusBadRequest)
				return
			}
			params[columns[0]] = primaryID
		}
		// now we have all parameters and can authorize
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationUpdate, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		if singleton {
			if params[this+"_id"] != "" && params[this+"_id"] != primaryID {
				http.Error(w, "identifier mismatch for "+this, http.StatusBadRequest)
				return
			}
		}

		revision := 0
		if r, ok := bodyJSON["revision"].(float64); ok {
			revision = int(r)
		}

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var currentRevision int
		retried := false
	Retry:
		current, object := createScanValuesAndObject(&time.Time{}, &currentRevision)
		err = tx.QueryRow(readQuery+"WHERE "+primary+"_id = $1 FOR UPDATE;", &primaryID).Scan(current...)
		if err == csql.ErrNoRows {
			// item does not exist yet. If we have the right permissions, we can create it. Otherwise
			// we are forced to return 404 Not Found
			if b.authorizationEnabled {
				auth := access.AuthorizationFromContext(r.Context())
				if !auth.IsAuthorized(resources, core.OperationCreate, params, rc.Permits) {
					tx.Rollback()
					http.Error(w, "no such "+this, http.StatusNotFound)
					return
				}
			}

			rec := httptest.NewRecorder()
			create(rec, r, bodyJSON)
			if rec.Code == http.StatusCreated {
				// all is good, we are done, we can rollback this transaction
				tx.Rollback()
				w.WriteHeader(http.StatusCreated)
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.Write(rec.Body.Bytes())
				return
			} else if rec.Code == http.StatusUnprocessableEntity && !retried {
				// race condition: somebody else has create the object right now
				retried = true
				goto Retry
			}
			err = tx.Rollback()
			http.Error(w, rec.Body.String(), rec.Code)
			return
		}
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if revision != 0 && revision != currentRevision {
			tx.Rollback()
			// revision does not match, return conflict status with the conflicting object
			w.WriteHeader(http.StatusConflict)
			jsonData, _ := json.Marshal(object)
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.Write(jsonData)
			return
		}
		mergeProperties(object)

		primaryUUID := *current[0].(*uuid.UUID)
		primaryID = primaryUUID.String()

		// for MethodPatch we get the existing object from the database and patch property by property
		if r.Method == http.MethodPatch {

			// convert object into generic json for patching (the datatypes are different compared to the database) in the database)
			body, _ := json.Marshal(object)
			var objectJSON map[string]interface{}
			json.Unmarshal(body, &objectJSON)

			// now bodyJSON from the request becomes a patch
			patchObject(objectJSON, bodyJSON)

			// rewrite this put request to contain the entire (patched) object
			bodyJSON = objectJSON
		}

		// build insert query and validate that we have all parameters
		values := make([]interface{}, len(columns)+1)

		var i int

		// validate core identifiers
		// Rationale: we have authorized the resource based on the parameters
		// in the URL, so we have to ensure that that the object to update
		// is that very object, and that the update does not try to
		// change its identity
		for i = 0; i < propertiesIndex; i++ {
			k := columns[i]

			values[i] = current[i]
			idAsString := values[i].(*uuid.UUID).String()

			// validate that the paramaters  match the object
			if params[k] != "all" && params[k] != idAsString {
				tx.Rollback()
				http.Error(w, "no such "+this, http.StatusNotFound)
				return
			}

			// validate that the body json matches the object
			value, ok := bodyJSON[k]
			// zero uuid counts as no uuid
			if ok && value != "00000000-0000-0000-0000-000000000000" && value != idAsString {
				tx.Rollback()
				http.Error(w, "illegal "+k, http.StatusBadRequest)
				return
			}
			// update the bodyJSON so we can validate
			bodyJSON[k] = values[i]
		}

		jsonData, _ := json.Marshal(bodyJSON)
		if rc.SchemaID != "" {
			if !b.jsonValidator.HasSchema(rc.SchemaID) {
				rlog.Errorf("ERROR: invalid configuration for resource %s, schemaID %s is unknown. Validation is deactivated for this resource", rc.Resource, rc.SchemaID)
			} else if err := b.jsonValidator.ValidateString(string(jsonData), rc.SchemaID); err != nil {
				rlog.Errorf("properties '%v' field does not follow schemaID %s, %v",
					string(jsonData), rc.SchemaID, err)
				http.Error(w, fmt.Sprintf("document '%v' field does not follow schemaID %s, %v",
					string(jsonData), rc.SchemaID, err), http.StatusBadRequest)
				return
			}
		}

		data, err := b.intercept(r.Context(), resource, core.OperationUpdate, primaryUUID, nil, jsonData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if data != nil {
			json.Unmarshal(data, &bodyJSON)
			if err != nil {
				http.Error(w, "interceptor: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}

		// extract the dynamic properties
		extract := map[string]interface{}{}
	property_loop:
		for key, value := range bodyJSON {
			for i := 0; i < propertiesIndex; i++ {
				if key == columns[i] {
					continue property_loop
				}
			}
			for i := propertiesIndex + 1; i < propertiesEndIndex; i++ {
				if key == columns[i] {
					continue property_loop
				}
			}
			if key == "created_at" || key == "revision" {
				continue
			}
			extract[key] = value
		}

		propertiesJSON, _ := json.Marshal(extract)
		values[i] = propertiesJSON
		i++

		for ; i < len(columns); i++ {
			value, ok := bodyJSON[columns[i]]
			if !ok {
				tx.Rollback()
				http.Error(w, "missing property or index "+columns[i], http.StatusBadRequest)
				return
			}
			values[i] = value
		}

		// next value is created_at. We only change it when explicitely requested
		createdAt := current[i]
		if value, ok := bodyJSON["created_at"]; ok {
			timestamp, _ := value.(string)
			t, err := time.Parse(time.RFC3339, timestamp)
			if err != nil {
				http.Error(w, "illegal created_at: "+err.Error(), http.StatusBadRequest)
				return
			}
			if !t.IsZero() {
				createdAt = t.UTC()
			}
		}
		values[i] = createdAt
		i++

		err = tx.QueryRow(updateQuery, values...).Scan(&primaryID)
		if err == csql.ErrNoRows {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		} else if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// re-read new values
		values, response := createScanValuesAndObject(&time.Time{}, &revision)
		err = tx.QueryRow(readQuery+"WHERE "+primary+"_id = $1;", &primaryID).Scan(values...)
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		mergeProperties(response)

		jsonData, _ = json.Marshal(response)
		err = b.commitWithNotification(r.Context(), tx, resource, core.OperationUpdate, *values[0].(*uuid.UUID), jsonData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	}

	// store the collection functions  for later usage in relations
	b.collectionFunctions[resource] = &collectionFunctions{
		list: list,
		read: read,
	}

	// CREATE
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		createWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPost)

	// UPDATE/CREATE with id
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		updateWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut, http.MethodPatch)

	// UPDATE/CREATE with fully qualified path
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		updateWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut, http.MethodPatch)

	// READ
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		readWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodGet)

	// PUT FOR STATIC PROPERTIES
	for i := staticPropertiesIndex; i < len(columns); i++ {
		property := columns[i]
		propertyRoute := fmt.Sprintf("%s/%s/{%s}", itemRoute, property, property)
		router.HandleFunc(propertyRoute, func(w http.ResponseWriter, r *http.Request) {
			logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
			updatePropertyWithAuth(w, r, property)
		}).Methods(http.MethodOptions, http.MethodPut)
		if singleton {
			propertyRoute := fmt.Sprintf("%s/%s/{%s}", singletonRoute, property, property)
			router.HandleFunc(propertyRoute, func(w http.ResponseWriter, r *http.Request) {
				logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
				updatePropertyWithAuth(w, r, property)
			}).Methods(http.MethodOptions, http.MethodPut)
		}
	}

	// LIST
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		listWithAuth(w, r, nil)
	}).Methods(http.MethodOptions, http.MethodGet)

	// DELETE
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		deleteWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

	// CLEAR
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		clearWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

	if !singleton {
		return
	}

	// READ
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		readWithAuth(w, r)

	}).Methods(http.MethodOptions, http.MethodGet)

	// UPDATE
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		updateWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut, http.MethodPatch)

	// DELETE
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		deleteWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

}
