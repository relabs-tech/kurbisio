// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend

import (
	"compress/gzip"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/lib/pq"

	"net/http"
	"net/http/httptest"
	"net/url"

	"github.com/google/uuid"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/backend/kss"
	"github.com/relabs-tech/kurbisio/core/csql"
	"github.com/relabs-tech/kurbisio/core/logger"
)

func (b *Backend) createCollectionResource(router *mux.Router, rc collectionConfiguration, singleton bool) {
	schema := b.db.Schema
	resource := rc.Resource

	nillog := logger.FromContext(nil)
	if singleton {
		nillog.Debugln("create singleton collection:", resource)
	} else {
		nillog.Debugln("create collection:", resource)
	}
	if rc.Description != "" {
		nillog.Debugln("  description:", rc.Description)
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
	ownerResource := ""
	ownerIndex := 1
	if singleton {
		if len(resource) < 2 {
			nillog.Errorf("singleton resource %s lacks owner", this)
			panic("invalid configuration")
		}
		owner = resources[len(resources)-2]
		ownerResource = strings.Join(resources[:len(resources)-1], "/")
		ownerIndex = 0
		primary = owner
		ownerIsSingleton, ok := b.collectionsAndSingletons[strings.TrimSuffix(rc.Resource, "/"+this)]
		if !ok {
			nillog.Errorf("owner of singleton resource %s does not exist: %s", this, owner)
			panic("invalid configuration")
		}
		if ownerIsSingleton {
			nillog.Errorf("owner of singleton resource %s must not be a singleton itself: %s", this, owner)
			panic("invalid configuration")
		}
	}
	dependencies := resources[:len(resources)-1]

	createQuery := fmt.Sprintf("CREATE table IF NOT EXISTS %s.\"%s\"", schema, resource)
	var createColumns, createColumnsLog []string
	var columns []string
	searchableColumns := []string{}

	if !singleton {
		columns = append(columns, this+"_id")
		searchableColumns = append(searchableColumns, this+"_id")
		createColumns = append(createColumns, this+"_id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY")
		createColumnsLog = append(createColumnsLog, this+"_id uuid NOT NULL")
	}

	createColumns = append(createColumns, "timestamp timestamp NOT NULL DEFAULT now()")
	createColumnsLog = append(createColumnsLog, "timestamp timestamp NOT NULL DEFAULT now()")
	createColumns = append(createColumns, "revision INTEGER NOT NULL DEFAULT 1")
	createColumnsLog = append(createColumnsLog, "revision INTEGER NOT NULL")

	var foreignColumns []string
	for i := len(dependencies) - 1; i >= 0; i-- {
		that := dependencies[i]
		createColumn := fmt.Sprintf("%s_id uuid NOT NULL", that)
		createColumns = append(createColumns, createColumn)
		createColumnsLog = append(createColumnsLog, createColumn)
		columns = append(columns, that+"_id")
		searchableColumns = append(searchableColumns, that+"_id")
		foreignColumns = append(foreignColumns, that+"_id")
	}
	majorSearchColumns := foreignColumns
	if singleton {
		majorSearchColumns = majorSearchColumns[1:]
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
	createColumnsLog = append(createColumnsLog, "properties json NOT NULL")
	// query to create all indices after the table creation
	createIndicesQuery := fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s\"(timestamp);",
		"sort_index_"+this+"_timestamp",
		schema, resource)

	if len(majorSearchColumns) > 0 {
		createIndicesQuery += fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s\"(%s,timestamp);",
			"sort_index_"+this+"_"+strings.Join(majorSearchColumns, "_")+"_timestamp",
			schema, resource, strings.Join(majorSearchColumns, ","))
	}
	createIndicesQueryLog := fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s/log\"(%s_id);",
		"sort_index_"+this+"_log_id",
		schema, resource, this)
	createIndicesQueryLog += fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s/log\"(timestamp);",
		"sort_index_"+this+"_log_timestamp",
		schema, resource)
	propertiesIndex := len(columns) // where properties start
	columns = append(columns, "properties")

	createPropertiesQuery := ""

	staticPropertiesIndex := len(columns) // where static properties start
	// static properties are varchars
	for _, property := range rc.StaticProperties {
		createPropertiesQuery += fmt.Sprintf("ALTER TABLE %s.\"%s\" ADD COLUMN IF NOT EXISTS \"%s\" varchar NOT NULL DEFAULT '';", schema, resource, property)
		columns = append(columns, property)
	}

	// static searchable properties are varchars with a non-unique index
	for _, property := range rc.SearchableProperties {
		createPropertiesQuery += fmt.Sprintf("ALTER TABLE %s.\"%s\" ADD COLUMN IF NOT EXISTS \"%s\" varchar NOT NULL DEFAULT '';", schema, resource, property)
		createIndicesQuery += fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s\"(%s);",
			"searchable_property_"+this+"_"+property,
			schema, resource, property)
		createIndicesQueryLog += fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s/log\"(%s);",
			"searchable_property_"+this+"_"+property,
			schema, resource, property)
		columns = append(columns, property)
		searchableColumns = append(searchableColumns, property)
	}

	propertiesEndIndex := len(columns) // where properties end

	// an external index is a unique varchar property.
	if len(rc.ExternalIndex) > 0 {
		name := rc.ExternalIndex
		createPropertiesQuery += fmt.Sprintf("ALTER TABLE %s.\"%s\" ADD COLUMN IF NOT EXISTS \"%s\" varchar NOT NULL DEFAULT '';", schema, resource, name)
		createIndicesQuery += fmt.Sprintf("CREATE UNIQUE index IF NOT EXISTS %s ON %s.\"%s\"(%s) WHERE %s <> '';",
			"external_index_"+this+"_"+name,
			schema, resource, name, name)
		// the log index is not unique
		createIndicesQueryLog += fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s/log\"(%s);",
			"external_index_"+this+"_"+name,
			schema, resource, name)
		columns = append(columns, name)
		searchableColumns = append(searchableColumns, name)
	}

	// the "device" collection gets an additional UUID column for the web token
	if this == "device" {
		createColumn := "token uuid NOT NULL DEFAULT uuid_generate_v4()"
		createColumns = append(createColumns, createColumn)
	}

	createQuery += "(" + strings.Join(createColumns, ", ") + ");" + createPropertiesQuery + createIndicesQuery

	var err error
	if b.updateSchema {
		_, err = b.db.Exec(createQuery)
		if err != nil {
			nillog.WithError(err).Errorf("Error while updating schema when running: %s", createQuery)
			panic(fmt.Sprintf("invalid configuration updating: err: %v", err))
		}
	}

	// if we have a default object and a valid schema, validate the default object
	if rc.Default != nil && rc.SchemaID != "" && b.jsonValidator.HasSchema(rc.SchemaID) {
		var defaultJSON map[string]interface{}
		err := json.Unmarshal(rc.Default, &defaultJSON)
		if err != nil {
			nillog.WithError(err).Errorf("parse error in backend configuration - default for %s: %s", this, err)
			panic("invalid configuration parse error")
		}
		// add dummy core identifiers
		var id uuid.UUID
		for i := 0; i < propertiesIndex; i++ {
			defaultJSON[columns[i]] = id
		}
		jsonData, _ := json.Marshal(defaultJSON)
		if err := b.jsonValidator.ValidateString(string(jsonData), rc.SchemaID); err != nil {
			nillog.WithError(err).Errorf("validating default for %s: field does not follow schemaID %s",
				resource, rc.SchemaID)
			panic("invalid configuration default")
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
		nillog.Debugln("  handle singleton routes:", singletonRoute, "GET,PUT,PATCH,DELETE")
		nillog.Debugln("  handle singleton routes:", listRoute, "GET,PUT,PATCH,DELETE")
		nillog.Debugln("  handle singleton routes:", itemRoute, "GET,PUT,PATCH,DELETE")
	} else {
		nillog.Debugln("  handle collection routes:", listRoute, "GET,POST,PUT,PATCH,DELETE")
		nillog.Debugln("  handle collection routes:", itemRoute, "GET,PUT,PATCH,DELETE")
	}

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", timestamp, revision FROM %s.\"%s\" ", schema, resource)
	sqlWhereOne := "WHERE " + compareIDsString(columns[:propertiesIndex])

	readQueryWithTotal := "SELECT " + strings.Join(columns, ", ") +
		fmt.Sprintf(", timestamp, revision, count(*) OVER() AS full_count FROM %s.\"%s\" ", schema, resource)
	readQueryMetaWithTotal := "SELECT " + strings.Join(columns[:propertiesIndex], ", ") +
		fmt.Sprintf(", timestamp, revision, count(*) OVER() AS full_count FROM %s.\"%s\" ", schema, resource)
	sqlWhereAll := "WHERE "
	if propertiesIndex > ownerIndex {
		sqlWhereAll += compareIDsString(columns[ownerIndex:propertiesIndex]) + " AND "
	}
	sqlWhereAll += fmt.Sprintf("($%d OR timestamp<=$%d) AND ($%d OR timestamp>=$%d) ",
		propertiesIndex-ownerIndex+1, propertiesIndex-ownerIndex+1+1, propertiesIndex-ownerIndex+1+2, propertiesIndex-ownerIndex+1+3)
	sqlPaginationDesc := fmt.Sprintf("ORDER BY timestamp DESC,%s DESC LIMIT $%d OFFSET $%d;",
		columns[0], propertiesIndex-ownerIndex+1+4, propertiesIndex-ownerIndex+1+5)

	sqlPaginationAsc := fmt.Sprintf("ORDER BY timestamp ASC,%s ASC LIMIT $%d OFFSET $%d;",
		columns[0], propertiesIndex-ownerIndex+1+4, propertiesIndex-ownerIndex+1+5)

	clearQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)
	sqlReturnObject := " RETURNING " + strings.Join(columns, ", ") + ", timestamp, revision"
	sqlReturnMeta := " RETURNING " + strings.Join(columns[:propertiesIndex], ", ") + ", timestamp"

	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource) + "(" + strings.Join(columns, ", ") + ", timestamp)"
	insertQuery += "VALUES(" + parameterString(len(columns)+1) + ")"
	insertQuery += " RETURNING " + primary + "_id;"

	insertQueryLog := fmt.Sprintf("INSERT INTO %s.\"%s/log\" ", schema, resource) + "(" + strings.Join(columns, ", ") + ", timestamp, revision)"
	insertQueryLog += "VALUES(" + parameterString(len(columns)+2) + ")"

	updateQuery := fmt.Sprintf("UPDATE %s.\"%s\" SET ", schema, resource)
	sets := make([]string, len(columns)-propertiesIndex)
	for i := propertiesIndex; i < len(columns); i++ {
		sets[i-propertiesIndex] = columns[i] + " = $" + strconv.Itoa(i+1)
	}
	updateQuery += strings.Join(sets, ", ") + ", timestamp = $" + strconv.Itoa(len(columns)+1)
	updateQuery += ", revision = revision + 1 " + sqlWhereOne + " RETURNING " + primary + "_id;"

	updatePropertyQuery := fmt.Sprintf("UPDATE %s.\"%s\" SET ", schema, resource)
	updatePropertyQuery += " %s = $" + strconv.Itoa(propertiesIndex+1)
	updatePropertyQuery += ", revision = revision + 1 " + sqlWhereOne + " RETURNING " + primary + "_id;"

	var singletonParentExistsQuery string
	if singleton {
		singletonParentExistsQuery = fmt.Sprintf("SELECT %s_id FROM %s.\"%s\" WHERE %s_id = $1;", owner, schema, ownerResource, owner)
	}

	createScanValuesAndObject := func(timestamp *time.Time, revision *int, extra ...interface{}) ([]interface{}, map[string]interface{}) {
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

		values[i] = timestamp
		object["timestamp"] = timestamp
		i++
		values[i] = revision
		object["revision"] = revision
		values = append(values, extra...)
		return values, object
	}

	createScanValuesAndObjectMeta := func(timestamp *time.Time, revision *int, extra ...interface{}) ([]interface{}, map[string]interface{}) {
		n := propertiesIndex + 1
		if revision != nil {
			n++
		}
		values := make([]interface{}, n, n+len(extra))
		object := map[string]interface{}{}
		var i int
		for ; i < propertiesIndex; i++ {
			values[i] = &uuid.UUID{}
			object[columns[i]] = values[i]
		}
		values[i] = timestamp
		object["timestamp"] = timestamp
		if revision != nil {
			i++
			values[i] = revision
			object["revision"] = revision
		}
		values = append(values, extra...)
		return values, object
	}

	createScanValuesAndObjectWithMeta := func(metaonly bool, timestamp *time.Time, revision *int, extra ...interface{}) ([]interface{}, map[string]interface{}) {
		if metaonly {
			return createScanValuesAndObjectMeta(timestamp, revision, extra...)
		}
		return createScanValuesAndObject(timestamp, revision, extra...)
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
			queryParameters     []interface{}
			sqlQuery            string
			limit               int = 100
			page                int = 1
			until               time.Time
			from                time.Time
			externalColumns     []string
			externalValues      []string
			externalOperators   []string
			filterJSONColumns   []string
			filterJSONValues    []string
			filterJSONOperators []string
			ascendingOrder      bool
			metaonly            bool
			err                 error
		)
		urlQuery := r.URL.Query()
		parameters := map[string]string{}
		var withCompanionUrls bool
		for key, array := range urlQuery {
			if key != "filter" && len(array) > 1 {
				http.Error(w, "illegal parameter array '"+key+"'", http.StatusBadRequest)
				return
			}
			value := array[0]
		switchStatement:
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

			case "filter", "search":
				for _, value := range array {
					var operator string
					i := strings.IndexRune(value, '=')
					if i < 0 {
						i = strings.IndexRune(value, '~')
						if i < 0 {
							err = fmt.Errorf("cannot parse filter, must be of type property=value or property~value")
							break
						} else {
							operator = " LIKE "
						}
					} else {
						operator = "="
					}
					filterKey := value[:i]
					filterValue := value[i+1:]

					found := false
					for _, searchableColumn := range searchableColumns {
						if filterKey == searchableColumn {
							externalValues = append(externalValues, filterValue)
							externalColumns = append(externalColumns, filterKey)
							found = true
							externalOperators = append(externalOperators, operator)
							break
						}
					}
					// This was not a search inside a columns, then we try to search in the json document
					if !found {
						if key == "search" {
							err = fmt.Errorf("unknown search property '%s'", filterKey)
							break switchStatement
						}
						filterJSONValues = append(filterJSONValues, filterValue)
						filterJSONColumns = append(filterJSONColumns, filterKey)
						filterJSONOperators = append(filterJSONOperators, operator)
					}
				}
			case "order":
				if value != "asc" && value != "desc" {
					err = fmt.Errorf("order must be asc or desc")
					break
				}
				ascendingOrder = (value == "asc")

			case "metaonly":
				metaonly, err = strconv.ParseBool(array[0])
				if err != nil {
					http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
					return
				}

			case "with_companion_urls":
				withCompanionUrls, err = strconv.ParseBool(array[0])
				if err != nil {
					http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
					return
				}

			default:
				err = fmt.Errorf("unknown")
			}

			parameters[key] = value
			if err != nil {
				nillog.Errorf("parameter '" + key + "': " + err.Error())
				http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
				return
			}
		}
		params := mux.Vars(r)
		selectors := map[string]string{}
		for i := ownerIndex; i < propertiesIndex; i++ { // skip ID
			selectors[columns[i]] = params[columns[i]]
		}
		if metaonly {
			sqlQuery = readQueryMetaWithTotal
		} else {
			sqlQuery = readQueryWithTotal
		}
		sqlQuery += sqlWhereAll
		if len(externalValues) == 0 && len(filterJSONValues) == 0 { // no filter(s), get entire collection
			queryParameters = make([]interface{}, propertiesIndex-ownerIndex+6)
		} else {
			queryParameters = make([]interface{}, propertiesIndex-ownerIndex+6+len(externalValues)+len(filterJSONValues))
			for i := range externalValues {
				sqlQuery += fmt.Sprintf("AND (%s%s$%d) ", externalColumns[i], externalOperators[i], propertiesIndex-ownerIndex+7+i)
				queryParameters[propertiesIndex-ownerIndex+6+i] = externalValues[i]
			}
			for i := range filterJSONValues {
				sqlQuery += fmt.Sprintf("AND (properties->>'%s'%s$%d) ", filterJSONColumns[i], filterJSONOperators[i], propertiesIndex-ownerIndex+7+i+len(externalValues))
				queryParameters[propertiesIndex-ownerIndex+6+i+len(externalValues)] = filterJSONValues[i]
			}
		}

		for i := ownerIndex; i < propertiesIndex; i++ { // skip ID
			queryParameters[i-ownerIndex] = params[columns[i]]
		}
		queryParameters[propertiesIndex-ownerIndex+0] = until.IsZero()
		queryParameters[propertiesIndex-ownerIndex+1] = until.UTC()
		queryParameters[propertiesIndex-ownerIndex+2] = from.IsZero()
		queryParameters[propertiesIndex-ownerIndex+3] = from.UTC()
		queryParameters[propertiesIndex-ownerIndex+4] = limit
		queryParameters[propertiesIndex-ownerIndex+5] = (page - 1) * limit

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

		// fmt.Printf("\n\nQUERY %#v parameters: %#v\n\n", sqlQuery, queryParameters)
		rows, err := b.db.Query(sqlQuery, queryParameters...)
		if err != nil {
			nillog.WithError(err).Errorf("Error 4721: cannot execute query `%s` %+v", sqlQuery, queryParameters)
			http.Error(w, "Error 4721", http.StatusInternalServerError)
			return
		}

		response := []interface{}{}
		defer rows.Close()
		var totalCount int
		for rows.Next() {
			var timestamp time.Time
			values, object := createScanValuesAndObjectWithMeta(metaonly, &timestamp, new(int), &totalCount)
			err := rows.Scan(values...)
			if err != nil {
				nillog.WithError(err).Errorf("Error 4725: cannot scan values")
				http.Error(w, "Error 4725", http.StatusInternalServerError)
				return
			}
			if !metaonly {
				var uploadURL string
				if rc.WithCompanionFile && withCompanionUrls && b.KssDriver != nil {
					var key string
					for i := 0; i < propertiesIndex; i++ {
						key += "/" + resources[i] + "_id/" + values[propertiesIndex-i-1].(*uuid.UUID).String()
					}

					validitySeconds := 900
					if rc.CompanionPresignedURLValidity > 0 {
						validitySeconds = rc.CompanionPresignedURLValidity
					}

					uploadURL, err = b.KssDriver.GetPreSignedURL(kss.Get, key, time.Second*time.Duration(validitySeconds))
					if err != nil {
						nillog.WithError(err).Errorf("Error 5736: list companion URL")
						http.Error(w, "Error 5736", http.StatusInternalServerError)
						return
					}
					object["companion_download_url"] = uploadURL
				}

				mergeProperties(object)
				// apply defaults if applicable
				if rc.Default != nil {
					var defaultJSON map[string]interface{}
					json.Unmarshal(rc.Default, &defaultJSON)
					patchObject(defaultJSON, object)
					object = defaultJSON
				}
			}

			// if we did not have from, take it from the first object
			if from.IsZero() {
				from = timestamp
			}
			response = append(response, object)
		}

		// do request interceptors
		jsonData, _ := json.MarshalWithOption(response, json.DisableHTMLEscape())
		data, err := b.intercept(r.Context(), resource, core.OperationList, uuid.UUID{}, selectors, parameters, jsonData)
		if err != nil {
			nillog.WithError(err).Errorf("Error 4726: cannot request interceptors")
			http.Error(w, "Error 4726", http.StatusInternalServerError)
			return
		}
		if data != nil {
			jsonData = data
		}

		if page > 0 && totalCount == 0 {
			// sql does not return total count if we ask beyond limits, hence
			// we need a second query
			queryParameters[propertiesIndex-ownerIndex+4] = 1
			queryParameters[propertiesIndex-ownerIndex+5] = 0
			rows, err := b.db.Query(sqlQuery, queryParameters...)
			if err != nil {
				nillog.WithError(err).Errorf("Error 4722: cannot execute query `%s` %v", sqlQuery, queryParameters)
				http.Error(w, "Error 4722", http.StatusInternalServerError)
				return
			}
			defer rows.Close()
			for rows.Next() {
				var timestamp time.Time
				values, _ := createScanValuesAndObject(&timestamp, new(int), &totalCount)
				err := rows.Scan(values...)
				if err != nil {
					nillog.WithError(err).Errorf("Error 4725: cannot scan values")
					http.Error(w, "Error 4725", http.StatusInternalServerError)
					return
				}
			}
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Pagination-Limit", strconv.Itoa(limit))
		w.Header().Set("Pagination-Total-Count", strconv.Itoa(totalCount))
		w.Header().Set("Pagination-Page-Count", strconv.Itoa(((totalCount-1)/limit)+1))
		w.Header().Set("Pagination-Current-Page", strconv.Itoa(page))
		if !from.IsZero() {
			w.Header().Set("Pagination-Until", from.Format(time.RFC3339Nano))
		}

		etag := bytesPlusTotalCountToEtag(jsonData, totalCount)
		w.Header().Set("Etag", etag)
		if ifNoneMatchFound(r.Header.Get("If-None-Match"), etag) {
			w.WriteHeader(http.StatusNotModified)
			return
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
		var err error

		params := mux.Vars(r)
		noIntercept := false
		urlQuery := r.URL.Query()
		for key, array := range urlQuery {
			switch key {
			case "nointercept":
				noIntercept, err = strconv.ParseBool(array[0])
				if err != nil {
					http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
					return
				}
			case "children":
				break
			default:
				http.Error(w, "parameter '"+key+"': unknown query parameter", http.StatusBadRequest)
				return
			}
		}

		selectors := map[string]string{}
		for i := 0; i < propertiesIndex; i++ {
			selectors[columns[i]] = params[columns[i]]
		}

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

		values, object := createScanValuesAndObject(&time.Time{}, new(int))
		err = b.db.QueryRow(readQuery+sqlWhereOne+subQuery+";", queryParameters...).Scan(values...)
		if err == csql.ErrNoRows {
			if singleton {
				var jsonData []byte
				// apply defaults if applicable
				primaryID, _ := uuid.Parse(params[owner+"_id"])

				// validate that the parent exists, and if not return not found
				var parentID uuid.UUID
				err = b.db.QueryRow(singletonParentExistsQuery, &primaryID).Scan(&parentID)
				if err == csql.ErrNoRows {
					http.Error(w, "no such "+this, http.StatusNotFound)
					return
				} else if err != nil {
					nillog.WithError(err).Errorf("Error 4788: cannot check parent of singleton")
					http.Error(w, "Error 4788", http.StatusInternalServerError)
					return
				}

				if rc.Default != nil {
					var bodyJSON map[string]interface{}
					json.Unmarshal(rc.Default, &bodyJSON)
					for i := 0; i < propertiesIndex; i++ {
						bodyJSON[columns[i]] = params[columns[i]]
					}
					jsonData, _ = json.Marshal(bodyJSON)
				}
				if !noIntercept {
					data, err := b.intercept(r.Context(), resource, core.OperationRead, primaryID, selectors, nil, jsonData)
					if err != nil {
						nillog.WithError(err).Errorf("Error 4751: interceptor")
						http.Error(w, "Error 4751", http.StatusInternalServerError)
						return
					}
					if data != nil {
						jsonData = data
					}
				}
				if jsonData != nil {
					etag := bytesToEtag(jsonData)
					w.Header().Set("Etag", etag)
					if ifNoneMatchFound(r.Header.Get("If-None-Match"), etag) {
						w.WriteHeader(http.StatusNotModified)
						return
					}
					w.Header().Set("Content-Type", "application/json; charset=utf-8")
					w.WriteHeader(http.StatusOK)
					w.Write(jsonData)
				} else {
					w.WriteHeader(http.StatusNoContent)
				}
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
				http.Error(w, "invalid uuid", status)
				return
			}
			nillog.WithError(err).Errorf("Error 4727: cannot QueryRow")
			http.Error(w, "Error 4727", status)
			return
		}
		mergeProperties(object)

		// apply defaults if applicable
		if rc.Default != nil {
			var defaultJSON map[string]interface{}
			json.Unmarshal(rc.Default, &defaultJSON)
			patchObject(defaultJSON, object)
			object = defaultJSON
		}

		if rc.WithCompanionFile && b.KssDriver != nil {
			var key string
			for i := 0; i < propertiesIndex; i++ {
				key += "/" + resources[i] + "_id/" + values[propertiesIndex-i-1].(*uuid.UUID).String()
			}

			validitySeconds := 900
			if rc.CompanionPresignedURLValidity > 0 {
				validitySeconds = rc.CompanionPresignedURLValidity
			}

			downloadURL, err := b.KssDriver.GetPreSignedURL(kss.Get, key, time.Second*time.Duration(validitySeconds))
			if err != nil {
				nillog.WithError(err).Errorf("Error 1736: get companion URL")
				http.Error(w, "Error 1736", http.StatusInternalServerError)
				return
			}
			object["companion_download_url"] = downloadURL
		}

		// do request interceptors
		jsonData, _ := json.MarshalWithOption(object, json.DisableHTMLEscape())
		data, err := b.intercept(r.Context(), resource, core.OperationRead, *values[0].(*uuid.UUID), selectors, nil, jsonData)
		if err != nil {
			nillog.WithError(err).Errorf("Error 4748: interceptor")
			http.Error(w, "Error 4748", http.StatusInternalServerError)
			return
		}
		if data != nil {
			jsonData = data
		}

		// add children if requested
		for key, array := range urlQuery {
			switch key {
			case "nointercept":
				break
			case "children":
				if data != nil { // data was changed in interceptor
					err = json.Unmarshal(jsonData, &object)
					if err != nil {
						nillog.WithError(err).Errorf("Error 4749: interceptor")
						http.Error(w, "Error 4749", http.StatusInternalServerError)
						return
					}
				}

				status, err := b.addChildrenToGetResponse(array, noIntercept, r, object)
				if err != nil {
					http.Error(w, err.Error(), status)
					return
				}
				jsonData, _ = json.MarshalWithOption(object, json.DisableHTMLEscape())
			default:
				http.Error(w, "parameter '"+key+"': unknown query parameter", http.StatusBadRequest)
				return
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

		value, err = url.PathUnescape(value)
		if err != nil {
			http.Error(w, fmt.Sprintf("cannot unescape %s, err: %v", value, err), http.StatusBadRequest)
			return
		}

		query := fmt.Sprintf(updatePropertyQuery, property)

		queryParameters := make([]interface{}, propertiesIndex+1)
		i := 0
		for ; i < propertiesIndex; i++ {
			queryParameters[i] = params[columns[i]]
		}
		queryParameters[i] = value

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			nillog.WithError(err).Errorf("Error 4729: cannot BeginTx")
			http.Error(w, "Error 4729", http.StatusInternalServerError)
			return
		}

		var primaryID uuid.UUID
		err = tx.QueryRow(query, queryParameters...).Scan(&primaryID)
		if err == csql.ErrNoRows {
			tx.Rollback()
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			tx.Rollback()
			nillog.WithError(err).Errorf("Error 4728: cannot QueryRow query:`%s`", query)
			http.Error(w, "Error 4728", http.StatusInternalServerError)
			return
		}
		notification := map[string]string{
			property: value,
		}
		notificationJSON, _ := json.MarshalWithOption(notification, json.DisableHTMLEscape())
		err = b.commitWithNotification(r.Context(), tx, resource, core.OperationUpdate, primaryID, notificationJSON)
		if err != nil {
			nillog.WithError(err).Errorf("Error 4744: sqlQuery `%s`", query)
			http.Error(w, "Error 4744", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}

	deleteWithAuth := func(w http.ResponseWriter, r *http.Request) {
		rlog := logger.FromContext(r.Context())
		params := mux.Vars(r)
		selectors := map[string]string{}
		for i := 0; i < propertiesIndex; i++ {
			selectors[columns[i]] = params[columns[i]]
		}

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

		_, err = b.intercept(r.Context(), resource, core.OperationDelete, primaryID, selectors, nil, nil)
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
			rlog.WithError(err).Errorf("Error 4729: cannot BeginTx")
			http.Error(w, "Error 4729", http.StatusInternalServerError)
			return
		}

		var timestamp time.Time
		values, object := createScanValuesAndObject(&timestamp, new(int))
		err = tx.QueryRow(deleteQuery+sqlWhereOne+sqlReturnObject, queryParameters...).Scan(values...)
		if err == csql.ErrNoRows {
			tx.Rollback()
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			tx.Rollback()
			rlog.WithError(err).Errorf("Error 4730: cannot QueryRow")
			http.Error(w, "Error 4730", http.StatusInternalServerError)
			return
		}
		if rc.needsKSS && b.KssDriver != nil {
			var key string
			for i := 0; i < propertiesIndex; i++ {
				key += "/" + resources[i] + "_id/" + values[propertiesIndex-i-1].(*uuid.UUID).String()
			}

			err = b.KssDriver.DeleteAllWithPrefix(key)
			if err != nil {
				rlog.WithError(err).Error("Could not DeleteAllWithPrefix key ", key)
			}
		}

		mergeProperties(object)
		jsonData, _ := json.MarshalWithOption(object, json.DisableHTMLEscape())

		var silent bool
		if s := r.URL.Query().Get("silent"); s != "" {
			silent, _ = strconv.ParseBool(s)
		}

		if silent {
			err = tx.Commit()

		} else {
			err = b.commitWithNotification(r.Context(), tx, resource, core.OperationDelete, primaryID, jsonData)
		}
		if err != nil {
			nillog.WithError(err).Errorf("Error 4750: cannot QueryRow")
			http.Error(w, "Error 4750", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}

	clearWithAuth := func(w http.ResponseWriter, r *http.Request) {
		var err error
		rlog := logger.FromContext(r.Context())

		params := mux.Vars(r)
		selectors := map[string]string{}
		for i := ownerIndex; i < propertiesIndex; i++ { // skip ID
			selectors[columns[i]] = params[columns[i]]
		}

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
				for _, searchableColumn := range searchableColumns {
					if filterKey == searchableColumn {
						externalValue = filterValue
						externalColumn = searchableColumn
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
				rlog.Errorf("parameter '" + key + "': " + err.Error())
				http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
				return
			}
			parameters[key] = value
		}

		_, err = b.intercept(r.Context(), resource, core.OperationClear, uuid.UUID{}, selectors, parameters, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			rlog.WithError(err).Errorf("Error 4731: BeginTx")
			http.Error(w, "Error 4731", http.StatusInternalServerError)
			return
		}

		if externalValue == "" { // delete entire collection
			sqlQuery = clearQuery + sqlWhereAll
			queryParameters = make([]interface{}, propertiesIndex-1+4)
			for i := ownerIndex; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-ownerIndex] = params[columns[i]]
			}
		} else {
			sqlQuery = clearQuery + sqlWhereAll + fmt.Sprintf("AND (%s=$%d)", externalColumn, propertiesIndex+4)
			queryParameters = make([]interface{}, propertiesIndex-ownerIndex+4+1)
			for i := ownerIndex; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-ownerIndex] = params[columns[i]]
			}
			queryParameters[propertiesIndex-ownerIndex+4] = externalValue
		}

		// add before and after and pagination
		queryParameters[propertiesIndex-ownerIndex+0] = until.IsZero()
		queryParameters[propertiesIndex-ownerIndex+1] = until.UTC()
		queryParameters[propertiesIndex-ownerIndex+2] = from.IsZero()
		queryParameters[propertiesIndex-ownerIndex+3] = from.UTC()

		rows, err := tx.Query(sqlQuery+sqlReturnMeta, queryParameters...)
		if err != nil {
			tx.Rollback()
			rlog.WithError(err).Errorf("Error 4732: sqlQuery `%s`", sqlQuery)
			http.Error(w, "Error 4732", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		if rc.needsKSS && b.KssDriver != nil {
			for rows.Next() {
				var timestamp time.Time
				values, _ := createScanValuesAndObjectWithMeta(true, &timestamp, nil)
				err := rows.Scan(values...)
				if err != nil {
					rlog.WithError(err).Errorf("Error 4725: cannot scan values")
					http.Error(w, "Error 4725", http.StatusInternalServerError)
					return
				}
				var key string
				for i := 0; i < propertiesIndex; i++ {
					key += "/" + resources[i] + "_id/" + values[propertiesIndex-i-1].(*uuid.UUID).String()
				}
				err = b.KssDriver.DeleteAllWithPrefix(key)
				if err != nil {
					rlog.WithError(err).Error("Could not delete key ", key)
				}
			}
		}

		// add collection identifiers to parameters for the notification
		for i := 1; i < propertiesIndex; i++ {
			idOrAll := params[columns[i]]
			if idOrAll != "all" {
				parameters[columns[i]] = idOrAll
			}
		}
		notificationJSON, _ := json.MarshalWithOption(parameters, json.DisableHTMLEscape())
		err = b.commitWithNotification(r.Context(), tx, resource, core.OperationClear, uuid.UUID{}, notificationJSON)
		if err != nil {
			rlog.WithError(err).Errorf("Error 4770: sqlQuery `%s`", sqlQuery)
			http.Error(w, "Error 4770", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}

	create := func(w http.ResponseWriter, r *http.Request, bodyJSON map[string]interface{}) {
		var err error

		rlog := logger.FromContext(r.Context())
		calledFromUpsert := bodyJSON != nil

		// low-key features for the backup/restore tool
		var silent, force bool
		if calledFromUpsert {
			if s := r.URL.Query().Get("silent"); s != "" {
				silent, _ = strconv.ParseBool(s)
			}
			if s := r.URL.Query().Get("force"); s != "" {
				force, _ = strconv.ParseBool(s)
			}
		}

		params := mux.Vars(r)
		selectors := map[string]string{}
		for i := ownerIndex; i < propertiesIndex; i++ { // skip ID
			selectors[columns[i]] = params[columns[i]]
		}

		if bodyJSON == nil {
			body := r.Body
			if r.Header.Get("Content-Encoding") == "gzip" || r.Header.Get("Kurbisio-Content-Encoding") == "gzip" {
				body, err = gzip.NewReader(r.Body)
				if err != nil {
					http.Error(w, "invalid gzipped json data: "+err.Error(), http.StatusBadRequest)
					return
				}
			}

			err := json.NewDecoder(body).Decode(&bodyJSON)
			if err != nil {
				http.Error(w, "invalid json data: "+err.Error(), http.StatusBadRequest)
				return
			}
		}

		// build insert query and validate that we have all parameters
		values := make([]interface{}, len(columns)+1)
		var i int

		if !calledFromUpsert {
			// the primary resource identifier, always create a new one unless we are called
			// from upsert.
			primaryID := uuid.New()
			// update the bodyJSON so we can validate
			bodyJSON[columns[0]] = primaryID
			values[0] = primaryID
			i++
		}

		for ; i < propertiesIndex; i++ { // the core identifiers, either from url or from json
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

		if rc.Default != nil {
			var defaultJSON map[string]interface{}
			json.Unmarshal(rc.Default, &defaultJSON)
			patchObject(defaultJSON, bodyJSON)
			bodyJSON = defaultJSON
		}

		jsonData, _ := json.MarshalWithOption(bodyJSON, json.DisableHTMLEscape())

		validateSchema := rc.SchemaID != "" && !force

		if validateSchema {
			if !b.jsonValidator.HasSchema(rc.SchemaID) {
				rlog.Errorf("ERROR: invalid configuration for resource %s, schemaID %s is unknown. Validation is deactivated for this resource", rc.Resource, rc.SchemaID)
			} else if err := b.jsonValidator.ValidateString(string(jsonData), rc.SchemaID); err != nil {
				rlog.WithError(err).Errorf("properties '%v' field does not follow schemaID %s",
					string(jsonData), rc.SchemaID)
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

		if !force {
			data, err := b.intercept(r.Context(), resource, core.OperationCreate, primaryUUID, selectors, nil, jsonData)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if data != nil {
				json.Unmarshal(data, &bodyJSON)
				if err != nil {
					rlog.WithError(err).Error("Error 2733: interceptor")
					http.Error(w, "Error 2733", http.StatusInternalServerError)
					return
				}
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
			if key == "timestamp" || key == "revision" {
				continue
			}
			extract[key] = value
		}

		propertiesJSON, _ := json.MarshalWithOption(extract, json.DisableHTMLEscape())
		values[i] = propertiesJSON
		i++

		// static properties and external indices, non mandatory
		for ; i < len(columns); i++ {
			value, ok := bodyJSON[columns[i]]
			if !ok {
				value = ""
			}
			values[i] = value
		}

		// next value is timestamp
		now := time.Now().UTC()
		timestamp := now
		if value, ok := bodyJSON["timestamp"]; ok {
			timestampAsString, _ := value.(string)
			t, err := time.Parse(time.RFC3339, timestampAsString)
			if err != nil {
				http.Error(w, "illegal timestamp: "+err.Error(), http.StatusBadRequest)
				return
			}
			if !t.IsZero() {
				timestamp = t.UTC()
			}
		}
		values[i] = &timestamp
		i++

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			rlog.WithError(err).Errorf("Error 4733: BeginTx")
			http.Error(w, "Error 4733", http.StatusInternalServerError)
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
			msg := "Error 4734"
			if err, ok := err.(*pq.Error); ok && (err.Code == "23505" || err.Code == "23502" || err.Code == "23503") {
				if err.Code == "23505" {
					// Non unique external keys are reported as code Code 23505
					status = http.StatusConflict
					msg = "constraint violation"
					rlog.WithError(err).Infof("Constraint violation: QueryRow query: `%s`", insertQuery)
				} else if err.Code == "23502" {
					// Not null constraints are reported as Code 23502
					status = http.StatusUnprocessableEntity
					msg = "constraint violation"
					rlog.WithError(err).Infof("Constraint violation: QueryRow query: `%s`", insertQuery)
				} else if err.Code == "23503" {
					// 23503 is FOREIGN KEY VIOLATION and means that the resource does not exist. This should only happen for singleton
					status = http.StatusNotFound
					msg = ""
				}
			} else {
				rlog.WithError(err).Errorf("Error 4734: QueryRow query: `%s`", insertQuery)
			}
			tx.Rollback()
			http.Error(w, msg, status)
			return
		}

		// re-read data and return as json
		values, object := createScanValuesAndObject(&timestamp, new(int))
		err = tx.QueryRow(readQuery+"WHERE "+primary+"_id = $1;", id).Scan(values...)
		if err != nil {
			tx.Rollback()
			rlog.WithError(err).Errorf("Error 4735: re-read object")
			http.Error(w, "Error 4735", http.StatusInternalServerError)
			return
		}

		var uploadURL string
		if rc.WithCompanionFile && b.KssDriver != nil {
			var key string
			for i := propertiesIndex - 1; i >= ownerIndex; i-- {
				key += "/" + columns[i] + "/" + selectors[columns[i]]
			}
			key += "/" + primary + "_id/" + object[primary+"_id"].(*uuid.UUID).String()

			validitySeconds := 900
			if rc.CompanionPresignedURLValidity > 0 {
				validitySeconds = rc.CompanionPresignedURLValidity
			}

			uploadURL, err = b.KssDriver.GetPreSignedURL(kss.Put, key, time.Second*time.Duration(validitySeconds))
			if err != nil {
				tx.Rollback()
				rlog.WithError(err).Errorf("Error 5736: create companion URL")
				http.Error(w, "Error 5736", http.StatusInternalServerError)
				return
			}
		}

		mergeProperties(object)
		jsonData, _ = json.MarshalWithOption(object, json.DisableHTMLEscape())

		if silent {
			err = tx.Commit()
		} else {
			err = b.commitWithNotification(r.Context(), tx, resource, core.OperationCreate, id, jsonData)
		}
		if err != nil {
			rlog.WithError(err).Error("Error 4737: commitWithNotification")
			http.Error(w, "Error 4737", http.StatusInternalServerError)
			return
		}

		// We add companion_upload_url after inserting in the database if needed
		if uploadURL != "" {
			object["companion_upload_url"] = uploadURL
			jsonData, _ = json.MarshalWithOption(object, json.DisableHTMLEscape())
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusCreated)
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

	upsertWithAuth := func(w http.ResponseWriter, r *http.Request) {
		var err error

		rlog := logger.FromContext(r.Context())

		// low-key features for the backup/restore tool
		var silent, force bool
		if s := r.URL.Query().Get("silent"); s != "" {
			silent, _ = strconv.ParseBool(s)
		}
		if s := r.URL.Query().Get("force"); s != "" {
			force, _ = strconv.ParseBool(s)
		}

		params := mux.Vars(r)
		selectors := map[string]string{}
		for i := 0; i < propertiesIndex; i++ {
			selectors[columns[i]] = params[columns[i]]
		}

		body := r.Body
		if r.Header.Get("Content-Encoding") == "gzip" || r.Header.Get("Kurbisio-Content-Encoding") == "gzip" {
			body, err = gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, "invalid gzipped json data: "+err.Error(), http.StatusBadRequest)
				return
			}
		}

		var bodyJSON map[string]interface{}
		err = json.NewDecoder(body).Decode(&bodyJSON)
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
			rlog.WithError(err).Errorf("Error 4736: Update of resource `%s`", resource)
			http.Error(w, "Error 4736", http.StatusInternalServerError)
			return
		}

		var timestamp time.Time
		var currentRevision int
		retried := false
	Retry:
		current, object := createScanValuesAndObject(&timestamp, &currentRevision)
		err = tx.QueryRow(readQuery+"WHERE "+primary+"_id = $1 FOR UPDATE;", &primaryID).Scan(current...)
		if err == csql.ErrNoRows {
			// item does not exist yet.
			if singleton {
				// This is OK for singletons (they conceptually always exist)
			} else if r.Method == http.MethodPatch {
				// cannot patch an object which does not exist
				tx.Rollback()
				http.Error(w, "no such "+this, http.StatusNotFound)
				return
			} else if b.authorizationEnabled {
				// normal upsert, check whether we can create the object
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
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusCreated)
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
			rlog.WithError(err).Error("Error 4737: Rollback")
			http.Error(w, "Error 4737", http.StatusInternalServerError)
			return
		}
		if revision != 0 && revision != currentRevision {
			tx.Rollback()
			// revision does not match, return conflict status with the conflicting object
			mergeProperties(object)
			jsonData, _ := json.MarshalWithOption(object, json.DisableHTMLEscape())
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(http.StatusConflict)
			w.Write(jsonData)
			return
		}
		mergeProperties(object)

		primaryUUID := *current[0].(*uuid.UUID)
		primaryID = primaryUUID.String()

		// for MethodPatch we get the existing object from the database and patch property by property
		if r.Method == http.MethodPatch {

			// convert object into generic json for patching (the datatypes are different compared to the database) in the database)
			body, _ := json.MarshalWithOption(object, json.DisableHTMLEscape())
			var objectJSON map[string]interface{}
			json.Unmarshal(body, &objectJSON)

			// now bodyJSON from the request becomes a patch
			patchObject(objectJSON, bodyJSON)

			// rewrite this put request to contain the entire (patched) object
			bodyJSON = objectJSON
		}

		// apply defaults if applicable
		if rc.Default != nil {
			var defaultJSON map[string]interface{}
			json.Unmarshal(rc.Default, &defaultJSON)
			patchObject(defaultJSON, bodyJSON)
			bodyJSON = defaultJSON
		}

		// build insert query and validate that we have all parameters
		values := make([]interface{}, len(columns)+1)

		var i int

		// validate core identifiers
		// Rationale: we have authorized the resource based on the parameters
		// in the URL, so we have to ensure that the object to update
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

		jsonData, _ := json.MarshalWithOption(bodyJSON, json.DisableHTMLEscape())
		validateSchema := rc.SchemaID != "" && !force
		if validateSchema {
			if !b.jsonValidator.HasSchema(rc.SchemaID) {
				rlog.Errorf("ERROR: invalid configuration for resource %s, schemaID %s is unknown. Validation is deactivated for this resource", rc.Resource, rc.SchemaID)
			} else if err := b.jsonValidator.ValidateString(string(jsonData), rc.SchemaID); err != nil {
				tx.Rollback()
				rlog.WithError(err).Errorf("properties '%v' field does not follow schemaID %s",
					string(jsonData), rc.SchemaID)
				http.Error(w, fmt.Sprintf("document '%v' field does not follow schemaID %s, %v",
					string(jsonData), rc.SchemaID, err), http.StatusBadRequest)
				return
			}
		}

		if !force {
			data, err := b.intercept(r.Context(), resource, core.OperationUpdate, primaryUUID, selectors, nil, jsonData)
			if err != nil {
				tx.Rollback()
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if data != nil {
				json.Unmarshal(data, &bodyJSON)
				if err != nil {
					tx.Rollback()
					rlog.WithError(err).Errorf("Error 4738: interceptor")
					http.Error(w, "Error 4738", http.StatusInternalServerError)
					return
				}
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
			if key == "timestamp" || key == "revision" {
				continue
			}
			extract[key] = value
		}

		propertiesJSON, _ := json.MarshalWithOption(extract, json.DisableHTMLEscape())
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

		// next value is timestamp. We only change it when explicitely requested
		if value, ok := bodyJSON["timestamp"]; ok {
			timestampAsString, _ := value.(string)
			t, err := time.Parse(time.RFC3339, timestampAsString)
			if err != nil {
				http.Error(w, "illegal timestamp: "+err.Error(), http.StatusBadRequest)
				return
			}
			if !t.IsZero() {
				timestamp = t.UTC()
			}
		}
		values[i] = timestamp
		i++

		err = tx.QueryRow(updateQuery, values...).Scan(&primaryID)
		if err == csql.ErrNoRows {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		} else if err != nil {
			tx.Rollback()
			rlog.WithError(err).Errorf("Error 4739: update object")
			http.Error(w, "Error 4739", http.StatusInternalServerError)
			return
		}

		// re-read new values and return as json
		values, response := createScanValuesAndObject(&timestamp, &revision)
		err = tx.QueryRow(readQuery+"WHERE "+primary+"_id = $1;", &primaryID).Scan(values...)
		if err != nil {
			tx.Rollback()
			rlog.WithError(err).Errorf("Error 4740: re-read object")
			http.Error(w, "Error 4740", http.StatusInternalServerError)
			return
		}
		mergeProperties(response)
		jsonData, _ = json.MarshalWithOption(response, json.DisableHTMLEscape())

		var uploadURL string
		if rc.WithCompanionFile && b.KssDriver != nil {
			var key string
			for i := propertiesIndex - 1; i >= ownerIndex; i-- {
				key += "/" + columns[i] + "/" + selectors[columns[i]]
			}
			key += "/" + primary + "_id/" + response[primary+"_id"].(*uuid.UUID).String()

			validitySeconds := 900
			if rc.CompanionPresignedURLValidity > 0 {
				validitySeconds = rc.CompanionPresignedURLValidity
			}

			uploadURL, err = b.KssDriver.GetPreSignedURL(kss.Put, key, time.Second*time.Duration(validitySeconds))
			if err != nil {
				tx.Rollback()
				rlog.WithError(err).Errorf("Error 5736: create companion URL")
				http.Error(w, "Error 5736", http.StatusInternalServerError)
				return
			}
		}

		if silent {
			err = tx.Commit()
		} else {
			err = b.commitWithNotification(r.Context(), tx, resource, core.OperationUpdate, *values[0].(*uuid.UUID), jsonData)
		}
		if err != nil {
			rlog.WithError(err).Errorf("Error 4739: commitWithNotification")
			http.Error(w, "Error 4739", http.StatusInternalServerError)
			return
		}

		// We add companion_upload_url after inserting in the database if needed
		if uploadURL != "" {
			response["companion_upload_url"] = uploadURL
			jsonData, _ = json.MarshalWithOption(response, json.DisableHTMLEscape())
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	}

	// store the collection functions  for later usage in relations
	b.collectionFunctions[resource] = &collectionFunctions{
		permits: rc.Permits,
		list:    list,
		read:    read,
	}

	// CREATE
	if !singleton {
		router.Handle(listRoute, handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
			createWithAuth(w, r)
		}))).Methods(http.MethodOptions, http.MethodPost)
	}

	// UPDATE/CREATE with id in json
	router.Handle(listRoute, handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		upsertWithAuth(w, r)
	}))).Methods(http.MethodOptions, http.MethodPut, http.MethodPatch)

	// UPDATE/CREATE with fully qualified path
	router.Handle(itemRoute, handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		upsertWithAuth(w, r)
	}))).Methods(http.MethodOptions, http.MethodPut, http.MethodPatch)

	// READ
	router.Handle(itemRoute, handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		readWithAuth(w, r)
	}))).Methods(http.MethodOptions, http.MethodGet)

	// PUT FOR STATIC PROPERTIES
	for i := staticPropertiesIndex; i < len(columns); i++ {
		property := columns[i]
		propertyRoute := fmt.Sprintf("%s/%s/{%s}", itemRoute, property, property)
		router.Handle(propertyRoute, handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
			updatePropertyWithAuth(w, r, property)
		}))).Methods(http.MethodOptions, http.MethodPut)
		if singleton {
			propertyRoute := fmt.Sprintf("%s/%s/{%s}", singletonRoute, property, property)
			router.Handle(propertyRoute, handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
				updatePropertyWithAuth(w, r, property)
			}))).Methods(http.MethodOptions, http.MethodPut)
		}
	}

	// LIST
	router.Handle(listRoute, handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		listWithAuth(w, r, nil)
	}))).Methods(http.MethodOptions, http.MethodGet)

	// DELETE
	router.Handle(itemRoute, handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		deleteWithAuth(w, r)
	}))).Methods(http.MethodOptions, http.MethodDelete)

	// CLEAR
	router.Handle(listRoute, handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		clearWithAuth(w, r)
	}))).Methods(http.MethodOptions, http.MethodDelete)

	if !singleton {
		return
	}

	// READ
	router.Handle(singletonRoute, handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		readWithAuth(w, r)

	}))).Methods(http.MethodOptions, http.MethodGet)

	// UPDATE
	router.Handle(singletonRoute, handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		upsertWithAuth(w, r)
	}))).Methods(http.MethodOptions, http.MethodPut, http.MethodPatch)

	// DELETE
	router.Handle(singletonRoute, handlers.CompressHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		deleteWithAuth(w, r)
	}))).Methods(http.MethodOptions, http.MethodDelete)

}
