// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

package backend

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/lib/pq"

	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/logger"
)

func (b *Backend) createBlobResource(router *mux.Router, rc BlobConfiguration) {
	schema := b.db.Schema
	resource := rc.Resource
	nillog := logger.FromContext(context.Background())
	nillog.Debugln("create blob:", resource)
	if rc.Description != "" {
		nillog.Debugln("  description:", rc.Description)
	}

	resources := strings.Split(rc.Resource, "/")
	this := resources[len(resources)-1]
	dependencies := resources[:len(resources)-1]

	createQuery := fmt.Sprintf("CREATE table IF NOT EXISTS %s.\"%s\"", schema, resource)
	createColumns := []string{
		this + "_id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY",
		"timestamp timestamp NOT NULL DEFAULT now()",
	}

	columns := []string{this + "_id"}
	searchableColumns := []string{columns[0]}

	for i := range dependencies {
		that := dependencies[i]
		createColumn := fmt.Sprintf("%s_id uuid NOT NULL", that)
		createColumns = append(createColumns, createColumn)
		columns = append(columns, that+"_id")
		searchableColumns = append(searchableColumns, that+"_id")
	}

	if len(dependencies) > 0 {
		foreignColumns := strings.Join(columns[1:], ",")
		createColumn := "FOREIGN KEY (" + foreignColumns + ") " +
			"REFERENCES " + schema + ".\"" + strings.Join(dependencies, "/") + "\" " +
			"(" + foreignColumns + ") ON DELETE CASCADE"
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
	createIndicesQuery := fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s\"(timestamp);",
		"sort_index_"+this+"_timestamp",
		schema, resource)
	propertiesIndex := len(columns) // where properties start
	columns = append(columns, "properties")

	jsonToHeader := map[string]string{}

	createPropertiesQuery := ""
	// static properties are varchars
	for _, property := range rc.StaticProperties {
		createPropertiesQuery += fmt.Sprintf("ALTER TABLE %s.\"%s\" ADD COLUMN IF NOT EXISTS \"%s\" varchar NOT NULL DEFAULT '';", schema, resource, property)
		columns = append(columns, property)
		jsonToHeader[property] = core.PropertyNameToCanonicalHeader(property)
	}

	// static searchable properties are varchars with a non-unique index
	for _, property := range rc.SearchableProperties {
		createPropertiesQuery += fmt.Sprintf("ALTER TABLE %s.\"%s\" ADD COLUMN IF NOT EXISTS \"%s\" varchar NOT NULL DEFAULT '';", schema, resource, property)
		createIndicesQuery += fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s\"(%s);",
			"searchable_property_"+this+"_"+property,
			schema, resource, property)
		columns = append(columns, property)
		jsonToHeader[property] = core.PropertyNameToCanonicalHeader(property)
		searchableColumns = append(searchableColumns, property)
	}

	propertiesEndIndex := len(columns) // where properties end

	// an external index is a mandatory and unique varchar property.
	if len(rc.ExternalIndex) > 0 {
		name := rc.ExternalIndex
		createPropertiesQuery += fmt.Sprintf("ALTER TABLE %s.\"%s\" ADD COLUMN IF NOT EXISTS \"%s\" varchar NOT NULL DEFAULT '';", schema, resource, name)
		createIndicesQuery = createIndicesQuery + fmt.Sprintf("CREATE UNIQUE index IF NOT EXISTS %s ON %s.\"%s\"(%s);",
			"external_index_"+this+"_"+name,
			schema, resource, name)
		columns = append(columns, name)
		jsonToHeader[name] = core.PropertyNameToCanonicalHeader(name)
		searchableColumns = append(searchableColumns, name)
	}

	// the actual blob data as bytes
	createColumn := "blob bytea NOT NULL"
	createColumns = append(createColumns, createColumn)

	createQuery += "(" + strings.Join(createColumns, ", ") + ");" + createPropertiesQuery + createIndicesQuery

	var err error
	if b.updateSchema {
		_, err = b.db.Exec(createQuery)
		if err != nil {
			panic(err)
		}
	}

	listRoute := ""
	itemRoute := ""
	for _, r := range resources {
		listRoute = itemRoute + "/" + core.Plural(r)
		itemRoute = itemRoute + "/" + core.Plural(r) + "/{" + r + "_id}"
	}

	nillog.Debugln("  handle blob routes:", listRoute, "GET,POST,DELETE")
	nillog.Debugln("  handle blob routes:", itemRoute, "GET,PUT, DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", timestamp, blob FROM %s.\"%s\" ", schema, resource)
	readQueryMeta := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", timestamp FROM %s.\"%s\" ", schema, resource)
	sqlWhereOne := "WHERE " + compareIDsString(columns[:propertiesIndex])
	sqlReturnMeta := " RETURNING " + strings.Join(columns, ", ") + ", timestamp"

	readQueryWithTotal := "SELECT " + strings.Join(columns, ", ") +
		fmt.Sprintf(", timestamp FROM %s.\"%s\" ", schema, resource)
	sqlWhereAll := "WHERE "
	if propertiesIndex > 1 {
		sqlWhereAll += compareIDsString(columns[1:propertiesIndex]) + " AND "
	}
	sqlWhereAll += fmt.Sprintf("($%d OR timestamp<=$%d) AND ($%d OR timestamp>=$%d) ",
		propertiesIndex, propertiesIndex+1, propertiesIndex+2, propertiesIndex+3)

	sqlPagination := fmt.Sprintf("ORDER BY timestamp DESC, %s  DESC LIMIT $%d OFFSET $%d;", columns[0], propertiesIndex+4, propertiesIndex+5)

	// Cursor pagination SQL (no offset, just limit)
	sqlCursorPaginationDesc := fmt.Sprintf("ORDER BY timestamp DESC, %s DESC LIMIT $%d;", columns[0], propertiesIndex+4)
	sqlCursorPaginationAsc := fmt.Sprintf("ORDER BY timestamp ASC, %s ASC LIMIT $%d;", columns[0], propertiesIndex+4)

	// External index queries - separate for cursor vs page pagination due to different parameter counts
	sqlWhereAllPlusOneExternalIndexCursor := sqlWhereAll + fmt.Sprintf("AND %%s = $%d ", propertiesIndex+5)
	sqlWhereAllPlusOneExternalIndexPage := sqlWhereAll + fmt.Sprintf("AND %%s = $%d ", propertiesIndex+6)

	clearQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)
	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource) + "(" + strings.Join(columns, ", ") + ", blob, timestamp)"
	insertQuery += "VALUES(" + parameterString(len(columns)+2) + ") "
	insertQuery += "ON CONFLICT (" + this + "_id) DO UPDATE SET " + this + "_id = $1 RETURNING " + this + "_id;"

	updateQuery := fmt.Sprintf("UPDATE %s.\"%s\" SET ", schema, resource)
	sets := make([]string, len(columns)-propertiesIndex)
	for i := propertiesIndex; i < len(columns); i++ {
		sets[i-propertiesIndex] = columns[i] + " = $" + strconv.Itoa(i+1)
	}
	updateQuery += strings.Join(sets, ", ") + ", blob = $" + strconv.Itoa(len(columns)+1)
	updateQuery += ", timestamp = $" + strconv.Itoa(len(columns)+2) + " " + sqlWhereOne + " RETURNING " + this + "_id;"

	insertUpdateQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource) + "(" + strings.Join(columns, ", ") + ", blob, timestamp)"
	insertUpdateQuery += "VALUES(" + parameterString(len(columns)+2) + ") ON CONFLICT (" + this + "_id) DO UPDATE SET "
	insertUpdateQuery += strings.Join(sets, ", ") + ", blob = $" + strconv.Itoa(len(columns)+1)
	insertUpdateQuery += ", timestamp = $" + strconv.Itoa(len(columns)+2) + " RETURNING " + this + "_id;"

	maxAge := ""
	if !rc.Mutable {
		rc.MaxAgeCache = 31536000
	}
	if rc.MaxAgeCache > 0 {
		maxAge = fmt.Sprintf("max-age=%d", rc.MaxAgeCache)
	}

	createScanValuesAndObject := func(timestamp *time.Time, extra ...interface{}) ([]interface{}, map[string]interface{}) {
		values := make([]interface{}, len(columns)+1, len(columns)+1+len(extra))
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
			externalIndex   string

			nextToken           string
			cursor              *PaginationCursor
			useCursorPagination bool
			ascendingOrder      bool = false // Default to descending like collection.go
		)

		urlQuery := r.URL.Query()
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
			case "next_token":
				nextToken = value
				if nextToken != "" {
					var decodedCursor PaginationCursor
					decodedCursor, err = DecodePaginationCursor(nextToken)
					if err != nil {
						break
					}
					cursor = &decodedCursor
					useCursorPagination = true
				}
			case "order":
				if value != "asc" && value != "desc" {
					err = fmt.Errorf("order must be asc or desc")
					break
				}
				ascendingOrder = (value == "asc")
			case "until":
				until, err = time.Parse(time.RFC3339, value)

			case "from":
				from, err = time.Parse(time.RFC3339, value)
			case "filter", "search":
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
						externalIndex = filterValue
						externalColumn = searchableColumn
						found = true
						break
					}
				}
				if !found {
					err = fmt.Errorf("unknown filter property '%s'", filterKey)
				}

			default:
				err = fmt.Errorf("unknown")
			}
			if err != nil {
				nillog.Errorf("parameter '%s': %s", key, err.Error())
				http.Error(w, "parameter '"+key+"': "+err.Error(), http.StatusBadRequest)
				return
			}
		}

		// Check mutual exclusion of page and next_token
		pageProvided := urlQuery.Has("page")
		if useCursorPagination && pageProvided {
			http.Error(w, "page and next_token parameters are mutually exclusive", http.StatusBadRequest)
			return
		}

		// If no cursor and no page specified, use cursor pagination by default
		if !useCursorPagination && !pageProvided {
			useCursorPagination = true
		}

		params := mux.Vars(r)

		// Build base parameters first - different sizes for cursor vs page pagination
		if useCursorPagination {
			if externalIndex == "" { // get entire collection
				sqlQuery = readQueryWithTotal + sqlWhereAll
				queryParameters = make([]interface{}, propertiesIndex-1+5) // No offset for cursor
				for i := 1; i < propertiesIndex; i++ {                     // skip ID
					queryParameters[i-1] = params[columns[i]]
				}
			} else {
				sqlQuery = fmt.Sprintf(readQueryWithTotal+sqlWhereAllPlusOneExternalIndexCursor, externalColumn)
				queryParameters = make([]interface{}, propertiesIndex-1+5+1) // No offset for cursor
				for i := 1; i < propertiesIndex; i++ {                       // skip ID
					queryParameters[i-1] = params[columns[i]]
				}
				queryParameters[propertiesIndex-1+5] = externalIndex
			}
		} else {
			if externalIndex == "" { // get entire collection
				sqlQuery = readQueryWithTotal + sqlWhereAll
				queryParameters = make([]interface{}, propertiesIndex-1+6) // Include offset for page
				for i := 1; i < propertiesIndex; i++ {                     // skip ID
					queryParameters[i-1] = params[columns[i]]
				}
			} else {
				sqlQuery = fmt.Sprintf(readQueryWithTotal+sqlWhereAllPlusOneExternalIndexPage, externalColumn)
				queryParameters = make([]interface{}, propertiesIndex-1+6+1) // Include offset for page
				for i := 1; i < propertiesIndex; i++ {                       // skip ID
					queryParameters[i-1] = params[columns[i]]
				}
				queryParameters[propertiesIndex-1+6] = externalIndex
			}
		}

		// add before and after and limit
		queryParameters[propertiesIndex-1+0] = until.IsZero()
		queryParameters[propertiesIndex-1+1] = until.UTC()
		queryParameters[propertiesIndex-1+2] = from.IsZero()
		queryParameters[propertiesIndex-1+3] = from.UTC()
		queryParameters[propertiesIndex-1+4] = limit + 1

		if useCursorPagination {
			if cursor != nil {
				// Add cursor parameters to the existing parameter list
				currentParamCount := len(queryParameters)
				queryParameters = append(queryParameters, cursor.Timestamp.UTC(), cursor.ID)

				// Add cursor condition based on ordering
				if ascendingOrder {
					sqlQuery += fmt.Sprintf("AND ((timestamp > $%d) OR (timestamp = $%d AND %s > $%d)) ",
						currentParamCount+1, currentParamCount+1, columns[0], currentParamCount+2)
				} else {
					sqlQuery += fmt.Sprintf("AND ((timestamp < $%d) OR (timestamp = $%d AND %s < $%d)) ",
						currentParamCount+1, currentParamCount+1, columns[0], currentParamCount+2)
				}
			}
			// Don't set offset parameter for cursor pagination
		} else {
			// Traditional page-based pagination - set offset parameter
			queryParameters[propertiesIndex-1+5] = (page - 1) * limit
		}

		if relation != nil {
			// inject subquery for relation
			sqlQuery += fmt.Sprintf(relation.subquery,
				compareIDsStringWithOffset(len(queryParameters), relation.columns))
			queryParameters = append(queryParameters, relation.queryParameters...)
		}

		if useCursorPagination {
			if ascendingOrder {
				sqlQuery += sqlCursorPaginationAsc
			} else {
				sqlQuery += sqlCursorPaginationDesc
			}
		} else {
			sqlQuery += sqlPagination
		}

		rows, err := b.db.Query(sqlQuery, queryParameters...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		response := []interface{}{}
		defer rows.Close()
		hasMoreData := false
		rowCount := 0
		var lastTimestamp time.Time
		var lastID uuid.UUID
		for rows.Next() {
			var timestamp time.Time
			values, object := createScanValuesAndObject(&timestamp)
			err := rows.Scan(values...)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			rowCount++
			if rowCount <= limit {
				// Store last timestamp and ID for cursor generation
				lastTimestamp = timestamp
				lastID = *values[0].(*uuid.UUID) // First value is always the ID

				mergeProperties(object)

				// if we did not have from, take it from the first object
				if from.IsZero() {
					from = timestamp
				}
				response = append(response, object)
			} else {
				// We have more data than the limit, so there's a next page
				hasMoreData = true
			}
		}

		jsonData, _ := json.Marshal(response)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Pagination-Limit", strconv.Itoa(limit))

		if useCursorPagination {
			// Cursor pagination headers
			if hasMoreData && len(response) > 0 {
				// Generate next cursor from the last item in the response
				nextCursor := PaginationCursor{
					Timestamp: lastTimestamp,
					ID:        lastID,
				}
				w.Header().Set("Pagination-Next-Token", nextCursor.Encode())
			}
			// Don't set page count headers for cursor pagination
		}
		if !useCursorPagination || page == 1 {
			// Traditional page pagination headers
			// Calculate page count based on whether we have more data
			pageCount := page
			if hasMoreData {
				pageCount = page + 1
			}
			w.Header().Set("Pagination-Page-Count", strconv.Itoa(pageCount))
			w.Header().Set("Pagination-Current-Page", strconv.Itoa(page))
		}

		if !from.IsZero() {
			w.Header().Set("Pagination-Until", from.Format(time.RFC3339Nano))
		}

		// Calculate etag based on content and pagination type
		var etagSeed int
		if useCursorPagination {
			etagSeed = len(response) // Use response length for cursor pagination
		} else {
			// Calculate page count for traditional pagination
			pageCount := page
			if hasMoreData {
				pageCount = page + 1
			}
			etagSeed = pageCount
		}

		etag := bytesPlusTotalCountToEtag(jsonData, etagSeed)
		w.Header().Set("Etag", etag)
		if ifNoneMatchFound(r.Header.Get("If-None-Match"), etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Write(jsonData)

	}

	listWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationList, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		list(w, r, nil)
	}

	read := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
		rlog := logger.FromContext(r.Context())
		params := mux.Vars(r)
		queryParameters := make([]interface{}, propertiesIndex)
		for i := 0; i < propertiesIndex; i++ {
			queryParameters[i] = params[columns[i]]
		}

		if queryParameters[0].(string) == "all" {
			http.Error(w, "all is not a valid "+this, http.StatusBadRequest)
			return
		}

		if rc.Mutable {
			if ifNoneMatch := r.Header.Get("If-None-Match"); len(ifNoneMatch) > 0 {
				// special blob handling for if-non-match: Since we only need the creation time
				// for calculating the etag, we prefer doing an extra query instead of
				// loading the entire binary blob into memory for no good reason
				var timestamp time.Time
				values, object := createScanValuesAndObject(&timestamp)
				err = b.db.QueryRow(readQueryMeta+sqlWhereOne+";", queryParameters...).Scan(values...)
				if err == sql.ErrNoRows {
					http.Error(w, "no such "+this, http.StatusNotFound)
					return
				}
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				etag := timeToEtag(timestamp)
				if ifNoneMatchFound(ifNoneMatch, etag) {
					// headers must be provided also in case If-None-Match is set
					w.Header().Set("Etag", etag)
					for i := propertiesIndex + 1; i < len(columns); i++ {
						k := columns[i]
						w.Header().Set(jsonToHeader[k], *object[k].(*string))
					}
					w.Header().Set("Etag", timeToEtag(timestamp))
					if len(maxAge) > 0 {
						w.Header().Set("Cache-Control", maxAge)
					}
					metaData, _ := json.Marshal(object)
					w.Header().Set("Kurbisio-Meta-Data", string(metaData))
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}

		var blob []byte
		var timestamp time.Time
		values, object := createScanValuesAndObject(&timestamp, &blob)

		err = b.db.QueryRow(readQuery+sqlWhereOne+";", queryParameters...).Scan(values...)
		if err == sql.ErrNoRows {
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

		if len(blob) == 0 && rc.StoredExternally && b.KssDriver != nil {
			var key string
			for i := 0; i < propertiesIndex; i++ {
				key += "/" + resources[i] + "_id/" + values[propertiesIndex-i-1].(*uuid.UUID).String()
			}
			file, err := b.KssDriver.DownloadData(key)
			if err != nil {
				rlog.WithError(err).Errorf("Error 5320: download data `%s`", key)
				http.Error(w, "Error 5320: data not available", http.StatusFailedDependency)
				return
			}
			blob = file
			w.Header().Set("Kurbisio-Source", "kss")
		}

		for i := propertiesIndex + 1; i < len(columns); i++ {
			k := columns[i]
			w.Header().Set(jsonToHeader[k], *object[k].(*string))
		}
		if rc.Mutable {
			w.Header().Set("Etag", timeToEtag(timestamp))
		}
		if len(maxAge) > 0 {
			w.Header().Set("Cache-Control", maxAge)
		}

		mergeProperties(object)

		metaData, _ := json.Marshal(object)
		w.Header().Set("Kurbisio-Meta-Data", string(metaData))
		w.Header().Set("Content-Length", strconv.Itoa(len(blob)))
		w.WriteHeader(http.StatusOK)
		w.Write(blob)
	}

	readWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationRead, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		read(w, r, nil)
	}

	createWithAuth := func(w http.ResponseWriter, r *http.Request) {
		rlog := logger.FromContext(r.Context())
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationCreate, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		blob, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		metaDataJSON := []byte(r.Header.Get("Kurbisio-Meta-Data"))
		if len(metaDataJSON) == 0 {
			metaDataJSON = []byte("{}")
		}

		var metaJSON map[string]json.RawMessage
		err = json.Unmarshal(metaDataJSON, &metaJSON)
		if err != nil {
			http.Error(w, "invalid meta data: "+err.Error(), http.StatusBadRequest)
			return
		}

		// build insert query and validate that we have all parameters
		values := make([]interface{}, len(columns)+2)
		var i int

		primaryID := uuid.New() // create always creates a new object
		values[i] = primaryID
		i++

		for ; i < propertiesIndex; i++ { // the core identifiers, either from url or from json
			param := params[columns[i]]
			if param == "all" || len(param) == 0 {
				var id, null uuid.UUID
				if j, ok := metaJSON[columns[i]]; ok {
					err = json.Unmarshal(j, &id)
					if err != nil {
						http.Error(w, "invalid "+columns[i]+" in meta data", http.StatusBadRequest)
						return
					}
				}
				if id == null {
					http.Error(w, "missing "+columns[i], http.StatusBadRequest)
					return
				}
				values[i] = id
			} else {
				values[i] = param
			}
		}

		// the meta data
		metaDataIndex := i
		i++

		// static properties and external indices, non mandatory
		for ; i < len(columns); i++ {
			value := r.Header.Get(jsonToHeader[columns[i]])
			if j, ok := metaJSON[columns[i]]; ok {
				json.Unmarshal(j, &value)
			}
			values[i] = value
		}

		// next is the blob itself
		if rc.StoredExternally && b.KssDriver != nil {
			values[i] = &[]byte{}
		} else {
			values[i] = &blob
		}
		i++

		// last value is timestamp
		timestamp := time.Now().UTC()
		if j, ok := metaJSON["timestamp"]; ok {
			json.Unmarshal(j, &timestamp)
		}
		values[i] = &timestamp

		// prune meta data
		for i = 0; i < len(columns); i++ {
			if i == propertiesIndex {
				continue
			}
			delete(metaJSON, columns[i])
		}
		metaDataJSON, _ = json.Marshal(metaJSON)
		values[metaDataIndex] = metaDataJSON

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var id uuid.UUID
		err = tx.QueryRow(insertQuery, values...).Scan(&id)
		if err != nil {
			status := http.StatusBadRequest
			// Non unique external keys are reported as code Code 23505
			if err, ok := err.(*pq.Error); ok && err.Code == "23505" {
				status = http.StatusConflict
			}
			tx.Rollback()
			http.Error(w, "cannot create "+this+": "+err.Error(), status)
			return
		}

		// re-read meta data and return as json
		values, response := createScanValuesAndObject(&time.Time{})
		err = tx.QueryRow(readQueryMeta+"WHERE "+this+"_id = $1;", id).Scan(values...)
		if err != nil {
			tx.Rollback()
			rlog.WithError(err).Errorf("Error 5322: create blob")
			http.Error(w, "Error 5322: cannot create object", http.StatusInternalServerError)
			return
		}

		if rc.StoredExternally && b.KssDriver != nil {
			var key string
			for i := 0; i < propertiesIndex; i++ {
				key += "/" + resources[i] + "_id/" + values[propertiesIndex-i-1].(*uuid.UUID).String()
			}
			err := b.KssDriver.UploadData(key, blob)
			if err != nil {
				tx.Rollback()
				rlog.WithError(err).Errorf("Error 5321: upload externally stored data `%s`", key)
				http.Error(w, "Error 5321: cannot store data", http.StatusFailedDependency)
				return
			}
		}

		jsonData, _ := json.Marshal(response)
		err = b.commitWithNotification(r.Context(), tx, resource, core.OperationCreate, id, jsonData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write(jsonData)
	}

	upsertWithAuth := func(w http.ResponseWriter, r *http.Request) {
		rlog := logger.FromContext(r.Context())

		// silent is a low-key feature for the backup/restore tool
		var silent bool
		if s := r.URL.Query().Get("silent"); s != "" {
			silent, _ = strconv.ParseBool(s)
		}

		params := mux.Vars(r)
		authorizedForCreate := false
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationUpdate, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
			authorizedForCreate = auth.IsAuthorized(core.OperationCreate, params, rc.Permits)
		}

		blob, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		metaDataJSON := []byte(r.Header.Get("Kurbisio-Meta-Data"))
		if len(metaDataJSON) == 0 {
			metaDataJSON = []byte("{}")
		}

		var metaJSON map[string]json.RawMessage
		err = json.Unmarshal(metaDataJSON, &metaJSON)
		if err != nil {
			http.Error(w, "invalid meta data: "+err.Error(), http.StatusBadRequest)
			return
		}

		values := make([]interface{}, len(columns)+2)
		var i int

		for ; i < propertiesIndex; i++ { // the core identifiers, either from url or from json
			param := params[columns[i]]
			if param == "all" || len(param) == 0 {
				var id, null uuid.UUID
				if j, ok := metaJSON[columns[i]]; ok {
					err = json.Unmarshal(j, &id)
					if err != nil {
						http.Error(w, "invalid "+columns[i]+" in meta data", http.StatusBadRequest)
						return
					}
				}

				if id == null {
					http.Error(w, "missing "+columns[i], http.StatusBadRequest)
					return
				}
				values[i] = id
			} else {
				values[i] = param
			}
		}

		// the meta data as dynamic properties
		metaDataIndex := i
		i++

		// static properties, non mandatory
		for ; i < propertiesEndIndex; i++ {
			value := r.Header.Get(jsonToHeader[columns[i]])
			if j, ok := metaJSON[columns[i]]; ok {
				json.Unmarshal(j, &value)
			}
			values[i] = value
		}

		// external (unique) indices, mandatory
		for ; i < len(columns); i++ {
			value := r.Header.Get(jsonToHeader[columns[i]])
			if j, ok := metaJSON[columns[i]]; ok {
				json.Unmarshal(j, &value)
			}
			if len(value) == 0 {
				http.Error(w, "missing external index "+columns[i], http.StatusBadRequest)
				return
			}
			values[i] = value
		}

		// next is the blob itself
		if rc.StoredExternally && b.KssDriver != nil {
			values[i] = &[]byte{}
		} else {
			values[i] = &blob
		}
		i++

		// last value is timestamp
		timestamp := time.Now().UTC()
		if j, ok := metaJSON["timestamp"]; ok {
			json.Unmarshal(j, &timestamp)
		}
		values[i] = &timestamp

		// prune meta data
		for i = 0; i < len(columns); i++ {
			if i == propertiesIndex {
				continue
			}
			delete(metaJSON, columns[i])
		}
		metaDataJSON, _ = json.Marshal(metaJSON)
		values[metaDataIndex] = metaDataJSON

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var primaryID uuid.UUID
		query := updateQuery
		if authorizedForCreate {
			query = insertUpdateQuery
		}
		if !rc.Mutable {
			query = insertQuery
		}
		err = tx.QueryRow(query, values...).Scan(&primaryID)
		if err == sql.ErrNoRows {
			tx.Rollback()
			if authorizedForCreate {
				http.Error(w, "cannot create "+this, http.StatusUnprocessableEntity)
			} else {
				http.Error(w, "no such "+this, http.StatusNotFound)
			}
			return
		}
		log.Printf("%v\n", values)
		log.Println(query)
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// re-read meta data and return as json
		values, response := createScanValuesAndObject(&time.Time{})
		err = tx.QueryRow(readQueryMeta+"WHERE "+this+"_id = $1;", &primaryID).Scan(values...)
		if err == sql.ErrNoRows {
			tx.Rollback()
			http.Error(w, "upsert failed, no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if rc.StoredExternally && b.KssDriver != nil {
			var key string
			for i := 0; i < propertiesIndex; i++ {
				key += "/" + resources[i] + "_id/" + values[propertiesIndex-i-1].(*uuid.UUID).String()
			}
			err := b.KssDriver.UploadData(key, blob)
			if err != nil {
				tx.Rollback()
				rlog.WithError(err).Errorf("Error 5323: upload externally stored data `%s`", key)
				http.Error(w, "Error 5323: cannot store data", http.StatusFailedDependency)
				return
			}
		}

		jsonData, _ := json.Marshal(response)

		if silent {
			err = tx.Commit()
		} else {
			err = b.commitWithNotification(r.Context(), tx, resource, core.OperationUpdate, *values[0].(*uuid.UUID), jsonData)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)

	}

	clearWithAuth := func(w http.ResponseWriter, r *http.Request) {

		rlog := logger.FromContext(r.Context())
		var err error

		params := mux.Vars(r)
		selectors := map[string]string{}
		const ownerIndex = 1
		for i := ownerIndex; i < propertiesIndex; i++ { // skip ID
			selectors[columns[i]] = params[columns[i]]
		}

		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationClear, params, rc.Permits) {
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
				rlog.Errorf("parameter '%s': %s", key, err.Error())
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
				values, _ := createScanValuesAndObject(&timestamp)
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
		notificationJSON, _ := json.Marshal(parameters)
		err = b.commitWithNotification(r.Context(), tx, resource, core.OperationClear, uuid.UUID{}, notificationJSON)
		if err != nil {
			rlog.WithError(err).Errorf("Error 4770: sqlQuery `%s`", sqlQuery)
			http.Error(w, "Error 4770", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)

	}

	deleteWithAuth := func(w http.ResponseWriter, r *http.Request) {
		rlog := logger.FromContext(r.Context())
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(core.OperationDelete, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
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
		var timestamp time.Time
		values, object := createScanValuesAndObject(&timestamp)
		err = tx.QueryRow(deleteQuery+sqlWhereOne+sqlReturnMeta, queryParameters...).Scan(values...)
		if err == sql.ErrNoRows {
			tx.Rollback()
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if rc.needsKSS && b.KssDriver != nil {
			var key string
			for i := 0; i < propertiesIndex; i++ {
				key += "/" + resources[i] + "_id/" + values[propertiesIndex-i-1].(*uuid.UUID).String()
			}
			err := b.KssDriver.DeleteAllWithPrefix(key)
			if err != nil {
				rlog.WithError(err).Errorf("Could not DeleteAllWithPrefix key `%s`", key)
				return
			}
		}
		mergeProperties(object)
		primaryID := values[0].(*uuid.UUID)
		jsonData, _ := json.MarshalWithOption(object, json.DisableHTMLEscape())
		err = b.commitWithNotification(r.Context(), tx, resource, core.OperationDelete, *primaryID, jsonData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}

	// store the collection helper for later usage in relations
	b.collectionFunctions[this] = &collectionFunctions{
		list: list,
		read: read,
	}

	// CREATE
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)
		createWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPost)

	// READ
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)
		readWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodGet)

	// UPDATE / CREATE with in in meta data
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)
		upsertWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut)

	// LIST
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)
		listWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodGet)

	// DELETE
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)
		deleteWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

	// CLEAR
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)
		clearWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

	// UPDATE / CREATE with fully qualified path
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Debugln("called route for", r.URL, r.Method)
		upsertWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut)
}

// ifNoneMatchFound returns true if etag is found in ifNoneMatch. The format of ifNoneMatch is one
// of the following:
// If-None-Match: "<etag_value>"
// If-None-Match: "<etag_value>", "<etag_value>", …
// If-None-Match: *
func ifNoneMatchFound(ifNoneMatch, etag string) bool {
	ifNoneMatch = strings.Trim(ifNoneMatch, " ")
	if len(ifNoneMatch) == 0 {
		return false
	}
	if ifNoneMatch == "*" {
		return true
	}
	for _, s := range strings.Split(ifNoneMatch, ",") {
		s = strings.Trim(s, " \"")
		t := strings.Trim(etag, " \"")
		if s == t {
			return true
		}
	}
	return false
}
