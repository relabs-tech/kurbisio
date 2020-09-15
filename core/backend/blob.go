package backend

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"

	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core"
	"github.com/relabs-tech/backends/core/access"
	"github.com/relabs-tech/backends/core/logger"
)

func (b *Backend) createBlobResource(router *mux.Router, rc blobConfiguration) {
	schema := b.db.Schema
	resource := rc.Resource
	rlog := logger.Default()
	rlog.Debugln("create blob:", resource)
	if rc.Description != "" {
		rlog.Debugln("  description:", rc.Description)
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

	for i := range dependencies {
		that := dependencies[i]
		createColumn := fmt.Sprintf("%s_id uuid NOT NULL", that)
		createColumns = append(createColumns, createColumn)
		columns = append(columns, that+"_id")
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

	// static properties are varchars
	for _, property := range rc.StaticProperties {
		createColumn := fmt.Sprintf("\"%s\" varchar NOT NULL DEFAULT ''", property)
		createColumns = append(createColumns, createColumn)
		columns = append(columns, property)
		jsonToHeader[property] = core.PropertyNameToCanonicalHeader(property)
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
		jsonToHeader[property] = core.PropertyNameToCanonicalHeader(property)
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
		jsonToHeader[name] = core.PropertyNameToCanonicalHeader(name)
	}

	// the actual blob data as bytea
	createColumn := "blob bytea NOT NULL"
	createColumns = append(createColumns, createColumn)

	createQuery += "(" + strings.Join(createColumns, ", ") + ");" + createIndicesQuery

	var err error
	if b.updateSchema {
		_, err = b.db.Query(createQuery)
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

	rlog.Debugln("  handle blob routes:", listRoute, "GET,POST,DELETE")
	rlog.Debugln("  handle blob routes:", itemRoute, "GET,PUT, DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", timestamp, blob FROM %s.\"%s\" ", schema, resource)
	readQueryMeta := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", timestamp FROM %s.\"%s\" ", schema, resource)
	sqlWhereOne := "WHERE " + compareIDsString(columns[:propertiesIndex])
	sqlReturnID := " RETURNING " + this + "_id;"

	readQueryWithTotal := "SELECT " + strings.Join(columns, ", ") +
		fmt.Sprintf(", timestamp, count(*) OVER() AS full_count FROM %s.\"%s\" ", schema, resource)
	sqlWhereAll := "WHERE "
	if propertiesIndex > 1 {
		sqlWhereAll += compareIDsString(columns[1:propertiesIndex]) + " AND "
	}
	sqlWhereAll += fmt.Sprintf("($%d OR timestamp<=$%d) AND ($%d OR timestamp>=$%d) ",
		propertiesIndex, propertiesIndex+1, propertiesIndex+2, propertiesIndex+3)

	sqlPagination := fmt.Sprintf("ORDER BY timestamp DESC, %s  DESC LIMIT $%d OFFSET $%d;", columns[0], propertiesIndex+4, propertiesIndex+5)

	sqlWhereAllPlusOneExternalIndex := sqlWhereAll + fmt.Sprintf("AND %%s = $%d ", propertiesIndex+6)

	clearQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" WHERE ", schema, resource) + compareIDsString(columns[1:propertiesIndex]) + ";"
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
	insertUpdateQuery += ", timestamp = $" + strconv.Itoa(len(columns)+2) + " " + sqlWhereOne + " RETURNING " + this + "_id;"

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
		)

		urlQuery := r.URL.Query()
		for key, array := range urlQuery {
			var err error
			if len(array) > 1 {
				http.Error(w, "illegal paramter array '"+key+"'", http.StatusBadRequest)
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
						externalIndex = filterValue
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
		}
		params := mux.Vars(r)
		if externalIndex == "" { // get entire collection
			sqlQuery = readQueryWithTotal + sqlWhereAll
			queryParameters = make([]interface{}, propertiesIndex-1+6)
			for i := 1; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-1] = params[columns[i]]
			}
		} else {
			sqlQuery = fmt.Sprintf(readQueryWithTotal+sqlWhereAllPlusOneExternalIndex, externalColumn)
			queryParameters = make([]interface{}, propertiesIndex-1+6+1)
			for i := 1; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-1] = params[columns[i]]
			}
			queryParameters[propertiesIndex-1+6] = externalIndex
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

		sqlQuery += sqlPagination

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
			var timestamp time.Time
			values, object := createScanValuesAndObject(&timestamp, &totalCount)
			err := rows.Scan(values...)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			mergeProperties(object)

			// if we did not have from, take it from the first object
			if from.IsZero() {
				from = timestamp
			}
			response = append(response, object)
		}

		jsonData, _ := json.Marshal(response)
		etag := bytesPlusTotalCountToEtag(jsonData, totalCount)
		// ETag must also be provided in headers in case If-None-Match is set
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
				values, response := createScanValuesAndObject(&timestamp)
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
						w.Header().Set(jsonToHeader[k], *response[k].(*string))
					}
					w.Header().Set("Etag", timeToEtag(timestamp))
					if len(maxAge) > 0 {
						w.Header().Set("Cache-Control", maxAge)
					}
					metaData, _ := json.Marshal(response)
					w.Header().Set("Kurbisio-Meta-Data", string(metaData))
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}

		var blob []byte
		var timestamp time.Time
		values, response := createScanValuesAndObject(&timestamp, &blob)

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
		for i := propertiesIndex + 1; i < len(columns); i++ {
			k := columns[i]
			w.Header().Set(jsonToHeader[k], *response[k].(*string))
		}
		if rc.Mutable {
			w.Header().Set("Etag", timeToEtag(timestamp))
		}
		if len(maxAge) > 0 {
			w.Header().Set("Cache-Control", maxAge)
		}

		mergeProperties(response)

		metaData, _ := json.Marshal(response)
		w.Header().Set("Kurbisio-Meta-Data", string(metaData))
		w.Header().Set("Content-Length", strconv.Itoa(len(blob)))
		w.WriteHeader(http.StatusOK)
		w.Write(blob)
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

	createWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationCreate, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		blob, err := ioutil.ReadAll(r.Body)
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

		values[i] = uuid.New() // create always creates a new object
		i++

		for ; i < propertiesIndex; i++ { // the core identifiers, either from url or from json
			param, _ := params[columns[i]]
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
		values[i] = &blob
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
				status = http.StatusUnprocessableEntity
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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
		// low-key feature for the backup/restore tool
		var silent bool
		if s := r.URL.Query().Get("silent"); s != "" {
			silent, _ = strconv.ParseBool(s)
		}

		params := mux.Vars(r)
		authorizedForCreate := false
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationUpdate, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
			authorizedForCreate = auth.IsAuthorized(resources, core.OperationCreate, params, rc.Permits)
		}

		blob, err := ioutil.ReadAll(r.Body)
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
			param, _ := params[columns[i]]
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
		values[i] = &blob
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

		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationClear, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		urlQuery := r.URL.Query()
		if len(urlQuery) > 0 {
			http.Error(w, "clear does not take any parameters", http.StatusBadRequest)
			return
		}

		queryParameters := make([]interface{}, propertiesIndex-1)
		for i := 1; i < propertiesIndex; i++ { // skip ID
			queryParameters[i-1] = params[columns[i]]
		}

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, err = tx.Query(clearQuery, queryParameters...)
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = b.commitWithNotification(r.Context(), tx, resource, core.OperationClear, uuid.UUID{}, []byte(""))
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

		queryParameters := make([]interface{}, propertiesIndex)
		for i := 0; i < propertiesIndex; i++ {
			queryParameters[i] = params[columns[i]]
		}

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var id uuid.UUID
		err = tx.QueryRow(deleteQuery+sqlWhereOne+sqlReturnID, queryParameters...).Scan(&id)
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

		notification := make(map[string]interface{})
		for i := 0; i < propertiesIndex; i++ {
			notification[columns[i]] = params[columns[i]]
		}
		jsonData, _ := json.Marshal(notification)
		err = b.commitWithNotification(r.Context(), tx, resource, core.OperationDelete, id, jsonData)
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
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		createWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPost)

	// READ
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		readWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodGet)

	// UPDATE / CREATE with in in meta data
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
		upsertWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut)

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

	// UPDATE / CREATE with fully qualified path
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		logger.FromContext(r.Context()).Infoln("called route for", r.URL, r.Method)
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
