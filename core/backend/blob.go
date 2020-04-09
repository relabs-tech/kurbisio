package backend

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"

	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core"
	"github.com/relabs-tech/backends/core/access"
	"github.com/relabs-tech/backends/core/sql"
)

func (b *Backend) createBlobResource(router *mux.Router, rc blobConfiguration) {
	schema := b.db.Schema
	resource := rc.Resource
	log.Println("create blob:", resource)

	resources := strings.Split(rc.Resource, "/")
	this := resources[len(resources)-1]
	dependencies := resources[:len(resources)-1]

	createQuery := fmt.Sprintf("CREATE table IF NOT EXISTS %s.\"%s\"", schema, resource)
	createColumns := []string{
		this + "_id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY",
		"created_at timestamp NOT NULL DEFAULT now()",
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
	uuidStr := this + "_id"
	createIndicesQuery := fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s\"(created_at, %s);",
		"sort_index_"+this+"_created_at",
		schema, resource, uuidStr)
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

	collectionRoute := ""
	itemRoute := ""
	for _, r := range resources {
		collectionRoute = itemRoute + "/" + core.Plural(r)
		itemRoute = itemRoute + "/" + core.Plural(r) + "/{" + r + "_id}"
	}

	log.Println("  handle blob routes:", collectionRoute, "GET,POST")
	log.Println("  handle blob routes:", itemRoute, "GET,PUT, DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", blob, created_at FROM %s.\"%s\" ", schema, resource)
	readQueryMetaDataOnly := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", created_at FROM %s.\"%s\" ", schema, resource)
	sqlWhereOne := "WHERE " + compareIDsString(columns[:propertiesIndex]) + ";"

	readQueryWithTotal := "SELECT " + strings.Join(columns, ", ") +
		fmt.Sprintf(", created_at, count(*) OVER() AS full_count FROM %s.\"%s\" ", schema, resource)
	sqlWhereAll := "WHERE "
	if propertiesIndex > 1 {
		sqlWhereAll += compareIDsString(columns[1:propertiesIndex]) + " AND "
	}
	sqlWhereAll += fmt.Sprintf("($%d OR created_at<=$%d) AND ($%d OR created_at>=$%d) ",
		propertiesIndex, propertiesIndex+1, propertiesIndex+2, propertiesIndex+3)

	sqlPagination := fmt.Sprintf("ORDER BY (created_at, %s)  DESC LIMIT $%d OFFSET $%d;", uuidStr, propertiesIndex+4, propertiesIndex+5)

	sqlWhereAllPlusOneExternalIndex := sqlWhereAll + fmt.Sprintf("AND %%s = $%d ", propertiesIndex+6)

	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource) + "(" + strings.Join(columns, ", ") + ", blob, created_at)"
	insertQuery += "VALUES(" + parameterString(len(columns)+2) + ") RETURNING " + this + "_id;"

	updateQuery := fmt.Sprintf("UPDATE %s.\"%s\" SET ", schema, resource)
	sets := make([]string, len(columns)-propertiesIndex)
	for i := propertiesIndex; i < len(columns); i++ {
		sets[i-propertiesIndex] = columns[i] + " = $" + strconv.Itoa(i+1)
	}
	updateQuery += strings.Join(sets, ", ") + ", blob = $" + strconv.Itoa(len(columns)+1) + " " + sqlWhereOne

	maxAge := ""
	if !rc.Mutable {
		rc.MaxAgeCache = 31536000
	}
	if rc.MaxAgeCache > 0 {
		maxAge = fmt.Sprintf("max-age=%d", rc.MaxAgeCache)
	}

	createScanValuesAndObject := func(createdAt *time.Time) ([]interface{}, map[string]interface{}) {
		values := make([]interface{}, len(columns)+1)
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
		return values, object
	}

	createScanValuesAndObjectWithBlob := func(blob *[]byte, createdAt *time.Time) ([]interface{}, map[string]interface{}) {
		values := make([]interface{}, len(columns)+2)
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
		values[i] = blob
		i++
		values[i] = createdAt
		object["created_at"] = createdAt
		return values, object
	}

	createScanValuesAndObjectForCollection := func(totalCount *int) ([]interface{}, map[string]interface{}) {
		values := make([]interface{}, len(columns)+2)
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
		createdAt := &time.Time{}
		values[i] = createdAt
		object["created_at"] = createdAt
		i++
		values[i] = totalCount
		return values, object
	}

	getCollection := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
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
			default:
				found := false
				for i := searchablePropertiesIndex; i < len(columns); i++ {
					if key == columns[i] {
						if found {
							err = fmt.Errorf("only one searchable property or external index allowed")
							break
						}
						externalIndex = value
						externalColumn = columns[i]
						found = true
					}
				}
				if !found {
					err = fmt.Errorf("unknown query parameter")
				}
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
			values, object := createScanValuesAndObjectForCollection(&totalCount)
			err := rows.Scan(values...)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			// if we did not have from, take it from the first object
			if from.IsZero() {
				from = *object["created_at"].(*time.Time)
			}
			response = append(response, object)
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		etag := bytesToEtag(jsonData)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Etag", etag)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Pagination-Limit", strconv.Itoa(limit))
		w.Header().Set("Pagination-Total-Count", strconv.Itoa(totalCount))
		w.Header().Set("Pagination-Page-Count", strconv.Itoa((totalCount/limit)+1))
		w.Header().Set("Pagination-Current-Page", strconv.Itoa(page))
		if !from.IsZero() {
			w.Header().Set("Pagination-Until", from.Format(time.RFC3339))
		}
		w.Write(jsonData)

	}

	getCollectionWithAuth := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationList, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		getCollection(w, r, nil)
	}

	getItem := func(w http.ResponseWriter, r *http.Request) {
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
				var createdAt time.Time
				values, _ := createScanValuesAndObject(&createdAt)
				err = b.db.QueryRow(readQueryMetaDataOnly+sqlWhereOne, queryParameters...).Scan(values...)
				if err == sql.ErrNoRows {
					http.Error(w, "no such "+this, http.StatusNotFound)
					return
				}
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				if timeToEtag(createdAt) == ifNoneMatch {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}

		var blob []byte
		var createdAt time.Time
		values, response := createScanValuesAndObjectWithBlob(&blob, &createdAt)

		err = b.db.QueryRow(readQuery+sqlWhereOne, queryParameters...).Scan(values...)
		if err == sql.ErrNoRows {
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for i := propertiesIndex + 1; i < len(columns); i++ {
			k := columns[i]
			w.Header().Set(jsonToHeader[k], *response[k].(*string))
		}
		if rc.Mutable {
			w.Header().Set("Etag", timeToEtag(createdAt))
		}
		if len(maxAge) > 0 {
			w.Header().Set("Cache-Control", maxAge)
		}
		w.Header().Set("Kurbisio-Meta-Data", string(*response["properties"].(*json.RawMessage)))
		w.Header().Set("Content-Length", strconv.Itoa(len(blob)))
		w.WriteHeader(http.StatusOK)
		w.Write(blob)
	}

	getItemWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationRead, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		getItem(w, r)
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

		metaData := []byte(r.Header.Get("Kurbisio-Meta-Data"))
		if len(metaData) == 0 {
			metaData = []byte("{}")
		} else {
			var metaJSON json.RawMessage
			err = json.Unmarshal(metaData, &metaJSON)
			if err != nil {
				http.Error(w, "invalid meta data: "+err.Error(), http.StatusBadRequest)
				return
			}
		}

		// build insert query and validate that we have all parameters
		values := make([]interface{}, len(columns)+2)
		values[0] = uuid.New()
		var i int

		for i = 1; i < propertiesIndex; i++ { // the core identifiers
			param, _ := params[columns[i]]
			values[i] = param
		}
		// the dynamic properties
		values[i] = metaData
		i++

		// static properties, non mandatory
		for ; i < propertiesEndIndex; i++ {
			values[i] = r.Header.Get(jsonToHeader[columns[i]])
		}

		// external (unique) indices, mandatory
		for ; i < len(columns); i++ {
			value := r.Header.Get(jsonToHeader[columns[i]])
			if len(value) == 0 {
				http.Error(w, "missing external index "+columns[i], http.StatusBadRequest)
				return
			}
			values[i] = value
		}

		// next is the blob itself
		values[i] = &blob
		i++

		// last value is created_at
		createdAt := time.Now().UTC()
		values[i] = &createdAt

		var id uuid.UUID
		err = b.db.QueryRow(insertQuery, values...).Scan(&id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// re-read meta data and return as json
		values, response := createScanValuesAndObject(&time.Time{})
		err = b.db.QueryRow(readQueryMetaDataOnly+"WHERE "+this+"_id = $1;", id).Scan(values...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		b.notify(resource, core.OperationCreate, "", jsonData)

		w.WriteHeader(http.StatusCreated)
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)
	}

	updateWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationDelete, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		blob, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		metaData := []byte(r.Header.Get("Kurbisio-Meta-Data"))
		if len(metaData) == 0 {
			metaData = []byte("{}")
		} else {
			var metaJSON json.RawMessage
			err = json.Unmarshal(metaData, &metaJSON)
			if err != nil {
				http.Error(w, "invalid meta data: "+err.Error(), http.StatusBadRequest)
				return
			}
		}

		values := make([]interface{}, len(columns)+1)
		var i int

		// first add the values for the where-query
		for i = 0; i < propertiesIndex; i++ {
			values[i] = params[columns[i]]
		}

		// the meta data as dynamic properties
		values[i] = metaData
		i++

		// build the update set
		for ; i < len(columns); i++ {
			k := columns[i]
			values[i] = r.Header.Get(jsonToHeader[k])
		}

		// next is the blob itself
		values[i] = &blob
		i++

		res, err := b.db.Exec(updateQuery, values...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		count, err := res.RowsAffected()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if count != 1 {
			http.Error(w, "no such "+this, http.StatusBadRequest)
			return
		}

		// re-read meta data and return as json
		values, response := createScanValuesAndObject(&time.Time{})
		err = b.db.QueryRow(readQueryMetaDataOnly+"WHERE "+this+"_id = $1;", params[columns[0]]).Scan(values...)
		if err == sql.ErrNoRows {
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		b.notify(resource, core.OperationUpdate, "", jsonData)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)

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

		res, err := b.db.Exec(deleteQuery+sqlWhereOne, queryParameters...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		count, err := res.RowsAffected()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if count == 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		notification := make(map[string]interface{})
		for i := 0; i < propertiesIndex; i++ {
			notification[columns[i]] = params[columns[i]]
		}
		jsonData, _ := json.MarshalIndent(notification, "", " ")
		b.notify(resource, core.OperationDelete, "", jsonData)

		w.WriteHeader(http.StatusNoContent)
	}

	collection := collectionHelper{
		getCollection: getCollection,
		getItem:       getItem,
	}

	// store the collection helper for later usage in relations
	b.collectionHelper[this] = &collection

	// CREATE
	router.HandleFunc(collectionRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		createWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPost)

	// READ
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		getItemWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodGet)

	// READ ALL
	router.HandleFunc(collectionRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		getCollectionWithAuth(w, r, nil)
	}).Methods(http.MethodOptions, http.MethodGet)

	// DELETE
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		deleteWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

	// UPDATE
	if rc.Mutable {
		router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
			log.Println("called route for", r.URL, r.Method)
			updateWithAuth(w, r)
		}).Methods(http.MethodOptions, http.MethodPut)
	}

}
