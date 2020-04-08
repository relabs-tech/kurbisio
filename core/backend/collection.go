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

func (b *Backend) createCollectionResource(router *mux.Router, rc collectionConfiguration, singleton bool) {
	schema := b.db.Schema
	resource := rc.Resource

	if singleton {
		log.Println("create singleton collection:", resource)
	} else {
		log.Println("create collection:", resource)
	}

	resources := strings.Split(rc.Resource, "/")
	this := resources[len(resources)-1]
	owner := ""
	if singleton {
		if len(resource) < 2 {
			panic(fmt.Errorf("singleton resource %s lacks owner", this))
		}
		owner = resources[len(resources)-2]
	}
	dependencies := resources[:len(resources)-1]

	createQuery := fmt.Sprintf("CREATE table IF NOT EXISTS %s.\"%s\"", schema, resource)
	createColumns := []string{
		this + "_id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY",
		"created_at timestamp NOT NULL DEFAULT now()",
		"state VARCHAR NOT NULL DEFAULT ''",
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

	if singleton {
		// force the resource itself to be singleton resource
		createColumn := "UNIQUE (" + owner + "_id )"
		createColumns = append(createColumns, createColumn)
	}

	createColumns = append(createColumns, "properties json NOT NULL DEFAULT '{}'::jsonb")
	// query to create all indices after the table creation
	createIndicesQuery := fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s\"(created_at);",
		"sort_index_"+this+"_created_at",
		schema, resource)
	propertiesIndex := len(columns) // where properties start
	columns = append(columns, "properties")

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
			panic(err)
		}
	}

	singletonRoute := ""
	collectionRoute := ""
	oneRoute := ""
	for _, r := range resources {
		singletonRoute = oneRoute + "/" + r
		collectionRoute = oneRoute + "/" + plural(r)
		oneRoute = oneRoute + "/" + plural(r) + "/{" + r + "_id}"
	}

	if singleton {
		log.Println("  handle singleton routes:", singletonRoute, "GET,PUT,PATCH,DELETE")
	}
	log.Println("  handle collection routes:", collectionRoute, "GET,POST,PUT,PATCH")
	log.Println("  handle collection routes:", oneRoute, "GET,DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", created_at, state FROM %s.\"%s\" ", schema, resource)
	sqlWhereOne := "WHERE " + compareIDsString(columns[:propertiesIndex])

	readQueryWithTotal := "SELECT " + strings.Join(columns, ", ") +
		fmt.Sprintf(", created_at, state, count(*) OVER() AS full_count FROM %s.\"%s\" ", schema, resource)
	sqlWhereAll := "WHERE "
	if propertiesIndex > 1 {
		sqlWhereAll += compareIDsString(columns[1:propertiesIndex]) + " AND "
	}
	sqlWhereAll += fmt.Sprintf("($%d OR created_at<=$%d) AND ($%d OR created_at>=$%d) AND state=$%d ",
		propertiesIndex, propertiesIndex+1, propertiesIndex+2, propertiesIndex+3, propertiesIndex+4)

	sqlPagination := fmt.Sprintf("ORDER BY created_at DESC LIMIT $%d OFFSET $%d;", propertiesIndex+5, propertiesIndex+6)

	sqlWhereAllPlusOneExternalIndex := sqlWhereAll + fmt.Sprintf("AND %%s = $%d ", propertiesIndex+7)

	sqlWhereSingle := ""
	if singleton {
		if propertiesIndex > 1 {
			sqlWhereSingle += "WHERE " + compareIDsString(columns[1:propertiesIndex])
		}
	}

	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)
	sqlReturnState := " RETURNING state;"

	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource) + "(" + strings.Join(columns, ", ") + ", created_at, state)"
	insertQuery += "VALUES(" + parameterString(len(columns)+2) + ") RETURNING " + this + "_id;"

	updateQuery := fmt.Sprintf("UPDATE %s.\"%s\" SET ", schema, resource)
	sets := make([]string, len(columns)-propertiesIndex)
	for i := propertiesIndex; i < len(columns); i++ {
		sets[i-propertiesIndex] = columns[i] + " = $" + strconv.Itoa(i+1)
	}
	updateQuery += strings.Join(sets, ", ") + ", created_at = $" + strconv.Itoa(len(columns)+1) + ", state = $" + strconv.Itoa(len(columns)+2) + " " + sqlWhereOne

	insertUpdateQuery := ""
	if singleton {
		insertUpdateQuery += fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource)
		insertUpdateQuery += "(" + strings.Join(columns, ", ") + ", created_at, state)"
		insertUpdateQuery += " VALUES(" + parameterString(len(columns)+2) + ") ON CONFLICT (" + owner + "_id) DO UPDATE SET "
		sets := make([]string, len(columns)-propertiesIndex)
		for i := propertiesIndex; i < len(columns); i++ {
			sets[i-propertiesIndex] = columns[i] + " = $" + strconv.Itoa(i+1)
		}
		insertUpdateQuery += strings.Join(sets, ", ") + ", created_at = $" + strconv.Itoa(len(columns)+1) + ", state = $" + strconv.Itoa(len(columns)+2)
		insertUpdateQuery += " RETURNING (xmax = 0) AS inserted, " + this + "_id;" // return whether we did insert or update, this is a psql trick
	}

	createScanValuesAndObject := func() ([]interface{}, map[string]interface{}) {
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
		var state string
		values[i] = &state
		object["state"] = &state
		return values, object
	}

	createScanValuesAndObjectForCollection := func(totalCount *int) ([]interface{}, map[string]interface{}) {
		values := make([]interface{}, len(columns)+3)
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
		var state string
		values[i] = &state
		object["state"] = &state
		i++
		values[i] = totalCount
		return values, object
	}

	getAll := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
		var (
			queryParameters []interface{}
			sqlQuery        string
			limit           int = 100
			page            int = 1
			until           time.Time
			from            time.Time
			state           string
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

			case "state":
				state = value

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
			queryParameters = make([]interface{}, propertiesIndex-1+7)
			for i := 1; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-1] = params[columns[i]]
			}
		} else {
			sqlQuery = fmt.Sprintf(readQueryWithTotal+sqlWhereAllPlusOneExternalIndex, externalColumn)
			queryParameters = make([]interface{}, propertiesIndex-1+7+1)
			for i := 1; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-1] = params[columns[i]]
			}
			queryParameters[propertiesIndex-1+7] = externalIndex
		}

		// add before and after and pagination
		queryParameters[propertiesIndex-1+0] = until.IsZero()
		queryParameters[propertiesIndex-1+1] = until.UTC()
		queryParameters[propertiesIndex-1+2] = from.IsZero()
		queryParameters[propertiesIndex-1+3] = from.UTC()
		queryParameters[propertiesIndex-1+4] = state
		queryParameters[propertiesIndex-1+5] = limit
		queryParameters[propertiesIndex-1+6] = (page - 1) * limit

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
			// hide "state" from response unless requested
			if len(state) == 0 {
				delete(object, "state")
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
		w.Write(jsonData)

	}

	getAllWithAuth := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationList, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		getAll(w, r, nil)
	}

	getOne := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		queryParameters := make([]interface{}, propertiesIndex)
		for i := 0; i < propertiesIndex; i++ {
			queryParameters[i] = params[columns[i]]
		}

		if queryParameters[0].(string) == "all" {
			http.Error(w, "all is not a valid "+this, http.StatusBadRequest)
			return
		}

		values, response := createScanValuesAndObject()
		err = b.db.QueryRow(readQuery+sqlWhereOne+";", queryParameters...).Scan(values...)
		if err == sql.ErrNoRows {
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		urlQuery := r.URL.Query()
		for key, array := range urlQuery {
			switch key {
			case "children":
				status, err := b.addChildrenToGetResponse(array, r, response)
				if err != nil {
					http.Error(w, err.Error(), status)
					return
				}
			default:
				http.Error(w, "parameter '"+key+"': unknown query parameter", http.StatusBadRequest)
			}
		}

		// hide "state" from response unless defined
		state := *response["state"].(*string)
		if len(state) == 0 {
			delete(response, "state")
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		etag := bytesToEtag(jsonData)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Etag", etag)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	}

	getOneWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationRead, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		getOne(w, r)
	}

	updateWithAuth := func(w http.ResponseWriter, r *http.Request) {
		body, _ := ioutil.ReadAll(r.Body)
		params := mux.Vars(r)

		var bodyJSON map[string]interface{}
		err := json.Unmarshal(body, &bodyJSON)
		if err != nil {
			http.Error(w, "invalid json data: "+err.Error(), http.StatusBadRequest)
			return
		}

		values := make([]interface{}, len(columns)+2)

		// primary id can come from parameter (fully qualified put) or from body json (collection put)
		if len(params[columns[0]]) == 0 {
			primaryID, ok := bodyJSON[columns[0]].(string)
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

		var i int

		// add and validate core identifiers
		for i = 0; i < propertiesIndex; i++ {
			key := columns[i]
			param := params[key]
			values[i] = param
			value, ok := bodyJSON[key]
			if ok && param != "all" && param != value.(string) {
				http.Error(w, "illegal "+key, http.StatusBadRequest)
				return
			}
		}

		// build the update set
		properties, ok := bodyJSON["properties"]
		if ok {
			propertiesJSON, _ := json.Marshal(properties)
			values[i] = propertiesJSON
		} else {
			values[i] = []byte("{}")
		}
		i++

		for ; i < len(columns); i++ {
			value, ok := bodyJSON[columns[i]]
			if !ok {
				http.Error(w, "missing property or index"+columns[i], http.StatusBadRequest)
				return
			}
			values[i] = value
		}

		// next value is created_at
		createdAt := time.Now().UTC()
		if value, ok := bodyJSON["created_at"]; ok {
			timestamp, ok := value.(string)
			if !ok {
				http.Error(w, "illegal created_at", http.StatusBadRequest)
				return
			}
			t, err := time.Parse(time.RFC3339, timestamp)
			if err != nil {
				http.Error(w, "illegal created_at: "+err.Error(), http.StatusBadRequest)
				return
			}
			createdAt = t.UTC()
		}
		values[i] = &createdAt
		i++

		// last value is state
		var state string
		if value, ok := bodyJSON["state"]; ok {
			state, ok = value.(string)
			if !ok {
				http.Error(w, "state must be a string", http.StatusBadRequest)
				return
			}
		}
		values[i] = &state
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

		// re-read new values
		queryParameters := make([]interface{}, propertiesIndex)
		for i := 0; i < propertiesIndex; i++ {
			queryParameters[i] = params[columns[i]]
		}
		values, response := createScanValuesAndObject()
		err = b.db.QueryRow(readQuery+sqlWhereOne+";", queryParameters...).Scan(values...)
		if err == sql.ErrNoRows {
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// hide "state" from response unless defined
		state = *response["state"].(*string)
		if len(state) == 0 {
			delete(response, "state")
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		b.notify(resource, core.OperationUpdate, state, jsonData)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	}

	doDeleteWithAuth := func(w http.ResponseWriter, r *http.Request) {
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

		var state string

		err = b.db.QueryRow(deleteQuery+sqlWhereOne+sqlReturnState, queryParameters...).Scan(&state)
		if err == sql.ErrNoRows {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		notification := make(map[string]interface{})
		for i := 0; i < propertiesIndex; i++ {
			notification[columns[i]] = params[columns[i]]
		}
		jsonData, _ := json.MarshalIndent(notification, state, " ")
		b.notify(resource, core.OperationDelete, state, jsonData)

		w.WriteHeader(http.StatusNoContent)

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
		body, _ := ioutil.ReadAll(r.Body)
		var bodyJSON map[string]interface{}
		err := json.Unmarshal(body, &bodyJSON)
		if err != nil {
			http.Error(w, "invalid json data: "+err.Error(), http.StatusBadRequest)
			return
		}

		// build insert query and validate that we have all parameters
		values := make([]interface{}, len(columns)+2)

		// the primary resource identifier, use as specified or create a new one
		primaryID, ok := bodyJSON[columns[0]]
		if !ok || primaryID == "00000000-0000-0000-0000-000000000000" {
			primaryID = uuid.New()
		}
		values[0] = primaryID
		var i int
		for i = 1; i < propertiesIndex; i++ { // the core identifiers
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
			if param == "all" && ok && value != "00000000-0000-0000-0000-000000000000" {
				values[i] = value
			} else {
				values[i] = param
			}
		}

		// the dynamic properties
		properties, ok := bodyJSON[columns[i]]
		if ok {
			propertiesJSON, _ := json.Marshal(properties)
			values[i] = propertiesJSON
		} else {
			values[i] = []byte("{}")
		}
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
			if !ok {
				http.Error(w, "illegal created_at", http.StatusBadRequest)
				return
			}
			if value != nil {
				timestamp, _ := value.(string)
				t, err := time.Parse(time.RFC3339, timestamp)
				if err != nil {
					http.Error(w, "illegal created_at: "+err.Error(), http.StatusBadRequest)
					return
				}
				createdAt = t.UTC()
			}
		}
		values[i] = &createdAt
		i++

		// last value is state
		var state string
		if value, ok := bodyJSON["state"]; ok {
			state, ok = value.(string)
			if !ok {
				http.Error(w, "state must be a string", http.StatusBadRequest)
				return
			}
		}
		values[i] = &state
		i++

		var id uuid.UUID
		err = b.db.QueryRow(insertQuery, values...).Scan(&id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// re-read data and return as json
		values, response := createScanValuesAndObject()
		err = b.db.QueryRow(readQuery+"WHERE "+this+"_id = $1;", id).Scan(values...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// hide "state" from response unless defined
		state = *response["state"].(*string)
		if len(state) == 0 {
			delete(response, "state")
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		b.notify(resource, core.OperationCreate, state, jsonData)

		w.WriteHeader(http.StatusCreated)
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)

	}

	collection := collectionHelper{
		getAll: getAll,
		getOne: getOne,
	}

	// store the collection helper for later usage in relations
	b.collectionHelper[this] = &collection

	// CREATE
	router.HandleFunc(collectionRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		createWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPost)

	// UPDATE
	router.HandleFunc(collectionRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		updateWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut)

	// UPDATE with fully qualified path
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		updateWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut)

	// PATCH (READ + UPDATE)
	b.createPatchRoute(router, oneRoute)

	// READ
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		getOneWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodGet)

	// READ ALL
	router.HandleFunc(collectionRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		getAllWithAuth(w, r, nil)
	}).Methods(http.MethodOptions, http.MethodGet)

	// DELETE
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		doDeleteWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

	if !singleton {
		return
	}

	insertUpdateSingletonWithAuth := func(w http.ResponseWriter, r *http.Request) {
		body, _ := ioutil.ReadAll(r.Body)
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationUpdate, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		var bodyJSON map[string]interface{}
		err := json.Unmarshal(body, &bodyJSON)
		if err != nil {
			http.Error(w, "invalid json data: "+err.Error(), http.StatusBadRequest)
			return
		}

		// build insert query and validate that we have all parameters
		values := make([]interface{}, len(columns)+2)
		// the primary resource identifier, use as specified or create a new one
		primaryID, ok := bodyJSON[columns[0]]
		if !ok || primaryID == "00000000-0000-0000-0000-000000000000" {
			primaryID = uuid.New()
		}
		values[0] = primaryID
		var i int

		// add and validate core identifiers
		for i = 1; i < propertiesIndex; i++ {
			key := columns[i]
			param := params[key]
			values[i] = param
			value, ok := bodyJSON[key]
			// zero uuid counts as no uuid for creation
			ok = ok && value != "00000000-0000-0000-0000-000000000000"

			if ok && param != "all" && param != value.(string) {
				http.Error(w, "illegal "+key, http.StatusBadRequest)
				return
			}
		}

		// build the update set
		properties, ok := bodyJSON["properties"]
		if ok {
			propertiesJSON, _ := json.Marshal(properties)
			values[i] = propertiesJSON
		} else {
			values[i] = []byte("{}")
		}
		sets[i-propertiesIndex] = "properties = $" + strconv.Itoa(i+1)
		i++

		for ; i < len(columns); i++ {
			k := columns[i]
			value, ok := bodyJSON[k]
			if !ok {
				http.Error(w, "missing property or index"+k, http.StatusBadRequest)
				return
			}
			values[i] = value
			sets[i-propertiesIndex] = k + " = $" + strconv.Itoa(i+1)
		}

		// next value is created_at
		createdAt := time.Now().UTC()
		if value, ok := bodyJSON["created_at"]; ok {
			timestamp, ok := value.(string)
			if !ok {
				http.Error(w, "illegal created_at", http.StatusBadRequest)
				return
			}
			t, err := time.Parse(time.RFC3339, timestamp)
			if err != nil {
				http.Error(w, "illegal created_at: "+err.Error(), http.StatusBadRequest)
				return
			}
			createdAt = t.UTC()
		}
		values[i] = &createdAt
		i++

		// last value is state
		var state string
		if value, ok := bodyJSON["state"]; ok {
			state, ok = value.(string)
			if !ok {
				http.Error(w, "state must be a string", http.StatusBadRequest)
				return
			}
		}
		values[i] = &state
		i++

		var inserted bool
		var id uuid.UUID
		err = b.db.QueryRow(insertUpdateQuery, values...).Scan(&inserted, &id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// re-read data and return as json
		values, response := createScanValuesAndObject()
		err = b.db.QueryRow(readQuery+"WHERE "+this+"_id = $1;", &id).Scan(values...)
		if err == sql.ErrNoRows {
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// hide "state" from response unless defined
		state = *response["state"].(*string)
		if len(state) == 0 {
			delete(response, "state")
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		if inserted {
			b.notify(resource, core.OperationCreate, state, jsonData)
		} else {
			b.notify(resource, core.OperationUpdate, state, jsonData)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)
	}

	getOneSingletonWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)

		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationRead, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		sqlQuery := readQuery + sqlWhereSingle + ";"
		queryParameters := make([]interface{}, propertiesIndex-1)
		for i := 1; i < propertiesIndex; i++ { // skip ID
			queryParameters[i-1] = params[columns[i]]
		}

		if propertiesIndex > 1 && queryParameters[propertiesIndex-2].(string) == "all" {
			http.Error(w, "all is not a valid "+owner+" for requesting a single "+this+". Did you want to say "+plural(this)+"?", http.StatusBadRequest)
			return
		}

		values, response := createScanValuesAndObject()
		err := b.db.QueryRow(sqlQuery, queryParameters...).Scan(values...)
		if err == sql.ErrNoRows {
			// not an error, singleton resources do always exist conceptually
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		urlQuery := r.URL.Query()
		for key, array := range urlQuery {
			switch key {
			case "children":
				status, err := b.addChildrenToGetResponse(array, r, response)
				if err != nil {
					http.Error(w, err.Error(), status)
					return
				}
			default:
				http.Error(w, "parameter '"+key+"': unknown query parameter", http.StatusBadRequest)
			}
		}
		// hide "state" from response unless defined
		state := *response["state"].(*string)
		if len(state) == 0 {
			delete(response, "state")
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		etag := bytesToEtag(jsonData)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Etag", etag)
		w.Header().Set("Content-Type", "application/json")

		w.Write(jsonData)
	}

	doDeleteSingletonWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationDelete, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
		queryParameters := make([]interface{}, propertiesIndex-1)
		for i := 1; i < propertiesIndex; i++ { // skip ID
			queryParameters[i-1] = params[columns[i]]
		}

		var state string
		err = b.db.QueryRow(deleteQuery+sqlWhereSingle+sqlReturnState, queryParameters...).Scan(&state)
		if err == sql.ErrNoRows {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
		notification := make(map[string]interface{})
		for i := 1; i < propertiesIndex; i++ { // skip ID
			notification[columns[i]] = params[columns[i]]
		}
		jsonData, _ := json.MarshalIndent(notification, "", " ")
		b.notify(resource, core.OperationDelete, state, jsonData)
	}

	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		getOneSingletonWithAuth(w, r)

	}).Methods(http.MethodOptions, http.MethodGet)

	// CREATE - UPDATE
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		insertUpdateSingletonWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut)

	// PATCH (READ + UPDATE)
	b.createPatchRoute(router, singletonRoute)

	// DELETE
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		doDeleteSingletonWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

}
