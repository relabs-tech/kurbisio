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

func (b *Backend) createCollectionResource(router *mux.Router, rc collectionConfiguration) {
	schema := b.db.Schema
	resource := rc.Resource

	hasNotificationCreate := false
	hasNotificationUpdate := false
	hasNotificationDelete := false
	for _, operation := range rc.Notifications {
		switch operation {
		case string(core.OperationCreate):
			hasNotificationCreate = true
		case string(core.OperationUpdate):
			hasNotificationUpdate = true
		case string(core.OperationDelete):
			hasNotificationDelete = true
		default:
			panic(fmt.Errorf("invalid notification '%s' for resource %s", operation, resource))
		}
	}

	resources := strings.Split(rc.Resource, "/")
	this := resources[len(resources)-1]
	dependencies := resources[:len(resources)-1]

	createQuery := fmt.Sprintf("CREATE table IF NOT EXISTS %s.\"%s\"", schema, resource)
	createColumns := []string{
		this + "_id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY",
		"created_at timestamp NOT NULL DEFAULT now()",
		"hidden boolean NOT NULL DEFAULT false",
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

	_, err := b.db.Query(createQuery)
	if err != nil {
		panic(err)
	}

	collectionRoute := ""
	oneRoute := ""
	for _, r := range resources {
		collectionRoute = oneRoute + "/" + plural(r)
		oneRoute = oneRoute + "/" + plural(r) + "/{" + r + "_id}"
	}

	log.Println("  handle collection routes:", collectionRoute, "GET,POST,PUT,PATCH")
	log.Println("  handle collection routes:", oneRoute, "GET,DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", created_at, hidden FROM %s.\"%s\" ", schema, resource)
	sqlWhereOne := "WHERE " + compareString(columns[:propertiesIndex]) + ";"

	readQueryWithTotal := "SELECT " + strings.Join(columns, ", ") +
		fmt.Sprintf(", created_at, hidden, count(*) OVER() AS full_count FROM %s.\"%s\" ", schema, resource)
	sqlWhereAll := "WHERE "
	if propertiesIndex > 1 {
		sqlWhereAll += compareString(columns[1:propertiesIndex]) + " AND "
	}
	sqlWhereAll += fmt.Sprintf("($%d OR created_at<=$%d) AND ($%d OR created_at>=$%d) AND hidden=$%d ",
		propertiesIndex, propertiesIndex+1, propertiesIndex+2, propertiesIndex+3, propertiesIndex+4)

	sqlPagination := fmt.Sprintf("ORDER BY created_at DESC LIMIT $%d OFFSET $%d;", propertiesIndex+5, propertiesIndex+6)

	sqlWhereAllPlusOneExternalIndex := sqlWhereAll + fmt.Sprintf("AND %%s = $%d ", propertiesIndex+7)

	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource) + "(" + strings.Join(columns, ", ") + ", created_at, hidden)"
	insertQuery += "VALUES(" + parameterString(len(columns)+2) + ") RETURNING " + this + "_id;"

	updateQuery := fmt.Sprintf("UPDATE %s.\"%s\" SET ", schema, resource)
	sets := make([]string, len(columns)-propertiesIndex)
	for i := propertiesIndex; i < len(columns); i++ {
		sets[i-propertiesIndex] = columns[i] + " = $" + strconv.Itoa(i+1)
	}
	updateQuery += strings.Join(sets, ", ") + ", created_at = $" + strconv.Itoa(len(columns)+1) + ", hidden = $" + strconv.Itoa(len(columns)+2) + " " + sqlWhereOne

	createScanValuesAndObject := func() ([]interface{}, map[string]interface{}) {
		n := len(columns)
		values := make([]interface{}, n+2)
		object := map[string]interface{}{}
		for i, k := range columns {
			if i < propertiesIndex {
				values[i] = &uuid.UUID{}
			} else if i > propertiesIndex {
				str := ""
				values[i] = &str
			} else {
				values[i] = &json.RawMessage{}
			}
			object[k] = values[i]
		}
		createdAt := &time.Time{}
		values[n] = createdAt
		object["created_at"] = createdAt
		hidden := false
		values[n+1] = &hidden
		object["hidden"] = &hidden
		return values, object
	}

	createScanValuesAndObjectForCollection := func(totalCount *int) ([]interface{}, map[string]interface{}) {
		n := len(columns)
		values := make([]interface{}, n+3)
		object := map[string]interface{}{}
		for i, k := range columns {
			if i < propertiesIndex {
				values[i] = &uuid.UUID{}
			} else if i > propertiesIndex {
				str := ""
				values[i] = &str
			} else {
				values[i] = &json.RawMessage{}
			}
			object[k] = values[i]
		}
		createdAt := &time.Time{}
		values[n] = createdAt
		object["created_at"] = createdAt
		hidden := false
		values[n+1] = &hidden
		object["hidden"] = &hidden
		values[n+2] = totalCount
		return values, object
	}

	getAll := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
		var queryParameters []interface{}
		var sqlQuery string
		limit := 100
		page := 1
		until := time.Time{}
		from := time.Time{}
		hidden := false
		externalColumn := ""
		externalIndex := ""

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

			case "hidden":
				hidden, err = strconv.ParseBool(value)

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
		queryParameters[propertiesIndex-1+4] = hidden
		queryParameters[propertiesIndex-1+5] = limit
		queryParameters[propertiesIndex-1+6] = (page - 1) * limit

		if relation != nil {
			// ingest subquery for relation
			sqlQuery += fmt.Sprintf(relation.subquery,
				compareStringWithOffset(len(queryParameters), relation.columns))
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
			// hide "hidden" from response unless requested
			if !hidden {
				delete(object, "hidden")
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

	getOne := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		queryParameters := make([]interface{}, propertiesIndex)
		for i := 0; i < propertiesIndex; i++ {
			queryParameters[i] = params[columns[i]]
		}

		values, response := createScanValuesAndObject()
		err = b.db.QueryRow(readQuery+sqlWhereOne, queryParameters...).Scan(values...)
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

		// hide "hidden" from response unless true
		if hidden, ok := response["hidden"].(bool); !ok || !hidden {
			delete(response, "hidden")
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

	update := func(w http.ResponseWriter, r *http.Request) {
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

		// add and validate core identifiers
		for i := 0; i < propertiesIndex; i++ {
			key := columns[i]
			param := params[key]
			values[i] = param
			value, ok := bodyJSON[key]
			if ok && param != value.(string) {
				http.Error(w, "illegal "+key, http.StatusBadRequest)
				return
			}
		}

		// build the update set
		for i := propertiesIndex; i < len(columns); i++ {
			k := columns[i]
			if i > propertiesIndex {
				value, ok := bodyJSON[k]
				if !ok {
					http.Error(w, "missing property or index"+k, http.StatusBadRequest)
					return
				}
				values[i] = value
			} else {
				properties, ok := bodyJSON[k]
				if ok {
					propertiesJSON, _ := json.Marshal(properties)
					values[i] = propertiesJSON
				} else {
					values[i] = []byte("{}")
				}
			}
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
		values[len(values)-2] = &createdAt

		// last value is hidden
		hidden := false
		if value, ok := bodyJSON["hidden"]; ok {
			hidden, ok = value.(bool)
			if !ok {
				http.Error(w, "illegal value for hidden: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
		values[len(values)-1] = &hidden

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
		err = b.db.QueryRow(readQuery+sqlWhereOne, queryParameters...).Scan(values...)
		if err == sql.ErrNoRows {
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		if hasNotificationUpdate && b.notifier != nil {
			b.notifier.Notify(resource, core.OperationUpdate, jsonData)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
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
		body, _ := ioutil.ReadAll(r.Body)
		params := mux.Vars(r)

		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationCreate, params, rc.Permits) {
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
		for i := 1; i < len(columns); i++ {
			k := columns[i]
			if i < propertiesIndex { // the core identifiers
				value, ok := bodyJSON[k]
				param, _ := params[k]
				if ok && param != value.(string) {
					http.Error(w, "illegal "+k, http.StatusBadRequest)
					return
				}
				values[i] = param
			} else if i == propertiesIndex { // the dynamic properties
				properties, ok := bodyJSON[k]
				if ok {
					propertiesJSON, _ := json.Marshal(properties)
					values[i] = propertiesJSON
				} else {
					values[i] = []byte("{}")
				}
			} else if i < propertiesEndIndex { // static properties, non mandatory
				value, ok := bodyJSON[k]
				if !ok {
					value = ""
				}
				values[i] = value
			} else { // external (unique) indices, mandatory
				value, ok := bodyJSON[k]
				if !ok {
					http.Error(w, "missing external index "+k, http.StatusBadRequest)
					return
				}
				values[i] = value
			}
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
		values[len(columns)] = &createdAt

		// last value is hidden
		hidden := false
		if value, ok := bodyJSON["hidden"]; ok {
			hidden, ok = value.(bool)
			if !ok {
				http.Error(w, "illegal value for hidden: "+err.Error(), http.StatusBadRequest)
				return
			}
		}
		values[len(columns)+1] = &hidden

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

		jsonData, _ := json.MarshalIndent(response, "", " ")
		if hasNotificationCreate && b.notifier != nil {
			b.notifier.Notify(resource, core.OperationCreate, jsonData)
		}

		w.WriteHeader(http.StatusCreated)
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)

	}).Methods(http.MethodPost)

	// UPDATE
	router.HandleFunc(collectionRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		update(w, r)
	}).Methods(http.MethodPut)

	// UPDATE with fully qualified path
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		update(w, r)
	}).Methods(http.MethodPut)

	// PATCH (READ + UPDATE)
	b.createPatchRoute(router, oneRoute)

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
		getOne(w, r)
	}).Methods(http.MethodGet)

	// READ ALL
	router.HandleFunc(collectionRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationList, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		getAll(w, r, nil)
	}).Methods(http.MethodGet)

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

		if hasNotificationDelete && b.notifier != nil {
			notification := make(map[string]interface{})
			for i := 0; i < propertiesIndex; i++ {
				notification[columns[i]] = params[columns[i]]
			}
			jsonData, _ := json.MarshalIndent(notification, "", " ")
			b.notifier.Notify(resource, core.OperationDelete, jsonData)
		}
		w.WriteHeader(http.StatusNoContent)

	}).Methods(http.MethodDelete)
}
