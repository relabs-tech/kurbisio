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
	"github.com/relabs-tech/backends/core/client"
	"github.com/relabs-tech/backends/core/registry"
	"github.com/relabs-tech/backends/core/sql"
)

// Backend is the generic rest backend
type Backend struct {
	config           backendConfiguration
	schema           string
	notifier         core.Notifier
	db               *sql.DB
	router           *mux.Router
	collectionHelper map[string]*collectionHelper
	// Registry is the JSON object registry for this backend's schema
	Registry *registry.Registry
}

// Builder is a builder helper for the Backend
type Builder struct {
	// Config is the JSON description of all resources and relations. This is mandatory.
	Config string
	// DB is a postgres database. This is mandatory.
	DB *sql.DB
	// Router is a mux router. This is mandatory.
	Router *mux.Router
	// Notifier inserts a database notifier to the backend. If the configuration requests
	// notifications, they will be sent to the notifier. This is optional.
	Notifier core.Notifier
}

// New realizes the actual backend. It creates the sql relations (if they
// do not exist) and adds actual routes to router
func New(bb *Builder) *Backend {

	var config backendConfiguration
	err := json.Unmarshal([]byte(bb.Config), &config)
	if err != nil {
		panic(fmt.Errorf("parse error in backend configuration: %s", err))
	}

	if bb.DB == nil {
		panic("DB is missing")
	}

	if bb.Router == nil {
		panic("Router is missing")
	}

	b := &Backend{
		config:           config,
		notifier:         bb.Notifier,
		db:               bb.DB,
		schema:           bb.DB.Schema,
		router:           bb.Router,
		collectionHelper: make(map[string]*collectionHelper),
		Registry:         registry.New(bb.DB),
	}

	access.HandleAuthorizationRoute(b.router)
	b.handleRoutes(b.router)
	return b
}

type relationInjection struct {
	subquery        string
	columns         []string
	queryParameters []interface{}
}

type collectionHelper struct {
	createScanValuesAndObject func() ([]interface{}, map[string]interface{})
	get                       func(w http.ResponseWriter, r *http.Request, relation *relationInjection)
	readQuery                 string
}

func plural(s string) string {
	if strings.HasSuffix(s, "y") {
		return strings.TrimSuffix(s, "y") + "ies"
	}
	return s + "s"
}

// returns $1,...,$n
func parameterString(n int) string {
	result := ""
	for i := 0; i < n; i++ {
		if i > 0 {
			result += ","
		}
		result += "$" + strconv.Itoa(i+1)
	}
	return result
}

// returns s[0]=$1 AND ... AND s[n-1]=$n
func compareString(s []string) string {
	result := ""
	i := 0
	for ; i < len(s); i++ {
		if i > 0 {
			result += " AND "
		}
		result += s[i] + " = $" + strconv.Itoa(i+1)
	}
	return result
}

// returns s[0]=$(offset+1) AND ... AND s[n-1]=$(offset+n)
func compareStringWithOffset(offset int, s []string) string {
	result := ""
	i := 0
	for ; i < len(s); i++ {
		if i > 0 {
			result += " AND "
		}
		result += s[i] + " = $" + strconv.Itoa(i+offset+1)
	}
	return result
}

func (b *Backend) createCollectionResource(router *mux.Router, rc collectionConfiguration) {
	schema := b.schema
	resource := rc.Resource
	log.Println("create resource:", resource)

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
	}

	columns := []string{this + "_id"}

	for i := range dependencies {
		that := dependencies[i]
		createColumn := fmt.Sprintf("%s_id uuid", that)
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
	createExternalIndicesQuery := "" // query to create all indices after the table creation
	propertiesIndex := len(columns)  // where properties start
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
		createExternalIndicesQuery = createExternalIndicesQuery + fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s\"(%s);",
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
		createExternalIndicesQuery = createExternalIndicesQuery + fmt.Sprintf("CREATE UNIQUE index IF NOT EXISTS %s ON %s.\"%s\"(%s);",
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

	createQuery += "(" + strings.Join(createColumns, ", ") + ");" + createExternalIndicesQuery

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

	log.Println("  handle routes:", collectionRoute, "GET,POST,PUT")
	log.Println("  handle routes:", oneRoute, "GET,DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", created_at FROM %s.\"%s\" ", schema, resource)
	sqlWhereOne := "WHERE " + compareString(columns[:propertiesIndex]) + ";"

	readQueryWithTotal := "SELECT " + strings.Join(columns, ", ") +
		fmt.Sprintf(", created_at, count(*) OVER() AS full_count FROM %s.\"%s\" ", schema, resource)
	sqlWhereAll := "WHERE "
	if propertiesIndex > 1 {
		sqlWhereAll += compareString(columns[1:propertiesIndex]) + " AND "
	}
	sqlWhereAll += fmt.Sprintf("($%d OR created_at<$%d) AND created_at>$%d ",
		propertiesIndex, propertiesIndex+1, propertiesIndex+2)

	sqlWhereAllPlusOneExternalIndex := sqlWhereAll + fmt.Sprintf("AND %%s = $%d ", propertiesIndex+5)

	sqlPagination := fmt.Sprintf("ORDER BY created_at DESC LIMIT $%d OFFSET $%d;", propertiesIndex+3, propertiesIndex+4)

	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource) + "(" + strings.Join(columns, ", ") + ", created_at)"
	insertQuery += "VALUES(" + parameterString(len(columns)+1) + ") RETURNING " + this + "_id;"

	createScanValuesAndObject := func() ([]interface{}, map[string]interface{}) {
		values := make([]interface{}, len(columns)+1)
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
		values[len(columns)] = createdAt
		object["created_at"] = values[len(columns)]
		return values, object
	}

	createScanValuesAndObjectForCollection := func(totalCount *int) ([]interface{}, map[string]interface{}) {
		values := make([]interface{}, len(columns)+2)
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
		values[len(columns)] = createdAt
		object["created_at"] = values[len(columns)]
		values[len(columns)+1] = &totalCount
		return values, object
	}

	getCollection := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
		var queryParameters []interface{}
		var sqlQuery string
		limit := 100
		page := 1
		before := time.Time{}
		after := time.Time{}
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
			case "before":
				before, err = time.Parse(time.RFC3339, value)

			case "after":
				after, err = time.Parse(time.RFC3339, value)
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
			queryParameters = make([]interface{}, propertiesIndex-1+5)
			for i := 1; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-1] = params[columns[i]]
			}
		} else {
			sqlQuery = fmt.Sprintf(readQueryWithTotal+sqlWhereAllPlusOneExternalIndex, externalColumn)
			queryParameters = make([]interface{}, propertiesIndex-1+5+1)
			for i := 1; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-1] = params[columns[i]]
			}
			queryParameters[propertiesIndex-1+5] = externalIndex
		}

		// add before and after and pagination
		queryParameters[propertiesIndex-1+0] = before.IsZero()
		queryParameters[propertiesIndex-1+1] = before.UTC()
		queryParameters[propertiesIndex-1+2] = after.UTC()
		queryParameters[propertiesIndex-1+3] = limit
		queryParameters[propertiesIndex-1+4] = (page - 1) * limit

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
			response = append(response, object)
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Pagination-Limit", strconv.Itoa(limit))
		w.Header().Set("Pagination-Total-Count", strconv.Itoa(totalCount))
		w.Header().Set("Pagination-Page-Count", strconv.Itoa(totalCount/limit))
		w.Header().Set("Pagination-Current-Page", strconv.Itoa(page))
		w.Write(jsonData)

	}

	collection := collectionHelper{
		readQuery:                 readQuery,
		createScanValuesAndObject: createScanValuesAndObject,
		get:                       getCollection,
	}

	// store the collection helper for later usage in relations
	b.collectionHelper[this] = &collection

	// POST
	router.HandleFunc(collectionRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		body, _ := ioutil.ReadAll(r.Body)
		params := mux.Vars(r)

		var bodyJSON map[string]interface{}
		err := json.Unmarshal(body, &bodyJSON)
		if err != nil {
			http.Error(w, "invalid json data: "+err.Error(), http.StatusBadRequest)
			return
		}

		// build insert query and validate that we have all parameters
		values := make([]interface{}, len(columns)+1)
		for i, k := range columns {
			if i < propertiesIndex { // the core identifiers
				value, ok := bodyJSON[k]
				if i == 0 { // the primary resource identifier, use as specified or create a new one
					if !ok || value == "00000000-0000-0000-0000-000000000000" {
						value = uuid.New()
					}
					values[i] = value
				} else {
					param, _ := params[k]
					if ok && param != value.(string) {
						http.Error(w, "illegal "+k, http.StatusBadRequest)
						return
					}
					values[i] = param
				}
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

		// last value is created_at
		createdAt := time.Now().UTC()
		if value, ok := bodyJSON["created_at"]; ok {
			timestamp, _ := value.(string)
			if len(timestamp) == 0 {
				createdAt = time.Time{}
			} else {
				t, err := time.Parse(time.RFC3339, timestamp)
				if err != nil {
					http.Error(w, "illegal created_at: "+err.Error(), http.StatusBadRequest)
					return
				}
				createdAt = t.UTC()
			}
		}
		values[len(columns)] = &createdAt

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

	// PUT
	router.HandleFunc(collectionRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		body, _ := ioutil.ReadAll(r.Body)
		params := mux.Vars(r)

		var bodyJSON map[string]interface{}
		err := json.Unmarshal(body, &bodyJSON)
		if err != nil {
			http.Error(w, "invalid json data: "+err.Error(), http.StatusBadRequest)
			return
		}

		query := fmt.Sprintf("UPDATE %s.\"%s\" SET ", schema, resource)
		sets := make([]string, len(columns))
		values := make([]interface{}, len(columns)+propertiesIndex+1)

		value, ok := bodyJSON[columns[0]]
		if !ok {
			http.Error(w, "missing "+columns[0], http.StatusBadRequest)
			return
		}
		// To simplify the code logic below, we add the primary id from the body json to the URL parameters.
		// This is because the PUT-route does not contain the primary ID for convenience
		params[columns[0]] = value.(string)

		// first add the values for the where-query.
		for i := 0; i < propertiesIndex; i++ {
			values[i] = params[columns[i]]
		}

		for i, k := range columns {
			if i < propertiesIndex {
				param, _ := params[k]
				value, ok := bodyJSON[k]
				if ok && param != value.(string) {
					http.Error(w, "illegal "+k, http.StatusBadRequest)
					return
				}
				values[propertiesIndex+i] = param

			} else if i > propertiesIndex {
				value, ok := bodyJSON[k]
				if !ok {
					http.Error(w, "missing property or index"+k, http.StatusBadRequest)
					return
				}
				values[propertiesIndex+i] = value
			} else {
				properties, ok := bodyJSON[k]
				if ok {
					propertiesJSON, _ := json.Marshal(properties)
					values[propertiesIndex+i] = propertiesJSON
				} else {
					values[propertiesIndex+i] = []byte("{}")
				}
			}
			sets[i] = k + " = $" + strconv.Itoa(propertiesIndex+1+i)
		}

		// last value is created_at
		createdAt := time.Now().UTC()
		if value, ok := bodyJSON["created_at"]; ok {
			timestamp, _ := value.(string)
			if len(timestamp) == 0 {
				createdAt = time.Time{}
			} else {
				t, err := time.Parse(time.RFC3339, timestamp)
				if err != nil {
					http.Error(w, "illegal created_at: "+err.Error(), http.StatusBadRequest)
					return
				}
				createdAt = t.UTC()
			}
		}
		values[len(values)-1] = &createdAt

		query += strings.Join(sets, ", ") + ", created_at = $" + strconv.Itoa(propertiesIndex+1+len(columns)) + " " + sqlWhereOne
		res, err := b.db.Exec(query, values...)
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
	}).Methods(http.MethodPut)

	// GET one
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
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

		jsonData, _ := json.MarshalIndent(response, "", " ")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	}).Methods(http.MethodGet)

	// GET collection
	router.HandleFunc(collectionRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		getCollection(w, r, nil)
	}).Methods(http.MethodGet)

	// DELETE one
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		params := mux.Vars(r)
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

	if rc.LoggedInRoutes {
		prefix := "/" + resource
		log.Println("  handle logged-in routes: "+prefix+"[/...]", "GET,POST,PUT,DELETE")

		replaceHandler := func(w http.ResponseWriter, r *http.Request) {
			log.Println("called logged-in route for", r.URL, r.Method)
			auth := access.AuthorizationFromContext(r.Context())
			newPrefix := ""
			for _, s := range resources {
				id, ok := auth.Identifier(s + "_id")
				if !ok {
					http.Error(w, resource+" not authorized", http.StatusUnauthorized)
					return
				}
				newPrefix += "/" + plural(s) + "/" + id.String()
			}
			r.URL.Path = newPrefix + strings.TrimPrefix(r.URL.Path, prefix)
			log.Println("redirect logged-in route to:", r.URL)
			router.ServeHTTP(w, r)
		}
		router.HandleFunc(prefix, replaceHandler).Methods(http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete)
		router.HandleFunc(prefix+"/{rest:.+}", replaceHandler).Methods(http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete)
	}
}

func (b *Backend) createSingletonResource(router *mux.Router, rc singletonConfiguration) {

	schema := b.schema
	resource := rc.Resource
	log.Println("create singleton resource:", resource)

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
	owner := resources[len(resources)-2]
	dependencies := resources[:len(resources)-1]

	createQuery := fmt.Sprintf("CREATE table IF NOT EXISTS %s.\"%s\"", schema, resource)
	createColumns := []string{
		this + "_id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY",
		"created_at timestamp NOT NULL DEFAULT now()",
	}

	columns := []string{this + "_id"}

	for i := range dependencies {
		that := dependencies[i]
		createColumn := fmt.Sprintf("%s_id uuid", that)
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

	// force the resource itself to be singleton resource
	createColumn := "UNIQUE (" + owner + "_id )"
	createColumns = append(createColumns, createColumn)

	createColumns = append(createColumns, "properties json NOT NULL DEFAULT '{}'::jsonb")
	propertiesIndex := len(columns)
	columns = append(columns, "properties")

	createQuery += "(" + strings.Join(createColumns, ", ") + ");"

	_, err := b.db.Query(createQuery)
	if err != nil {
		panic(err)
	}

	singletonRoute := ""
	oneRoute := ""
	for _, r := range resources {
		singletonRoute = oneRoute + "/" + r
		oneRoute = oneRoute + "/" + plural(r) + "/{" + r + "_id}"
	}

	log.Println("  handle singleton routes:", singletonRoute, "GET,PUT,DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", created_at FROM %s.\"%s\" ", schema, resource)
	sqlWhereSingle := ""
	if propertiesIndex > 1 {
		sqlWhereSingle += "WHERE " + compareString(columns[1:propertiesIndex])
	}
	sqlWhereSingle += ";"
	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource)
	insertQuery += "(" + strings.Join(columns, ", ") + ", created_at)"
	insertQuery += " VALUES(" + parameterString(len(columns)+1) + ") ON CONFLICT (" + owner + "_id) DO UPDATE SET "
	sets := make([]string, len(columns)-1)
	for i := 1; i < len(columns); i++ {
		sets[i-1] = columns[i] + " = $" + strconv.Itoa(i+1)
	}
	insertQuery += strings.Join(sets, ", ") + ", created_at = $" + strconv.Itoa(len(columns)+1)
	insertQuery += " RETURNING (xmax = 0) AS inserted;" // return whether we did insert or update, this is a psql trick

	createScanValuesAndObject := func() ([]interface{}, map[string]interface{}) {
		values := make([]interface{}, len(columns)+1)
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
		values[len(columns)] = &time.Time{}
		object["created_at"] = values[len(columns)]
		return values, object
	}

	// PUT singleton
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		body, _ := ioutil.ReadAll(r.Body)
		params := mux.Vars(r)

		var bodyJSON map[string]interface{}
		err := json.Unmarshal(body, &bodyJSON)
		if err != nil {
			http.Error(w, "invalid json data: "+err.Error(), http.StatusBadRequest)
			return
		}

		// build insert query and validate that we have all parameters
		values := make([]interface{}, len(columns)+1)
		for i, k := range columns {
			if i < propertiesIndex { // the core identifiers
				value, ok := bodyJSON[k]
				if i == 0 { // the primary resource identifier, use as specified or create a new one
					if !ok || value == "00000000-0000-0000-0000-000000000000" {
						value = uuid.New()
					}
					values[i] = value
				} else {
					param, _ := params[k]
					if ok && param != value.(string) {
						http.Error(w, "illegal "+k, http.StatusBadRequest)
						return
					}
					values[i] = param
				}
			} else if i == propertiesIndex { // the dynamic properties
				properties, ok := bodyJSON[k]
				if ok {
					propertiesJSON, _ := json.Marshal(properties)
					values[i] = propertiesJSON
				} else {
					values[i] = []byte("{}")
				}
			}
		}

		// last value is created_at
		createdAt := time.Now().UTC()
		if value, ok := bodyJSON["created_at"]; ok {
			timestamp, _ := value.(string)
			if len(timestamp) == 0 {
				createdAt = time.Time{}
			} else {
				t, err := time.Parse(time.RFC3339, timestamp)
				if err != nil {
					http.Error(w, "illegal created_at: "+err.Error(), http.StatusBadRequest)
					return
				}
				createdAt = t.UTC()
			}
		}
		values[len(columns)] = &createdAt

		var inserted bool
		err = b.db.QueryRow(insertQuery, values...).Scan(&inserted)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// re-read data and return as json
		sqlQuery := readQuery + sqlWhereSingle
		queryParameters := make([]interface{}, propertiesIndex-1)
		for i := 1; i < propertiesIndex; i++ { // skip ID
			queryParameters[i-1] = params[columns[i]]
		}

		values, response := createScanValuesAndObject()
		err = b.db.QueryRow(sqlQuery, queryParameters...).Scan(values...)
		if err == sql.ErrNoRows {
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		if b.notifier != nil {
			if inserted {
				if hasNotificationCreate {
					b.notifier.Notify(resource, core.OperationCreate, jsonData)
				}
			} else {
				if hasNotificationUpdate {
					b.notifier.Notify(resource, core.OperationUpdate, jsonData)
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)
	}).Methods(http.MethodPut)

	// GET singleton
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		sqlQuery := readQuery + sqlWhereSingle
		queryParameters := make([]interface{}, propertiesIndex-1)
		for i := 1; i < propertiesIndex; i++ { // skip ID
			queryParameters[i-1] = params[columns[i]]
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

		w.Header().Set("Content-Type", "application/json")
		jsonData, _ := json.MarshalIndent(response, "", " ")
		w.Write(jsonData)
	}).Methods(http.MethodGet)

	// DELETE singleton
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		params := mux.Vars(r)

		queryParameters := make([]interface{}, propertiesIndex-1)
		for i := 1; i < propertiesIndex; i++ { // skip ID
			queryParameters[i-1] = params[columns[i]]
		}

		res, err := b.db.Exec(deleteQuery+sqlWhereSingle, queryParameters...)
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

		w.WriteHeader(http.StatusNoContent)
		if hasNotificationDelete && b.notifier != nil {
			notification := make(map[string]interface{})
			for i := 1; i < propertiesIndex; i++ { // skip ID
				notification[columns[i]] = params[columns[i]]
			}
			jsonData, _ := json.MarshalIndent(notification, "", " ")
			b.notifier.Notify(resource, core.OperationDelete, jsonData)
		}

	}).Methods(http.MethodDelete)
}

// HandleRoutes adds all necessary handlers for the specified configuration
func (b *Backend) handleRoutes(router *mux.Router) {

	log.Println("backend: HandleRoutes")

	for _, rc := range b.config.Collections {
		b.createCollectionResource(router, rc)
	}

	for _, rc := range b.config.Singletons {
		b.createSingletonResource(router, rc)
	}

	for _, rc := range b.config.Relations {
		b.createRelationResource(router, rc)
	}
}

func (b *Backend) addChildrenToGetResponse(children []string, r *http.Request, response map[string]interface{}) (int, error) {
	var all []string
	for _, child := range children {
		all = append(all, strings.Split(child, ",")...)
	}
	client := client.New(b.router).WithContext(r.Context())
	for _, child := range all {
		if strings.ContainsRune(child, '/') {
			return http.StatusBadRequest, fmt.Errorf("invalid child %s", child)
		}
		var childJSON interface{}
		status, err := client.Get(r.URL.Path+"/"+child, &childJSON)
		if err != nil {
			return status, err
		}
		response[child] = &childJSON
	}
	return http.StatusOK, nil
}

func (b *Backend) createRelationResource(router *mux.Router, rc relationConfiguration) {
	schema := b.schema
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
		createColumn := c + "_id uuid"
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
	sqlWhereOne := fmt.Sprintf("WHERE %s_id IN (SELECT %s_id FROM %s.\"%s\" WHERE %s);", this, this, schema, resource, compareString(resourceColumns))
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

	// GET all
	router.HandleFunc(allRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		queryParameters := make([]interface{}, len(resourceColumns)-1)
		for i := 0; i < len(resourceColumns)-1; i++ { // skip ID
			queryParameters[i] = params[resourceColumns[i]]
		}
		injectRelation := &relationInjection{
			subquery:        sqlInjectRelation,
			columns:         resourceColumns[:len(resourceColumns)-1],
			queryParameters: queryParameters,
		}

		collection.get(w, r, injectRelation)
	}).Methods(http.MethodGet)

	// GET one
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		queryParameters := make([]interface{}, len(resourceColumns))
		for i := 0; i < len(resourceColumns); i++ {
			queryParameters[i] = params[resourceColumns[i]]
		}

		values, response := collection.createScanValuesAndObject()
		err := b.db.QueryRow(collection.readQuery+sqlWhereOne, queryParameters...).Scan(values...)
		if err == sql.ErrNoRows {
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		jsonData, _ := json.MarshalIndent(response, "", " ")
		w.Write(jsonData)
	}).Methods(http.MethodGet)

	// PUT one
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		params := mux.Vars(r)
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

	// DELETE one
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		params := mux.Vars(r)
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
