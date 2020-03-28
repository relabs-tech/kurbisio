package backend

import (
	// "context"
	// "crypto/tls"
	// "crypto/x509"
	// "database/sql"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	// "fmt"
	"io/ioutil"
	// "log"
	// "net"
	"net/http"

	// _ "net/http/pprof"

	// "os"
	// "os/signal"
	// "strings"
	// "sync"
	// "syscall"
	// "time"

	// "github.com/DrmagicE/gmqtt"
	// "github.com/DrmagicE/gmqtt/pkg/packets"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq" // load database driver for postgres
	"github.com/relabs-tech/backends/core"
	"github.com/relabs-tech/backends/core/access"
	"github.com/relabs-tech/backends/core/client"
	"github.com/relabs-tech/backends/core/registry"
)

// Backend is the generic rest backend
type Backend struct {
	config             backendConfiguration
	schema             string
	notifier           core.Notifier
	db                 *sql.DB
	router             *mux.Router
	scanValueFunctions map[string]func() ([]interface{}, map[string]interface{})
	readQuery          map[string]string
	// Registry is the JSON object registry for this backend's schema
	Registry *registry.Registry
}

// BackendBuilder is a builder helper for the Backend
type BackendBuilder struct {
	// Config is the JSON description of all resources and relations. This is mandatory.
	Config string
	// DB is a postgres database. This is mandatory.
	DB *sql.DB
	// Schema is optional. When set, the backend uses the data schema name for
	// generated sql relations. The default schema is "public"
	Schema string
	// Router is a mux router. This is mandatory.
	Router *mux.Router
	// Notifier inserts a database notifier to the backend. If the configuration requests
	// notifications, they will be sent to the notifier. This is optional.
	Notifier core.Notifier
}

// MustNewBackend realizes the actual backend. It creates the sql relations (if they
// do not exist) and adds actual routes to router
func MustNewBackend(bb *BackendBuilder) *Backend {

	var config backendConfiguration
	err := json.Unmarshal([]byte(bb.Config), &config)
	if err != nil {
		panic(err)
	}
	schema := bb.Schema
	if len(schema) == 0 {
		schema = "public"
	}

	if bb.DB == nil {
		panic("DB is missing")
	}

	if bb.Router == nil {
		panic("Router is missing")
	}

	err = bb.DB.Ping()
	if err != nil {
		panic(err)
	}

	b := &Backend{
		schema:             schema,
		config:             config,
		notifier:           bb.Notifier,
		db:                 bb.DB,
		router:             bb.Router,
		readQuery:          make(map[string]string),
		scanValueFunctions: make(map[string]func() ([]interface{}, map[string]interface{})),
		Registry:           registry.MustNew(bb.DB, schema),
	}

	access.HandleAuthorizationRoute(b.router)
	b.handleRoutes(b.router)
	return b
}

// backendConfiguration holds a complete backend configuration
type backendConfiguration struct {
	Resources []resourceConfiguration `json:"resources"`
	Relations []relationConfiguration `json:"relations"`
}

// resourceConfiguration is one single resource table
type resourceConfiguration struct {
	Resource              string   `json:"resource"`
	Single                bool     `json:"single"`
	LoggedInRoutes        bool     `json:"logged_in_routes"`
	ExternalUniqueIndices []string `json:"external_unique_indices"`
	ExternalIndices       []string `json:"external_indices"`
	StaticProperties      []string `json:"static_properties"`
	Notifications         []string `json:"notifications"`
}

// relationConfiguration is a n:m relation from a resource table or another relation
type relationConfiguration struct {
	Resource string `json:"resource"`
	Origin   string `json:"origin"`
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
func compareString(s []string, extra ...string) string {
	result := ""
	i := 0
	for ; i < len(s); i++ {
		if i > 0 {
			result += " AND "
		}
		result += s[i] + " = $" + strconv.Itoa(i+1)
	}
	for j := 0; j < len(extra); j++ {
		if i+j > 0 {
			result += " AND "
		}
		result += extra[j] + " = $" + strconv.Itoa(i+j+1)
	}
	return result
}

func (b *Backend) createBackendHandlerResource(router *mux.Router, rc resourceConfiguration) {
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

	if len(columns) > 1 {
		createColumn := "UNIQUE (" + strings.Join(columns, ",") + ")"
		createColumns = append(createColumns, createColumn)
	}

	createColumns = append(createColumns, "properties json NOT NULL DEFAULT '{}'::jsonb")
	propertiesIndex := len(columns)
	columns = append(columns, "properties")

	for _, index := range rc.ExternalUniqueIndices {
		createColumn := fmt.Sprintf("\"%s\" varchar NOT NULL", index)
		uniqueColumn := fmt.Sprintf("UNIQUE(\"%s\")", index)
		createColumns = append(createColumns, createColumn)
		createColumns = append(createColumns, uniqueColumn)
		columns = append(columns, index)
	}

	createExternalIndicesQuery := ""
	for _, index := range rc.ExternalIndices {
		createColumn := fmt.Sprintf("\"%s\" varchar NOT NULL", index)
		createExternalIndicesQuery = createExternalIndicesQuery + fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s\"(%s);",
			"external_index_"+this+"_"+index,
			schema, resource, index)
		createColumns = append(createColumns, createColumn)
		columns = append(columns, index)
	}

	for _, property := range rc.StaticProperties {
		createColumn := fmt.Sprintf("\"%s\" varchar NOT NULL", property)
		createColumns = append(createColumns, createColumn)
		columns = append(columns, property)
	}

	createQuery += "(" + strings.Join(createColumns, ", ") + ");" + createExternalIndicesQuery

	_, err := b.db.Query(createQuery)
	if err != nil {
		panic(err)
	}

	allRoute := ""
	oneRoute := ""
	for _, r := range resources {
		allRoute = oneRoute + "/" + plural(r)
		oneRoute = oneRoute + "/" + plural(r) + "/{" + r + "_id}"
	}

	log.Println("  handle routes:", allRoute, "GET,POST,PUT")
	log.Println("  handle routes:", oneRoute, "GET,DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", created_at FROM %s.\"%s\" ", schema, resource)
	sqlWhereOne := "WHERE " + compareString(columns[:propertiesIndex]) + ";"
	sqlWhereAll := ""
	if propertiesIndex > 1 {
		sqlWhereAll += "WHERE " + compareString(columns[1:propertiesIndex])
	}
	sqlWhereAll += " ORDER BY created_at DESC;"
	sqlWhereAllPlusOneExternalIndex := ""
	sqlWhereAllPlusOneExternalIndex += "WHERE " + compareString(columns[1:propertiesIndex], "%s") + ";"
	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource) + "(" + strings.Join(columns[1:], ", ") + ", created_at)"
	insertQuery += "VALUES(" + parameterString(len(columns)) + ") RETURNING " + this + "_id;"

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

	// store scan values function and read query for later use in relations
	b.scanValueFunctions[this] = createScanValuesAndObject
	b.readQuery[this] = readQuery

	// POST
	router.HandleFunc(allRoute, func(w http.ResponseWriter, r *http.Request) {
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
		values := make([]interface{}, len(columns))
		for i, k := range columns {
			if i < propertiesIndex {
				if i == 0 { // skip ID, we get it generated from database
					_, ok := bodyJSON[k]
					if ok {
						http.Error(w, "must not specify "+k, http.StatusBadRequest)
						return
					}
					continue
				}
				param, _ := params[k]
				value, ok := bodyJSON[k]
				if ok && param != value.(string) {
					http.Error(w, "illegal "+k, http.StatusBadRequest)
					return
				}
				values[i-1] = param

			} else if i > propertiesIndex {
				value, ok := bodyJSON[k]
				if !ok {
					http.Error(w, "missing property "+k, http.StatusBadRequest)
					return
				}
				values[i-1] = value
			} else {
				properties, ok := bodyJSON[k]
				if ok {
					propertiesJSON, _ := json.Marshal(properties)
					values[i-1] = propertiesJSON
				} else {
					values[i-1] = []byte("{}")
				}
			}
		}

		// last value is created_at
		createdAt := time.Now().UTC()
		if value, ok := bodyJSON["created_at"]; ok {
			t, err := time.Parse(time.RFC3339, value.(string))
			if err != nil {
				http.Error(w, "illegal created_at: "+err.Error(), http.StatusBadRequest)
				return
			}
			createdAt = t.UTC()
		}
		values[len(columns)-1] = &createdAt

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
	router.HandleFunc(allRoute, func(w http.ResponseWriter, r *http.Request) {
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

		// minor trick. We add the primary id form the json to the parameters. This is because the route does
		// not contain the primary ID for convenience
		value, ok := bodyJSON[columns[0]]
		if !ok {
			http.Error(w, "missing "+columns[0], http.StatusBadRequest)
			return
		}
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
					http.Error(w, "missing property "+k, http.StatusBadRequest)
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
			t, err := time.Parse(time.RFC3339, value.(string))
			if err != nil {
				http.Error(w, "illegal created_at: "+err.Error(), http.StatusBadRequest)
				return
			}
			createdAt = t.UTC()
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

		status, err := b.maybeAddChildrenToGetResponse(r, response)
		if err != nil {
			http.Error(w, err.Error(), status)
			return
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	}).Methods(http.MethodGet)

	// GET all
	router.HandleFunc(allRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		var queryParameters []interface{}
		var sqlQuery string
		externalColumn := ""
		externalIndex := ""

		urlQuery := r.URL.Query()
		for i := propertiesIndex + 1; i < len(columns); i++ {
			if externalIndex = urlQuery.Get(columns[i]); externalIndex != "" {
				externalColumn = columns[i]
				break
			}
		}

		params := mux.Vars(r)
		if externalIndex == "" { // get entire collection
			sqlQuery = readQuery + sqlWhereAll
			queryParameters = make([]interface{}, propertiesIndex-1)
			for i := 1; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-1] = params[columns[i]]
			}
		} else {
			sqlQuery = fmt.Sprintf(readQuery+sqlWhereAllPlusOneExternalIndex, externalColumn)
			queryParameters = make([]interface{}, propertiesIndex)
			for i := 1; i < propertiesIndex; i++ { // skip ID
				queryParameters[i-1] = params[columns[i]]
			}
			queryParameters[propertiesIndex-1] = externalIndex
		}

		rows, err := b.db.Query(sqlQuery, queryParameters...)
		if err != nil {
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest) // TODO when is this StatusInternalServerError?
				return
			}
		}
		response := []interface{}{}
		defer rows.Close()
		for rows.Next() {
			values, object := createScanValuesAndObject()
			err := rows.Scan(values...)
			if err != nil {
				log.Println("error when scanning: ", err.Error())
			}
			response = append(response, object)
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		w.Header().Set("Content-Type", "application/json")
		w.Write(jsonData)
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

func (b *Backend) createBackendHandlerSingleResource(router *mux.Router, rc resourceConfiguration) {

	schema := b.schema
	resource := rc.Resource
	log.Println("create single resource:", resource)

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

	if len(columns) > 1 {
		createColumn := "UNIQUE (" + strings.Join(columns, ",") + ")"
		createColumns = append(createColumns, createColumn)
	}
	createColumn := "UNIQUE (" + owner + "_id )" // force the resource to be single
	createColumns = append(createColumns, createColumn)

	createColumns = append(createColumns, "properties json NOT NULL DEFAULT '{}'::jsonb")
	propertiesIndex := len(columns)
	columns = append(columns, "properties")

	for _, index := range rc.ExternalUniqueIndices {
		createColumn := fmt.Sprintf("\"%s\" varchar NOT NULL", index)
		uniqueColumn := fmt.Sprintf("UNIQUE(\"%s\")", index)
		createColumns = append(createColumns, createColumn)
		createColumns = append(createColumns, uniqueColumn)
		columns = append(columns, index)
	}

	createExternalIndicesQuery := ""
	for _, index := range rc.ExternalIndices {
		createColumn := fmt.Sprintf("\"%s\" varchar NOT NULL", index)
		createExternalIndicesQuery = createExternalIndicesQuery + fmt.Sprintf("CREATE index IF NOT EXISTS %s ON %s.\"%s\"(%s);",
			"external_index_"+this+"_"+index,
			schema, resource, index)
		createColumns = append(createColumns, createColumn)
		columns = append(columns, index)
	}

	for _, property := range rc.StaticProperties {
		createColumn := fmt.Sprintf("\"%s\" varchar NOT NULL", property)
		createColumns = append(createColumns, createColumn)
		columns = append(columns, property)
	}

	createQuery += "(" + strings.Join(createColumns, ", ") + ");" + createExternalIndicesQuery

	_, err := b.db.Query(createQuery)
	if err != nil {
		panic(err)
	}

	allRoute := ""
	oneRoute := ""
	for _, r := range resources {
		allRoute = oneRoute + "/" + r
		oneRoute = oneRoute + "/" + plural(r) + "/{" + r + "_id}"
	}

	log.Println("  handle single routes:", allRoute, "GET,PUT,DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", created_at FROM %s.\"%s\" ", schema, resource)
	sqlWhereSingle := ""
	if propertiesIndex > 1 {
		sqlWhereSingle += "WHERE " + compareString(columns[1:propertiesIndex])
	}
	sqlWhereSingle += ";"
	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource)
	insertQuery += "(" + strings.Join(columns[1:], ", ") + ", created_at)"
	insertQuery += " VALUES(" + parameterString(len(columns)) + ") ON CONFLICT (" + owner + "_id) DO UPDATE SET "
	sets := make([]string, len(columns)-1)
	for i := 1; i < len(columns); i++ {
		sets[i-1] = columns[i] + " = $" + strconv.Itoa(i)
	}
	insertQuery += strings.Join(sets, ", ") + ", created_at = $" + strconv.Itoa(len(columns))
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

	// store scan values function and read query for later use in relations
	b.scanValueFunctions[this] = createScanValuesAndObject
	b.readQuery[this] = readQuery

	// PUT single
	router.HandleFunc(allRoute, func(w http.ResponseWriter, r *http.Request) {
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
		values := make([]interface{}, len(columns))
		for i, k := range columns {
			if i < propertiesIndex {
				if i == 0 { // skip ID, we get it generated from database
					continue
				}
				param, _ := params[k]
				value, ok := bodyJSON[k]
				if ok && param != value.(string) {
					http.Error(w, "illegal "+k, http.StatusBadRequest)
					return
				}
				values[i-1] = param

			} else if i > propertiesIndex {
				value, ok := bodyJSON[k]
				if !ok {
					http.Error(w, "missing property "+k, http.StatusBadRequest)
					return
				}
				values[i-1] = value
			} else {
				properties, ok := bodyJSON[k]
				if ok {
					propertiesJSON, _ := json.Marshal(properties)
					values[i-1] = propertiesJSON
				} else {
					values[i-1] = []byte("{}")
				}
			}
		}

		// last value is created_at
		createdAt := time.Now().UTC()
		if value, ok := bodyJSON["created_at"]; ok {
			t, err := time.Parse(time.RFC3339, value.(string))
			if err != nil {
				http.Error(w, "illegal created_at: "+err.Error(), http.StatusBadRequest)
				return
			}
			createdAt = t.UTC()
		}
		values[len(columns)-1] = &createdAt

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

	// GET single
	router.HandleFunc(allRoute, func(w http.ResponseWriter, r *http.Request) {
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
			// not an error, single resource are always conceptually there
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		status, err := b.maybeAddChildrenToGetResponse(r, response)
		if err != nil {
			http.Error(w, err.Error(), status)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		jsonData, _ := json.MarshalIndent(response, "", " ")
		w.Write(jsonData)
	}).Methods(http.MethodGet)

	// DELETE single
	router.HandleFunc(allRoute, func(w http.ResponseWriter, r *http.Request) {
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

	for _, rc := range b.config.Resources {
		if rc.Single {
			b.createBackendHandlerSingleResource(router, rc)
		} else {
			b.createBackendHandlerResource(router, rc)
		}
	}

	for _, rc := range b.config.Relations {
		b.createBackendHandlerRelation(router, rc)
	}
}

func (b *Backend) maybeAddChildrenToGetResponse(r *http.Request, response map[string]interface{}) (int, error) {
	if children, ok := r.URL.Query()["children"]; ok {
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
	}
	return http.StatusOK, nil
}

func (b *Backend) createBackendHandlerRelation(router *mux.Router, rc relationConfiguration) {
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

	readQuery := b.readQuery[this]
	createScanValuesAndObject := b.scanValueFunctions[this]

	sqlWhereAll := fmt.Sprintf("WHERE %s_id IN (SELECT %s_id FROM %s.\"%s\" WHERE %s);", this, this, schema, resource, compareString(resourceColumns[:len(resourceColumns)-1]))
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

		rows, err := b.db.Query(readQuery+sqlWhereAll, queryParameters...)
		if err != nil {
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest) // TODO when is this StatusInternalServerError?
				return
			}
		}
		response := []interface{}{}
		defer rows.Close()
		for rows.Next() {
			values, object := createScanValuesAndObject()
			err := rows.Scan(values...)
			if err != nil {
				log.Println("error when scanning: ", err.Error())
			}
			response = append(response, object)
		}

		w.Header().Set("Content-Type", "application/json")
		jsonData, _ := json.MarshalIndent(response, "", " ")
		w.Write(jsonData)
	}).Methods(http.MethodGet)

	// GET one
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		queryParameters := make([]interface{}, len(resourceColumns))
		for i := 0; i < len(resourceColumns); i++ {
			queryParameters[i] = params[resourceColumns[i]]
		}

		values, response := createScanValuesAndObject()
		err := b.db.QueryRow(readQuery+sqlWhereOne, queryParameters...).Scan(values...)
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
