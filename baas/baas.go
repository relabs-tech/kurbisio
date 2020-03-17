package baas

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
)

// Backend is the generic rest backend
type Backend struct {
	db                 *sql.DB
	schema             string
	config             backendConfiguration
	scanValueFunctions map[string]func() ([]interface{}, map[string]interface{})
	readQuery          map[string]string
}

// MustNewBackend creates a new backend
func MustNewBackend(configurationJSON string) *Backend {
	var config backendConfiguration
	err := json.Unmarshal([]byte(configurationJSON), &config)
	if err != nil {
		panic(err)
	}

	b := &Backend{
		schema:             "public",
		config:             config,
		readQuery:          make(map[string]string),
		scanValueFunctions: make(map[string]func() ([]interface{}, map[string]interface{})),
	}

	return b
}

// WithSchema sets a database schema name for the generated sql relations. The default
// schema is "public".
func (b *Backend) WithSchema(schema string) *Backend {
	b.schema = schema
	return b
}

// Create creates the sql relations (if they do not exist) and adds routes to the passed router
func (b *Backend) Create(db *sql.DB, router *mux.Router) *Backend {
	b.db = db
	initQuery := fmt.Sprintf("CREATE extension IF NOT EXISTS \"uuid-ossp\";CREATE extension IF NOT EXISTS \"uuid-ossp\"; CREATE schema IF NOT EXISTS \"%s\"", b.schema)
	_, err := b.db.Exec(initQuery)
	if err != nil {
		panic(err)
	}

	b.handleRoutes(router)
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
	ExternalUniqueIndices []string `json:"external_unique_indices"`
	ExternalIndices       []string `json:"external_indices"`
	ExtraProperties       []string `json:"extra_properties"`
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

	for _, property := range rc.ExtraProperties {
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
	for _, o := range resources {
		allRoute = oneRoute + "/" + plural(o)
		oneRoute = oneRoute + "/" + plural(o) + "/{" + o + "_id}"
	}

	log.Println("  handle routes:", allRoute, "GET,POST,PUT")
	log.Println("  handle routes:", oneRoute, "GET,DELETE")

	sqlValues := "VALUES(" + parameterString(len(columns)-1) + ") RETURNING " + this + "_id"
	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(" FROM %s.\"%s\" ", schema, resource)
	sqlWhereOne := "WHERE " + compareString(columns[:propertiesIndex]) + ";"
	sqlWhereAll := ""
	if propertiesIndex > 1 {
		sqlWhereAll += "WHERE " + compareString(columns[1:propertiesIndex])
	}
	sqlWhereAll += " ORDER BY created_at;"
	sqlWhereAllPlusOneExternalIndex := ""
	sqlWhereAllPlusOneExternalIndex += "WHERE " + compareString(columns[1:propertiesIndex], "%s") + ";"
	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	createScanValuesAndObject := func() ([]interface{}, map[string]interface{}) {
		values := make([]interface{}, len(columns))
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
			http.Error(w, "invalid json data", http.StatusBadRequest)
			return
		}

		// build insert query and validate that we have all parameters
		query := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource)
		values := make([]interface{}, len(columns)-1)
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
		var id uuid.UUID
		query += "(" + strings.Join(columns[1:], ", ") + ")" + sqlValues + ";"
		log.Println("QUERY:", query)

		err = b.db.QueryRow(query, values...).Scan(&id)
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

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(response)

	}).Methods(http.MethodPost)

	// PUT
	router.HandleFunc(allRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		body, _ := ioutil.ReadAll(r.Body)
		params := mux.Vars(r)

		var bodyJSON map[string]interface{}
		err := json.Unmarshal(body, &bodyJSON)
		if err != nil {
			http.Error(w, "invalid json data", http.StatusBadRequest)
			return
		}

		query := fmt.Sprintf("UPDATE %s.\"%s\" SET ", schema, resource)
		sets := make([]string, len(columns))
		values := make([]interface{}, len(columns)+propertiesIndex)

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
		query += strings.Join(sets, ", ") + " " + sqlWhereOne
		log.Println("QUERY:", query)
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
		fmt.Println("query:", readQuery+sqlWhereOne)
		err = b.db.QueryRow(readQuery+sqlWhereOne, queryParameters...).Scan(values...)
		if err == sql.ErrNoRows {
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(response)

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
		fmt.Println("query:", readQuery+sqlWhereOne)
		err = b.db.QueryRow(readQuery+sqlWhereOne, queryParameters...).Scan(values...)
		if err == sql.ErrNoRows {
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(response)

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

		w.Header().Set("Content-Type", "application/json")
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(response)

	}).Methods(http.MethodGet)

	// DELETE one
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		params := mux.Vars(r)
		queryParameters := make([]interface{}, propertiesIndex)
		for i := 0; i < propertiesIndex; i++ {
			queryParameters[i] = params[columns[i]]
		}

		fmt.Println("query:", deleteQuery+sqlWhereOne, queryParameters)

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

		if count > 0 {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}

	}).Methods(http.MethodDelete)
}

func (b *Backend) createBackendHandlerSingleResource(router *mux.Router, rc resourceConfiguration) {

	schema := b.schema
	resource := rc.Resource
	log.Println("create single resource:", resource)

	resources := strings.Split(rc.Resource, "/")
	this := resources[len(resources)-1]
	owner := resources[len(resources)-2]
	dependencies := resources[:len(resources)-1]

	createQuery := fmt.Sprintf("CREATE table IF NOT EXISTS %s.\"%s\"", schema, resource)
	createColumns := []string{
		this + "_id uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY",
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

	for _, property := range rc.ExtraProperties {
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
	for _, o := range resources {
		allRoute = oneRoute + "/" + o
		oneRoute = oneRoute + "/" + plural(o) + "/{" + o + "_id}"
	}

	log.Println("  handle single routes:", allRoute, "GET,PUT,DELETE")

	sqlValues := "VALUES(" + parameterString(len(columns)-1) + ") "
	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(" FROM %s.\"%s\" ", schema, resource)
	sqlWhereSingle := ""
	if propertiesIndex > 1 {
		sqlWhereSingle += "WHERE " + compareString(columns[1:propertiesIndex])
	}
	sqlWhereSingle += ";"
	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	createScanValuesAndObject := func() ([]interface{}, map[string]interface{}) {
		values := make([]interface{}, len(columns))
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
			http.Error(w, "invalid json data", http.StatusBadRequest)
			return
		}

		// build insert query and validate that we have all parameters
		query := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource)
		values := make([]interface{}, 2*len(columns)-2)
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
		query += "(" + strings.Join(columns[1:], ", ") + ")" + sqlValues + " ON CONFLICT (" + owner + "_id) DO UPDATE SET "
		sets := make([]string, len(columns)-1)

		offset := len(columns) - 1
		// now build the update query
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
				values[offset+i-1] = param

			} else if i > propertiesIndex {
				value, ok := bodyJSON[k]
				if !ok {
					http.Error(w, "missing property "+k, http.StatusBadRequest)
					return
				}
				values[offset+i-1] = value
			} else {
				properties, ok := bodyJSON[k]
				if ok {
					propertiesJSON, _ := json.Marshal(properties)
					values[offset+i-1] = propertiesJSON
				} else {
					values[offset+i-1] = []byte("{}")
				}
			}
			sets[i-1] = k + " = $" + strconv.Itoa(offset+i)
		}
		query += strings.Join(sets, ", ") + ";"

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

		w.Header().Set("Content-Type", "application/json")
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(response)

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
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(response)

	}).Methods(http.MethodGet)

	// DELETE single
	router.HandleFunc(allRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		params := mux.Vars(r)

		queryParameters := make([]interface{}, propertiesIndex-1)
		for i := 1; i < propertiesIndex; i++ { // skip ID
			queryParameters[i-1] = params[columns[i]]
		}

		fmt.Println("query:", deleteQuery+sqlWhereSingle, queryParameters)

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

		if count > 0 {
			w.WriteHeader(http.StatusNoContent)
		} else {
			w.WriteHeader(http.StatusNotFound)
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

	for _, s := range resources {
		resourceColumns = append(resourceColumns, s+"_id")
		columns[s] = s
	}
	for _, s := range origins {
		originColumns = append(originColumns, s+"_id")
		columns[s] = s
	}

	for s := range columns {
		createColumn := s + "_id uuid"
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
	for _, o := range resources {
		allRoute = oneRoute + "/" + plural(o)
		oneRoute = oneRoute + "/" + plural(o) + "/{" + o + "_id}"
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
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(response)
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
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		encoder.Encode(response)
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
