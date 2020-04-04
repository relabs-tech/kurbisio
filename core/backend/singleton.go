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

func (b *Backend) createSingletonResource(router *mux.Router, rc singletonConfiguration) {

	schema := b.db.Schema
	resource := rc.Resource
	log.Println("create singleton:", resource)

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

	log.Println("  handle singleton routes:", singletonRoute, "GET,PUT,PATCH,DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", created_at FROM %s.\"%s\" ", schema, resource)
	sqlWhereSingle := ""
	if propertiesIndex > 1 {
		sqlWhereSingle += "WHERE " + compareString(columns[1:propertiesIndex])
	}
	sqlWhereSingle += ";"
	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	insertUpdateQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource)
	insertUpdateQuery += "(" + strings.Join(columns, ", ") + ", created_at)"
	insertUpdateQuery += " VALUES(" + parameterString(len(columns)+1) + ") ON CONFLICT (" + owner + "_id) DO UPDATE SET "
	sets := make([]string, len(columns)-propertiesIndex)
	for i := propertiesIndex; i < len(columns); i++ {
		sets[i-propertiesIndex] = columns[i] + " = $" + strconv.Itoa(i+1)
	}
	insertUpdateQuery += strings.Join(sets, ", ") + ", created_at = $" + strconv.Itoa(len(columns)+1)
	insertUpdateQuery += " RETURNING (xmax = 0) AS inserted, " + this + "_id;" // return whether we did insert or update, this is a psql trick

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

	// CREATE - UPDATE
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		body, _ := ioutil.ReadAll(r.Body)
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationUpdate, access.QualifierAll, params, rc.Permissions) {
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
		values := make([]interface{}, len(columns)+1)
		// the primary resource identifier, use as specified or create a new one
		primaryID, ok := bodyJSON[columns[0]]
		if !ok || primaryID == "00000000-0000-0000-0000-000000000000" {
			primaryID = uuid.New()
		}
		values[0] = primaryID
		// add and validate core identifiers
		for i := 1; i < propertiesIndex; i++ {
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
			sets[i-propertiesIndex] = k + " = $" + strconv.Itoa(i+1)
		}

		// last value is created_at
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
		values[len(values)-1] = &createdAt

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

	// READ
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationRead, access.QualifierOne, params, rc.Permissions) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
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

		jsonData, _ := json.MarshalIndent(response, "", " ")
		etag := bytesToEtag(jsonData)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Etag", etag)
		w.Header().Set("Content-Type", "application/json")

		w.Write(jsonData)
	}).Methods(http.MethodGet)

	// PATCH (READ + UPDATE)
	b.createPatchRoute(router, singletonRoute)

	// DELETE
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationDelete, access.QualifierAll, params, rc.Permissions) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}
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
