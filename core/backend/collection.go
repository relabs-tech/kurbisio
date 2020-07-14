package backend

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"

	"net/http"
	"net/http/httptest"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core"
	"github.com/relabs-tech/backends/core/access"
	"github.com/relabs-tech/backends/core/csql"
)

func (b *Backend) createCollectionResource(router *mux.Router, rc collectionConfiguration, singleton bool) {
	schema := b.db.Schema
	resource := rc.Resource

	if singleton {
		log.Println("create singleton collection:", resource)
	} else {
		log.Println("create collection:", resource)
	}
	if rc.Description != "" {
		log.Println("  description:", rc.Description)
	}

	if rc.PropertiesSchemaID != "" {
		if !b.jsonValidator.HasSchema(rc.PropertiesSchemaID) {
			log.Printf("ERROR: invalid configuration for resource %s, schemaID %s is unknown. Validation is deactivated for this resource",
				rc.Resource, rc.PropertiesSchemaID)
		}
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
		"revision INTEGER NOT NULL DEFAULT 1",
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
	singletonConstraint := ""
	if len(columns) > 1 {
		createColumn := "UNIQUE (" + strings.Join(columns, ",") + ")"
		createColumns = append(createColumns, createColumn)
	}

	if singleton {
		// force the resource itself to be singleton resource
		singletonConstraint = fmt.Sprintf("only_one_%s_per_%s", this, owner)
		createColumn := "CONSTRAINT " + singletonConstraint + " UNIQUE (" + strings.Join(columns, ",") + ")"
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
			log.Printf("Error while updating schema when running: %s", createQuery)
			panic(err)
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
		log.Println("  handle singleton routes:", singletonRoute, "GET,PUT,PATCH,DELETE")
	}
	log.Println("  handle collection routes:", listRoute, "GET,POST,PUT,PATCH,DELETE")
	log.Println("  handle collection routes:", itemRoute, "GET,PUT,PATCH,DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", created_at, revision, state FROM %s.\"%s\" ", schema, resource)
	sqlWhereOne := "WHERE " + compareIDsString(columns[:propertiesIndex])

	readQueryWithTotal := "SELECT " + strings.Join(columns, ", ") +
		fmt.Sprintf(", created_at, revision, state, count(*) OVER() AS full_count FROM %s.\"%s\" ", schema, resource)
	sqlWhereAll := "WHERE "
	if propertiesIndex > 1 {
		sqlWhereAll += compareIDsString(columns[1:propertiesIndex]) + " AND "
	}
	sqlWhereAll += fmt.Sprintf("($%d OR created_at<=$%d) AND ($%d OR created_at>=$%d) AND state=$%d ",
		propertiesIndex, propertiesIndex+1, propertiesIndex+2, propertiesIndex+3, propertiesIndex+4)
	sqlPagination := fmt.Sprintf("ORDER BY created_at DESC,%s DESC LIMIT $%d OFFSET $%d;", columns[0], propertiesIndex+5, propertiesIndex+6)

	sqlWhereAllPlusOneExternalIndex := sqlWhereAll + fmt.Sprintf("AND %%s = $%d ", propertiesIndex+7)

	clearQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" WHERE ", schema, resource) + compareIDsString(columns[1:propertiesIndex]) + ";"
	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)
	sqlReturnState := " RETURNING " + this + "_id, state;"

	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource) + "(" + strings.Join(columns, ", ") + ", created_at, state)"
	insertQuery += "VALUES(" + parameterString(len(columns)+2) + ")"
	if singleton {
		insertQuery += "ON CONFLICT ON CONSTRAINT \"" + singletonConstraint + "\" DO NOTHING"
	}
	insertQuery += " RETURNING " + this + "_id;"

	updateQuery := fmt.Sprintf("UPDATE %s.\"%s\" SET ", schema, resource)
	sets := make([]string, len(columns)-propertiesIndex)
	for i := propertiesIndex; i < len(columns); i++ {
		sets[i-propertiesIndex] = columns[i] + " = $" + strconv.Itoa(i+1)
	}
	updateQuery += strings.Join(sets, ", ") + ", created_at = $" + strconv.Itoa(len(columns)+1) + ", state = $" + strconv.Itoa(len(columns)+2)
	updateQuery += ", revision = revision + 1 " + sqlWhereOne + " RETURNING " + this + "_id;"

	createScanValuesAndObject := func(createdAt *time.Time, revision *int, extra ...interface{}) ([]interface{}, map[string]interface{}) {
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

		values[i] = createdAt
		object["created_at"] = createdAt
		i++
		values[i] = revision
		object["revision"] = revision
		values = append(values, extra...)
		return values, object
	}

	list := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
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
			var createdAt time.Time
			values, object := createScanValuesAndObject(&createdAt, new(int), &state, &totalCount)
			err := rows.Scan(values...)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if len(state) > 0 {
				object["state"] = &state
			}
			// if we did not have from, take it from the first object
			if from.IsZero() {
				from = createdAt
			}
			response = append(response, object)
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		etag := bytesPlusTotalCountToEtag(jsonData, totalCount)
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

	item := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
		params := mux.Vars(r)

		if singleton {
			if len(params[columns[0]]) == 0 || params[columns[0]] == "all" {
				// no primary id, we need an owner
				ownerID := params[owner+"_id"]
				if len(ownerID) == 0 || ownerID == "all" {
					http.Error(w, "all is not a valid "+owner+" for requesting a single "+this+". Did you want to say "+core.Plural(this)+"?", http.StatusBadRequest)
					return
				}
				params[columns[0]] = "all"
			}
		} else {
			if params[columns[0]] == "all" {
				http.Error(w, "all is not a valid "+this, http.StatusBadRequest)
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

		var state string
		values, response := createScanValuesAndObject(&time.Time{}, new(int), &state)
		err = b.db.QueryRow(readQuery+sqlWhereOne+subQuery+";", queryParameters...).Scan(values...)
		if err == csql.ErrNoRows {
			if singleton {
				w.WriteHeader(http.StatusNoContent)
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
			}
			http.Error(w, err.Error(), status)
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

		if len(state) > 0 {
			response["state"] = &state
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
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

	itemWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		if b.authorizationEnabled {
			auth := access.AuthorizationFromContext(r.Context())
			if !auth.IsAuthorized(resources, core.OperationRead, params, rc.Permits) {
				http.Error(w, "not authorized", http.StatusUnauthorized)
				return
			}
		}

		item(w, r, nil)
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

		// for singleton, primary id can be "all"
		if singleton && len(params[columns[0]]) == 0 {
			params[columns[0]] = "all"
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
		var state string
		var primaryID uuid.UUID
		err = b.db.QueryRow(deleteQuery+sqlWhereOne+sqlReturnState, queryParameters...).Scan(&primaryID, &state)
		if err == csql.ErrNoRows {
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
		jsonData, _ := json.MarshalIndent(notification, "", " ")
		err = b.commitWithNotification(tx, resource, state, core.OperationDelete, primaryID, jsonData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)

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

		_, err = b.db.Query(clearQuery, queryParameters...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}

	create := func(w http.ResponseWriter, r *http.Request, bodyJSON map[string]interface{}) {
		params := mux.Vars(r)
		if bodyJSON == nil {
			err := json.NewDecoder(r.Body).Decode(&bodyJSON)
			if err != nil {
				http.Error(w, "invalid json data: "+err.Error(), http.StatusBadRequest)
				return
			}
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
			if param == "all" {
				if ok && value != "00000000-0000-0000-0000-000000000000" {
					values[i] = value
				} else {
					http.Error(w, "missing "+columns[i], http.StatusBadRequest)
					return
				}
			} else {
				values[i] = param
			}
		}

		// the dynamic properties
		properties, ok := bodyJSON[columns[i]]
		if ok {
			propertiesJSON, _ := json.Marshal(properties)
			if rc.PropertiesSchemaID != "" {
				if !b.jsonValidator.HasSchema(rc.PropertiesSchemaID) {
					log.Printf("ERROR: invalid configuration for resource %s, schemaID %s is unknown. Validation is deactivated for this resource", rc.Resource, rc.PropertiesSchemaID)
				} else if err := b.jsonValidator.ValidateString(string(propertiesJSON), rc.PropertiesSchemaID); err != nil {
					http.Error(w, fmt.Sprintf("properties '%v' field does not follow schemaID %s, %v",
						string(propertiesJSON), rc.PropertiesSchemaID, err), http.StatusBadRequest)
					return
				}
			}
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

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var id uuid.UUID
		err = tx.QueryRow(insertQuery, values...).Scan(&id)
		if err == csql.ErrNoRows {
			tx.Rollback()
			http.Error(w, "singleton "+this+" already exists", http.StatusConflict)
			return
		} else if err != nil {
			status := http.StatusInternalServerError
			// Non unique external keys are reported as code Code 23505
			if err, ok := err.(*pq.Error); ok && err.Code == "23505" {
				status = http.StatusConflict
			}
			tx.Rollback()
			http.Error(w, err.Error(), status)
			return
		}

		// re-read data and return as json
		values, response := createScanValuesAndObject(&time.Time{}, new(int), &state)
		err = tx.QueryRow(readQuery+"WHERE "+this+"_id = $1;", id).Scan(values...)
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if len(state) > 0 {
			response["state"] = state
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		err = b.commitWithNotification(tx, resource, state, core.OperationCreate, id, jsonData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
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

	updateWithAuth := func(w http.ResponseWriter, r *http.Request) {
		params := mux.Vars(r)
		var bodyJSON map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&bodyJSON)
		if err != nil {
			http.Error(w, "invalid json data: "+err.Error(), http.StatusBadRequest)
			return
		}

		// primary id can come from parameter (fully qualified put) or from body json (collection put).
		// For singleton, primary id can be "all", but then we need an owner
		primaryID := params[columns[0]]
		var ownerID string
		if len(primaryID) == 0 {
			if singleton {
				primaryID = "all"
				ownerID = params[owner+"_id"]
				if len(ownerID) == 0 || ownerID == "all" {
					var ok bool
					ownerID, ok = bodyJSON[owner+"_id"].(string)
					if !ok {
						http.Error(w, "missing "+owner+"_id", http.StatusBadRequest)
						return
					}
					params[owner+"_id"] = ownerID
				}
			} else {
				var ok bool
				primaryID, ok = bodyJSON[columns[0]].(string)
				if !ok {
					http.Error(w, "missing "+columns[0], http.StatusBadRequest)
					return
				}
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

		revision := 0
		if r, ok := bodyJSON["revision"].(float64); ok {
			revision = int(r)
		}

		tx, err := b.db.BeginTx(r.Context(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var currentRevision int
		retried := false
	Retry:

		current, object := createScanValuesAndObject(&time.Time{}, &currentRevision, new(string))
		if singleton && primaryID == "all" {
			err = tx.QueryRow(readQuery+"WHERE "+owner+"_id = $1 FOR UPDATE;", &ownerID).Scan(current...)
		} else {
			err = tx.QueryRow(readQuery+"WHERE "+this+"_id = $1 FOR UPDATE;", &primaryID).Scan(current...)
		}
		if err == csql.ErrNoRows {
			// item does not exist yet. If we have the right permissions, we can create it. Otherwise
			// we are forced to return 404 Not Found
			if b.authorizationEnabled {
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
				w.WriteHeader(http.StatusCreated)
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.Write(rec.Body.Bytes())
				return
			} else if rec.Code == http.StatusConflict && !retried {
				// race condition: somebody else has create the object right now
				retried = true
				goto Retry
			}
			err = tx.Rollback()
			http.Error(w, "no such "+this, http.StatusNotFound)
			return
		}
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if revision != 0 && revision != currentRevision {
			tx.Rollback()
			http.Error(w, this+" revision does not match", http.StatusConflict)
			return
		}

		primaryID = current[0].(*uuid.UUID).String()

		// for MethodPatch we get the existing object from the database and patch property by property
		if r.Method == http.MethodPatch {

			// convert object into generic json for patching (the datatypes are different compared to the database) in the database)
			body, _ := json.Marshal(object)
			var objectJSON map[string]interface{}
			json.Unmarshal(body, &objectJSON)

			// now bodyJSON from the request becomes a patch
			patchObject(objectJSON, bodyJSON)

			// rewrite this put request to contain the entire (patched) object
			bodyJSON = objectJSON
		}

		// build insert query and validate that we have all parameters
		values := make([]interface{}, len(columns)+2)

		var i int

		// validate core identifiers
		// Rationale: we have authorized the resource based on the parameters
		// in the URL, so we have to ensure that that the object to update
		// is that very object, and that the update does not try to
		// change its identity
		for i = 0; i < propertiesIndex; i++ {
			k := columns[i]

			values[i] = current[i]
			idAsString := values[i].(*uuid.UUID).String()

			// validate that the paramaters  match the object
			values[i] = current[i]
			if params[k] != "all" && params[k] != idAsString {
				tx.Rollback()
				http.Error(w, "no such "+this, http.StatusNotFound)
				return
			}

			// validate that the body json matches the object
			value, ok := bodyJSON[k]
			// zero uuid counts as no uuid
			ok = ok && value.(string) != "00000000-0000-0000-0000-000000000000"
			if ok && value.(string) != idAsString {
				tx.Rollback()
				http.Error(w, "illegal "+k, http.StatusConflict)
				return
			}
		}

		// build the update set
		properties, ok := bodyJSON["properties"]
		if ok {
			propertiesJSON, _ := json.Marshal(properties)
			if rc.PropertiesSchemaID != "" {
				if !b.jsonValidator.HasSchema(rc.PropertiesSchemaID) {
					log.Printf("ERROR: invalid configuration for resource %s, schemaID %s is unknown. Validation is deactivated for this resource",
						rc.Resource, rc.PropertiesSchemaID)
				} else if err := b.jsonValidator.ValidateString(string(propertiesJSON), rc.PropertiesSchemaID); err != nil {
					http.Error(w, fmt.Sprintf("properties '%v' field does not follow schemaID %s, %v",
						string(propertiesJSON), rc.PropertiesSchemaID, err), http.StatusBadRequest)
					return
				}
			}
			values[i] = propertiesJSON
		} else {
			values[i] = []byte("{}")
		}
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

		// next value is created_at
		createdAt := time.Now().UTC()
		if value, ok := bodyJSON["created_at"]; ok {
			timestamp, ok := value.(string)
			if !ok {
				tx.Rollback()
				http.Error(w, "illegal created_at", http.StatusBadRequest)
				return
			}
			t, err := time.Parse(time.RFC3339, timestamp)
			if err != nil {
				tx.Rollback()
				http.Error(w, "illegal created_at: "+err.Error(), http.StatusBadRequest)
				return
			}
			createdAt = t.UTC()
		}
		values[i] = &createdAt
		i++

		// then state
		var state string
		if value, ok := bodyJSON["state"]; ok {
			state, ok = value.(string)
			if !ok {
				tx.Rollback()
				http.Error(w, "state must be a string", http.StatusBadRequest)
				return
			}
		}
		values[i] = &state
		i++

		err = tx.QueryRow(updateQuery, values...).Scan(&primaryID)
		if err == csql.ErrNoRows {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		} else if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// re-read new values
		values, response := createScanValuesAndObject(&time.Time{}, &revision, &state)
		err = tx.QueryRow(readQuery+"WHERE "+this+"_id = $1;", &primaryID).Scan(values...)
		if err != nil {
			tx.Rollback()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if len(state) > 0 {
			response["state"] = state
		}

		jsonData, _ := json.MarshalIndent(response, "", " ")
		err = b.commitWithNotification(tx, resource, state, core.OperationUpdate, *values[0].(*uuid.UUID), jsonData)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	}

	// store the collection functions  for later usage in relations
	b.collectionFunctions[resource] = &collectionFunctions{
		list: list,
		item: item,
	}

	// CREATE
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		createWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPost)

	// UPDATE/CREATE with id
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		updateWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut, http.MethodPatch)

	// UPDATE/CREATE with fully qualified path
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		updateWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut, http.MethodPatch)

	// READ
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		itemWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodGet)

	// READ ALL
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		listWithAuth(w, r, nil)
	}).Methods(http.MethodOptions, http.MethodGet)

	// DELETE
	router.HandleFunc(itemRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		deleteWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

	// CLEAR
	router.HandleFunc(listRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		clearWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

	if !singleton {
		return
	}

	// READ
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		itemWithAuth(w, r)

	}).Methods(http.MethodOptions, http.MethodGet)

	// UPDATE
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		updateWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodPut, http.MethodPatch)

	// DELETE
	router.HandleFunc(singletonRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		deleteWithAuth(w, r)
	}).Methods(http.MethodOptions, http.MethodDelete)

}
