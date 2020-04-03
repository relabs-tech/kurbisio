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
	"github.com/relabs-tech/backends/core/sql"
)

func (b *Backend) createBlobResource(router *mux.Router, rc blobConfiguration) {
	schema := b.db.Schema
	resource := rc.Resource
	log.Println("create blob:", resource)

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

	jsonToHeader := map[string]string{}

	// static properties are varchars
	for _, property := range rc.StaticProperties {
		createColumn := fmt.Sprintf("\"%s\" varchar NOT NULL DEFAULT ''", property)
		createColumns = append(createColumns, createColumn)
		columns = append(columns, property)
		jsonToHeader[property] = jsonNameToCanonicalHeader(property)
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
		jsonToHeader[property] = jsonNameToCanonicalHeader(property)
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
		jsonToHeader[name] = jsonNameToCanonicalHeader(name)
	}

	// the actual blob data as bytea
	createColumn := "blob bytea NOT NULL"
	createColumns = append(createColumns, createColumn)

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

	log.Println("  handle blob routes:", collectionRoute, "GET,POST")
	log.Println("  handle blob routes:", oneRoute, "GET,PUT, DELETE")

	readQuery := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", blob, created_at FROM %s.\"%s\" ", schema, resource)
	readQueryMetaDataOnly := "SELECT " + strings.Join(columns, ", ") + fmt.Sprintf(", created_at FROM %s.\"%s\" ", schema, resource)
	sqlWhereOne := "WHERE " + compareString(columns[:propertiesIndex]) + ";"

	readQueryWithTotal := "SELECT " + strings.Join(columns, ", ") +
		fmt.Sprintf(", created_at, count(*) OVER() AS full_count FROM %s.\"%s\" ", schema, resource)
	sqlWhereAll := "WHERE "
	if propertiesIndex > 1 {
		sqlWhereAll += compareString(columns[1:propertiesIndex]) + " AND "
	}
	sqlWhereAll += fmt.Sprintf("($%d OR created_at<=$%d) AND created_at>=$%d ",
		propertiesIndex, propertiesIndex+1, propertiesIndex+2)

	sqlWhereAllPlusOneExternalIndex := sqlWhereAll + fmt.Sprintf("AND %%s = $%d ", propertiesIndex+5)

	sqlPagination := fmt.Sprintf("ORDER BY created_at DESC LIMIT $%d OFFSET $%d;", propertiesIndex+3, propertiesIndex+4)

	deleteQuery := fmt.Sprintf("DELETE FROM %s.\"%s\" ", schema, resource)

	insertQuery := fmt.Sprintf("INSERT INTO %s.\"%s\" ", schema, resource) + "(" + strings.Join(columns, ", ") + ", blob, created_at)"
	insertQuery += "VALUES(" + parameterString(len(columns)+2) + ") RETURNING " + this + "_id;"

	updateQuery := fmt.Sprintf("UPDATE %s.\"%s\" SET ", schema, resource)
	sets := make([]string, len(columns)-propertiesIndex)
	for i := propertiesIndex; i < len(columns); i++ {
		sets[i-propertiesIndex] = columns[i] + " = $" + strconv.Itoa(i+1)
	}
	updateQuery += strings.Join(sets, ", ") + ", blob = $" + strconv.Itoa(len(columns)+1) + " " + sqlWhereOne

	maxAge := "max-age=31536000" // one year worth of seconds
	if rc.MaxAgeCache > 0 {
		maxAge = fmt.Sprintf("max-age=%d", rc.MaxAgeCache)
	} else if rc.MaxAgeCache < 0 {
		maxAge = ""
	}

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

	createScanValuesAndObjectWithBlob := func(blob *[]byte) ([]interface{}, map[string]interface{}) {
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
		values[len(columns)] = blob
		createdAt := &time.Time{}
		values[len(columns)+1] = createdAt
		object["created_at"] = values[len(columns)+1]
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
		values[len(columns)+1] = totalCount
		return values, object
	}

	getCollection := func(w http.ResponseWriter, r *http.Request, relation *relationInjection) {
		var queryParameters []interface{}
		var sqlQuery string
		limit := 100
		page := 1
		until := time.Time{}
		from := time.Time{}
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
		queryParameters[propertiesIndex-1+0] = until.IsZero()
		queryParameters[propertiesIndex-1+1] = until.UTC()
		queryParameters[propertiesIndex-1+2] = from.UTC()
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

		var blob []byte
		values, response := createScanValuesAndObjectWithBlob(&blob)

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
		w.Header().Set("Kurbisio-Meta-Data", string(*response["properties"].(*json.RawMessage)))
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Length", strconv.Itoa(len(blob)))
		if len(maxAge) > 0 {
			w.Header().Set("Cache-Control", maxAge)
		}
		w.Write(blob)
	}

	collection := collectionHelper{
		getCollection: getCollection,
		getOne:        getOne,
	}

	// store the collection helper for later usage in relations
	b.collectionHelper[this] = &collection

	// CREATE
	router.HandleFunc(collectionRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		blob, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		params := mux.Vars(r)

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
		for i := 1; i < len(columns); i++ {
			k := columns[i]
			if i < propertiesIndex { // the core identifiers
				param, _ := params[k]
				values[i] = param
			} else if i == propertiesIndex { // the dynamic properties
				values[i] = metaData
			} else if i < propertiesEndIndex { // static properties, non mandatory
				values[i] = r.Header.Get(jsonToHeader[k])
			} else { // external (unique) indices, mandatory
				value := r.Header.Get(jsonToHeader[k])
				if len(value) == 0 {
					http.Error(w, "missing external index "+k, http.StatusBadRequest)
					return
				}
				values[i] = value
			}
		}

		// next is the blob itself
		values[len(columns)] = &blob

		// last value is created_at
		createdAt := time.Now().UTC()
		values[len(columns)+1] = &createdAt

		var id uuid.UUID
		err = b.db.QueryRow(insertQuery, values...).Scan(&id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// re-read meta data and return as json
		values, response := createScanValuesAndObject()
		err = b.db.QueryRow(readQueryMetaDataOnly+"WHERE "+this+"_id = $1;", id).Scan(values...)
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
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		blob, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		params := mux.Vars(r)

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

		// first add the values for the where-query.
		for i := 0; i < propertiesIndex; i++ {
			values[i] = params[columns[i]]
		}

		// the meta data as dynamic properties
		values[propertiesIndex] = metaData

		// build the update set
		for i := propertiesIndex + 1; i < len(columns); i++ {
			k := columns[i]
			values[i] = r.Header.Get(jsonToHeader[k])
		}

		// next is the blob itself
		values[len(columns)] = &blob

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
		values, response := createScanValuesAndObject()
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
		if hasNotificationUpdate && b.notifier != nil {
			b.notifier.Notify(resource, core.OperationUpdate, jsonData)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	}).Methods(http.MethodPut)

	// READ
	router.HandleFunc(oneRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)

		getOne(w, r)
	}).Methods(http.MethodGet)

	// LIST
	router.HandleFunc(collectionRoute, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method)
		getCollection(w, r, nil)
	}).Methods(http.MethodGet)

	// DELETE
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

}
