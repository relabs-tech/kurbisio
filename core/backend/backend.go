package backend

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"net/http"

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
		router:           bb.Router,
		collectionHelper: make(map[string]*collectionHelper),
		Registry:         registry.New(bb.DB),
	}

	access.HandleAuthorizationRoute(b.router)
	b.handleRoutes(b.router)
	return b
}

type anyResourceConfiguration struct {
	resource   string
	collection *collectionConfiguration
	singleton  *singletonConfiguration
	blob       *blobConfiguration
	relation   *relationConfiguration
}

type byDepth []anyResourceConfiguration

func (r byDepth) Len() int {
	return len(r)
}
func (r byDepth) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
func (r byDepth) Less(i, j int) bool {
	return strings.Count(r[i].resource, "/") < strings.Count(r[j].resource, "/")
}

// HandleRoutes adds all necessary handlers for the specified configuration
func (b *Backend) handleRoutes(router *mux.Router) {

	log.Println("backend: HandleRoutes")

	// we combine all types of resources into one and sort them by depth. Rationale: dependencies of
	// resources must be generated first, otherwise we cannot enforce those dependencies via sql
	// foreign keys
	allResources := []anyResourceConfiguration{}
	for i := range b.config.Collections {
		rc := &b.config.Collections[i]
		allResources = append(allResources, anyResourceConfiguration{resource: rc.Resource, collection: rc})
	}

	for i := range b.config.Singletons {
		rc := &b.config.Singletons[i]
		allResources = append(allResources, anyResourceConfiguration{resource: rc.Resource, singleton: rc})
	}

	for i := range b.config.Blobs {
		rc := &b.config.Blobs[i]
		allResources = append(allResources, anyResourceConfiguration{resource: rc.Resource, blob: rc})
	}

	for i := range b.config.Relations {
		rc := &b.config.Relations[i]
		allResources = append(allResources, anyResourceConfiguration{resource: rc.Resource, relation: rc})
	}
	sort.Sort(byDepth(allResources))

	for _, rc := range allResources {
		if rc.collection != nil {
			b.createCollectionResource(router, *rc.collection)
		}
		if rc.singleton != nil {
			b.createSingletonResource(router, *rc.singleton)
		}
		if rc.blob != nil {
			b.createBlobResource(router, *rc.blob)
		}
		if rc.relation != nil {
			b.createRelationResource(router, *rc.relation)
		}
	}
}

type relationInjection struct {
	subquery        string
	columns         []string
	queryParameters []interface{}
}

type collectionHelper struct {
	getCollection func(w http.ResponseWriter, r *http.Request, relation *relationInjection)
	getOne        func(w http.ResponseWriter, r *http.Request)
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

// propertyNameToCanonicalHeader converts kurbisio JSON property names
// to their canonical header representation. Example: "content_type"
// becomes "Content-Type".
func jsonNameToCanonicalHeader(property string) string {
	parts := strings.Split(property, "_")
	for i := 0; i < len(parts); i++ {
		s := parts[i]
		if len(s) == 0 {
			continue
		}
		s = strings.ToLower(s)
		runes := []rune(s)
		r := runes[0]
		if 'A' <= r && r <= 'Z' {
			r += 'a' - 'A'
			runes[0] = r
		}
		parts[i] = string(runes)
	}
	return strings.Join(parts, "-")
}
