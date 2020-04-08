package backend

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

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
	db               *sql.DB
	router           *mux.Router
	collectionHelper map[string]*collectionHelper
	// Registry is the JSON object registry for this backend's schema
	Registry             *registry.Registry
	authorizationEnabled bool
	updateSchema         bool
	handlers             []notificationHandler
}

// Builder is a builder helper for the Backend
type Builder struct {
	// Config is the JSON description of all resources and relations. This is mandatory.
	Config string
	// DB is a postgres database. This is mandatory.
	DB *sql.DB
	// Router is a mux router. This is mandatory.
	Router *mux.Router
	// If AuthorizationEnabled is true, the backend requires auhorization for each route
	// in the request context, as specified in the configuration.
	AuthorizationEnabled bool
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
		config:               config,
		db:                   bb.DB,
		router:               bb.Router,
		collectionHelper:     make(map[string]*collectionHelper),
		Registry:             registry.New(bb.DB),
		authorizationEnabled: bb.AuthorizationEnabled,
	}

	registry := b.Registry.Accessor("_backend_")
	var currentVersion string
	registry.Read("schema_version", &currentVersion)
	newVersion := fmt.Sprintf("%x", sha1.Sum([]byte(bb.Config)))
	b.updateSchema = newVersion != currentVersion
	log.Println("new configuration - will update database schema")

	b.handleCORS(b.router)
	access.HandleAuthorizationRoute(b.router)
	b.handleRoutes(b.router)
	if b.updateSchema {
		registry.Write("schema_version", newVersion)
	}
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
			b.createCollectionResource(router, *rc.collection, false)
		}
		if rc.singleton != nil {
			// a singleton is a a specialized collection
			tmp := collectionConfiguration{
				Resource: rc.singleton.Resource,
				Permits:  rc.singleton.Permits,
			}
			b.createCollectionResource(router, tmp, true)
		}
		if rc.blob != nil {
			b.createBlobResource(router, *rc.blob)
		}
		if rc.relation != nil {
			b.createRelationResource(router, *rc.relation)
		}
	}

	for _, sc := range b.config.Shortcuts {
		b.createShortcut(router, sc)
	}
}

type relationInjection struct {
	subquery        string
	columns         []string
	queryParameters []interface{}
}

type collectionHelper struct {
	getAll func(w http.ResponseWriter, r *http.Request, relation *relationInjection)
	getOne func(w http.ResponseWriter, r *http.Request)
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
func compareIDsString(s []string) string {
	result := ""
	i := 0
	for ; i < len(s); i++ {
		if i > 0 {
			result += " AND "
		}
		result += fmt.Sprintf("($%d = 'all' OR %s = $%d::UUID)", i+1, s[i], i+1)
	}
	return result
}

// returns s[0]=$(offset+1) AND ... AND s[n-1]=$(offset+n)
func compareIDsStringWithOffset(offset int, s []string) string {
	result := ""
	i := 0
	for ; i < len(s); i++ {
		if i > 0 {
			result += " AND "
		}
		result += fmt.Sprintf("($%d::VARCHAR = 'all' OR %s = $%d)", i+offset+1, s[i], i+offset+1)
	}
	return result
}

func timeToEtag(t time.Time) string {
	return fmt.Sprintf("\"%x\"", sha1.Sum([]byte(t.String())))
}
func bytesToEtag(b []byte) string {
	return fmt.Sprintf("\"%x\"", sha1.Sum(b))
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
			return status, fmt.Errorf("cannot get child %s", child)
		}
		response[child] = &childJSON
	}
	return http.StatusOK, nil
}

// clever recursive function to patch a generic json object.
func patchObject(object map[string]interface{}, patch map[string]interface{}) {
	for k, v := range patch {
		oc, ocok := object[k].(map[string]interface{})
		pc, pcok := v.(map[string]interface{})
		if ocok && pcok {
			patchObject(oc, pc)
		} else {
			object[k] = v
		}
	}
}

func (b *Backend) createPatchRoute(router *mux.Router, route string) {
	router.HandleFunc(route, func(w http.ResponseWriter, r *http.Request) {
		log.Println("called route for", r.URL, r.Method, "- will do GET and PUT")
		patch, _ := ioutil.ReadAll(r.Body)
		var patchJSON map[string]interface{}
		err := json.Unmarshal(patch, &patchJSON)
		if err != nil {
			http.Error(w, "invalid json data: "+err.Error(), http.StatusBadRequest)
			return
		}

		client := client.New(b.router).WithContext(r.Context())

		var objectJSON map[string]interface{}
		status, err := client.Get(r.URL.Path, &objectJSON)
		if err != nil {
			http.Error(w, "patch: cannot read object: "+err.Error(), status)
			return
		}

		patchObject(objectJSON, patchJSON)

		var response []byte
		status, err = client.Put(r.URL.Path, objectJSON, &response)
		if err != nil {
			http.Error(w, "patch: cannot update object: "+err.Error(), status)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(response)

	}).Methods(http.MethodOptions, http.MethodPatch)
}

func (b *Backend) createShortcut(router *mux.Router, sc shortcutConfiguration) {
	shortcut := sc.Shortcut
	target := sc.Target

	targetResources := strings.Split(target, "/")
	prefix := "/" + shortcut
	var matchPrefix string
	var targetDoc string
	for _, s := range targetResources {
		targetDoc += "/" + plural(s) + "/{" + s + "_id}"
		matchPrefix += "/" + plural(s) + "/" + s + "_id"
	}

	log.Println("create shortcut from", shortcut, "to", targetDoc)
	log.Println("  handle shortcut routes: "+prefix+"[/...]", "GET,POST,PUT,PATCH,DELETE")

	replaceHandler := func(w http.ResponseWriter, r *http.Request) {
		log.Println("called shortcut route for", r.URL, r.Method)
		tail := strings.TrimPrefix(r.URL.Path, prefix)

		var match mux.RouteMatch
		r.URL.Path = matchPrefix + tail
		log.Println("try to match route", r.URL.Path)
		if !router.Match(r, &match) {
			log.Println("got a match")
			http.NotFound(w, r)
			return
		}

		auth := access.AuthorizationFromContext(r.Context())
		authorized := auth.HasRole("admin")
		for i := 0; i < len(sc.Roles) && !authorized; i++ {
			role := sc.Roles[i]
			authorized = (auth.HasRole(role) || (auth.HasRoles() && role == "everybody") || role == "public")
		}
		if !authorized {
			http.Error(w, "not authorized", http.StatusUnauthorized)
			return
		}
		newPrefix := ""
		for _, s := range targetResources {
			id, ok := auth.Selector(s + "_id")
			if !ok {
				http.Error(w, fmt.Sprintf("missing selector for %s", s), http.StatusBadRequest)
				return
			}
			newPrefix += "/" + plural(s) + "/" + id
		}
		r.URL.Path = newPrefix + tail
		log.Println("redirect shortcut route to:", r.URL)
		router.ServeHTTP(w, r)
	}
	router.HandleFunc(prefix, replaceHandler).Methods(http.MethodOptions, http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodPost, http.MethodDelete)
	router.HandleFunc(prefix+"/{rest:.+}", replaceHandler).Methods(http.MethodOptions, http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodPost, http.MethodDelete)
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

// Notification is a database notification. Receive them
// with RequestNotification()
type Notification struct {
	Resource  string
	Operation core.Operation
	State     string
	Payload   []byte
}

type notificationHandler struct {
	resource  string
	operation core.Operation
	state     string
	callback  *func(Notification) bool
}

// RequestNotification requests a specific type of database notifications.
// The handlers are called in the same order the notification request have been received.
// If a handler returns true, subsequent handlers for the same notification type will not be called.
func (b *Backend) RequestNotification(resource string, operation core.Operation, state string, handler *func(Notification) bool) {
	notifier := notificationHandler{
		resource:  resource,
		operation: operation,
		state:     state,
		callback:  handler,
	}
	log.Printf("install notification handler for %s - %s (state=\"%s\")", resource, operation, state)
	b.handlers = append(b.handlers, notifier)
}

// RemoveNotificationHandler removes a previously installed notification handler. Returns the number
// of instances that were removed
func (b *Backend) RemoveNotificationHandler(handler *func(Notification) bool) int {
	count := len(b.handlers)
	remaining := b.handlers[:0]
	for i := 0; i < len(b.handlers); i++ {
		if b.handlers[i].callback != handler {
			remaining = append(remaining, b.handlers[i])
		} else {
			log.Printf("remove notification handler for %s - %s (state=\"%s\")", b.handlers[i].resource, b.handlers[i].operation, b.handlers[i].state)
		}
	}
	b.handlers = remaining
	return count - len(b.handlers)
}

func (b *Backend) notify(resource string, operation core.Operation, state string, payload []byte) {
	for _, handler := range b.handlers {
		if handler.resource == resource &&
			handler.operation == operation &&
			handler.state == state {

			notification := Notification{
				Resource:  resource,
				Operation: operation,
				State:     state,
				Payload:   payload,
			}

			if (*handler.callback)(notification) {
				break
			}
		}
	}
}
