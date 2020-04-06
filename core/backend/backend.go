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
	notifier         core.Notifier
	db               *sql.DB
	router           *mux.Router
	collectionHelper map[string]*collectionHelper
	// Registry is the JSON object registry for this backend's schema
	Registry             *registry.Registry
	authorizationEnabled bool
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
		notifier:             bb.Notifier,
		db:                   bb.DB,
		router:               bb.Router,
		collectionHelper:     make(map[string]*collectionHelper),
		Registry:             registry.New(bb.DB),
		authorizationEnabled: bb.AuthorizationEnabled,
	}

	b.handleCORS(b.router)
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
			return status, err
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

	}).Methods(http.MethodPatch)
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
	router.HandleFunc(prefix, replaceHandler).Methods(http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodPost, http.MethodDelete)
	router.HandleFunc(prefix+"/{rest:.+}", replaceHandler).Methods(http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodPost, http.MethodDelete)
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