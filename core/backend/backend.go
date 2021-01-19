package backend

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"net/http"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/relabs-tech/backends/core"
	"github.com/relabs-tech/backends/core/access"
	"github.com/relabs-tech/backends/core/client"
	"github.com/relabs-tech/backends/core/csql"
	"github.com/relabs-tech/backends/core/logger"
	"github.com/relabs-tech/backends/core/registry"
	"github.com/relabs-tech/backends/core/schema"
)

// InternalDatabaseSchemaVersion is a sequential versioning number of the database schema.
// If it increases, the backend will try to update the schema.
const InternalDatabaseSchemaVersion = 2

// Backend is the generic rest backend
type Backend struct {
	config              backendConfiguration
	db                  *csql.DB
	router              *mux.Router
	collectionFunctions map[string]*collectionFunctions
	relations           map[string]string
	// Registry is the JSON object registry for this backend's schema
	Registry             registry.Registry
	authorizationEnabled bool
	updateSchema         bool

	collectionsAndSingletons map[string]bool
	callbacks                map[string]jobHandler
	interceptors             map[string]requestHandler

	pipelineConcurrency int

	jobsInsertQuery, jobsInsertIfNotExistQuery, jobsCancelQuery, jobsUpdateQuery, jobsDeleteQuery string

	processJobsAsyncRuns    bool
	processJobsAsyncTrigger chan struct{}
	hasJobsToProcess        bool
	hasJobsToProcessLock    sync.Mutex

	jsonValidator *schema.Validator
}

// Builder is a builder helper for the Backend
type Builder struct {
	// Config is the JSON description of all resources and relations. This is mandatory.
	Config string
	// DB is a postgres database. This is mandatory.
	DB *csql.DB
	// Router is a mux router. This is mandatory.
	Router *mux.Router
	// If AuthorizationEnabled is true, the backend requires auhorization for each route
	// in the request context, as specified in the configuration.
	AuthorizationEnabled bool

	// Number of concurrent pipeline executors. Default is 5.
	PipelineConcurrency int

	// JSONSchemas is a list of top level JSON Schemas as strings.
	JSONSchemas []string

	// JSONSchemasRefs is a list of references JSON Schemas as strings.
	JSONSchemasRefs []string

	// The loglevel to be used by the logger. Default is "info""
	LogLevel string

	// if true, always update the schema. Otherwise only update when the schema json has changed.
	UpdateSchema bool
}

// New realizes the actual backend. It creates the sql relations (if they
// do not exist) and adds actual routes to router
func New(bb *Builder) *Backend {
	rand.Seed(time.Now().UTC().UnixNano())

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

	pipelineConcurrency := 5
	if bb.PipelineConcurrency > 0 {
		pipelineConcurrency = bb.PipelineConcurrency
	}

	jsonValidator, err := schema.NewValidator([]string{ConfigSchemaJSON}, nil)
	if err != nil {
		log.Fatalf("Cannot created json Validator %v", err)
	}

	err = jsonValidator.ValidateString(bb.Config, "https://kurbis.io/schemas/config.json")
	if err != nil {
		log.Fatalf("Invalid json %v", err)
	}

	b := &Backend{
		config:                   config,
		db:                       bb.DB,
		router:                   bb.Router,
		collectionFunctions:      make(map[string]*collectionFunctions),
		relations:                make(map[string]string),
		Registry:                 registry.New(bb.DB),
		authorizationEnabled:     bb.AuthorizationEnabled,
		callbacks:                make(map[string]jobHandler),
		interceptors:             make(map[string]requestHandler),
		collectionsAndSingletons: make(map[string]bool),
		pipelineConcurrency:      pipelineConcurrency,
		updateSchema:             bb.UpdateSchema,
	}

	logLevel := logrus.InfoLevel

	if bb.LogLevel != "" {
		switch strings.ToLower(bb.LogLevel) {
		case "info":
			logLevel = logrus.InfoLevel
			break
		case "debug":
			logLevel = logrus.DebugLevel
			break
		case "warning":
		case "warn":
			logLevel = logrus.WarnLevel
			break
		case "error":
			logLevel = logrus.ErrorLevel
			break
		default:
			fmt.Println("Unkown loglevel, using INFO")
			break
		}
	}
	logger.InitLogger(logLevel)

	b.jsonValidator, err = schema.NewValidator(bb.JSONSchemas, bb.JSONSchemasRefs)
	if err != nil {
		logger.Default().Fatalf("Cannot create json Validator %v", err)
	}

	registry := b.Registry.Accessor("_backend_")
	var currentVersion string
	newVersion := fmt.Sprintf("%d/%x", InternalDatabaseSchemaVersion, sha1.Sum([]byte(bb.Config)))

	if !b.updateSchema {
		registry.Read("schema_version", &currentVersion)
		b.updateSchema = newVersion != currentVersion
	}

	advisoryLock := len(b.db.Schema) // lock number is the schema length, a bit primity as a check sum but does the job
	if b.updateSchema {
		_, err := b.db.Exec(fmt.Sprintf("SELECT pg_advisory_lock(%d);", advisoryLock))
		if err != nil {
			logger.Default().Fatalf("Cannot obtain schema update advisory lock %v", err)
		}
		logger.Default().Infoln("new configuration - will update database schema")

		logger.AddRequestID(b.router)
		b.handleCORS()
		access.HandleAuthorizationRoute(b.router)
		b.handleResourceRoutes()
		b.handleStatistics(b.router)
		b.handleVersion(b.router)
		b.handleJobs(b.router)
		registry.Write("schema_version", newVersion)
		_, err = b.db.Exec(fmt.Sprintf("SELECT pg_advisory_unlock(%d);", advisoryLock))
		if err != nil {
			logger.Default().Fatalf("Cannot release schema update advisory lock %v", err)
		}
	} else {
		logger.Default().Debugln("use previous schema version")
	}

	return b
}

type anyResourceConfiguration struct {
	collection *collectionConfiguration
	singleton  *singletonConfiguration
	blob       *blobConfiguration
	relation   *relationConfiguration
}

func (rc anyResourceConfiguration) depth() int {
	if rc.collection != nil {
		return strings.Count(rc.collection.Resource, "/")
	}
	if rc.singleton != nil {
		return strings.Count(rc.singleton.Resource, "/")
	}
	if rc.blob != nil {
		return strings.Count(rc.blob.Resource, "/")
	}
	if rc.relation != nil {
		left := strings.Count(rc.relation.Left, "/")
		right := strings.Count(rc.relation.Right, "/")
		if left > right {
			return left
		}
		return right
	}
	return 0
}

type byDepth []anyResourceConfiguration

func (r byDepth) Len() int {
	return len(r)
}
func (r byDepth) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
func (r byDepth) Less(i, j int) bool {
	return r[i].depth() < r[j].depth()
}

// handleResourceRoutes adds all necessary handlers for the specified configuration
func (b *Backend) handleResourceRoutes() {

	logger.FromContext(nil).Debugln("backend: handle resource routes")
	router := b.router

	// we combine all types of resources into one and sort them by depth. Rationale: dependencies of
	// resources must be generated first, otherwise we cannot enforce those dependencies via sql
	// foreign keys
	allResources := []anyResourceConfiguration{}
	for i := range b.config.Collections {
		rc := &b.config.Collections[i]
		allResources = append(allResources, anyResourceConfiguration{collection: rc})
		b.collectionsAndSingletons[rc.Resource] = false
	}

	for i := range b.config.Singletons {
		rc := &b.config.Singletons[i]
		allResources = append(allResources, anyResourceConfiguration{singleton: rc})
		b.collectionsAndSingletons[rc.Resource] = true
	}

	for i := range b.config.Blobs {
		rc := &b.config.Blobs[i]
		allResources = append(allResources, anyResourceConfiguration{blob: rc})
	}

	for i := range b.config.Relations {
		rc := &b.config.Relations[i]
		allResources = append(allResources, anyResourceConfiguration{relation: rc})
	}
	sort.Sort(byDepth(allResources))

	for _, rc := range allResources {
		if rc.collection != nil {
			b.createCollectionResource(router, *rc.collection, false)
		}
		if rc.singleton != nil {
			// a singleton is a specialized collection
			tmp := collectionConfiguration{
				Resource:             rc.singleton.Resource,
				Permits:              rc.singleton.Permits,
				SchemaID:             rc.singleton.SchemaID,
				Description:          rc.singleton.Description,
				StaticProperties:     rc.singleton.StaticProperties,
				SearchableProperties: rc.singleton.SearchableProperties,
				WithLog:              rc.singleton.WithLog,
				Default:              rc.singleton.Default,
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

type collectionFunctions struct {
	permits []access.Permit
	list    func(w http.ResponseWriter, r *http.Request, relation *relationInjection)
	read    func(w http.ResponseWriter, r *http.Request, relation *relationInjection)
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
		result += fmt.Sprintf("($%d='all' OR %s=$%d::UUID)", i+1, s[i], i+1)
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
		result += fmt.Sprintf("($%d='all' OR %s=$%d::UUID)", i+offset+1, s[i], i+offset+1)
	}
	return result
}

func timeToEtag(t time.Time) string {
	return fmt.Sprintf("\"%x\"", sha1.Sum([]byte(t.String())))
}
func bytesToEtag(b []byte) string {
	return fmt.Sprintf("\"%x\"", sha1.Sum(b))
}
func bytesPlusTotalCountToEtag(b []byte, t int) string {
	return fmt.Sprintf("\"%x%x\"", sha1.Sum(b), t)
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

func (b *Backend) hasCollectionOrSingleton(resource string) bool {
	_, ok := b.collectionsAndSingletons[resource]
	return ok
}

func (b *Backend) addChildrenToGetResponse(children []string, r *http.Request, response map[string]interface{}) (int, error) {
	var all []string
	for _, child := range children {
		all = append(all, strings.Split(child, ",")...)
	}
	client := client.NewWithRouter(b.router).WithContext(r.Context())
	for _, child := range all {
		if strings.ContainsRune(child, '/') {
			return http.StatusBadRequest, fmt.Errorf("invalid child %s", child)
		}
		var childJSON interface{}
		status, err := client.RawGet(r.URL.Path+"/"+child, &childJSON)
		if err != nil {
			return status, fmt.Errorf("cannot get child %s", child)
		}
		response[child] = &childJSON
	}
	return http.StatusOK, nil
}

func (b *Backend) createShortcut(router *mux.Router, sc shortcutConfiguration) {
	shortcut := sc.Shortcut
	target := sc.Target

	targetResources := strings.Split(target, "/")
	prefix := "/" + shortcut
	var matchPrefix string
	var targetDoc string
	for _, s := range targetResources {
		targetDoc += "/" + core.Plural(s) + "/{" + s + "_id}"
		matchPrefix += "/" + core.Plural(s) + "/" + s + "_id"
	}

	rlog := logger.FromContext(nil)
	rlog.Debugln("create shortcut from", shortcut, "to", targetDoc)
	rlog.Debugln("  handle shortcut routes: "+prefix+"[/...]", "GET,POST,PUT,PATCH,DELETE")

	replaceHandler := func(w http.ResponseWriter, r *http.Request) {
		rlog := logger.FromContext(r.Context())
		rlog.Infoln("called shortcut route for", r.URL, r.Method)

		tail := strings.TrimPrefix(r.URL.Path, prefix)

		var match mux.RouteMatch
		r.URL.Path = matchPrefix + tail
		rlog.Debugln("try to match route", r.URL.Path)
		if !router.Match(r, &match) {
			rlog.Errorf("Found no match for %s", r.URL.Path)
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
			newPrefix += "/" + core.Plural(s) + "/" + id
		}
		r.URL.Path = newPrefix + tail
		rlog.Info("redirect shortcut route to ", r.URL)
		router.ServeHTTP(w, r)
	}
	router.HandleFunc(prefix, replaceHandler).Methods(http.MethodOptions, http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodPost, http.MethodDelete)
	router.HandleFunc(prefix+"/{rest:.+}", replaceHandler).Methods(http.MethodOptions, http.MethodGet, http.MethodPut, http.MethodPatch, http.MethodPost, http.MethodDelete)
}

// Router returns the mux.Router for this backend
func (b *Backend) Router() *mux.Router {
	return b.router
}
