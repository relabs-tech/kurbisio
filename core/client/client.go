/*Package client provides easy and fast in-process access to a REST api

Instead of marshalling HTTP, the client talks directly to the mux router. The client
is the tool of choice if one request handler needs to call other handlers to fulfill
its task. It is also perfectly suited for unit tests.
*/
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core"
	"github.com/relabs-tech/backends/core/access"
)

// Client provides easy access to the REST API.
//
type Client struct {
	router *mux.Router
	auth   *access.Authorization
	ctx    context.Context
}

// NewWithRouter creates a client to make pseudo-REST requests to the backend,
// through the mux router
//
// WithAuthorization() adds an authorization to the request context.
// WithContext() specifies a different base context all together.
func NewWithRouter(router *mux.Router) Client {
	return Client{
		router: router,
	}
}

// WithAdminAuthorization returns a new client with admin authorizations
func (c Client) WithAdminAuthorization() Client {
	c.auth = &access.Authorization{
		Roles: []string{"admin"},
	}
	return c
}

// WithAuthorization returns a new client with specific authorizations
func (c Client) WithAuthorization(auth *access.Authorization) Client {
	c.auth = auth
	return c
}

// WithContext returns a new client with specific request context
func (c Client) WithContext(ctx context.Context) Client {
	c.ctx = ctx
	return c
}

func (c Client) context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	if c.auth != nil {
		return access.ContextWithAuthorization(context.Background(), c.auth)
	}
	return context.Background()
}

// Collection represents a collection of particular resource
type Collection struct {
	client     *Client
	resources  []string
	selectors  map[string]uuid.UUID
	parameters []string
}

// Collection returns a new collection client
func (c Client) Collection(resource string) Collection {
	return Collection{
		client:    &c,
		resources: strings.Split(resource, "/"),
	}
}

// WithSelector returns a new collection client with a selector added
func (r Collection) WithSelector(key string, value uuid.UUID) Collection {
	// we want a true copy to avoid side effects
	selectors := map[string]uuid.UUID{strings.TrimSuffix(key, "_id"): value}
	for k, v := range r.selectors {
		selectors[k] = v
	}
	return Collection{
		client:     r.client,
		resources:  r.resources,
		selectors:  selectors,
		parameters: r.parameters,
	}
}

// WithParent returns a new collection client with a parent selector added
func (r Collection) WithParent(parentID uuid.UUID) Collection {
	if len(r.resources) < 2 {
		panic("no parent resource to select")
	}
	return r.WithSelector(r.resources[len(r.resources)-2], parentID)
}

// WithFilter returns a new collection client with a URL parameter added.
// Filters apply only to lists.
func (r Collection) WithFilter(key string, value string) Collection {

	parameter := key + "=" + value
	return Collection{
		client:    r.client,
		resources: r.resources,
		selectors: r.selectors,
		// we want a true copy to avoid side effects
		parameters: append(append([]string{}, r.parameters...), parameter),
	}
}

// WithState returns a new collection client with a URL parameter for state added.
// Filters apply only to lists.
func (r Collection) WithState(state string) Collection {
	return r.WithFilter("state", state)
}

func (r Collection) paths() (collectionPath, singletonPath string) {
	var itemPath string
	for _, resource := range r.resources {
		singletonPath = itemPath + "/" + resource
		collectionPath = itemPath + "/" + core.Plural(resource)
		param := "all"
		if selector, ok := r.selectors[resource]; ok {
			param = selector.String()
		}
		itemPath = itemPath + "/" + core.Plural(resource) + "/" + param
	}
	if len(r.parameters) > 0 {
		collectionPath += "?" + strings.Join(r.parameters, "&")
	}

	return
}

// CollectionPath returns the created path for the collection plus optional query strings
func (r Collection) CollectionPath() string {
	path, _ := r.paths()
	return path
}

// SingletonPath returns the created path for a singleton
func (r Collection) SingletonPath() string {
	_, path := r.paths()
	return path
}

// Create creates a new item
func (r Collection) Create(body interface{}, result interface{}) error {
	_, err := r.client.RawPost(r.CollectionPath(), body, result)
	return err
}

// Update updates (or creates) an item. The item must be fully
// qualified, i.e. it must contain all identifiers.
func (r Collection) Update(body interface{}, result interface{}) error {
	_, err := r.client.RawPut(r.CollectionPath(), body, result)
	return err
}

// Clear deletes the entire collection
//
// This operation does not accept any filters nor does it generate notifications.
// If you need filters or delete notifications, you should iterate of the items
// and delete them one by one.
func (r Collection) Clear() error {
	_, err := r.client.RawDelete(r.CollectionPath())
	return err
}

// List gets the entire collection up until the specified limit.
//
// If you potentially need multiple pages, use FirstPage() instead.
//
func (r Collection) List(result interface{}) error {
	_, err := r.client.RawGet(r.CollectionPath(), result)
	return err
}

// Item represents a single item in a collection
type Item struct {
	col         Collection
	id          uuid.UUID
	isSingleton bool
}

// Item gets an item from a collection
func (r Collection) Item(id uuid.UUID) Item {
	return Item{col: r, id: id}
}

// Singleton gets a singleton from this collection
func (r Collection) Singleton() Item {
	return Item{col: r, isSingleton: true}
}

// Path returns the created path for this item
func (r Item) Path() string {
	if r.isSingleton {
		return r.col.SingletonPath()
	}
	return r.col.CollectionPath() + "/" + r.id.String()
}

// Subcollection returns a subcollection for this item
func (r Item) Subcollection(resource string) Collection {
	col := r.col.WithSelector(r.col.resources[len(r.col.resources)-1], r.id)
	// we want a true copy to avoid side effects
	col.resources = append(append([]string{}, r.col.resources...), resource)
	return col
}

// Read reads an item from a collection
func (r Item) Read(result interface{}) error {
	_, err := r.col.client.RawGet(r.Path(), result)
	return err
}

// Delete deletes an item from a collection
func (r Item) Delete() error {
	_, err := r.col.client.RawDelete(r.Path())
	return err
}

// Update updates an item
func (r Item) Update(body interface{}, result interface{}) error {
	_, err := r.col.client.RawPut(r.Path(), body, result)
	return err
}

// Patch updates selected fields of an item
func (r Item) Patch(body interface{}, result interface{}) error {
	_, err := r.col.client.RawPatch(r.Path(), body, result)
	return err
}

// Page is a requester for one page in a collection
type Page struct {
	r         Collection
	page      int
	pageCount int
}

// FirstPage returns a requester for the first page of a collection
//
// Do not specify the page filter when using the page requester, as
// it manages page itself. You can set all others filters, including
// limit.
func (r Collection) FirstPage() Page {
	return Page{page: 1, r: r}
}

// HasData returns true if the page has data
func (p Page) HasData() bool {
	return p.page == 1 || p.page <= p.pageCount
}

// Get gets one page of the collection
func (p *Page) Get(result interface{}) error {
	path := p.r.WithFilter("page", strconv.Itoa(p.page)).CollectionPath()
	_, header, err := p.r.client.RawGetWithHeader(path, map[string]string{}, result)
	if err != nil {
		return err
	}
	pageCount, err := strconv.Atoi(header.Get("Pagination-Page-Count"))
	if err == nil {
		p.pageCount = pageCount
	}
	found := false
	for i := 0; i < len(p.r.parameters) && !found; i++ {
		found = strings.HasPrefix(p.r.parameters[i], "until=")
	}
	if !found {
		until := header.Get("Pagination-Until")
		if len(until) > 0 {
			p.r = p.r.WithFilter("until", until)
		}
	}
	return nil
}

// Next returns the next page
func (p Page) Next() Page {
	return Page{
		r:         p.r,
		page:      p.page + 1,
		pageCount: p.pageCount,
	}
}

// RawGet gets the resource from path. Expects http.StatusOK as response, otherwise it will
// flag an error. Returns the actual http status code.
//
// The path can be extend with query strings.
//
// result can be map[string]interface{} or a raw *[]byte
func (c Client) RawGet(path string, result interface{}) (int, error) {
	r, _ := http.NewRequestWithContext(c.context(), http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	c.router.ServeHTTP(rec, r)

	status := rec.Code
	if status == http.StatusNoContent {
		return status, nil

	}
	if status != http.StatusOK {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s", status, http.StatusOK, rec.Body.String())
	}

	var err error
	if rec.Body != nil && result != nil {
		if raw, ok := result.(*[]byte); ok {
			*raw = rec.Body.Bytes()
		} else {
			err = json.Unmarshal(rec.Body.Bytes(), result)
		}
	}
	return status, err
}

// RawGetWithHeader gets the resource from path. Expects http.StatusOK as response, otherwise it will
// flag an error. Returns the actual http status code and the header.
//
// The path can be extend with query strings.
//
// result can be map[string]interface{} or a raw *[]byte
func (c *Client) RawGetWithHeader(path string, header map[string]string, result interface{}) (int, http.Header, error) {
	r, _ := http.NewRequestWithContext(c.context(), http.MethodGet, path, nil)
	for key, value := range header {
		r.Header.Add(key, value)
	}

	rec := httptest.NewRecorder()
	c.router.ServeHTTP(rec, r)
	res := rec.Result()

	status := res.StatusCode
	if status == http.StatusNoContent {
		return status, res.Header, nil

	}

	if status != http.StatusOK {
		return status, res.Header, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s", status, http.StatusOK, rec.Body.String())
	}

	var err error
	if rec.Body != nil && result != nil {
		if raw, ok := result.(*[]byte); ok {
			*raw = rec.Body.Bytes()
		} else {
			err = json.Unmarshal(rec.Body.Bytes(), result)
		}
	}
	return status, res.Header, err
}

// RawGetBlobWithHeader gets a binary resource from path. Expects http.StatusOK as response, otherwise it will
// flag an error.
//
// The path can be extend with query strings.
//
// Returns the actual http status code and the return header
func (c *Client) RawGetBlobWithHeader(path string, header map[string]string, blob *[]byte) (int, http.Header, error) {
	r, _ := http.NewRequestWithContext(c.context(), http.MethodGet, path, nil)
	for key, value := range header {
		r.Header.Add(key, value)
	}

	rec := httptest.NewRecorder()
	c.router.ServeHTTP(rec, r)

	res := rec.Result()
	status := res.StatusCode
	if status == http.StatusNoContent {
		return status, res.Header, nil

	}

	if status != http.StatusOK {
		return status, res.Header, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s", status, http.StatusOK, rec.Body.String())
	}

	if rec.Body != nil {
		*blob = rec.Body.Bytes()
	}
	return status, res.Header, nil
}

// RawPost posts a resource to path. Expects http.StatusCreated as response, otherwise it will
// flag an error. Returns the actual http status code.
//
// The path can be extend with query strings.
//
// body can also be a []byte, result can also be raw *[]byte
func (c Client) RawPost(path string, body interface{}, result interface{}) (int, error) {

	var err error
	j, ok := body.([]byte)
	if !ok {
		j, err = json.MarshalIndent(body, "", "  ")
		if err != nil {
			return http.StatusBadRequest, err
		}
	}

	r, _ := http.NewRequestWithContext(c.context(), http.MethodPost, path, bytes.NewBuffer(j))
	rec := httptest.NewRecorder()
	c.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusCreated {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s", status, http.StatusCreated, rec.Body.String())
	}

	if rec.Body != nil && result != nil {
		if raw, ok := result.(*[]byte); ok {
			*raw = rec.Body.Bytes()
		} else {
			err = json.Unmarshal(rec.Body.Bytes(), result)
		}
	}
	return status, err
}

// RawPostBlob posts a resource to path. Expects http.StatusCreated as response, otherwise it will
// flag an error. Returns the actual http status code.
//
// The path can be extend with query strings.
//
func (c Client) RawPostBlob(path string, header map[string]string, blob []byte, result interface{}) (int, error) {

	r, _ := http.NewRequestWithContext(c.context(), http.MethodPost, path, bytes.NewBuffer(blob))
	for key, value := range header {
		r.Header.Add(key, value)
	}
	rec := httptest.NewRecorder()
	c.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusCreated {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s", status, http.StatusCreated, rec.Body.String())
	}
	var err error
	if rec.Body != nil && result != nil {
		err = json.Unmarshal(rec.Body.Bytes(), result)
	}
	return status, err
}

// RawPut puts a resource to path. Expects http.StatusOK, http.StatusCreated or http.StatusNoContent as valid responses,
// otherwise it will flag an error. Returns the actual http status code.
//
// The path can be extend with query strings.
//
// body can also be a []byte, result can also be raw *[]byte
func (c Client) RawPut(path string, body interface{}, result interface{}) (int, error) {

	var err error
	j, ok := body.([]byte)
	if !ok {
		j, err = json.MarshalIndent(body, "", "  ")
		if err != nil {
			return http.StatusBadRequest, err
		}
	}

	r, _ := http.NewRequestWithContext(c.context(), http.MethodPut, path, bytes.NewBuffer(j))
	rec := httptest.NewRecorder()
	c.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusOK && status != http.StatusCreated && status != http.StatusNoContent {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v or %v. Error: %s", status, http.StatusOK, http.StatusNoContent, rec.Body.String())
	}
	if rec.Body != nil && result != nil {
		if raw, ok := result.(*[]byte); ok {
			*raw = rec.Body.Bytes()
		} else {
			err = json.Unmarshal(rec.Body.Bytes(), result)
		}
	}
	return status, err
}

// RawPutBlob puts a binary resource to path. Expects http.StatusOK or http.StatusNoContent as valid responses,
// otherwise it will flag an error.
//
// The path can be extend with query strings.
//
// Returns the actual http status code.
func (c Client) RawPutBlob(path string, header map[string]string, blob []byte, result interface{}) (int, error) {

	r, _ := http.NewRequestWithContext(c.context(), http.MethodPut, path, bytes.NewBuffer(blob))
	for key, value := range header {
		r.Header.Add(key, value)
	}
	rec := httptest.NewRecorder()
	c.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusOK && status != http.StatusNoContent {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v or %v. Error: %s", status, http.StatusOK, http.StatusNoContent, rec.Body.String())
	}

	var err error
	if rec.Body != nil && result != nil {
		err = json.Unmarshal(rec.Body.Bytes(), result)
	}
	return status, err
}

// RawPatch puts a patch to path. Expects http.StatusOK or http.StatusNoContent as valid responses,
// otherwise it will flag an error. Returns the actual http status code.
//
// The path can be extend with query strings.
//
// body can also be a []byte, result can also be raw *[]byte
func (c Client) RawPatch(path string, body interface{}, result interface{}) (int, error) {

	var err error
	j, ok := body.([]byte)
	if !ok {
		j, err = json.MarshalIndent(body, "", "  ")
		if err != nil {
			return http.StatusBadRequest, err
		}
	}

	r, _ := http.NewRequestWithContext(c.context(), http.MethodPatch, path, bytes.NewBuffer(j))
	rec := httptest.NewRecorder()
	c.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusOK && status != http.StatusNoContent {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v or %v. Error: %s", status, http.StatusOK, http.StatusNoContent, rec.Body.String())
	}
	if rec.Body != nil && result != nil {
		if raw, ok := result.(*[]byte); ok {
			*raw = rec.Body.Bytes()
		} else {
			err = json.Unmarshal(rec.Body.Bytes(), result)
		}
	}
	return status, err
}

// RawDelete deletes the resource at path. Expects http.StatusNoContent as response, otherwise it will
// flag an error.
//
// The path can be extend with query strings.
//
// Returns the actual http status code.
func (c Client) RawDelete(path string) (int, error) {
	r, _ := http.NewRequestWithContext(c.context(), http.MethodDelete, path, nil)
	rec := httptest.NewRecorder()
	c.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusNoContent {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s", status, http.StatusNoContent, rec.Body.String())
	}
	return status, nil
}
