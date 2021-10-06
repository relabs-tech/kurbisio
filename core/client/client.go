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
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core"
	"github.com/relabs-tech/backends/core/access"
)

// Client provides easy access to the REST API.
//
type Client struct {
	router     *mux.Router
	httpClient *http.Client
	url        string
	token      string
	auth       *access.Authorization
	ctx        context.Context
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

// NewWithURL creates a client to make REST requests to the backend
//
// WithToken adds an authorization token to the request header.
func NewWithURL(url string) Client {
	return Client{
		url:        url,
		httpClient: &http.Client{Timeout: 20 * time.Second},
	}
}

// WithToken returns a new client with admin authorizations
func (c Client) WithToken(token string) Client {
	c.token = token
	return c
}

// WithAdminAuthorization returns a new client with admin authorizations
// (this works only directly against the mux router, for a normal client
//  use WithToken()))
func (c Client) WithAdminAuthorization() Client {
	return c.WithRole("admin")
}

// WithRole returns a new client with role authorization
// (this works only directly against the mux router, for a normal client
//  use WithToken()))
func (c Client) WithRole(role string) Client {
	c.auth = &access.Authorization{
		Roles: []string{role},
	}
	return c
}

// WithAuthorization returns a new client with specific authorizations
// (this works only directly against the mux router, for a normal client
//  use WithToken())
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
	ctx := c.ctx
	if c.ctx == nil {
		ctx = context.Background()
	}
	if c.auth != nil {
		ctx = access.ContextWithAuthorization(ctx, c.auth)
	}
	return ctx
}

// Collection represents a collection of particular resource
type Collection struct {
	client     *Client
	resources  []string
	selectors  map[string]string
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
	selectors := map[string]string{strings.TrimSuffix(key, "_id"): value.String()}
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

// WithSelectors returns a new collection client with all selectors added
func (r Collection) WithSelectors(keyValues map[string]string) Collection {
	selectors := map[string]string{}
	for k, v := range keyValues {
		selectors[strings.TrimSuffix(k, "_id")] = v
	}
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

// WithParameter returns a new collection client with a URL parameter added.
func (r Collection) WithParameter(key string, value string) Collection {

	parameter := url.QueryEscape(key) + "=" + url.QueryEscape(value)

	return Collection{
		client:    r.client,
		resources: r.resources,
		selectors: r.selectors,
		// we want a true copy to avoid side effects
		parameters: append(append([]string{}, r.parameters...), parameter),
	}
}

// WithParameters returns a new collection client with all URL parameters added.
func (r Collection) WithParameters(keyValues map[string]string) Collection {
	var parameters []string
	for key, value := range keyValues {
		parameter := url.QueryEscape(key) + "=" + url.QueryEscape(value)
		parameters = append(parameters, parameter)
	}
	return Collection{
		client:    r.client,
		resources: r.resources,
		selectors: r.selectors,
		// we want a true copy to avoid side effects
		parameters: append(append([]string{}, r.parameters...), parameters...),
	}
}

// WithFilter returns a new collection client with a URL filter parameter added.
// This is a shortcut for WithParameter("filter", key+"="+value)
func (r Collection) WithFilter(key string, value string) Collection {
	return r.WithParameter("filter", key+"="+value)
}

func (r Collection) paths() (collectionPath, singletonPath string) {
	var itemPath string
	for _, resource := range r.resources {
		singletonPath = itemPath + "/" + resource
		collectionPath = itemPath + "/" + core.Plural(resource)
		param := "all"
		if selector, ok := r.selectors[resource]; ok {
			param = selector
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

// Create always creates a new item.
//
// The operation corresponds to a POST request.
//
// Expects http.StatusCreated as response, otherwise it will
// flag an error. Returns the actual http status code.
//
// body can also be a []byte, result can also be raw *[]byte.
// result can be nil.
func (r Collection) Create(body interface{}, result interface{}) (int, error) {
	return r.client.RawPost(r.CollectionPath(), body, result)
}

// CreateBlob always creates a new blob item.
//
// The operation corresponds to a POST request.
//
// Expects http.StatusCreated as response, otherwise it will
// flag an error. Returns the actual http status code.
// result can be nil.
func (r Collection) CreateBlob(blob []byte, meta interface{}, result interface{}) (int, error) {
	var err error
	j, ok := meta.([]byte)
	if !ok {
		j, err = json.Marshal(meta)
		if err != nil {
			return http.StatusBadRequest, err
		}
	}
	header := map[string]string{
		"Kurbisio-Meta-Data": string(j),
	}
	return r.client.RawPostBlob(r.CollectionPath(), header, blob, result)
}

// Upsert updates an item, or creates it if it doesn't exist yet.
// The item must be fully qualified, i.e. it must contain all identifiers, either in the
// body itself or as selectors.
//
// The operation corresponds to a PUT request.
//
// Expects http.StatusOK, http.StatusCreated or http.StatusNoContent as valid responses,
// otherwise it will flag an error. Returns the actual http status code.
//
// In case of http.StatusConflict, the conflicting version of the object has been returned as result.
//
// body can also be a []byte, result can also be raw *[]byte.
// result can be nil.
func (r Collection) Upsert(body interface{}, result interface{}) (int, error) {
	return r.client.RawPut(r.CollectionPath(), body, result)
}

// UpsertBlob updates a blob item, or creates it if it doesn't exist yet.
// The blob item must be fully qualified, i.e. it must contain all identifiers, either in the
// meta body itself or as selectors.
//
// The operation corresponds to a PUT request.
//
// Expects http.StatusOK, http.StatusCreated or http.StatusNoContent as valid responses,
// otherwise it will flag an error. Returns the actual http status code.
// result can be nil.
func (r Collection) UpsertBlob(blob []byte, meta interface{}, result interface{}) (int, error) {
	var err error
	j, ok := meta.([]byte)
	if !ok {
		j, err = json.Marshal(meta)
		if err != nil {
			return http.StatusBadRequest, err
		}
	}
	header := map[string]string{
		"Kurbisio-Meta-Data": string(j),
	}
	return r.client.RawPutBlob(r.CollectionPath(), header, blob, result)
}

// Clear deletes the entire collection
//
// This operation does not accept any filters nor does it generate notifications.
// If you need filters or delete notifications, you should iterate of the items
// and delete them one by one.
//
// The operation corresponds to a DELETE request.
//
// Expects http.StatusNoContent as response, otherwise it will
// flag an error.
func (r Collection) Clear() (int, error) {
	return r.client.RawDelete(r.CollectionPath())
}

// List gets the entire collection up until the specified limit.
//
// If you potentially need multiple pages, use FirstPage() instead.
//
//
// The operation corresponds to a GET request.
//
// Expects http.StatusOK as response, otherwise it will
// flag an error. Returns the actual http status code.
//
// result can be map[string]interface{} or a raw *[]byte.
func (r Collection) List(result interface{}) (int, error) {
	return r.client.RawGet(r.CollectionPath(), result)
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
//
// The operation corresponds to a GET request.
//
// Expects http.StatusOK as response, otherwise it will
// flag an error. Returns the actual http status code.
//
// result can also be map[string]interface{} or a raw *[]byte.
func (r Item) Read(result interface{}) (int, error) {
	return r.col.client.RawGet(r.Path(), result)
}

// Delete deletes an item from a collection
//
// The operation corresponds to a DELETE request.
//
// Expects http.StatusNoContent as response, otherwise it will
// flag an error.
//
// Returns the actual http status code.
func (r Item) Delete() (int, error) {
	return r.col.client.RawDelete(r.Path())
}

// Upsert updates an item, or creates if it doesn't exist yet.
// The item must be fully qualified, i.e. it must contain all identifiers, either in the
// body itself or as selectors.
//
// The operation corresponds to a PUT request.
//
// Expects http.StatusOK, http.StatusCreated or http.StatusNoContent as valid responses,
// otherwise it will flag an error. Returns the actual http status code.
//
// In case of http.StatusConflict, the conflicting version of the object has been returned as result.
//
// body can also be a []byte, result can also be raw *[]byte.
// result can be nil.
func (r Item) Upsert(body interface{}, result interface{}) (int, error) {
	return r.col.client.RawPut(r.Path(), body, result)
}

// UpdateProperty updates a single static property in the fastest possible
// way. Note: this method does trigger an update resource notificatino, but
// not with the entire object, only with the updated property.
//
// The operation corresponds to a PUT request.
//
// Expects http.StatusOK, http.StatusCreated or http.StatusNoContent as valid responses,
// otherwise it will flag an error. Returns the actual http status code.
func (r Item) UpdateProperty(jsonName string, value string) (int, error) {
	return r.col.client.RawPut(r.Path()+"/"+jsonName+"/"+value, nil, nil)
}

// Patch updates selected fields of an item
//
// Expects http.StatusOK, http.StatusCreated or http.StatusNoContent as valid responses,
// otherwise it will flag an error. Returns the actual http status code.
//
// body can also be a []byte, result can also be raw *[]byte.
// result can be nil.
func (r Item) Patch(body interface{}, result interface{}) (int, error) {
	return r.col.client.RawPatch(r.Path(), body, result)
}

// Page is a requester for one page in a collection
type Page struct {
	r          Collection
	page       int
	pageCount  int
	totalCount int
}

// FirstPage returns a requester for the first page of a collection
//
// Do not specify the page filter when using the page requester, as
// it manages page itself. You can set all others parameters, including
// limit.
func (r Collection) FirstPage() Page {
	return Page{page: 1, r: r}
}

// HasData returns true if the page has data (by definition true for the first page)
func (p Page) HasData() bool {
	return p.page == 1 || p.page <= p.pageCount
}

// TotalCount returns the total number of elements (only available after you have called Get on the page)
func (p Page) TotalCount() int {
	return p.totalCount
}

// Get gets one page of the collection
func (p *Page) Get(result interface{}) (int, error) {
	path := p.r.WithParameter("page", strconv.Itoa(p.page)).CollectionPath()
	status, header, err := p.r.client.RawGetWithHeader(path, map[string]string{}, result)
	if err != nil {
		return status, err
	}
	pageCount, err := strconv.Atoi(header.Get("Pagination-Page-Count"))
	if err == nil {
		p.pageCount = pageCount
	}
	totalCount, err := strconv.Atoi(header.Get("Pagination-Total-Count"))
	if err == nil {
		p.totalCount = totalCount
	}
	return status, nil
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
// result can be map[string]interface{} or a raw *[]byte.
// result can be nil.
func (c Client) RawGet(path string, result interface{}) (int, error) {
	r, _ := http.NewRequestWithContext(c.context(), http.MethodGet, c.url+path, nil)

	var err error
	var res *http.Response
	var resBody []byte
	if c.router != nil {
		rec := httptest.NewRecorder()
		c.router.ServeHTTP(rec, r)
		res = rec.Result()
		resBody = rec.Body.Bytes()
	} else {
		if c.token != "" {
			r.Header.Add("Authorization", "Bearer "+c.token)
		}
		res, err = c.httpClient.Do(r)
		if err != nil {
			return http.StatusInternalServerError, err
		}
		defer res.Body.Close()
		resBody, _ = io.ReadAll(res.Body)
	}
	status := res.StatusCode
	if status == http.StatusNoContent {
		return status, nil

	}
	if status != http.StatusOK {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s",
			status, http.StatusOK, strings.TrimSpace(string(resBody)))
	}

	if resBody != nil && result != nil {
		if raw, ok := result.(*[]byte); ok {
			*raw = resBody
		} else {
			err = json.Unmarshal(resBody, result)
		}
	}
	return status, err
}

// RawGetWithHeader gets the resource from path. Expects http.StatusOK as response, otherwise it will
// flag an error. Returns the actual http status code and the header.
//
// The path can be extend with query strings.
//
// result can be map[string]interface{} or a raw *[]byte.
// result can be nil.
func (c Client) RawGetWithHeader(path string, header map[string]string, result interface{}) (int, http.Header, error) {
	r, _ := http.NewRequestWithContext(c.context(), http.MethodGet, c.url+path, nil)
	for key, value := range header {
		r.Header.Add(key, value)
	}

	var err error
	var res *http.Response
	var resBody []byte
	if c.router != nil {
		rec := httptest.NewRecorder()
		c.router.ServeHTTP(rec, r)
		res = rec.Result()
		resBody = rec.Body.Bytes()
	} else {
		if c.token != "" {
			r.Header.Add("Authorization", "Bearer "+c.token)
		}
		res, err = c.httpClient.Do(r)
		if err != nil {
			return http.StatusInternalServerError, nil, err
		}
		defer res.Body.Close()
		resBody, _ = io.ReadAll(res.Body)
	}
	status := res.StatusCode

	if status == http.StatusNoContent {
		return status, res.Header, nil
	}

	if status != http.StatusOK {
		return status, res.Header, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s",
			status, http.StatusOK, strings.TrimSpace(string(resBody)))
	}

	if resBody != nil && result != nil {
		if raw, ok := result.(*[]byte); ok {
			*raw = resBody
		} else {
			err = json.Unmarshal(resBody, result)
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
	r, _ := http.NewRequestWithContext(c.context(), http.MethodGet, c.url+path, nil)
	for key, value := range header {
		r.Header.Add(key, value)
	}

	var err error
	var res *http.Response
	var resBody []byte
	if c.router != nil {
		rec := httptest.NewRecorder()
		c.router.ServeHTTP(rec, r)
		res = rec.Result()
		resBody = rec.Body.Bytes()
	} else {
		if c.token != "" {
			r.Header.Add("Authorization", "Bearer "+c.token)
		}
		res, err = c.httpClient.Do(r)
		if err != nil {
			return http.StatusInternalServerError, nil, err
		}
		defer res.Body.Close()
		resBody, _ = io.ReadAll(res.Body)
	}

	status := res.StatusCode
	if status == http.StatusNoContent {
		return status, res.Header, nil

	}

	if status != http.StatusOK {
		return status, res.Header, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s",
			status, http.StatusOK, strings.TrimSpace(string(resBody)))
	}

	if resBody != nil {
		*blob = resBody
	}
	return status, res.Header, nil
}

// RawPost posts a resource to path. Expects http.StatusCreated as response, otherwise it will
// flag an error. Returns the actual http status code.
//
// The path can be extend with query strings.
//
// body can also be a []byte, result can also be raw *[]byte.
// result can be nil.
func (c Client) RawPost(path string, body interface{}, result interface{}) (int, error) {

	var err error
	j, ok := body.([]byte)
	if !ok {
		j, err = json.Marshal(body)
		if err != nil {
			return http.StatusBadRequest, fmt.Errorf("POST to %s: %w", path, err)
		}
	}

	r, _ := http.NewRequestWithContext(c.context(), http.MethodPost, c.url+path, bytes.NewBuffer(j))
	var res *http.Response
	var resBody []byte
	if c.router != nil {
		rec := httptest.NewRecorder()
		c.router.ServeHTTP(rec, r)
		res = rec.Result()
		resBody = rec.Body.Bytes()
	} else {
		if c.token != "" {
			r.Header.Add("Authorization", "Bearer "+c.token)
		}
		res, err = c.httpClient.Do(r)
		if err != nil {
			return http.StatusInternalServerError, err
		}
		defer res.Body.Close()
		resBody, _ = io.ReadAll(res.Body)
	}
	status := res.StatusCode
	if status != http.StatusCreated && status != http.StatusOK {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s",
			status, http.StatusCreated, strings.TrimSpace(string(resBody)))
	}

	if resBody != nil && result != nil {
		if raw, ok := result.(*[]byte); ok {
			*raw = resBody
		} else {
			err = json.Unmarshal(resBody, result)
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

	r, _ := http.NewRequestWithContext(c.context(), http.MethodPost, c.url+path, bytes.NewBuffer(blob))
	for key, value := range header {
		r.Header.Add(key, value)
	}
	var err error
	var res *http.Response
	var resBody []byte
	if c.router != nil {
		rec := httptest.NewRecorder()
		c.router.ServeHTTP(rec, r)
		res = rec.Result()
		resBody = rec.Body.Bytes()
	} else {
		if c.token != "" {
			r.Header.Add("Authorization", "Bearer "+c.token)
		}
		res, err = c.httpClient.Do(r)
		if err != nil {
			return http.StatusInternalServerError, err
		}
		defer res.Body.Close()
		resBody, _ = io.ReadAll(res.Body)
	}
	status := res.StatusCode

	if status != http.StatusCreated {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s",
			status, http.StatusCreated, strings.TrimSpace(string(resBody)))
	}
	if resBody != nil && result != nil {
		err = json.Unmarshal(resBody, result)
	}
	return status, err
}

// RawPut puts a resource to path. Expects http.StatusOK, http.StatusCreated or http.StatusNoContent as valid responses,
// otherwise it will flag an error. Returns the actual http status code.
//
// In case of http.StatusConflict, the conflicting version of the object has been returned as result.
//
// The path can be extend with query strings.
//
// body can also be a []byte, result can also be raw *[]byte.
// result can be nil.
func (c Client) RawPut(path string, body interface{}, result interface{}) (int, error) {

	var err error
	j, ok := body.([]byte)
	if !ok {
		j, err = json.Marshal(body)
		if err != nil {
			return http.StatusBadRequest, fmt.Errorf("PUT to %s: %w", path, err)
		}
	}

	r, _ := http.NewRequestWithContext(c.context(), http.MethodPut, c.url+path, bytes.NewBuffer(j))
	var res *http.Response
	var resBody []byte
	if c.router != nil {
		rec := httptest.NewRecorder()
		c.router.ServeHTTP(rec, r)
		res = rec.Result()
		resBody = rec.Body.Bytes()
	} else {
		if c.token != "" {
			r.Header.Add("Authorization", "Bearer "+c.token)
		}
		res, err = c.httpClient.Do(r)
		if err != nil {
			return http.StatusInternalServerError, err
		}
		defer res.Body.Close()
		resBody, _ = io.ReadAll(res.Body)
	}
	status := res.StatusCode

	// we do not return just yet in case of http.StatusConflict to be able to return the conflicting object
	if status != http.StatusOK && status != http.StatusCreated && status != http.StatusNoContent && status != http.StatusConflict {
		return status, fmt.Errorf("put got status=%d body=%s", status, strings.TrimSpace(string(resBody)))
	}
	if resBody != nil && result != nil {
		if raw, ok := result.(*[]byte); ok {
			*raw = resBody
		} else {
			err = json.Unmarshal(resBody, result)
		}
	}
	if status == http.StatusConflict {
		return status, fmt.Errorf("conflict while writing to path:'%s', wanted to write %+v, conflict: %+v", path, body, result)
	}
	return status, err
}

// RawPutBlob puts a binary resource to path. Expects http.StatusOK, http.StatusCreated or http.StatusNoContent as valid responses,
// otherwise it will flag an error.
//
// The path can be extend with query strings.
//
// Returns the actual http status code.
// result can be nil.
func (c Client) RawPutBlob(path string, header map[string]string, blob []byte, result interface{}) (int, error) {

	r, _ := http.NewRequestWithContext(c.context(), http.MethodPut, c.url+path, bytes.NewBuffer(blob))
	for key, value := range header {
		r.Header.Add(key, value)
	}
	var err error
	var res *http.Response
	var resBody []byte
	if c.router != nil {
		rec := httptest.NewRecorder()
		c.router.ServeHTTP(rec, r)
		res = rec.Result()
		resBody = rec.Body.Bytes()
	} else {
		if c.token != "" {
			r.Header.Add("Authorization", "Bearer "+c.token)
		}
		res, err = c.httpClient.Do(r)
		if err != nil {
			return http.StatusInternalServerError, err
		}
		defer res.Body.Close()
		resBody, _ = io.ReadAll(res.Body)
	}
	status := res.StatusCode

	if status != http.StatusOK && status != http.StatusCreated && status != http.StatusNoContent {
		return status, fmt.Errorf(strings.TrimSpace(string(resBody)))
	}
	if resBody != nil && result != nil {
		err = json.Unmarshal(resBody, result)
	}
	return status, err
}

// RawPatch puts a patch to path. Expects http.StatusOK, http.StatusCreated,  or http.StatusNoContent as valid responses,
// otherwise it will flag an error. Returns the actual http status code.
//
// The path can be extend with query strings.
//
// body can also be a []byte, result can also be raw *[]byte.
// result can be nil.
func (c Client) RawPatch(path string, body interface{}, result interface{}) (int, error) {

	var err error
	j, ok := body.([]byte)
	if !ok {
		j, err = json.Marshal(body)
		if err != nil {
			return http.StatusBadRequest, err
		}
	}

	r, _ := http.NewRequestWithContext(c.context(), http.MethodPatch, c.url+path, bytes.NewBuffer(j))
	var res *http.Response
	var resBody []byte
	if c.router != nil {
		rec := httptest.NewRecorder()
		c.router.ServeHTTP(rec, r)
		res = rec.Result()
		resBody = rec.Body.Bytes()
	} else {
		if c.token != "" {
			r.Header.Add("Authorization", "Bearer "+c.token)
		}
		res, err = c.httpClient.Do(r)
		if err != nil {
			return http.StatusInternalServerError, err
		}
		defer res.Body.Close()
		resBody, _ = io.ReadAll(res.Body)
	}
	status := res.StatusCode
	if status != http.StatusOK && status != http.StatusCreated && status != http.StatusNoContent {
		return status, fmt.Errorf(strings.TrimSpace(string(resBody)))
	}
	if resBody != nil && result != nil {
		if raw, ok := result.(*[]byte); ok {
			*raw = resBody
		} else {
			err = json.Unmarshal(resBody, result)
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
	r, _ := http.NewRequestWithContext(c.context(), http.MethodDelete, c.url+path, nil)
	var err error
	var res *http.Response
	var resBody []byte
	if c.router != nil {
		rec := httptest.NewRecorder()
		c.router.ServeHTTP(rec, r)
		res = rec.Result()
		resBody = rec.Body.Bytes()
	} else {
		if c.token != "" {
			r.Header.Add("Authorization", "Bearer "+c.token)
		}
		res, err = c.httpClient.Do(r)
		if err != nil {
			return http.StatusInternalServerError, err
		}
		defer res.Body.Close()
		resBody, _ = io.ReadAll(res.Body)
	}
	status := res.StatusCode
	if status != http.StatusNoContent {
		return status, fmt.Errorf(strings.TrimSpace(string(resBody)))
	}
	return status, nil
}

// PostMultipart upload data using a Multipart Form
func (c Client) PostMultipart(url string, data []byte) (status int, err error) {
	// Prepare a form that you will submit to that URL.
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	var fw io.Writer
	if fw, err = w.CreateFormFile("file", "file"); err != nil {
		return
	}

	if _, err = fw.Write(data); err != nil {
		return
	}

	w.Close()

	req, err := http.NewRequest("PUT", url, &b)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", w.FormDataContentType())

	var res *http.Response
	if c.router != nil {
		rec := httptest.NewRecorder()
		c.router.ServeHTTP(rec, req)
		res = rec.Result()
	} else {

		if c.token != "" {
			req.Header.Add("Authorization", "Bearer "+c.token)
		}
		res, err = c.httpClient.Do(req)
		if err != nil {
			if res != nil {
				return res.StatusCode, err
			}
			return 0, err
		}
		defer res.Body.Close()
	}
	status = res.StatusCode
	if status != http.StatusOK {
		err = fmt.Errorf("bad status: %s", res.Status)
	}
	return
}
