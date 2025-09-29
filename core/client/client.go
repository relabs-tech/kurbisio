// Copyright 2021 Dalarub & Ettrich GmbH - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited
// Proprietary and confidential
// info@dalarub.com
//

/*
Package client provides easy and fast in-process access to a REST api

Instead of marshalling HTTP, the client talks directly to the mux router. The client
is the tool of choice if one request handler needs to call other handlers to fulfill
its task. It is also perfectly suited for unit tests.
*/
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/relabs-tech/kurbisio/core"
	"github.com/relabs-tech/kurbisio/core/access"
	"github.com/relabs-tech/kurbisio/core/pointers"
)

// Client provides easy access to the REST API.
type Client struct {
	router     *mux.Router
	httpClient *http.Client
	url        string
	token      string
	auth       *access.Authorization
	ctx        context.Context

	defaultHeaders map[string]string
}

// NewWithRouter creates a client to make pseudo-REST requests to the backend,
// through the mux router
//
// WithAuthorization() adds an authorization to the request context.
// WithContext() specifies a different base context all together.
func NewWithRouter(router *mux.Router) Client {
	return Client{
		router:         router,
		defaultHeaders: map[string]string{},
	}
}

// WithHeader returns a new client with a default header added
func (c Client) WithHeader(key string, value string) Client {
	c.defaultHeaders[key] = value
	return c
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

// UpdateToken updates the token of the client
func (c *Client) UpdateToken(token string) {
	c.token = token
}

// WithAdminAuthorization returns a new client with admin authorizations
// (this works only directly against the mux router, for a normal client
//
//	use WithToken()))
func (c Client) WithAdminAuthorization() Client {
	return c.WithRole("admin")
}

// WithRole returns a new client with role authorization
// (this works only directly against the mux router, for a normal client
//
//	use WithToken()))
func (c Client) WithRole(role string) Client {
	c.auth = &access.Authorization{
		Roles: []string{role},
	}
	return c
}

// WithRoleAndSelector returns a new client with role authorization
// (this works only directly against the mux router, for a normal client
//
//	use WithToken()))
func (c Client) WithRoleAndSelector(role string, selector string, value uuid.UUID) Client {
	if !strings.HasSuffix(selector, "_id") {
		selector += "_id"
	}
	c.auth = &access.Authorization{
		Roles:     []string{role},
		Selectors: map[string]string{selector: value.String()},
	}
	return c
}

// WithAuthorization returns a new client with specific authorizations
// (this works only directly against the mux router, for a normal client
//
//	use WithToken())
func (c Client) WithAuthorization(auth *access.Authorization) Client {
	c.auth = auth
	return c
}

// WithContext returns a new client with specific request context
func (c Client) WithContext(ctx context.Context) Client {
	c.ctx = ctx
	return c
}

func (c Client) Context() context.Context {
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
	prefix     string
	client     *Client
	resources  []string
	selectors  map[string]string
	parameters []string
}

// Collection returns a new collection client. It works
// for normal collections and relations.
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
		prefix:     r.prefix,
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
		prefix:     r.prefix,
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

// From returns a new collection client with a 'from' time parameter added.
func (r Collection) From(from time.Time) Collection {
	// remove previous from parameter if any
	for i, p := range r.parameters {
		if strings.HasPrefix(p, "from=") {
			r.parameters = append(r.parameters[:i], r.parameters[i+1:]...)
			break
		}
	}

	return r.WithParameter("from", from.Format(time.RFC3339Nano))
}

// Until returns a new collection client with an 'until' time parameter added.
func (r Collection) Until(until time.Time) Collection {
	// remove previous until parameter if any
	for i, p := range r.parameters {
		if strings.HasPrefix(p, "until=") {
			r.parameters = append(r.parameters[:i], r.parameters[i+1:]...)
			break
		}
	}

	return r.WithParameter("until", until.Format(time.RFC3339Nano))
}

// WithParameter returns a new collection client with a URL parameter added.
func (r Collection) WithParameter(key string, value string) Collection {

	parameter := url.QueryEscape(key) + "=" + url.QueryEscape(value)

	return Collection{
		client:    r.client,
		prefix:    r.prefix,
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
		prefix:    r.prefix,
		resources: r.resources,
		selectors: r.selectors,
		// we want a true copy to avoid side effects
		parameters: append(append([]string{}, r.parameters...), parameters...),
	}
}

// WithLeft returns a new collection client with a left selector added. This only
// works if the collection is a directional relation
func (r Collection) WithLeft(leftID uuid.UUID) Collection {
	return r.WithParameter("left", leftID.String())
}

// WithRight returns a new collection client with a right selector added. This only
// works if the collection is a directional relation
func (r Collection) WithRight(rightID uuid.UUID) Collection {
	return r.WithParameter("right", rightID.String())
}

// WithEither returns a new collection client with a either selector added. This only
// works if the collection is a non-directional relation
func (r Collection) WithEither(eitherID uuid.UUID) Collection {
	return r.WithParameter("either", eitherID.String())
}

// WithFilter returns a new collection client with a URL filter parameter added.
// This is a shortcut for WithParameter("filter", key+"="+value)
func (r Collection) WithFilter(key string, value string) Collection {
	return r.WithParameter("filter", key+"="+value)
}

func (r Collection) WithSearch(key, value string) Collection {
	return r.WithParameter("search", key+"~"+value)
}

func (r Collection) paths() (collectionPath, singletonPath string) {
	itemPath := r.prefix
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
	parameters  []string
}

// Item gets an item from the collection.
// If the collection is a relation, use RelationShip()
func (r Collection) Item(id uuid.UUID) Item {
	return Item{col: r, id: id}
}

// Singleton gets a singleton from the collection
func (r Collection) Singleton() Item {
	return Item{col: r, isSingleton: true}
}

// WithParameter returns a new item client with a URL parameter added.
func (r Item) WithParameter(key string, value string) Item {
	parameter := url.QueryEscape(key) + "=" + url.QueryEscape(value)

	return Item{
		id:          r.id,
		isSingleton: r.isSingleton,
		col:         r.col,
		// we want a true copy to avoid side effects
		parameters: append(append([]string{}, r.parameters...), parameter),
	}
}

// WithParameters returns a new item client with all URL parameters added.
func (r Item) WithParameters(keyValues map[string]string) Item {
	var parameters []string
	for key, value := range keyValues {
		parameter := url.QueryEscape(key) + "=" + url.QueryEscape(value)
		parameters = append(parameters, parameter)
	}
	return Item{
		id:          r.id,
		isSingleton: r.isSingleton,
		col:         r.col,
		parameters:  append(append([]string{}, r.parameters...), parameters...),
	}
}

// Path returns the created path for this item
func (r Item) Path() string {
	var path string
	if r.isSingleton {
		path = r.col.SingletonPath()
	} else {
		path = r.col.CollectionPath() + "/" + r.id.String()
	}
	if len(r.parameters) > 0 {
		path += "?" + strings.Join(r.parameters, "&")
	}
	return path
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
// Optional singleton children are added to the response.
//
// result can also be map[string]interface{} or a raw *[]byte.
func (r Item) Read(result interface{}, children ...string) (int, error) {
	if len(children) > 0 {
		return r.WithParameter("children", strings.Join(children, ",")).Read(result)
	}
	return r.col.client.RawGet(r.Path(), result)
}

// Read reads a blob item from a collection
//
// The operation corresponds to a GET request.
//
// Expects http.StatusOK as response, otherwise it will
// flag an error. Returns the actual http status code.
//
// meta can also be map[string]interface{} or a raw *[]byte.

func (r Item) ReadBlobWithMeta(blob *[]byte, meta interface{}) (int, error) {
	status, header, err := r.col.client.RawGetBlobWithHeader(r.Path(), nil, blob)
	if md := header.Get("Kurbisio-Meta-Data"); meta != "" {
		if raw, ok := meta.(*[]byte); ok {
			*raw = []byte(md)
		} else {
			err = json.Unmarshal([]byte(md), meta)
		}
	}
	return status, err
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

type Relationship struct {
	col     Collection
	leftID  uuid.UUID
	rightID uuid.UUID
}

// Relationship returns a relationship from the collection
func (r Collection) Relationship(leftID, rightID uuid.UUID) Relationship {
	return Relationship{col: r, leftID: leftID, rightID: rightID}
}

// Path returns the created path for this item
func (r Relationship) Path() string {
	return r.col.CollectionPath() + "/" + r.leftID.String() + ":" + r.rightID.String()
}

// Read reads a relationship from a collection
//
// The operation corresponds to a GET request.
//
// Expects http.StatusOK as response, otherwise it will
// flag an error. Returns the actual http status code.
//
// result can also be map[string]interface{} or a raw *[]byte.
func (r Relationship) Read(result interface{}) (int, error) {
	return r.col.client.RawGet(r.Path(), result)
}

// Delete deletes a relationship from a collection
//
// The operation corresponds to a DELETE request.
//
// Expects http.StatusNoContent as response, otherwise it will
// flag an error.
//
// Returns the actual http status code.
func (r Relationship) Delete() (int, error) {
	return r.col.client.RawDelete(r.Path())
}

// Upsert updates a relationship, or creates if it doesn't exist yet.
// The relationship must be fully qualified, i.e. it must contain all identifiers, either in the
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
func (r Relationship) Upsert(body interface{}, result interface{}) (int, error) {
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
func (r Relationship) UpdateProperty(jsonName string, value string) (int, error) {
	return r.col.client.RawPut(r.Path()+"/"+jsonName+"/"+value, nil, nil)
}

// Patch updates selected fields of a relationship
//
// Expects http.StatusOK, http.StatusCreated or http.StatusNoContent as valid responses,
// otherwise it will flag an error. Returns the actual http status code.
//
// body can also be a []byte, result can also be raw *[]byte.
// result can be nil.
func (r Relationship) Patch(body interface{}, result interface{}) (int, error) {
	return r.col.client.RawPatch(r.Path(), body, result)
}

// Page is a requester for one page in a collection
type Page struct {
	r Collection
	// the cursor to fetch this page
	cursor *string

	// the cursor to fetch the next page, only populated after Get()
	nextCursor *string
}

// FirstPage returns a requester for the first page of a collection,
// using the cursor pagination
// You can set others parameters, including limit.
func (r Collection) FirstPage() Page {
	return Page{r: r}
}

// HasData returns true if the page has data (by definition true for the first page)
func (p Page) HasData() bool {
	return p.cursor == nil || *p.cursor != ""
}

// Get gets one page of the collection
func (p *Page) Get(result interface{}) (int, error) {
	c := p.r
	if p.cursor != nil {
		c = c.WithParameter("next_token", *p.cursor)
	}
	path := c.CollectionPath()
	status, headers, err := p.r.client.RawGetWithHeader(path, map[string]string{}, result)
	p.nextCursor = pointers.StringPtr(headers.Get("Pagination-Next-Token"))
	if err != nil {
		return status, err
	}
	return status, nil
}

// Next returns the next page
func (p Page) Next() Page {
	return Page{
		r:      p.r,
		cursor: p.nextCursor,
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
	r, _ := http.NewRequestWithContext(c.Context(), http.MethodGet, c.url+path, nil)
	for key, value := range c.defaultHeaders {
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
	r, _ := http.NewRequestWithContext(c.Context(), http.MethodGet, c.url+path, nil)
	for key, value := range c.defaultHeaders {
		r.Header.Add(key, value)
	}

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
	r, _ := http.NewRequestWithContext(c.Context(), http.MethodGet, c.url+path, nil)
	for key, value := range c.defaultHeaders {
		r.Header.Add(key, value)
	}
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

// RawPostWithHeader posts a resource to path. Expects http.StatusCreated as response, otherwise it will
// flag an error. Returns the actual http status code.
//
// The path can be extend with query strings.
//
// body can also be a []byte, result can also be raw *[]byte.
// result can be nil.
func (c Client) RawPostWithHeader(path string, headers map[string]string, body interface{}, result interface{}) (int, error) {
	var err error
	j, ok := body.([]byte)
	if !ok {
		j, err = json.Marshal(body)
		if err != nil {
			return http.StatusBadRequest, fmt.Errorf("POST to %s: %w", path, err)
		}
	}

	r, err := http.NewRequestWithContext(c.Context(), http.MethodPost, c.url+path, bytes.NewBuffer(j))
	if err != nil {
		return http.StatusInternalServerError, fmt.Errorf("POST to %s: %w", path, err)
	}
	for key, value := range c.defaultHeaders {
		r.Header.Add(key, value)
	}

	for key, value := range headers {
		r.Header.Add(key, value)
	}

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

// RawPost posts a resource to path. Expects http.StatusCreated as response, otherwise it will
// flag an error. Returns the actual http status code.
//
// The path can be extend with query strings.
//
// body can also be a []byte, result can also be raw *[]byte.
// result can be nil.
func (c Client) RawPost(path string, body interface{}, result interface{}) (int, error) {
	return c.RawPostWithHeader(path, nil, body, result)
}

// RawPostBlob posts a resource to path. Expects http.StatusCreated as response, otherwise it will
// flag an error. Returns the actual http status code.
//
// The path can be extend with query strings.
func (c Client) RawPostBlob(path string, header map[string]string, blob []byte, result interface{}) (int, error) {

	r, _ := http.NewRequestWithContext(c.Context(), http.MethodPost, c.url+path, bytes.NewBuffer(blob))
	for key, value := range c.defaultHeaders {
		r.Header.Add(key, value)
	}
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

	r, _ := http.NewRequestWithContext(c.Context(), http.MethodPut, c.url+path, bytes.NewBuffer(j))
	for key, value := range c.defaultHeaders {
		r.Header.Add(key, value)
	}
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
		return status, fmt.Errorf("handler returned wrong status code: got %v. Error: %s",
			status, strings.TrimSpace(string(resBody)))
	}
	if resBody != nil && result != nil {
		if raw, ok := result.(*[]byte); ok {
			*raw = resBody
		} else {
			err = json.Unmarshal(resBody, result)
		}
	}
	if status == http.StatusConflict {
		return status, fmt.Errorf("conflict while writing to path '%s', wanted to write %s, conflict: %s", path, string(j), string(resBody))
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

	r, _ := http.NewRequestWithContext(c.Context(), http.MethodPut, c.url+path, bytes.NewBuffer(blob))
	for key, value := range c.defaultHeaders {
		r.Header.Add(key, value)
	}
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
		return status, errors.New(strings.TrimSpace(string(resBody)))
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

	r, _ := http.NewRequestWithContext(c.Context(), http.MethodPatch, c.url+path, bytes.NewBuffer(j))
	for key, value := range c.defaultHeaders {
		r.Header.Add(key, value)
	}
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
		return status, errors.New(strings.TrimSpace(string(resBody)))
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
	r, _ := http.NewRequestWithContext(c.Context(), http.MethodDelete, c.url+path, nil)
	for key, value := range c.defaultHeaders {
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
	if status != http.StatusNoContent {
		return status, errors.New(strings.TrimSpace(string(resBody)))
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
	for key, value := range c.defaultHeaders {
		req.Header.Add(key, value)
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
