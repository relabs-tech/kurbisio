/*Package client provides easy and fast in-process access to a REST api

Instead of marshalling HTTP, the client talks directly to the mux router. The client
is the tool of choice if one request handler needs to call other handlers to fullfil
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

	"github.com/gorilla/mux"
	"github.com/relabs-tech/backends/core/access"
)

// Client provides easy access to the REST API.
//
type Client struct {
	router *mux.Router
	auth   *access.Authorization
	ctx    context.Context
}

// New creates a client to make pseudo-REST requests to the backend.
//
// WithAuthorization() adds an authorization to the request context.
// WithContext() specifies a different base context all together.
func New(router *mux.Router) Client {
	return Client{
		router: router,
	}
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

func (c *Client) context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	if c.auth != nil {
		return c.auth.ContextWithAuthorization(context.Background())
	}
	return context.Background()
}

// Get gets the resource from path. Expects http.StatusOK as response, otherwise it will
// flag an error. Returns the actual http status code.
func (c *Client) Get(path string, result interface{}) (int, error) {
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

	err := json.Unmarshal(rec.Body.Bytes(), result)
	return status, err
}

// Post posts a resource to path. Expects http.StatusCreated as response, otherwise it will
// flag an error. Returns the actual http status code.
func (c *Client) Post(path string, body interface{}, result interface{}) (int, error) {

	j, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		return http.StatusBadRequest, err
	}

	r, _ := http.NewRequestWithContext(c.context(), http.MethodPost, path, bytes.NewBuffer(j))
	rec := httptest.NewRecorder()
	c.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusCreated {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s", status, http.StatusCreated, rec.Body.String())
	}

	err = json.Unmarshal(rec.Body.Bytes(), result)
	return status, err
}

// Put puts a resource to path. Expects http.StatusOK or http.StatusNoContent as valid responses,
// otherwise it will flag an error. Returns the actual http status code.
func (c *Client) Put(path string, body interface{}, result interface{}) (int, error) {

	j, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		return http.StatusBadRequest, err
	}

	r, _ := http.NewRequestWithContext(c.context(), http.MethodPut, path, bytes.NewBuffer(j))
	rec := httptest.NewRecorder()
	c.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusOK && status != http.StatusNoContent {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v or %v. Error: %s", status, http.StatusOK, http.StatusNoContent, rec.Body.String())
	}

	err = json.Unmarshal(rec.Body.Bytes(), result)
	return status, err
}

// Delete deletes the resource at path. Expects http.StatusNoContent as response, otherwise it will
// flag an error. Returns the actual http status code.
func (c *Client) Delete(path string) (int, error) {
	r, _ := http.NewRequestWithContext(c.context(), http.MethodDelete, path, nil)
	rec := httptest.NewRecorder()
	c.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusNoContent {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s", status, http.StatusNoContent, rec.Body.String())
	}
	return status, nil
}
