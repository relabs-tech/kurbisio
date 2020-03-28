package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/gorilla/mux"
)

// Client provides easy access to the REST API.
//
// For convenience, ClientWithAuthorization() adds
// a ContextKeyAuthorization to the request context.
type Client struct {
	router *mux.Router
	auth   *Authorization
	ctx    context.Context
}

// NewClient creates a client to make pseudo-REST requests to the backend.
func NewClient(router *mux.Router) *Client {
	return &Client{
		router: router,
	}
}

// NewClientWithAuthorization creates a client with specific authorizations to make
// pseudo-REST requests to the backend.
func (b *Backend) NewClientWithAuthorization(auth *Authorization) *Client {
	return &Client{
		router: b.router,
		auth:   auth,
	}
}

// NewClientWithContext creates a client with specific context to make
// pseudo-REST requests to the backend.
func (b *Backend) NewClientWithContext(ctx context.Context) *Client {
	return &Client{
		router: b.router,
		ctx:    ctx,
	}
}

func (c *Client) context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	if c.auth != nil {
		return context.WithValue(context.Background(), contextKeyAuthorization, c.auth)
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
