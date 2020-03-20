package baas

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"

	"github.com/gorilla/mux"
)

// Admin gives admin access to the REST API
type Admin struct {
	router *mux.Router
}

// Admin creates an object to make pseudp-REST requests to the backend as admin
func (b *Backend) Admin() *Admin {
	return &Admin{router: b.router}
}

// ContextKey is the type for context keys. Go linter does not like plain strings
type ContextKey string

// various context keys
const (
	ContextKeyRole ContextKey = "role"
)

// Get a resource from path as admin
func (a *Admin) Get(path string, result interface{}) (int, error) {
	ctx := context.WithValue(context.Background(), ContextKeyRole, "admin")
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	a.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusOK {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s", status, http.StatusOK, rec.Body.String())
	}

	role, _ := r.Context().Value(ContextKeyRole).(string)
	log.Println("role=", role)

	// encoder := json.NewEncoder(w)
	// encoder.SetIndent("", "  ")
	// encoder.Encode(response)

	err := json.Unmarshal(rec.Body.Bytes(), result)
	return status, err
}

// Post a resource to path as admin
func (a *Admin) Post(path string, body interface{}, result interface{}) (int, error) {

	j, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		return http.StatusBadRequest, err
	}

	ctx := context.WithValue(context.Background(), ContextKeyRole, "admin")
	r, _ := http.NewRequestWithContext(ctx, http.MethodPost, path, bytes.NewBuffer(j))
	rec := httptest.NewRecorder()
	a.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusCreated {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s", status, http.StatusCreated, rec.Body.String())
	}

	err = json.Unmarshal(rec.Body.Bytes(), result)
	return status, err
}

// Put a resource to path as admin
func (a *Admin) Put(path string, body interface{}, result interface{}) (int, error) {

	j, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		return http.StatusBadRequest, err
	}

	ctx := context.WithValue(context.Background(), ContextKeyRole, "admin")
	r, _ := http.NewRequestWithContext(ctx, http.MethodPut, path, bytes.NewBuffer(j))
	rec := httptest.NewRecorder()
	a.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusOK && status != http.StatusNoContent {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v or %v. Error: %s", status, http.StatusOK, http.StatusNoContent, rec.Body.String())
	}

	err = json.Unmarshal(rec.Body.Bytes(), result)
	return status, err
}

// Delete a resource at path as admin
func (a *Admin) Delete(path string) (int, error) {
	ctx := context.WithValue(context.Background(), ContextKeyRole, "admin")
	r, _ := http.NewRequestWithContext(ctx, http.MethodDelete, path, nil)
	rec := httptest.NewRecorder()
	a.router.ServeHTTP(rec, r)

	status := rec.Code
	if status != http.StatusNoContent {
		return status, fmt.Errorf("handler returned wrong status code: got %v want %v. Error: %s", status, http.StatusNoContent, rec.Body.String())
	}
	return status, nil
}
